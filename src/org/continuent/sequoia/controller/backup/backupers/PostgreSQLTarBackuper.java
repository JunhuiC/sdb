/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks
 * Contact: sequoia@continuent.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Dylan Hansen, Mathieu Peltier, Olivier Fambon.
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backup.BackupManager;
import org.continuent.sequoia.controller.backup.DumpTransferInfo;

/**
 * This class defines a Backuper for PostgreSQL databases. This backuper creates
 * dumps in the tar format, using the "--format=t" switch on pg_dump.
 * <p>
 * Supported URLs are:
 * <ul>
 * <li>jdbc:postgresql://host:port/dbname?param1=foo,param2=bar</li>
 * <li>jdbc:postgresql://host/dbname?param1=foo,param2=bar</li>
 * <li>jdbc:postgresql:dbname?param1=foo,param2=bar</li>
 * </ul>
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @author <a href="mailto:dhansen@h2st.com">Dylan Hansen</a>
 * @author <a href="mailto:mathieu.peltier@emicnetworks.com">Mathieu Peltier</a>
 * @author <a href="mailto:olivier.fambon@emicnetworks.com">Olivier Fambon</a>
 * @version 1.0
 */
public class PostgreSQLTarBackuper extends AbstractPostgreSQLBackuper
{
  // Logger
  static Trace               logger      = Trace
                                             .getLogger(PostgreSQLTarBackuper.class
                                                 .getName());

  /**
   * The dump format for this (family of) backuper.
   */
  public static final String DUMP_FORMAT = "PostgreSQL Tar Dump";

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getDumpFormat()
   */
  public String getDumpFormat()
  {
    return DUMP_FORMAT;
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#backup(DatabaseBackend,
   *      String, String, String, String, ArrayList)
   */
  public Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    // Parse the URL for the connection information
    String url = backend.getURL();
    PostgreSQLUrlInfo info = new AbstractPostgreSQLBackuper.PostgreSQLUrlInfo(
        url);

    if (logger.isDebugEnabled())
      logger.debug("Backing up database '" + info.getDbName() + "' on host '"
          + info.getHost() + ":" + info.getPort() + "'");

    try
    {
      // Create the path, if it does not already exist
      File pathDir = new File(path);
      if (!pathDir.exists())
      {
        pathDir.mkdirs();
        pathDir.mkdir();
      }
      String fullPath = getDumpPhysicalPath(path, dumpName) + ".tar";

      int exitValue = -1;
      String dumpOptions = "";
      if (pgDumpFlags != null)
      {
        if (pgDumpFlags.indexOf("-F") >= 0
            || pgDumpFlags.indexOf("--format=") >= 0)
        {
          logger.error("Invalid option in pgDumpFlags \"" + pgDumpFlags
              + "\". You are not allowed to set the format of the dump!");
        }
        else
        {
          dumpOptions = " " + pgDumpFlags + " ";
        }
      }
      if (useAuthentication)
      {
        if (logger.isDebugEnabled())
          logger.debug("Performing backup using authentication");
        int timeout = -1;
        if (dumpTimeout != null)
        {
          try
          {
            timeout = Integer.parseInt(dumpTimeout);
          }
          catch (NumberFormatException e)
          {
            logger.error("\"" + dumpTimeout
                + "\" is not a valid dump-timeout value!");
            timeout = -1;
          }
        }
        String[] expectFeed = makeExpectDialogueWithAuthentication("pg_dump",
            info, " --format=t -f " + fullPath + dumpOptions, login, password,
            timeout);

        String[] cmd = makeExpectCommandReadingStdin();

        exitValue = executeNativeCommand(expectFeed, cmd);
      }
      else
      {
        if (pgDumpFlags != null)
        {
          if (pgDumpFlags.indexOf("-U") >= 0 || pgDumpFlags.indexOf("-W") >= 0)
          {
            logger
                .error("Invalid option in pgDumpFlags \""
                    + pgDumpFlags
                    + "\". Set \"authentication=true\" if you want to use authentication!");
          }
          else
          {
            dumpOptions = " " + pgDumpFlags + " ";
          }
        }

        String cmd = makeCommand("pg_dump", info, " --format=t -f " + fullPath
            + dumpOptions, login);
        exitValue = executeNativeCommand(cmd);
      }

      if (exitValue != 0)
      {
        printErrors();
        throw new BackupException(
            "pg_dump execution did not complete successfully!");
      }

    }
    catch (Exception e)
    {
      String msg = "Error while performing backup";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }

    return new Date(System.currentTimeMillis());
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#restore(DatabaseBackend,
   *      String, String, String, String, ArrayList)
   */
  public void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    // Parse the URL for the connection information
    String url = backend.getURL();
    PostgreSQLUrlInfo info = new AbstractPostgreSQLBackuper.PostgreSQLUrlInfo(
        url);

    if (logger.isDebugEnabled())
      logger.debug("Restoring database '" + info.getDbName() + "' on host '"
          + info.getHost() + ":" + info.getPort() + "'");

    // Check to see if the given path + dumpName exists
    String fullPath = getDumpPhysicalPath(path, dumpName) + ".tar";
    File dump = new File(fullPath);
    if (!dump.exists())
      throw new BackupException("Backup '" + fullPath + "' does not exist!");

    try
    {
      if (useAuthentication)
      {
        if (logger.isInfoEnabled())
          logger.info("Performing database operations using authentication");

        // Drop the database if it already exists
        if (logger.isDebugEnabled())
          logger.debug("Dropping database '" + info.getDbName() + "'");

        String[] expectFeed = makeExpectDialogueWithAuthentication("dropdb",
            info, "", login, password, 60); // 1 minute timeout

        String[] cmd = makeExpectCommandReadingStdin();

        if (executeNativeCommand(expectFeed, cmd) != 0)
        {
          printErrors();
          throw new BackupException(
              "dropdb execution did not complete successfully!");
        }

        // Re-create the database, use the specified encoding if provided
        if (logger.isDebugEnabled())
          logger.debug("Re-creating '" + info.getDbName() + "'");

        expectFeed = makeExpectDialogueWithAuthentication("createdb", info,
            encoding != null ? "--encoding=" + encoding + " " : "", login,
            password, 60); // 1 minute timeout

        cmd = makeExpectCommandReadingStdin();

        if (executeNativeCommand(expectFeed, cmd) != 0)
        {
          printErrors();
          throw new BackupException(
              "createdb execution did not complete successfully!");
        }

        // Run a pre-restore script, if specified
        if (preRestoreScript != null)
        {
          if (logger.isDebugEnabled())
            logger.debug("Running pre-restore script '" + preRestoreScript
                + "' on '" + info.getDbName() + "'");

          expectFeed = makeExpectDialogueWithAuthentication("psql", info,
              "--pset pager -f " + preRestoreScript, login, password, 300); // 5
          // minutes
          // timeout

          cmd = makeExpectCommandReadingStdin();

          if (executeNativeCommand(expectFeed, cmd) != 0)
          {
            printErrors();
            throw new BackupException(
                "psql execution did not complete successfully!");
          }
        }

        // Use the pg_restore command to rebuild the database
        if (logger.isDebugEnabled())
          logger.debug("Rebuilding '" + info.getDbName() + "' from dump '"
              + dumpName + "'");

        int timeout = -1;
        if (restoreTimeout != null)
        {
          try
          {
            timeout = Integer.parseInt(restoreTimeout);
          }
          catch (NumberFormatException e)
          {
            logger.error("\"" + restoreTimeout
                + "\" is not a valid restore-timeout value!");
            timeout = -1;
          }
        }

        expectFeed = makeExpectDialogueWithAuthentication("pg_restore", info,
            "--format=t -d " + info.getDbName() + " " + fullPath, login,
            password, timeout); // 30 minutes timeout

        cmd = makeExpectCommandReadingStdin();

        if (executeNativeCommand(expectFeed, cmd) != 0)
        {
          printErrors();
          throw new BackupException(
              "pg_restore execution did not complete successfully!");
        }
        
        // Run a post-restore script, if specified
        if (postRestoreScript != null)
        {
          if (logger.isDebugEnabled())
            logger.debug("Running post-restore script '" + postRestoreScript
                + "' on '" + info.getDbName() + "'");

          expectFeed = makeExpectDialogueWithAuthentication("psql", info,
              "--pset pager -f " + postRestoreScript, login, password, 300); // 5
          // minutes
          // timeout

          cmd = makeExpectCommandReadingStdin();

          if (executeNativeCommand(expectFeed, cmd) != 0)
          {
            printErrors();
            throw new BackupException(
                "psql execution did not complete successfully!");
          }
        }
        
      }
      else
      // No authentication
      {
        // Drop the database if it already exists
        if (logger.isDebugEnabled())
          logger.debug("Dropping database '" + info.getDbName() + "'");

        String dropCmd = makeCommand("dropdb", info, "", login);
        if (executeNativeCommand(dropCmd) != 0)
        {
          printErrors();
          throw new BackupException(
              "dropdb execution did not complete successfully!");
        }

        // Re-create the database, use the specified encoding if provided
        if (logger.isDebugEnabled())
          logger.debug("Re-creating '" + info.getDbName() + "'");

        String createCmd = makeCommand("createdb", info, encoding != null
            ? "--encoding=" + encoding + " "
            : "", login);
        if (executeNativeCommand(createCmd) != 0)
        {
          printErrors();
          throw new BackupException(
              "createdb execution did not complete successfully!");
        }

        // Run a pre-restore script, if specified
        if (preRestoreScript != null)
        {
          if (logger.isDebugEnabled())
            logger.debug("Running pre-restore script '" + preRestoreScript
                + "' on '" + info.getDbName() + "'");

          String preRestoreCmd = makeCommand("psql", info, "--pset pager -f "
              + preRestoreScript, login);
          if (executeNativeCommand(preRestoreCmd) != 0)
          {
            printErrors();
            throw new BackupException(
                "psql execution did not complete successfully!");
          }
        }

        // Use the psql command to rebuild the database
        if (logger.isDebugEnabled())
          logger.debug("Rebuilding '" + info.getDbName() + "' from dump '"
              + dumpName + "'");

        String replayCmd = makeCommand("pg_restore", info, "--format=t -d "
            + info.getDbName() + " " + fullPath, login);
        if (executeNativeCommand(replayCmd) != 0)
        {
          printErrors();
          throw new BackupException(
              "pg_restore execution did not complete successfully!");
        }
        
        // Run a post-restore script, if specified
        if (postRestoreScript != null)
        {
          if (logger.isDebugEnabled())
            logger.debug("Running post-restore script '" + postRestoreScript
                + "' on '" + info.getDbName() + "'");

          String postRestoreCmd = makeCommand("psql", info, "--pset pager -f "
              + postRestoreScript, login);
          if (executeNativeCommand(postRestoreCmd) != 0)
          {
            printErrors();
            throw new BackupException(
                "psql execution did not complete successfully!");
          }
        }
        
      }
    }
    catch (Exception e)
    {
      String msg = "Error while performing backup";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#fetchDump(org.continuent.sequoia.controller.backup.DumpTransferInfo,
   *      java.lang.String, java.lang.String)
   */
  public void fetchDump(DumpTransferInfo dumpTransferInfo, String path,
      String dumpName) throws BackupException, IOException
  {
    BackupManager.fetchDumpFile(dumpTransferInfo, path, dumpName + ".tar");
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#deleteDump(java.lang.String,
   *      java.lang.String)
   */
  public void deleteDump(String path, String dumpName) throws BackupException
  {
    super.deleteDump(path, dumpName + ".tar");
  }

}
