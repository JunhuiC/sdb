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
 * 
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backup.BackupManager;
import org.continuent.sequoia.controller.backup.DumpTransferInfo;

/**
 * This class defines a Backuper for PostgreSQL databases. This backuper makes
 * dumps in a plain-text format.
 * *****************************************************************************
 * This class backups PostgreSQL database usig pg_dump. backup command from
 * console executes the pg_dump with split, and example pg_dump dbName -h
 * 127.0.0.1 -p 5432 -U postgres | split -a 4 -b 10m - /tmp/dumpFile which does
 * spliting ONE single dump file to splitted files. Size of the splitted files
 * are determined by virtual xml file in options for Backuper, and example
 * <Backuper backuperName="splitter"
 * className="org.continuent.sequoia.controller.backup.backupers.PostgreSQLSplitPlainTextBackuper"
 * options="zip=true,splitSize=10m"/> default size is 1000m Restore command
 * restores the DB from splitted files, and example cat /tmp/dumpFile* | psql
 * dbName -h 127.0.0.1 -p 5432 -U postgres restore checks the existing of dump
 * file for first suffix dumpFileaaaa (suffix is set to 4) Contributer also
 * added two methods and one varibale to AbstractPostgreSQLBackuper class
 * protected String[] makeSplitCommand(String command, PostgreSQLUrlInfo info,
 * String options, String login) protected String[]
 * makeSplitCommandWithAuthentication(String command, PostgreSQLUrlInfo info,
 * String options, String login, String password, boolean isPsql) static
 * protected String SPLIT_SIZE = "1000m"; Contributor for this class: Gurkan
 * Ozfidan
 * *****************************************************************************
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
public class PostgreSQLSplitPlainTextBackuper
    extends AbstractPostgreSQLBackuper
{
  // Logger
  static Trace               logger      = Trace
                                             .getLogger(PostgreSQLSplitPlainTextBackuper.class
                                                 .getName());

  /**
   * The dump format for this (family of) backuper.
   */
  public static final String DUMP_FORMAT = "PostgreSQL Split Plain Text Dump";

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getDumpFormat()
   */
  public String getDumpFormat()
  {
    return DUMP_FORMAT;
  }

  /**
   * Backups the DB using pg_dump/split commands; It creates splitted dump files
   * instead of one single(huge) dump file
   * 
   * @see org.continuent.sequoia.controller.backup.Backuper#backup(DatabaseBackend,
   *      String, String, String, String, ArrayList)
   */
  public Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList tables) throws BackupException
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

      String fullPath = getDumpPhysicalPath(path, dumpName);

      int exitValue = -1;
      if (useAuthentication)
      {
        if (logger.isDebugEnabled())
          logger.debug("Performing backup using authentication");

        String[] gCmd = makeSplitCommandWithAuthentication("pg_dump", info,
            " | split -a 4 -b 10m - " + fullPath, login, password, false);

        exitValue = executeNativeCommand(gCmd);
      }
      else
      {
        String[] gCmdArray = makeSplitCommand("pg_dump", info,
            " | split -a 4 -b " + splitSize + " - " + fullPath, login);

        exitValue = executeNativeCommand(gCmdArray);
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
   * Restores the DB using cat/psql commands; It restores the DB using splitted
   * dump files.
   * 
   * @see org.continuent.sequoia.controller.backup.Backuper#restore(DatabaseBackend,
   *      String, String, String, String, ArrayList)
   */
  public void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList tables) throws BackupException
  {
    // Parse the URL for the connection information
    String url = backend.getURL();
    PostgreSQLUrlInfo info = new AbstractPostgreSQLBackuper.PostgreSQLUrlInfo(
        url);

    if (logger.isDebugEnabled())
      logger.debug("Restoring database '" + info.getDbName() + "' on host '"
          + info.getHost() + ":" + info.getPort() + "'");

    // Check to see if the given path + dumpName exists
    String fullPath = getDumpPhysicalPath(path, dumpName);

    File dump = new File(fullPath + "aaaa");// checking only first split file,
    // suffix length is 4
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

        String[] dropCmd = makeCommandWithAuthentication("dropdb", info, "",
            login, password, false);
        if (executeNativeCommand(dropCmd) != 0)
        {
          printErrors();
          throw new BackupException(
              "dropdb execution did not complete successfully!");
        }

        // Re-create the database, use the specified encoding if provided
        if (logger.isDebugEnabled())
          logger.debug("Re-creating '" + info.getDbName() + "'");

        String[] createCmd = makeCommandWithAuthentication("createdb", info,
            encoding != null ? "--encoding=" + encoding + " " : "", login,
            password, false);
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

          String[] preRestoreCmd = makeCommandWithAuthentication("psql", info,
              " -f " + preRestoreScript, login, password, true);

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

        String[] replayCmd = makeSplitCommandWithAuthentication("cat" + " "
            + fullPath + "* | psql", info, "", login, password, false);

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

          String[] postRestoreCmd = makeCommandWithAuthentication("psql", info,
              " -f " + postRestoreScript, login, password, true);

          if (executeNativeCommand(postRestoreCmd) != 0)
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

          String preRestoreCmd = makeCommand("psql", info, " -f "
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

        String[] cmdArray = makeSplitCommand("cat" + " " + fullPath
            + "* | psql", info, "", login);

        if (executeNativeCommand(cmdArray) != 0)
        {
          printErrors();
          throw new BackupException(
              "psql execution did not complete successfully!");
        }
        
        // Run a post-restore script, if specified
        if (postRestoreScript != null)
        {
          if (logger.isDebugEnabled())
            logger.debug("Running post-restore script '" + postRestoreScript
                + "' on '" + info.getDbName() + "'");

          String postRestoreCmd = makeCommand("psql", info, " -f "
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
    BackupManager.fetchDumpFile(dumpTransferInfo, path, dumpName + ".sql");
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#deleteDump(java.lang.String,
   *      java.lang.String)
   */
  public void deleteDump(String path, String dumpName) throws BackupException
  {
    File dir = new File(path);

    // Only list files that start with the given dump name
    String[] files = dir.list(new DumpNameFilter(dumpName));

    for (int i = 0; i < files.length; i++)
    {
      File toRemove = new File(getDumpPhysicalPath(path, files[i]));
      if (logger.isDebugEnabled())
        logger.debug("Deleting dump " + toRemove);
      toRemove.delete();
    }
  }

  /**
   * Private class that filters files based on beginning of file name. This
   * should be equal to the dump file(s) we want to delete.
   * 
   * @author Dylan Hansen
   */
  class DumpNameFilter implements FilenameFilter
  {
    private String dumpName;

    /**
     * Creates a new instance of DumpNameFilter
     * 
     * @param dumpName Dump name for set of file(s)
     * @author Dylan Hansen
     */
    public DumpNameFilter(String dumpName)
    {
      this.dumpName = dumpName;
    }

    /**
     * Does this file match the filter?
     * 
     * @param dir Directory of file, not used
     * @param theDump Name of current dump we're checking
     * @author Dylan Hansen
     */
    public boolean accept(File dir, String theDump)
    {
      return (theDump.startsWith(dumpName) && theDump.length() == dumpName
          .length() + 4);
    }
  }

}
