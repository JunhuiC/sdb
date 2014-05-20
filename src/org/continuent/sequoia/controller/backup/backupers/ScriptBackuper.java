/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backup.Backuper;

/**
 * This class defines a ScriptBackuper, which performs all backup functions by
 * invoking a generic wrapper script conventionally named 'backupmgr'. The
 * following options are accepted in the backuper definition: <p/>
 * <ul>
 * <li>bindir -- Directory containing backup script.</li>
 * <li>dumpFormat -- Dump format returned by getDumpFormat()</li>
 * <li>options -- Options to be passed into wrapper using --flags option</li>
 * <li>scriptname -- Wrapper script name if other than backupmgr.</li>
 * <li>urlDecoderClassname -- Class used to decode host, port, and database
 * from URL</li>
 * </ul>
 * <p/> The wrapper script semantics are described briefly below.
 * <code><pre>NAME
 *    backupmgr 
 * SYNTAX
 *    backupmgr --op {backup | restore | delete | test}
 *      --host hostname --port port --database dbname --login dblogin
 *      --password dbpass --dumpname name --dumppath dirpath --flags options
 *      --tables tables-to-backup
 * DESCRIPTION
 *    Perform backup, restore, and log deletion operations.  The operations
 *    are defined using the --op argument, which is always supplied. 
 *    
 *    backup - Perform a backup
 *    restore - Restore a backup
 *    delete - Delete a backup
 *    
 *    The backupmgr script must encapsulate fully all operations, including
 *    if necessary dropping and recreating databases or other administrative
 *    functions associated with supported operations. 
 *    
 *  RETURN CODE
 *    Scripts must return 0 if successful.  Any other value is considered 
 *    to indicate backup failure.  In addition, the script is considered to 
 *    have failed if it writes to stderr.  Stderr from underlying processes
 *    must therefore be suppressed or redirected if it does not represent
 *    an error.   
 * </pre></code> For additional invocation details please refer to method
 * implementations. Note: For now, we default to dump server semantics provided
 * by AbstractBackuper.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @versi2on 1.0
 * @see AbstractBackuper
 */
public class ScriptBackuper extends AbstractBackuper
{
  // Options used by this backuper.
  String            bindir              = null;
  String            urlDecoderClassname = "org.continuent.sequoia.controller.backup.backupers.BasicUrlParser";
  String            scriptName          = "backupmgr";
  String            flags               = null;
  String            dumpFormat          = "User defined";

  // Executor for native commands.
  NativeCommandExec nativeCommandExec   = new NativeCommandExec();

  /**
   * Returns the dump format.
   * 
   * @see org.continuent.sequoia.controller.backup.Backuper#getDumpFormat()
   */
  public String getDumpFormat()
  {
    return dumpFormat;
  }

  /**
   * Stores options of interest to this backuper.
   * 
   * @see Backuper#setOptions(java.lang.String)
   */
  public void setOptions(String options)
  {
    super.setOptions(options);
    bindir = applyOption(bindir, "bindir");
    urlDecoderClassname = applyOption(urlDecoderClassname,
        "urlDecoderClassname");
    scriptName = applyOption(scriptName, "scriptName");
    flags = applyOption(flags, "flags");
    dumpFormat = applyOption(dumpFormat, "dumpFormat");
  }

  // Returns option from map if the option has been set.
  private String applyOption(String defaultValue, String optionName)
  {
    String optionValue = (String) optionsMap.get(optionName);
    return (optionValue == null) ? defaultValue : optionValue;
  }

  /**
   * Invokes a backup operation on the backup management script.
   * 
   * @see org.continuent.sequoia.controller.backup.Backuper#backup(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    // Generate and execute the command.
    String cmd = getBackupRestoreCommand("backup", backend, login, password,
        dumpName, path, tables);
    if (!nativeCommandExec.safelyExecNativeCommand(cmd, null, null, 0,
        getIgnoreStdErrOutput()))
    {
      throw new BackupException("Backup operation failed with errors; see log");
    }

    return new Date();
  }

  /**
   * Invokes a restore operation on the backup management script.
   * 
   * @see org.continuent.sequoia.controller.backup.Backuper#restore(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    // Generate and execute the command.
    String cmd = getBackupRestoreCommand("restore", backend, login, password,
        dumpName, path, tables);
    if (!nativeCommandExec.safelyExecNativeCommand(cmd, null, null, 0,
        getIgnoreStdErrOutput()))
    {
      throw new BackupException("Restore operation failed with errors; see log");
    }
  }

  /**
   * Invokes a restore operation on the backup management script.
   * 
   * @see org.continuent.sequoia.controller.backup.Backuper#deleteDump(java.lang.String,
   *      java.lang.String)
   */
  public void deleteDump(String path, String dumpName) throws BackupException
  {
    String backupCmd = getBackupCommand();
    String cmd = backupCmd + " --op delete --dumpName '" + dumpName
        + "' --dumpPath '" + path;
    if (!nativeCommandExec.safelyExecNativeCommand(cmd, null, null, 0,
        getIgnoreStdErrOutput()))
    {
      throw new BackupException(
          "Dump delete operation failed with errors; see log");
    }
  }

  // Returns an instantiated URL parser. We do this at the time of a backup
  // operation so that we can pass a nice error message back to the caller.
  private JdbcUrlParser getJdbcParser() throws BackupException
  {
    if (urlDecoderClassname == null)
    {
      return new BasicUrlParser();
    }
    else
    {
      try
      {
        JdbcUrlParser parser = (JdbcUrlParser) Class.forName(
            urlDecoderClassname).newInstance();
        return parser;
      }
      catch (ClassNotFoundException e)
      {
        throw new BackupException("Unable to find URL decoder class: "
            + urlDecoderClassname);
      }
      catch (InstantiationException e)
      {
        throw new BackupException("Unable to instantiate URL decoder class: "
            + urlDecoderClassname);
      }
      catch (IllegalAccessException e)
      {
        throw new BackupException(
            "Unable to access URL decoder class or its default constructor: "
                + urlDecoderClassname);
      }
      catch (Throwable t)
      {
        throw new BackupException(
            "Unexpected error while instantiating URL decoder class; see log",
            t);
      }
    }
  }

  // Returns the command string for a backup or restore command with full
  // options.
  private String getBackupRestoreCommand(String operation,
      DatabaseBackend backend, String login, String password, String dumpName,
      String path, ArrayList<?> tables) throws BackupException
  {
    // Parse the backend URL.
    JdbcUrlParser urlParser = getJdbcParser();
    urlParser.setUrl(backend.getURL());

    // Create the table list, if any.
    String tableList = null;
    if (tables != null)
    {
      Iterator<?> iter = tables.iterator();
      while (iter.hasNext())
      {
        String tableName = (String) iter.next();
        if (tableList == null)
          tableList = tableName;
        else
          tableList += "," + tableName;
      }
    }

    // Construct the command.
    String backupCmd = getBackupCommand();
    String cmd = backupCmd + " --op " + operation + " --host "
        + urlParser.getHost() + " --port " + urlParser.getPort()
        + " --database " + urlParser.getDbName() + " --login " + login
        + " --password " + password + " --dumpName " + dumpName
        + " --dumpPath " + path;
    if (tableList != null)
    {
      cmd += " --tables " + tableList;
    }
    if (flags != null)
    {
      cmd += " --flags " + flags;
    }

    return cmd;
  }

  // Returns the backup command as a path.
  private String getBackupCommand()
  {
    if (bindir == null)
      return scriptName;
    else
      return bindir + File.separatorChar + scriptName;
  }
}