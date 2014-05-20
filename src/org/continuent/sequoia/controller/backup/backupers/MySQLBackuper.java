/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent Inc.
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
 * Initial developer(s): Mykola Paliyenko.
 * Contributor(s): Emmanuel Cecchet, Stephane Giron
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.connection.SimpleConnectionManager;

/**
 * MySQL backuper inspired from the PostgreSQL backuper.
 * <p>
 * Options for this backuper are:
 * 
 * <pre>
 * - bindir: path to mysqldump binary
 * - dumpServer: address to bind the dump server
 * </pre>
 * 
 * @author <a href="mailto:mpaliyenko@gmail.com">Mykola Paliyenko</a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 */
public class MySQLBackuper extends AbstractBackuper
{
  private static final String DEFAULT_MYSQL_PORT = "3306";

  private static final String DEFAULT_MYSQL_HOST = "localhost";

  static Trace                logger             = Trace
                                                     .getLogger(MySQLBackuper.class
                                                         .getName());
  /** end user logger */
  static Trace                endUserLogger      = Trace
                                                     .getLogger("org.continuent.sequoia.enduser");

  /** CommandExec instance for running native commands. * */
  protected NativeCommandExec nativeCmdExec      = new NativeCommandExec();

  /**
   * Creates a new <code>MySQLBackuper</code> object
   */
  public MySQLBackuper()
  {
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getDumpFormat()
   */
  public String getDumpFormat()
  {
    return "MySQL raw dump";
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#backup(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    String url = backend.getURL();
    if (!url.startsWith("jdbc:mysql:"))
    {
      throw new BackupException("Unsupported db url " + url);
    }
    MySQLUrlInfo info = new MySQLUrlInfo(url);

    try
    {
      File pathDir = new File(path);
      if (!pathDir.exists())
      {
        pathDir.mkdirs();
        pathDir.mkdir();
      }
      String dumpPath = getDumpPhysicalPath(path, dumpName);
      if (logger.isDebugEnabled())
      {
        logger.debug("Dumping " + backend.getURL() + " in " + dumpPath);
      }

      String executablePath = (optionsMap.containsKey("bindir")
          ? (String) optionsMap.get("bindir") + File.separator
          : "")
          + "mysqldump";

      int exitValue = safelyExecuteNativeCommand(executablePath
          + getBackupStoredProceduresOption(executablePath) + " -h "
          + info.getHost() + " --port=" + info.getPort() + " -u" + login
          + " --password=" + password + " " + info.getDbName(), dumpPath, true);

      if (exitValue != 0)
      {
        printErrors();
        throw new BackupException(
            "mysqldump execution did not complete successfully!");
      }
      performPostBackup(backend, login, password, dumpPath);
    }
    catch (Exception e)
    {
      String msg = "Error while performing backup";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }

    return new Date();
  }

  /**
   * Perform all post backup operations that are required :
   * <ul>
   * <li>save auto-generated keys values</li>
   * </ul>
   * 
   * @param backend backend that is used for the backup
   * @param login login that is used
   * @param password password that is used
   * @param dumpPath path and file where the backup file is generated
   * @throws SQLException if an exception occurs when querying the database
   * @throws UnreachableBackendException if a connection cannot be taken on this
   *           backend
   * @throws IOException if a problem occurs when writing into the backup file
   */
  private void performPostBackup(DatabaseBackend backend, String login,
      String password, String dumpPath) throws SQLException,
      UnreachableBackendException, IOException
  {
    BufferedWriter out = null;

    // If there are any auto-generated keys on tables, the backup will not
    // contain these. We need to save it at the end of the dump to be able to
    // restart the "auto_generated" column at the right value.
    SimpleConnectionManager simpleConnectionManager = new SimpleConnectionManager(
        backend.getURL(), backend.getName(), login, password, backend
            .getDriverPath(), backend.getDriverClassName());
    try
    {
      simpleConnectionManager.initializeConnections();
      PooledConnection pooledConn = simpleConnectionManager.getConnection();
      Connection conn = pooledConn.getConnection();

      ResultSet rs = conn.createStatement().executeQuery("show table status");
      while (rs.next())
      {
        long lastAutoIncrement = rs.getLong("Auto_increment");
        if (lastAutoIncrement > 1)
        {
          if (out == null)
            out = new BufferedWriter(new FileWriter(dumpPath, true));

          out.write("ALTER TABLE `"
              + rs.getString("Name").replaceAll("`", "``")
              + "` AUTO_INCREMENT=" + lastAutoIncrement + ";");

          out.newLine();
        }

      }
      if (out != null)
      {
        out.flush();
        out.close();
      }
      rs.close();
      conn.close();
    }
    finally
    {
      simpleConnectionManager.finalizeConnections();
    }
  }

  /**
   * Returns the option to supply to mysql so that it backups stored-procedures
   * and functions in the dump. This can be either "" (mysql versions prior to
   * 5.0.13 do not know about stored procedures) or " --routines" (mysql 5.0.13
   * and above).
   * 
   * @param executablePath the path to the mysql utility used for making dumps
   * @return the option to supply to mysql so that it backups strored-procedures
   *         and functions in the dump
   * @throws IOException in case of error when communicating with the
   *           sub-process.
   * @throws IllegalArgumentException if no version information can be found in
   *           'executablePath'
   */
  private static String getBackupStoredProceduresOption(String executablePath)
      throws IOException, IllegalArgumentException
  {
    int majorVersion = Integer.parseInt(getMajorVersion(executablePath));
    if (majorVersion < 5)
      return "";
    else if (majorVersion == 5)
    {
      String minorVersionString = getMinorVersion(executablePath);
      float minorVersion = Float.parseFloat(minorVersionString);
      if (minorVersion < 1)
      {
        // --routines supported from v5.0.13 upwards
        // Need to check for extra value (i.e. 0.9 < 0.13)
        String extraVersionString;
        try
        {
          extraVersionString = minorVersionString.substring(minorVersionString
              .indexOf('.') + 1);
        }
        catch (IndexOutOfBoundsException e)
        { // No minor version number
          return "";
        }
        float extraVersion = Float.parseFloat(extraVersionString);
        if (extraVersion < 13)
          return "";
      }
    }

    return " --routines";
  }

  /**
   * Returns the major version number for specified mysql native utility.
   * 
   * @param executablePath the path to the mysql native utility
   * @return the major version number for specified mysql native utility. *
   * @throws IOException in case of error when communicating with the
   *           sub-process.
   * @throws IllegalArgumentException if no version information can be found in
   *           'executablePath'
   */
  private static String getMajorVersion(String executablePath)
      throws IOException, IllegalArgumentException
  {
    Process p = Runtime.getRuntime().exec(executablePath + " --version");
    BufferedReader pout = new BufferedReader(new InputStreamReader(p
        .getInputStream()));

    String versionString = pout.readLine();
    // sample version string:
    // "/usr/bin/mysql Ver 14.12 Distrib 5.0.18, for pc-linux-gnu (i686) using
    // readline 5.0"
    Pattern regex = Pattern
        .compile("mysql.*Ver ([0-9.]*) Distrib ([0-9])\\.([0-9.]*)");
    Matcher m = regex.matcher(versionString);
    if (!m.find())
      throw new IllegalArgumentException(
          "Can not find version information for " + executablePath);

    return m.group(2);
  }

  /**
   * Returns the minor version number for specified mysql native utility.
   * 
   * @param executablePath the path to the mysql native utility
   * @return the minor version number for specified mysql native utility. *
   * @throws IOException in case of error when communicating with the
   *           sub-process.
   * @throws IllegalArgumentException if no version information can be found in
   *           'executablePath'
   */
  private static String getMinorVersion(String executablePath)
      throws IOException, IllegalArgumentException
  {
    Process p = Runtime.getRuntime().exec(executablePath + " --version");
    BufferedReader pout = new BufferedReader(new InputStreamReader(p
        .getInputStream()));

    String versionString = pout.readLine();
    // sample version string:
    // "/usr/bin/mysql Ver 14.12 Distrib 5.0.18, for pc-linux-gnu (i686) using
    // readline 5.0"
    Pattern regex = Pattern
        .compile("mysql.*Ver ([0-9.]*) Distrib ([0-9])\\.([0-9.]*)");
    Matcher m = regex.matcher(versionString);
    if (!m.find())
      throw new IllegalArgumentException(
          "Can not find version information for " + executablePath);

    return m.group(3);
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#restore(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    String url = backend.getURL();
    if (!url.startsWith("jdbc:mysql:"))
    {
      throw new BackupException("Unsupported db url " + url);
    }
    MySQLUrlInfo info = new MySQLUrlInfo(url);
    try
    {
      File pathDir = new File(path);
      if (!pathDir.exists())
      {
        pathDir.mkdirs();
        pathDir.mkdir();
      }
      String dumpPath = getDumpPhysicalPath(path, dumpName);
      if (logger.isDebugEnabled())
      {
        logger.debug("Restoring " + backend.getURL() + " from " + dumpPath);
      }

      String executablePath = (optionsMap.containsKey("bindir")
          ? (String) optionsMap.get("bindir") + File.separator
          : "");

      // Drop the database if it already exists
      if (logger.isDebugEnabled())
        logger.debug("Dropping database '" + info.getDbName() + "'");

      String mysqladminExecutablePath = executablePath + "mysqladmin";
      if (executeNativeCommand(mysqladminExecutablePath + " -h "
          + info.getHost() + " --port=" + info.getPort() + " -f -u" + login
          + " --password=" + password + " drop " + info.getDbName()) != 0)
      {
        // Errors can happen there, e.g. if the database does not exist yet.
        // Just log them, and carry-on...
        printErrors();
      }

      // Create database
      if (logger.isDebugEnabled())
        logger.debug("Creating database '" + info.getDbName() + "'");

      if (executeNativeCommand(mysqladminExecutablePath + " -h "
          + info.getHost() + " --port=" + info.getPort() + " -f -u" + login
          + " --password=" + password + " create " + info.getDbName()) != 0)
      {
        // Errors can happen there, e.g. if the database does not exist yet.
        // Just log them, and carry-on...
        printErrors();
        throw new BackupException("Failed to create database '"
            + info.getDbName() + "'");
      }

      // Load dump
      String mysqlExecutablePath = executablePath + "mysql";
      int exitValue = safelyExecuteNativeCommand(mysqlExecutablePath + " -h "
          + info.getHost() + " --port=" + info.getPort() + " -u" + login
          + " --password=" + password + " " + info.getDbName(), dumpPath, false);

      if (exitValue != 0)
      {
        printErrors();
        throw new BackupException(
            "mysql execution did not complete successfully!");
      }
    }
    catch (Exception e)
    {
      String msg = "Error while performing restore";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#deleteDump(java.lang.String,
   *      java.lang.String)
   */
  public void deleteDump(String path, String dumpName) throws BackupException
  {
    File toRemove = new File(getDumpPhysicalPath(path, dumpName));
    if (logger.isDebugEnabled())
      logger.debug("Deleting compressed dump " + toRemove);
    toRemove.delete();
  }

  /**
   * Get the dump physical path from its logical name
   * 
   * @param path the path where the dump is stored
   * @param dumpName dump logical name
   * @return path to zip file
   */
  private String getDumpPhysicalPath(String path, String dumpName)
  {
    return path + File.separator + dumpName;
  }

  /**
   * Allow to parse MySQL URL.
   */
  protected class MySQLUrlInfo
  {
    private boolean isLocal;

    private String  host;

    private String  port;

    private String  dbName;

    // Used to parse url
    private Pattern pattern = Pattern
                                .compile("jdbc:mysql:((//([a-zA-Z0-9_\\-.]+|\\[[a-fA-F0-9:]+])((:(\\d+))|))/|)([a-zA-Z][a-zA-Z0-9_\\-]*)(\\?.*)?");

    Matcher         matcher;

    /**
     * Creates a new <code>MySQLUrlInfo</code> object, used to parse the
     * postgresql jdbc options. If host and/or port aren't specified, will
     * default to localhost:3306. Note that database name must be specified.
     * 
     * @param url the MySQL JDBC url to parse
     */
    public MySQLUrlInfo(String url)
    {
      matcher = pattern.matcher(url);

      if (matcher.matches())
      {
        if (matcher.group(3) != null)
          host = matcher.group(3);
        else
          host = DEFAULT_MYSQL_HOST;

        if (matcher.group(6) != null)
          port = matcher.group(6);
        else
          port = DEFAULT_MYSQL_PORT;

        dbName = matcher.group(7);
      }
    }

    /**
     * Gets the HostParameters of this postgresql jdbc url as a String that can
     * be used to pass into cmd line/shell calls.
     * 
     * @return a string that can be used to pass into a cmd line/shell call.
     */
    public String getHostParametersString()
    {
      if (isLocal)
      {
        return "";
      }
      else
      {
        return "-h " + host + " --port=" + port;
      }
    }

    /**
     * Gets the database name part of this postgresql jdbc url.
     * 
     * @return the database name part of this postgresql jdbc url.
     */
    public String getDbName()
    {
      return dbName;
    }

    /**
     * Gets the host part of this postgresql jdbc url.
     * 
     * @return the host part of this postgresql jdbc url.
     */
    public String getHost()
    {
      return host;
    }

    /**
     * Gets the port part of this postgresql jdbc url.
     * 
     * @return the port part of this postgresql jdbc url.
     */
    public String getPort()
    {
      return port;
    }

    /**
     * Checks whether this postgresql jdbc url refers to a local db or not, i.e.
     * has no host specified, e.g. jdbc:postgresql:myDb.
     * 
     * @return true if this postgresql jdbc url has no host specified, i.e.
     *         refers to a local db.
     */
    public boolean isLocal()
    {
      return isLocal;
    }

  }

  /**
   * Executes a native operating system command. Output of these commands is
   * captured and logged.
   * 
   * @param command String of command to execute
   * @return 0 if successful, any number otherwise
   * @throws IOException
   * @throws InterruptedException
   */
  protected int executeNativeCommand(String command) throws IOException,
      InterruptedException
  {
    return nativeCmdExec.executeNativeCommand(command, null, null, 0,
        getIgnoreStdErrOutput());
  }

  /**
   * Executes a native operating system dump or restore command. Output of these
   * commands is carefully captured and logged.
   * 
   * @param command String of command to execute
   * @param dumpPath path to the dump file (either source or dest, depending on
   *          specified 'backup' parameter.
   * @param backup specifies whether we are doing a backup (true) or a restore
   *          (false). This sets the semantics of the dumpPath parameter to
   *          resp. dest or source file name.
   * @return 0 if successful, 1 otherwise
   */
  protected int safelyExecuteNativeCommand(String command, String dumpPath,
      boolean backup) throws IOException
  {
    if (backup)
      return nativeCmdExec.safelyExecNativeCommand(command, null,
          new FileOutputStream(dumpPath), 0, getIgnoreStdErrOutput()) ? 0 : 1;
    else
    {
      FileInputStream dumpStream = new FileInputStream(dumpPath);
      return nativeCmdExec.safelyExecNativeCommand(command,
          new NativeCommandInputSource(dumpStream), null, 0,
          getIgnoreStdErrOutput()) ? 0 : 1;
    }
  }

  protected void printErrors()
  {
    ArrayList<?> errors = nativeCmdExec.getStderr();
    Iterator<?> it = errors.iterator();
    while (it.hasNext())
    {
      String msg = (String) it.next();
      logger.info(msg);
      endUserLogger.error(msg);
    }
  }
}