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
 * Initial developer(s): Adam Fletcher, Mykola Paliyenko.
 * Contributor(s): Emmanuel Cecchet, Stephane Giron
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * MSSQL backuper inspired from the PostgreSQL backuper. <br>
 * This backuper takes the following options: <br>
 * urlHeader: expected URL header for the backend. Default is
 * "jdbc:jtds:sqlserver:" <br>
 * driverClassName: driver class name to load. Default is the driver class
 * defined for the targeted backend. If you need to use a specific driver for
 * backup/restore operation, you can force it here.
 * <p>
 * To use it, edit the virtual database config's <Backup> node:
 * 
 * <pre>
 * <Backup>
 *   <Backuper backuperName="MSSQLServer" className="org.continuent.sequoia.controller.backup.backupers.MSSQLBackuper" options=""/>
 *   <Backuper backuperName="MSSQLServerWithjTDSDriver" className="org.continuent.sequoia.controller.backup.backupers.MSSQLBackuper" options="urlHeader=jdbc:jtds:sqlserver:,driverClassName=net.sourceforge.jtds.jdbc.Driver"/>
 *   <Backuper backuperName="MSSQLServerWithMSDriver" className="org.continuent.sequoia.controller.backup.backupers.MSSQLBackuper" options="urlHeader=jdbc:microsoft:sqlserver:,driverClassName=com.microsoft.jdbc.sqlserver.SQLServerDriver"/>
 * </Backup>
 *</pre>
 * 
 * Then in the console to take the backups:<br>
 * backup mybackend database.dump MSSQLServer \\<fileserver>\<share>
 * 
 * @author <a href="mailto:adamf@powersteeringsoftware.com">Adam Fletcher</a>
 * @author <a href="mailto:mpaliyenko@gmail.com">Mykola Paliyenko</a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 */
public class MSSQLBackuper extends AbstractBackuper
{

  private static final String DEFAULT_MSSQL_PORT       = "1433";
  private static final String DEFAULT_MSSQL_HOST       = "localhost";
  private static final String JTDS_URL                 = "jdbc:jtds:sqlserver:";
  private static final String DEFAULT_JDBC_URL         = JTDS_URL;

  private static final Object URL_OPTION               = "urlHeader";
  private static final Object DRIVER_CLASS_NAME_OPTION = "driverClassName";

  static Trace                logger                   = Trace
                                                           .getLogger(MSSQLBackuper.class
                                                               .getName());

  private ArrayList           errors;

  /**
   * Creates a new <code>MSSQLBackuper</code> object
   */
  public MSSQLBackuper()
  {
    errors = new ArrayList();
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getDumpFormat()
   */
  public String getDumpFormat()
  {
    return "MSSQL raw dump";
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#backup(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList tables) throws BackupException
  {
    String url = backend.getURL();

    String expectedUrl = (String) optionsMap.get(URL_OPTION);
    if (expectedUrl == null)
      expectedUrl = DEFAULT_JDBC_URL;

    if (!url.startsWith(expectedUrl))
    {
      throw new BackupException("Unsupported db url " + url);
    }
    MSSQLUrlInfo info = new MSSQLUrlInfo(url);
    Connection con;

    try
    {
      String driverClassName = (String) optionsMap
          .get(DRIVER_CLASS_NAME_OPTION);
      if (driverClassName == null)
        driverClassName = backend.getDriverClassName();

      // Load the driver and connect to the database
      Class.forName(driverClassName);
      con = DriverManager.getConnection(url + ";user=" + login + ";password="
          + password);
    }
    catch (Exception e)
    {
      String msg = "Error while performing backup during creation of connection";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }

    try
    {
      File pathDir = new File(path);
      if (!pathDir.exists())
      {
        pathDir.mkdirs();
        pathDir.mkdir();
      }

      /*
       * What should be done here: 1) verify the path is a UNC path 2) connect
       * to the server via JDBC 3) issue the 'BACKUP <dbname> TO DISK='<path>'
       * <path> must be UNC connect to the configured backend with JDBC
       */
      // String dumpPath = path + File.separator + dumpName;
      // we don't use File.seperator here because we are using UNC paths
      String dumpPath = path + "\\" + dumpName;

      if (logger.isDebugEnabled())
      {
        logger.debug("Dumping " + backend.getURL() + " in " + dumpPath);
      }

      Statement stmt = con.createStatement();
      String sqlStatement = "BACKUP DATABASE " + info.getDbName()
          + " TO DISK = '" + dumpPath + "'";

      logger.debug("sql statement for backup: " + sqlStatement);

      boolean backupResult = stmt.execute(sqlStatement);

      if (backupResult)
      {
        String msg = "BACKUP returned false";
        logger.error(msg);
        throw new BackupException(msg);
      }
    }
    catch (Exception e)
    {
      String msg = "Error while performing backup";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }
    finally
    {
      try
      {
        con.close();
      }
      catch (Exception e)
      {
        String msg = "Error while performing backup during close connection";
        logger.error(msg, e);
        throw new BackupException(msg, e);
      }
    }
    return new Date();
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#restore(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList tables) throws BackupException
  {
    String url = backend.getURL();

    String expectedUrl = (String) optionsMap.get(URL_OPTION);

    if (expectedUrl == null)
      expectedUrl = DEFAULT_JDBC_URL;

    if (!url.startsWith(expectedUrl))
    {
      throw new BackupException("Unsupported db url " + url);
    }

    MSSQLUrlInfo info = new MSSQLUrlInfo(url);
    Connection con;
    try
    {
      String driverClassName = (String) optionsMap
          .get(DRIVER_CLASS_NAME_OPTION);
      if (driverClassName == null)
        driverClassName = backend.getDriverClassName();

      // Load the driver and connect to the database
      Class.forName(driverClassName);

      // NOTE: you have to connect to the master database to restore,
      // because if you connect to the current DB you are USING the db
      // and therefore LOCKING the db.

      String dbName = null;

      // DB name syntax with jTDS JDBC driver
      if (expectedUrl == JTDS_URL)
      {
        dbName = "/master";
      }

      // DB name syntax with microsoft JDBC driver
      else
      {
        dbName = ";DatabaseName=master";
      }

      con = DriverManager.getConnection(expectedUrl + "//" + info.getHost()
          + ":" + info.getPort() + dbName + ";user=" + login + ";password="
          + password);
    }
    catch (Exception e)
    {
      String msg = "Error while performing restore during creation of connection";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }

    try
    {
      File pathDir = new File(path);
      if (!pathDir.exists())
      {
        pathDir.mkdirs();
        pathDir.mkdir();
      }
      String dumpPath = path + File.separator + dumpName;
      if (logger.isDebugEnabled())
      {
        logger.debug("Restoring " + backend.getURL() + " from " + dumpPath);
      }

      Statement stmt = con.createStatement();

      String sqlAtatement = "RESTORE DATABASE " + info.getDbName()
          + " FROM DISK = '" + dumpPath + "'";

      boolean backupResult = stmt.execute(sqlAtatement);

      if (backupResult)
      {
        String msg = "restore returned false";
        logger.error(msg);
        throw new BackupException(msg);
      }
    }
    catch (Exception e)
    {
      String msg = "Error while performing restore";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }
    finally
    {
      try
      {
        con.close();
      }
      catch (Exception e)
      {
        String msg = "Error while performing restore during close connection";
        logger.error(msg, e);
        throw new BackupException(msg, e);
      }
    }
  }

  String getProcessOutput(Process process) throws IOException
  {
    StringBuffer sb = new StringBuffer();

    InputStream pis = process.getInputStream();
    InputStream pes = process.getErrorStream();

    int c = pis.read();
    while (c != -1)
    {
      sb.append(new Character((char) c));
      c = pis.read();
    }
    c = pes.read();
    while (c != -1)
    {
      sb.append(new Character((char) c));
      c = pes.read();
    }

    return sb.toString();
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
   * Allow to parse PostgreSQL URL.
   */
  protected class MSSQLUrlInfo
  {
    private boolean isLocal;

    private String  host;

    private String  port;

    private String  dbName;

    /**
     * Creates a new <code>MSSQLUrlInfo</code> object, used to parse the MSSQL
     * JDBC options. If host and/or port aren't specified, will default to
     * localhost:1433. Note that database name must be specified.
     * 
     * @param url the MSSQL JDBC url to parse
     */
    public MSSQLUrlInfo(String url) throws BackupException
    {
      String expectedUrl = (String) optionsMap.get(URL_OPTION);

      if (expectedUrl == null)
        expectedUrl = DEFAULT_JDBC_URL;

      // Used to parse url
      Pattern pattern = Pattern
          .compile(
              expectedUrl
                  + "//([a-zA-Z0-9_\\-.]+|\\[[a-fA-F0-9:]+])(:(\\d+))?" // hostname
                                                                        // and
                                                                        // optional
                                                                        // port
                  /* either microsoft JDBC driver or jTDS JDBC driver */
                  + "(;.*DatabaseName=([a-zA-Z][a-zA-Z0-9_\\-]*).*|/([a-zA-Z][a-zA-Z0-9_\\-]*))?(\\?.*|;.*)?",
              Pattern.CASE_INSENSITIVE);

      Matcher matcher;

      matcher = pattern.matcher(url);

      if (matcher.matches())
      {
        if (matcher.group(1) != null)
          host = matcher.group(1);
        else
          host = DEFAULT_MSSQL_HOST;

        if (matcher.group(3) != null)
          port = matcher.group(3);
        else
          port = DEFAULT_MSSQL_PORT;

        /* DatabaseName value with the microsoft JDBC driver */
        dbName = matcher.group(5);

        /* dbName for jTDS JDBC driver, can be null */
        if (dbName == null)
          dbName = matcher.group(6);
      }
      else
      {
        throw new BackupException("Unsupported JDBC driver " + expectedUrl
            + " or incorrectdb url " + url);
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
      logger.debug("getHostParamertsString host: " + host + " port: " + port);

      if (isLocal)
      {
        return "//localhost";
      }
      else
      {
        return "//" + host + ":" + port;
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

  protected void printErrors()
  {
    Iterator it = errors.iterator();
    while (it.hasNext())
    {
      logger.info(it.next());
    }
  }
}
