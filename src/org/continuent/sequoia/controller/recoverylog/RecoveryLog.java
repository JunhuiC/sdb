/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Julie Marguerite, Greg Ward, Jess Sightler, Jean-Bernard van Zuylen, Charles Cordingley.
 */

package org.continuent.sequoia.controller.recoverylog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.management.BackendState;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.connection.DriverManager;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.loadbalancer.tasks.BeginTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CallableStatementExecuteTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.ClosePersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CommitTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.OpenPersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.ReleaseSavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackToSavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.SavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.StatementExecuteQueryTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.StatementExecuteTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.StatementExecuteUpdateTask;
import org.continuent.sequoia.controller.recoverylog.events.DeleteLogEntriesAndCheckpointBetweenEvent;
import org.continuent.sequoia.controller.recoverylog.events.FindClosePersistentConnectionEvent;
import org.continuent.sequoia.controller.recoverylog.events.FindCommitEvent;
import org.continuent.sequoia.controller.recoverylog.events.FindLastIdEvent;
import org.continuent.sequoia.controller.recoverylog.events.FindRollbackEvent;
import org.continuent.sequoia.controller.recoverylog.events.GetCheckpointLogEntryEvent;
import org.continuent.sequoia.controller.recoverylog.events.GetCheckpointLogIdEvent;
import org.continuent.sequoia.controller.recoverylog.events.GetNumberOfLogEntriesEvent;
import org.continuent.sequoia.controller.recoverylog.events.GetUpdateCountEvent;
import org.continuent.sequoia.controller.recoverylog.events.LogCommitEvent;
import org.continuent.sequoia.controller.recoverylog.events.LogEntry;
import org.continuent.sequoia.controller.recoverylog.events.LogRequestCompletionEvent;
import org.continuent.sequoia.controller.recoverylog.events.LogRequestEvent;
import org.continuent.sequoia.controller.recoverylog.events.LogRollbackEvent;
import org.continuent.sequoia.controller.recoverylog.events.RemoveCheckpointEvent;
import org.continuent.sequoia.controller.recoverylog.events.ResetLogEvent;
import org.continuent.sequoia.controller.recoverylog.events.ShiftLogEntriesEvent;
import org.continuent.sequoia.controller.recoverylog.events.ShutdownLogEvent;
import org.continuent.sequoia.controller.recoverylog.events.StoreCheckpointWithLogIdEvent;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.RequestFactory;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;

/**
 * Recovery Log using a database accessed through JDBC.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>*
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public class RecoveryLog implements XmlComponent
{
  static final String         BEGIN                                            = "begin";
  private static final String COMMIT                                           = "commit";
  private static final String ROLLBACK                                         = "rollback";
  private static final String CLOSE_PERSISTENT_CONNECTION                      = "close";
  private static final String OPEN_PERSISTENT_CONNECTION                       = "open";
  /**
   * Unkown user used for transaction abort when the transactional context
   * cannot be retrieved
   */
  public static final String  UNKNOWN_USER                                     = "_u u_";

  /** Index of the log id column in the log table */
  public static final int     COLUMN_INDEX_LOG_ID                              = 1;
  /** Index of the vlogin column in the log table */
  public static final int     COLUMN_INDEX_VLOGIN                              = 2;
  /** Index of the sql column in the log table */
  public static final int     COLUMN_INDEX_SQL                                 = 3;
  /** Index of the sql_param column in the log table */
  public static final int     COLUMN_INDEX_SQL_PARAMS                          = 4;
  /** Index of the auto_conn_tran column in the log table */
  public static final int     COLUMN_INDEX_AUTO_CONN_TRAN                      = 5;
  /** Index of the transaction id column in the log table */
  public static final int     COLUMN_INDEX_TRANSACTION_ID                      = 6;
  /** Index of the request id column in the log table */
  public static final int     COLUMN_INDEX_REQUEST_ID                          = 7;
  /** Index of the exec_status column in the log table */
  public static final int     COLUMN_INDEX_EXEC_STATUS                         = 8;
  /** Index of the exec_time column in the log table */
  public static final int     COLUMN_INDEX_EXEC_TIME                           = 9;
  /** Index of the update_count column in the log table */
  public static final int     COLUMN_INDEX_UPDATE_COUNT                        = 10;

  static Trace                logger                                           = Trace
                                                                                   .getLogger("org.continuent.sequoia.controller.recoverylog");
  static Trace                endUserLogger                                    = Trace
                                                                                   .getLogger("org.continuent.sequoia.enduser");

  private RequestFactory      requestFactory                                   = ControllerConstants.CONTROLLER_FACTORY
                                                                                   .getRequestFactory();

  /** Number of backends currently recovering */
  private long                recoveringNb                                     = 0;

  /** Size of the pendingRecoveryTasks queue used by the recover thread */
  protected int               recoveryBatchSize;

  /** Driver class name. */
  private String              driverClassName;

  /** Driver name. */
  private String              driverName;

  /** Driver URL. */
  private String              url;

  /** User's login. */
  private String              login;

  /** User's password. */
  private String              password;

  /** Database connection and sundry prepared statements. */
  private Connection          internalConnectionManagedByGetDatabaseConnection = null;

  /**
   * Recovery log table options.
   * 
   * @see #setLogTableCreateStatement(String, String, String, String, String,
   *      String, String, String, String, String, String, String)
   */
  private String              logTableCreateTable;
  private String              logTableName;
  private String              logTableCreateStatement;
  private String              logTableLogIdType;
  private String              logTableVloginType;
  private String              logTableSqlColumnName;
  private String              logTableSqlType;
  private String              logTableAutoConnTranColumnType;
  private String              logTableTransactionIdType;
  private String              logTableRequestIdType;
  private String              logTableExecTimeType;
  private String              logTableUpdateCountType;
  private String              logTableExtraStatement;

  /**
   * Checkpoint table options.
   * 
   * @see #setCheckpointTableCreateStatement(String, String, String, String,
   *      String)
   */
  private String              checkpointTableCreateTable;
  private String              checkpointTableName;
  private String              checkpointTableCreateStatement;
  private String              checkpointTableNameType;
  private String              checkpointTableLogIdType;
  private String              checkpointTableExtraStatement;

  /**
   * Backend table options
   * 
   * @see #setBackendTableCreateStatement(String, String, String, String,
   *      String, String, String)
   */
  private String              backendTableCreateStatement;
  private String              backendTableName;
  private String              backendTableCreateTable;
  private String              backendTableDatabaseName;
  private String              backendTableExtraStatement;
  private String              backendTableCheckpointName;
  private String              backendTableBackendState;
  private String              backendTableBackendName;

  /**
   * Dump table options
   * 
   * @see #setDumpTableCreateStatement(String, String, String, String, String,
   *      String, String, String, String, String, String)
   */
  private String              dumpTableCreateStatement;
  private String              dumpTableCreateTable;
  private String              dumpTableName;
  private String              dumpTableDumpNameColumnType;
  private String              dumpTableDumpDateColumnType;
  private String              dumpTableDumpPathColumnType;
  private String              dumpTableDumpFormatColumnType;
  private String              dumpTableCheckpointNameColumnType;
  private String              dumpTableBackendNameColumnType;
  private String              dumpTableTablesColumnName;
  private String              dumpTableTablesColumnType;
  private String              dumpTableExtraStatementDefinition;

  /** Current maximum value of the primary key in logTableName. */
  private long                logTableId                                       = 0;

  /** Timeout for SQL requests. */
  private int                 timeout;

  private LoggerThread        loggerThread;

  private boolean             isShuttingDown                                   = false;

  /** timer used to auto-close the jdbc connection */
  private Timer               timer                                            = new Timer();
  private int                 autocloseTimeout;

  /** optional connection-checking feature */
  private boolean             checkConnectionValidity;

  /** Recoverylog failures "management" */
  boolean                     isDirty                                          = false;

  /**
   * Creates a new <code>RecoveryLog</code> instance.
   * 
   * @param driverName the driverClassName name.
   * @param driverClassName the driverClassName class name.
   * @param url the JDBC URL.
   * @param login the login to use to connect to the database.
   * @param password the password to connect to the database.
   * @param requestTimeout timeout in seconds for update queries.
   * @param recoveryBatchSize number of queries that can be accumulated into a
   *          batch when recovering
   */
  public RecoveryLog(String driverName, String driverClassName, String url,
      String login, String password, int requestTimeout, int recoveryBatchSize)
  {
    this.driverName = driverName;
    this.driverClassName = driverClassName;
    this.url = url;
    this.login = login;
    this.password = password;
    this.timeout = requestTimeout;
    if (recoveryBatchSize < 1)
    {
      logger
          .warn("RecoveryBatchSize was set to a value lesser than 1, resetting value to 1.");
      recoveryBatchSize = 1;
    }
    this.recoveryBatchSize = recoveryBatchSize;

    if (url.startsWith("jdbc:hsqldb:file:"))
    {
      if (url.indexOf("shutdown=true") == -1)
      {
        this.url = url + ";shutdown=true";
        String msg = "Hsqldb RecoveryLog url has no shutdown=true option.\n"
            + "This prevents the recovery log from shutting down correctly.\n"
            + "Please update your vdb.xml file.\n" + "Setting url to '"
            + this.url + "'.";
        logger.warn(msg);
        endUserLogger.warn(msg);
      }
    }

    // Connect to the database
    try
    {
      getDatabaseConnection();
    }
    catch (SQLException e)
    {
      throw new RuntimeException("Unable to connect to the database: " + e);
    }

    // Logger thread will be created in checkRecoveryLogTables()
    // after database has been initialized
  }

  //
  // Database manipulation and access
  //

  /**
   * used by vdb.xml parser (element Recoverylog) to enable and set optional
   * autoclosing mechanism. Default value is 0 (no auto-close).
   */
  public void setAutoCloseTimeout(int seconds)
  {
    autocloseTimeout = seconds;
  }

  /**
   * Enable optional connection validity check
   */
  public void setCheckConnectionValidity()
  {
    checkConnectionValidity = true;
  }

  /**
   * Gets a managed connection for writing events to the recovery log.
   */
  public RecoveryLogConnectionManager getRecoveryLogConnectionManager()
  {
    return new RecoveryLogConnectionManager(url, login, password, driverName,
        driverClassName, logTableName, autocloseTimeout);
  }

  /**
   * Gets a connection to the database.
   * 
   * @return a connection to the database
   * @exception SQLException if an error occurs.
   * @see #invalidateInternalConnection()
   */
  private synchronized Connection getDatabaseConnection() throws SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Request for database connection");
    try
    {
      if (internalConnectionManagedByGetDatabaseConnection == null)
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("recovery.jdbc.connect", new String[]{url,
              login}));
        internalConnectionManagedByGetDatabaseConnection = DriverManager
            .getConnection(url, login, password, driverName, driverClassName);
      }
      else
      {
        if (checkConnectionValidity)
          checkDatabaseConnection();
      }

      if (isDirty)
      {
        logger.warn("Recovery log is dirty. Clearing. Please use restore log.");

        /*
         * Note: we must set isDirty to false prior to calling
         * resetRecoveryLog() because resetRecoveryLog() calls into
         * getDatabaseConnection() and thus recurses just here...
         */
        isDirty = false;
        try
        {
          resetRecoveryLog(false);
        }
        catch (Exception e)
        {
          isDirty = true;
        }
      }

      if (autocloseTimeout > 0)
        scheduleCloserTask();

      return internalConnectionManagedByGetDatabaseConnection;
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("recovery.jdbc.connect.failed", e);
      if (logger.isDebugEnabled())
        logger.debug(msg, e);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      invalidateInternalConnection();
      String msg = Translate.get("recovery.jdbc.connect.failed", e);
      if (logger.isDebugEnabled())
        logger.debug(msg, e);
      throw new SQLException(msg);
    }
  }

  /**
   * Checks that the connection to the recoverylog database is still valid and
   * if it is not attempts one reconnect.
   * 
   * @throws SQLException if an error happens at re-connection
   */
  private void checkDatabaseConnection() throws SQLException
  {
    try
    {
      internalConnectionManagedByGetDatabaseConnection
          .getTransactionIsolation();
    }
    catch (SQLException e)
    {
      internalConnectionManagedByGetDatabaseConnection = DriverManager
          .getConnection(url, login, password, driverName, driverClassName);
    }
  }

  /**
   * Executes an update on the internal database connection with appropriate
   * synchronization.
   */
  private void execDatabaseUpdate(String update) throws SQLException
  {
    Statement stmt = null;
    try
    {
      stmt = getDatabaseConnection().createStatement();
      stmt.execute(update);
    }
    finally
    {
      if (stmt != null)
      {
        try
        {
          stmt.close();
        }
        catch (SQLException e)
        {
        }
      }
    }
  }

  private void scheduleCloserTask()
  {
    timer.cancel();
    timer = new Timer();
    timer.schedule(new TimerTask()
    {
      public void run()
      {
        invalidateInternalConnection();
      }
    }, autocloseTimeout * 1000);
  }

  /**
   * Increments the value of logTableId.
   */
  synchronized long incrementLogTableId()
  {
    logTableId++;
    return logTableId;
  }

  /**
   * Checks if the tables (log and checkpoint) already exist, and create them if
   * needed.
   */
  private void intializeDatabase() throws SQLException
  {
    boolean createLogTable = true;
    boolean createCheckpointTable = true;
    boolean createBackendTable = true;
    boolean createDumpTable = true;
    Connection connection;
    // Check if tables exist
    try
    {
      connection = getDatabaseConnection();

      if (connection == null)
        throw new SQLException(Translate.get("recovery.jdbc.connect.failed",
            "null connection returned by DriverManager"));

      connection.setAutoCommit(false);
      // Get DatabaseMetaData
      DatabaseMetaData metaData = connection.getMetaData();

      // Get a description of tables matching the catalog, schema, table name
      // and type.
      // Sending in null for catalog and schema drop them from the selection
      // criteria. Replace the last argument in the getTables method with
      // types below to obtain only database tables. (Sending in null
      // retrieves all types).
      String[] types = {"TABLE", "VIEW"};
      ResultSet rs = metaData.getTables(null, null, "%", types);

      // Get tables metadata
      String tableName;
      while (rs.next())
      {
        // 1 is table catalog, 2 is table schema, 3 is table name, 4 is type
        tableName = rs.getString(3);
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("recovery.jdbc.table.found", tableName));
        if (tableName.equalsIgnoreCase(logTableName))
        {
          if (tableName.compareTo(logTableName) != 0)
            logger.warn(Translate.get("recovery.jdbc.logtable.case.mismatch",
                new String[]{logTableName, tableName}));
          createLogTable = false;
          // initialize logTableId
          PreparedStatement p = null;
          try
          {
            ResultSet result = null;
            p = connection
                .prepareStatement("SELECT MAX(log_id) AS max_log_id FROM "
                    + logTableName);
            result = p.executeQuery();
            if (result.next())
              logTableId = result.getLong(1);
            else
              logTableId = 0;
            p.close();
          }
          catch (SQLException e)
          {
            try
            {
              if (p != null)
                p.close();
            }
            catch (Exception ignore)
            {
            }
            throw new RuntimeException(Translate.get(
                "recovery.jdbc.logtable.getvalue.failed", e));
          }

        }
        if (tableName.equalsIgnoreCase(checkpointTableName))
        {
          if (tableName.compareTo(checkpointTableName) != 0)
            logger.warn(Translate.get(
                "recovery.jdbc.checkpointtable.case.mismatch", new String[]{
                    checkpointTableName, tableName}));
          createCheckpointTable = false;
          if (logger.isDebugEnabled())
          {
            // Dump checkpoints.
            StringBuffer sb = new StringBuffer();
            sb.append("Checkpoint list...");

            Map checkpoints = this.getCheckpoints();
            Iterator checkPointIterator = checkpoints.keySet().iterator();
            while (checkPointIterator.hasNext())
            {
              String name = (String) checkPointIterator.next();
              String logId = (String) checkpoints.get(name);
              sb.append("\n");
              sb.append("name=[").append(name).append("] log_id=[").append(
                  logId).append("]");
            }
            logger.debug(sb.toString());
          }
        }
        else if (tableName.equalsIgnoreCase(backendTableName))
        {
          if (tableName.compareTo(backendTableName) != 0)
            logger.warn(Translate.get(
                "recovery.jdbc.backendtable.case.mismatch", new String[]{
                    backendTableName, tableName}));
          createBackendTable = false;
        }
        else if (tableName.equalsIgnoreCase(dumpTableName))
        {
          if (tableName.compareTo(dumpTableName) != 0)
            logger.warn(Translate.get("recovery.jdbc.dumptable.case.mismatch",
                new String[]{backendTableName, tableName}));
          createDumpTable = false;
        }
      }
      try
      {
        connection.commit();
        connection.setAutoCommit(true);
      }
      catch (Exception ignore)
      {
        // Read-only transaction we don't care
      }
    }
    catch (SQLException e)
    {
      logger.error(Translate.get("recovery.jdbc.table.no.description"), e);
      throw e;
    }

    // Create the missing tables
    Statement stmt = null;
    if (createLogTable)
    {
      if (logger.isInfoEnabled())
        logger.info(Translate
            .get("recovery.jdbc.logtable.create", logTableName));
      try
      {
        stmt = connection.createStatement();
        stmt.executeUpdate(logTableCreateStatement);
        stmt.close();
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get(
            "recovery.jdbc.logtable.create.failed", new String[]{logTableName,
                e.getMessage()}));
      }
    }
    if (createCheckpointTable)
    {
      if (logger.isInfoEnabled())
        logger.info(Translate.get("recovery.jdbc.checkpointtable.create",
            checkpointTableName));
      try
      {
        stmt = connection.createStatement();
        stmt.executeUpdate(checkpointTableCreateStatement);
        stmt.close();
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get(
            "recovery.jdbc.checkpointtable.create.failed", new String[]{
                checkpointTableName, e.getMessage()}));
      }

      // Add an initial checkpoint in the table
      setInitialEmptyRecoveryLogCheckpoint();
    }
    if (createBackendTable)
    {
      if (logger.isInfoEnabled())
        logger.info(Translate.get("recovery.jdbc.backendtable.create",
            backendTableName));
      try
      {
        stmt = connection.createStatement();
        stmt.executeUpdate(backendTableCreateStatement);
        stmt.close();
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get(
            "recovery.jdbc.backendtable.create.failed", new String[]{
                backendTableName, e.getMessage()}));
      }
    }
    if (createDumpTable)
    {
      if (logger.isInfoEnabled())
        logger.info(Translate.get("recovery.jdbc.dumptable.create",
            dumpTableName));
      try
      {
        stmt = connection.createStatement();
        stmt.executeUpdate(dumpTableCreateStatement);
        stmt.close();
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get(
            "recovery.jdbc.dumptable.create.failed", new String[]{
                dumpTableName, e.getMessage()}));
      }
    }
    if (stmt != null) // created some tables: consider we did an init
      setLastManDown();
  }

  /**
   * Adds a checkpoint called InitialEmptyRecoveryLog in the checkpoint table.
   * The checkpoint points to the current logTableId.
   * 
   * @throws SQLException if an error occurs
   */
  private void setInitialEmptyRecoveryLogCheckpoint() throws SQLException
  {
    String checkpointName = "Initial_empty_recovery_log";
    PreparedStatement pstmt = null;
    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Storing checkpoint " + checkpointName + " at request id "
            + logTableId);
      pstmt = getDatabaseConnection().prepareStatement(
          "INSERT INTO " + checkpointTableName + " VALUES(?,?)");
      pstmt.setString(1, checkpointName);
      pstmt.setLong(2, logTableId);
      pstmt.executeUpdate();
      pstmt.close();
    }
    catch (SQLException e)
    {
      try
      {
        if (pstmt != null)
          pstmt.close();
      }
      catch (Exception ignore)
      {
      }
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.store.failed", new String[]{checkpointName,
              e.getMessage()}));
    }
  }

  /**
   * Return the recovery log size in number of rows
   * 
   * @return recovery log size
   * @throws SQLException if an error occurs accessing the recovery log
   */
  public int getRecoveryLogSize() throws SQLException
  {
    Statement s = null;
    ResultSet rs = null;
    try
    {
      s = getDatabaseConnection().createStatement();
      rs = s.executeQuery("select count(*) from " + getLogTableName());

      if (!rs.next())
      {
        logger.error("Failed to get count of log entries.");
        return -1;
      }
      return rs.getInt(1);
    }
    finally
    {
      if (s != null)
        try
        {
          s.close();
        }
        catch (Exception ignore)
        {
        }
      if (rs != null)
        try
        {
          rs.close();
        }
        catch (Exception ignore)
        {
        }
    }
  }

  /**
   * Check the recovery log consistency for the first controller starting in a
   * cluster (might come back from a complete cluster outage).
   * 
   * @throws SQLException if a recovery log access error occurs
   */
  public void checkRecoveryLogConsistency() throws SQLException
  {
    logger.info(Translate.get("recovery.consistency.checking"));

    PreparedStatement stmt = null;
    ResultSet rs = null;
    PreparedStatement updateStmt = null;
    try
    {
      // Look for requests with the execution status still to EXECUTING.
      String columnsToSelect;
      if (logger.isWarnEnabled()) // Get SQL for warning message
        columnsToSelect = "SELECT log_id," + getLogTableSqlColumnName()
            + " FROM ";
      else
        columnsToSelect = "SELECT log_id FROM ";
      stmt = getDatabaseConnection().prepareStatement(
          columnsToSelect + getLogTableName() + " WHERE exec_status LIKE ?");
      stmt.setString(1, LogEntry.EXECUTING);
      rs = stmt.executeQuery();

      if (rs.next())
      { // Change entry status to UNKNOWN
        updateStmt = getDatabaseConnection()
            .prepareStatement(
                "UPDATE " + getLogTableName()
                    + " SET exec_status=? WHERE log_id=?");
        updateStmt.setString(1, LogEntry.UNKNOWN);
        do
        {
          long logId = rs.getLong(1);
          if (logger.isWarnEnabled())
            logger
                .warn("Log entry "
                    + logId
                    + " ("
                    + rs.getString(getLogTableSqlColumnName())
                    + ") still has an executing status in the recovery log. Switching to unknown state.");
          updateStmt.setLong(2, logId);
          updateStmt.executeUpdate();
        }
        while (rs.next());
        updateStmt.close();
      }
      rs.close();
      stmt.close();

      // Look for open transactions
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT transaction_id FROM " + getLogTableName() + " r1 WHERE r1."
              + getLogTableSqlColumnName()
              + " LIKE ? AND NOT EXISTS (SELECT transaction_id FROM "
              + getLogTableName()
              + " r2 WHERE r1.transaction_id=r2.transaction_id AND (r1."
              + getLogTableSqlColumnName() + " LIKE ? OR r2."
              + getLogTableSqlColumnName() + " LIKE ?))");

      stmt.setString(1, BEGIN + "%");
      stmt.setString(2, COMMIT);
      stmt.setString(3, ROLLBACK);

      rs = stmt.executeQuery();
      while (rs.next())
      {
        // Add a rollback in the log
        long tid = rs.getLong(1);
        if (logger.isWarnEnabled())
          logger.warn("Transaction " + tid
              + " has not completed. Inserting a rollback in the recovery log");
        long logId = logRollback(new TransactionMetaData(tid, 0, UNKNOWN_USER,
            false, 0));
        logRequestCompletion(logId, true, 0);
      }
      rs.close();
    }
    catch (SQLException e)
    {
      logger.error("Failed to check recovery log consistency", e);
      throw new SQLException(Translate.get(
          "recovery.consistency.checking.failed", e.getMessage()));
    }
    finally
    {
      try
      {
        if (rs != null)
          rs.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (updateStmt != null)
          updateStmt.close();
      }
      catch (Exception ignore)
      {
      }
    }

  }

  /**
   * Reset the current log table id and delete the recovery log information
   * older than the given checkpoint. This method also deletes all entries in
   * the checkpoint table. This method is asynchronous: the delete is performed
   * via a post to the logger thread.
   * 
   * @param checkpointName the checkpoint name to delete from.
   * @param newCheckpointId the new checkpoint identifier
   * @throws SQLException if an error occurs
   */
  public void resetLogTableIdAndDeleteRecoveryLog(String checkpointName,
      long newCheckpointId) throws SQLException
  {
    long oldId = getCheckpointLogId(checkpointName);
    synchronized (this)
    {
      // resetLog cleans the recovery log table, resets the checkpointTable and
      // renumber the queries since oldId (checkpoint assigned to the transfer).
      loggerThread
          .log(new ResetLogEvent(oldId, newCheckpointId, checkpointName));
      logTableId = newCheckpointId + logTableId - oldId;
    }
  }

  /**
   * Reset the recovery log by deleting the log table and checkpoint table. This
   * method also clears checkpoint names in the dump table. This method should
   * only be used to re-initialize the virtual database at initial startup or
   * after a complete cluster outage.
   * 
   * @param resetLogId True if the logId has to be reset. This is used only with
   *          "initialize <backend> force" command.
   * @throws SQLException if a database access error occurs
   */
  public void resetRecoveryLog(boolean resetLogId) throws SQLException
  {
    PreparedStatement pstmt = null;
    try
    {
      // Clean the tables

      if (logger.isDebugEnabled())
        logger.debug("Deleting " + logTableName + " table.");
      pstmt = getDatabaseConnection().prepareStatement(
          "DELETE FROM " + logTableName);
      pstmt.executeUpdate();
      pstmt.close();

      if (resetLogId)
        // Ensure logTableId points to the right place for an empty log!
        logTableId = 0;

      if (logger.isDebugEnabled())
        logger.debug("Deleting " + checkpointTableName + " table.");
      pstmt = getDatabaseConnection().prepareStatement(
          "DELETE FROM " + checkpointTableName);
      pstmt.executeUpdate();
      pstmt.close();

      if (logger.isDebugEnabled())
        logger.debug("Resetting checkpoint associated to dumps in "
            + dumpTableName + " table.");
      pstmt = getDatabaseConnection().prepareStatement(
          "UPDATE " + dumpTableName + " SET checkpoint_name=?");
      pstmt.setString(1, "");
      pstmt.executeUpdate();
      pstmt.close();
    }
    catch (SQLException e)
    {
      String msg = "Error while resetting recovery log";
      logger.error(msg, e);
      try
      {
        if (pstmt != null)
          pstmt.close();
      }
      catch (Exception ignore)
      {
      }
      throw new SQLException(msg + " (" + e + ")");
    }

    // Put back an initial empty recovery log in the checkpoint table
    setInitialEmptyRecoveryLogCheckpoint();
  }

  /**
   * Invalidate the connection and associated prepared statements when an error
   * occurs so that the next call to getDatabaseConnection() re-allocates a new
   * connection.
   * 
   * @see #getDatabaseConnection()
   */
  protected synchronized void invalidateInternalConnection()
  {
    internalConnectionManagedByGetDatabaseConnection = null;
  }

  //
  //
  // Logging related methods
  //
  //

  /**
   * Log a transaction abort. This is used only for transaction that were
   * started but where no request was executed, which is in fact an empty
   * transaction. The underlying implementation might safely discard the
   * corresponding begin from the log as an optimization.
   * 
   * @param tm The transaction marker metadata
   * @return the identifier of the entry in the recovery log
   */
  public long logAbort(TransactionMetaData tm)
  {
    // We have to perform exactly the same job as a rollback
    return logRollback(tm);
  }

  /**
   * Log the beginning of a new transaction (always as a SUCCESS not EXECUTING).
   * 
   * @param tm The transaction marker metadata
   * @return the identifier of the entry in the recovery log
   */
  public long logBegin(TransactionMetaData tm)
  {
    // Store the begin in the database
    long id = incrementLogTableId();
    LogEntry logEntry;
    if (tm.isPersistentConnection())
    { // Append the persistent connection id to the SQL
      logEntry = new LogEntry(id, tm.getLogin(), BEGIN + " "
          + tm.getPersistentConnectionId(), null, LogEntry.TRANSACTION, tm
          .getTransactionId());
    }
    else
    {
      logEntry = new LogEntry(id, tm.getLogin(), BEGIN, null,
          LogEntry.TRANSACTION, tm.getTransactionId());
    }
    logEntry.setExecutionStatus(LogEntry.SUCCESS);
    loggerThread.log(new LogRequestEvent(logEntry));
    return id;
  }

  /**
   * Log the closing of the given persistent connection (always as a SUCCESS).
   * 
   * @param login the login executing the request
   * @param persistentConnectionId the id of the persistent connection to close
   * @return the identifier of the entry in the recovery log
   */
  public long logClosePersistentConnection(String login,
      long persistentConnectionId)
  {
    long id = incrementLogTableId();
    LogEntry logEntry = new LogEntry(id, login, CLOSE_PERSISTENT_CONNECTION,
        null, LogEntry.PERSISTENT_CONNECTION, persistentConnectionId);
    logEntry.setExecutionStatus(LogEntry.SUCCESS);
    loggerThread.log(new LogRequestEvent(logEntry));
    return id;
  }

  /**
   * Log a transaction commit.
   * 
   * @param tm The transaction marker metadata
   * @return the identifier of the entry in the recovery log
   */
  public long logCommit(TransactionMetaData tm)
  {
    if (tm.isReadOnly())
    {
      // don't log commit, if trx is read only
      return 0;
    }
    else
    {
      long id = incrementLogTableId();
      loggerThread.log(new LogCommitEvent(new LogEntry(id, tm.getLogin(),
          COMMIT, null, LogEntry.TRANSACTION, tm.getTransactionId())));
      tm.setLogId(id);
      return id;
    }
  }

  /**
   * Log a log entry in the recovery log.
   * 
   * @param logEntry the log entry to to be written in the recovery log.
   */
  public void logLogEntry(LogEntry logEntry)
  {
    loggerThread.log(new LogRequestEvent(logEntry));
  }

  /**
   * Log the opening of the given persistent connection (always as a SUCCESS).
   * 
   * @param login the login executing the request
   * @param persistentConnectionId the id of the persistent connection to open
   * @return the identifier of the entry in the recovery log
   */
  public long logOpenPersistentConnection(String login,
      long persistentConnectionId)
  {
    long id = incrementLogTableId();
    LogEntry logEntry = new LogEntry(id, login, OPEN_PERSISTENT_CONNECTION,
        null, LogEntry.PERSISTENT_CONNECTION, persistentConnectionId);
    logEntry.setExecutionStatus(LogEntry.EXECUTING);
    loggerThread.log(new LogRequestEvent(logEntry));
    return id;
  }

  /**
   * Log a transaction savepoint removal.
   * 
   * @param tm The transaction marker metadata
   * @param name The name of the savepoint to log
   * @return the identifier of the entry in the recovery log
   */
  public long logReleaseSavepoint(TransactionMetaData tm, String name)
  {
    long id = incrementLogTableId();
    loggerThread.log(new LogRequestEvent(new LogEntry(id, tm.getLogin(),
        "release " + name, null, LogEntry.TRANSACTION, tm.getTransactionId())));
    return id;
  }

  private LogEntry buildLogEntry(AbstractRequest request, long id,
      long execTime, int updateCount)
  {
    String autoConnTrans;
    long tid;
    if (request.isAutoCommit())
    {
      if (request.isPersistentConnection())
      {
        autoConnTrans = LogEntry.PERSISTENT_CONNECTION;
        tid = request.getPersistentConnectionId();
      }
      else
      {
        autoConnTrans = LogEntry.AUTOCOMMIT;
        tid = 0;
      }
    }
    else
    {
      autoConnTrans = LogEntry.TRANSACTION;
      tid = request.getTransactionId();
    }
    LogEntry logEntry = new LogEntry(id, request.getLogin(), request
        .getSqlOrTemplate(), request.getPreparedStatementParameters(),
        autoConnTrans, tid, request.getEscapeProcessing(), request.getId(),
        execTime, updateCount);
    return logEntry;
  }

  /**
   * Log a request (read or write) that is going to execute and set the logId on
   * the request object.
   * 
   * @param request The request to log (read or write)
   * @return the identifier of the entry in the recovery log
   */
  public long logRequestExecuting(AbstractRequest request)
  {
    long id = incrementLogTableId();
    long execTime = 0;
    if (request.getStartTime() != 0)
    {
      if (request.getEndTime() != 0)
        execTime = request.getEndTime() - request.getStartTime();
      else
        execTime = System.currentTimeMillis() - request.getStartTime();
    }
    loggerThread.log(new LogRequestEvent(
        buildLogEntry(request, id, execTime, 0)));
    request.setLogId(id);
    return id;
  }

  /**
   * Update the completion status of a request completion in the recovery log
   * given its id.
   * 
   * @param logId recovery log id for this request as it was originally logged
   * @param success true if the request execution was successful
   * @param execTime request execution time in ms
   */
  public void logRequestCompletion(long logId, boolean success, long execTime)
  {
    // if logID is 0, the log event was filtered out
    if (logId == 0)
      return;

    logRequestExecuteUpdateCompletion(logId, success, 0, execTime);
  }

  /**
   * Update the completion status of a request completion in the recovery log
   * given its id.
   * 
   * @param logId recovery log id for this request as it was originally logged
   * @param success true if the request execution was successful
   * @param execTimeInMs request execution time in ms
   * @param updateCount request update count when successful
   */
  public void logRequestCompletion(long logId, boolean success,
      long execTimeInMs, int updateCount)
  {
    // if logID is 0, the log event was filtered out
    if (logId == 0)
      return;

    logRequestExecuteUpdateCompletion(logId, success, updateCount, execTimeInMs);

  }

  /**
   * Update the completion status of a request executed with executeUpdate().
   * 
   * @param logId recovery log id for this request as it was originally logged
   * @param success true if the request execution was successful
   * @param updateCount the number of updated rows returned by executeUpdate()
   *          (meaningful only if success)
   * @param execTime request execution time in ms
   */
  public void logRequestExecuteUpdateCompletion(long logId, boolean success,
      int updateCount, long execTime)
  {
    // if logID is 0, the log event was filtered out
    // (.e.g. commit for read only trx)
    if (logId == 0)
      return;

    loggerThread.log(new LogRequestCompletionEvent(logId, success, updateCount,
        execTime));
  }

  /**
   * Log a transaction rollback.
   * 
   * @param tm The transaction marker metadata
   * @return the identifier of the entry in the recovery log
   */
  public long logRollback(TransactionMetaData tm)
  {
    long id = incrementLogTableId();
    // Some backends started a recovery process, log the rollback
    loggerThread.log(new LogRollbackEvent(new LogEntry(id, tm.getLogin(),
        ROLLBACK, null, LogEntry.TRANSACTION, tm.getTransactionId())));
    tm.setLogId(id);
    return id;
  }

  /**
   * Log a transaction rollback to a savepoint
   * 
   * @param tm The transaxtion marker metadata
   * @param savepointName The name of the savepoint
   * @return the identifier of the entry in the recovery log
   */
  public long logRollbackToSavepoint(TransactionMetaData tm,
      String savepointName)
  {
    long id = incrementLogTableId();
    loggerThread.log(new LogRequestEvent(new LogEntry(id, tm.getLogin(),
        "rollback " + savepointName, null, LogEntry.TRANSACTION, tm
            .getTransactionId())));
    tm.setLogId(id);
    return id;
  }

  /**
   * Log a transaction savepoint.
   * 
   * @param tm The transaction marker metadata
   * @param name The name of the savepoint to log
   * @return the identifier of the entry in the recovery log
   */
  public long logSetSavepoint(TransactionMetaData tm, String name)
  {
    long id = incrementLogTableId();
    loggerThread
        .log(new LogRequestEvent(new LogEntry(id, tm.getLogin(), "savepoint "
            + name, null, LogEntry.TRANSACTION, tm.getTransactionId())));
    tm.setLogId(id);
    return id;
  }

  /**
   * Throw an SQLException if the recovery log is shutting down
   * 
   * @throws SQLException if recovery log is shutting down
   */
  private synchronized void checkIfShuttingDown() throws SQLException
  {
    if (isShuttingDown)
      throw new SQLException(
          "Recovery log is shutting down, log access has been denied");
  }

  /**
   * Shutdown the recovery log and all its threads by enqueueing a log shutdown
   * event and waiting until it is processed.
   */
  public void shutdown()
  {
    synchronized (this)
    {
      if (isShuttingDown)
        return;
      isShuttingDown = true;
    }

    this.timer.cancel();

    if (loggerThread != null)
    {
      ShutdownLogEvent event = new ShutdownLogEvent();
      synchronized (event)
      {
        loggerThread.log(event);
        try
        {
          event.wait();
        }
        catch (InterruptedException e)
        {
          logger.warn("Thread interrupted while awaiting log shutdown", e);
        }
      }
      try
      {
        loggerThread.join();
      }
      catch (InterruptedException e)
      {
        logger.warn("Thread interrupted while awaiting loggerThread shutdown",
            e);
      }
    }

    if (url.startsWith("jdbc:hsqldb:file:"))
    {
      try
      {
        execDatabaseUpdate("SHUTDOWN");
      }
      catch (SQLException e)
      {
        logger.warn("Error while shuting down hsqldb", e);
      }
    }
  }

  //
  // Recovery process
  //

  /**
   * Notify the recovery log that a recovery process has started.
   */
  public synchronized void beginRecovery()
  {
    recoveringNb++;
  }

  /**
   * Possibly clean the recovery log after all recovery process are done. This
   * removes all failed statements or transactions without side effect from the
   * recovery log.
   * 
   * @exception SQLException if an error occurs.
   */
  protected void cleanRecoveryLog() throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;

    ResultSet rs = null;
    PreparedStatement pstmt = null;
    try
    {
      // Get the list of failed requests
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT log_id," + logTableSqlColumnName + " FROM " + logTableName
              + " WHERE exec_status LIKE ?");
      stmt.setString(1, LogEntry.FAILED);
      rs = stmt.executeQuery();
      if (rs.next())
      { // Check entries to see if statement can be removed
        pstmt = getDatabaseConnection().prepareStatement(
            "DELETE FROM " + logTableName + " WHERE log_id=?");
        do
        {
          String sql = rs.getString(2);
          AbstractRequest decodedRequest = requestFactory.requestFromString(
              sql, false, false, timeout, "\n");
          // For now delete all requests that are not stored procedures

          // TODO: Add a flag on requests to tell whether they have
          // unrollback-able side effects (SEQUOIA-587)
          if (decodedRequest instanceof StoredProcedure)
          {
            pstmt.setLong(1, rs.getLong(1));
            pstmt.executeUpdate();
          }
        }
        while (rs.next());
        pstmt.close();
      }
      rs.close();
      stmt.close();
    }
    catch (SQLException e)
    {
      invalidateInternalConnection();
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (pstmt != null)
          pstmt.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (rs != null)
          rs.close();
      }
      catch (Exception ignore)
      {
      }
      throw new SQLException("Unable get cleanup recovery log (" + e + ")");
    }
  }

  /**
   * Notify the recovery log that a recovery process has finished. If this is
   * the last recovery process to finish, the cleanRecoveryLog method is called
   * 
   * @see #cleanRecoveryLog()
   */
  public synchronized void endRecovery()
  {
    recoveringNb--;
    if (recoveringNb == 0)
    {
      try
      {
        cleanRecoveryLog();
      }
      catch (SQLException e)
      {
        logger.error(Translate.get("recovery.cleaning.failed"), e);
      }
    }
  }

  /**
   * Retrieve recovery information on a backend. This includes, the last known
   * state of the backend, and the last known checkpoint
   * 
   * @param databaseName the virtual database name
   * @param backendName the backend name
   * @return <code>BackendRecoveryInfo<code> instance or <code>null</code> if the backend does not exist
   * @throws SQLException if a database access error occurs
   */
  public BackendRecoveryInfo getBackendRecoveryInfo(String databaseName,
      String backendName) throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;
    String checkpoint = null;
    int backendState = BackendState.UNKNOWN;
    try
    {
      // 1. Get the reference point to delete
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT * FROM " + backendTableName
              + " WHERE backend_name LIKE ? AND database_name LIKE ?");
      stmt.setString(1, backendName);
      stmt.setString(2, databaseName);
      ResultSet rs = stmt.executeQuery();

      if (rs.next())
      {
        checkpoint = rs.getString("checkpoint_name");
        backendState = rs.getInt("backend_state");
      }
      rs.close();
    }
    catch (SQLException e)
    {
      invalidateInternalConnection();
      logger.info(
          "An error occured while retrieving backend recovery information", e);
      throw e;
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
    return new BackendRecoveryInfo(backendName, checkpoint, backendState,
        databaseName);
  }

  /**
   * Return the current log id.
   * 
   * @return the current log id.
   */
  protected long getCurrentLogId()
  {
    return logTableId;
  }

  /**
   * Get the id of the last request logged in the recovery log.
   * 
   * @param controllerId the controller ID which determines the search space for
   *          request IDs
   * @return the last request id.
   * @throws SQLException if an error occured while retrieving the id.
   */
  public long getLastRequestId(long controllerId) throws SQLException
  {
    String request = "select max(request_id) from " + logTableName
        + " where (request_id > ?) and (request_id <= ?)";
    return getLastId(request, controllerId, null);
  }

  /**
   * Get the id of the last transaction logged in the recovery log.
   * 
   * @param controllerId the controller ID which determines the search space for
   *          transaction IDs
   * @return the last transaction id.
   * @throws SQLException if an error occured while retrieving the id.
   */
  public long getLastTransactionId(long controllerId) throws SQLException
  {
    String request = "select max(transaction_id) from " + logTableName
        + " where (transaction_id > ?) and (transaction_id <= ?)";
    return getLastId(request, controllerId, null);
  }

  /**
   * Get the id of the last transaction logged in the recovery log.
   * 
   * @param controllerId the controller ID which determines the search space for
   *          transaction IDs
   * @return the last transaction id.
   * @throws SQLException if an error occured while retrieving the id.
   */
  public long getLastConnectionId(long controllerId) throws SQLException
  {
    String request = "select max(transaction_id) from " + logTableName
        + " where (transaction_id > ?) and (transaction_id <= ?) " + " and "
        + getLogTableSqlColumnName() + " like ?";
    return getLastId(request, controllerId, OPEN_PERSISTENT_CONNECTION);
  }

  /**
   * Return the last ID found in the log which was allocated for (by) a given
   * controller ID if any, or controller ID if no previous id was used in this
   * space.<br>
   * Helper method used to get last transaction/request/connection IDs.
   * 
   * @param controllerId the controller ID which determines the search space for
   *          already used IDs.
   * @param sql optional SQL to match (null if useless)
   * @return the last ID found in the log which was allocated for controller ID
   *         if any, or controller ID if no previous ID was used in this space.
   * @throws SQLException if an error occured while inspecting the log.
   */
  private long getLastId(String request, long controllerId, String sql)
      throws SQLException
  {
    checkIfShuttingDown();

    FindLastIdEvent event = new FindLastIdEvent(request, controllerId, sql);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while retrieving last ID for controller ID"
                + controllerId + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.getValue();
  }

  /**
   * Returns the recoveringNb value.
   * 
   * @return Returns the recoveringNb.
   */
  public long getRecoveringNb()
  {
    return recoveringNb;
  }

  /**
   * Number of queries that can be accumulated into a batch when recovering
   * 
   * @return the recovery batch size
   */
  public int getRecoveryBatchSize()
  {
    return recoveryBatchSize;
  }

  /**
   * Returns true if the recovery log has logged a Begin for the given
   * transaction id.
   * 
   * @param tid the transaction identifier
   * @return true if begin has been logged for this transaction
   * @throws SQLException if the query fails on the recovery database
   */
  public boolean hasLoggedBeginForTransaction(Long tid) throws SQLException
  {
    // Check for something in the queue first
    if (loggerThread.hasLogEntryForTransaction(tid.longValue()))
      return true;

    // Check in the database
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    try
    {
      pstmt = getDatabaseConnection().prepareStatement(
          "select transaction_id from " + logTableName
              + " where transaction_id=?");
      // If something was logged for this transaction then begin was logged
      // first so if the ResultSet is not empty the begin has been logged.
      pstmt.setLong(1, tid.longValue());
      rs = pstmt.executeQuery();
      return rs.next();
    }
    catch (SQLException e)
    {
      invalidateInternalConnection();
      throw e;
    }
    finally
    {
      try
      {
        if (rs != null)
          rs.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (pstmt != null)
          pstmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Returns <code>true</code> if at least one backend has started a recover
   * process.
   * 
   * @return <code>boolean</code>
   */
  public synchronized boolean isRecovering()
  {
    return recoveringNb > 0;
  }

  /**
   * Get the next log entry from the recovery log given the id of the previous
   * log entry.
   * 
   * @param previousLogEntryId previous log entry identifier
   * @return the next log entry from the recovery log or null if no further
   *         entry can be found
   * @throws SQLException if an error occurs while accesing the recovery log
   */
  public LogEntry getNextLogEntry(long previousLogEntryId) throws SQLException
  {
    checkIfShuttingDown();

    ResultSet rs = null;
    boolean emptyResult;
    PreparedStatement stmt = null;
    try
    {
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT * FROM " + logTableName + " WHERE log_id=?");
      // Note that the statement is closed in the finally block
      do
      {
        previousLogEntryId++;
        stmt.setLong(1, previousLogEntryId);
        // Close ResultSet of previous loop step to free resources.
        if (rs != null)
          rs.close();
        rs = stmt.executeQuery();
        emptyResult = !rs.next();
      }
      while (emptyResult && (previousLogEntryId <= logTableId));

      // No more request after this one
      if (emptyResult)
        return null;

      // Read columns in order to prevent issues with MS SQL Server as reported
      // Charles Cordingley.
      long id = rs.getLong(COLUMN_INDEX_LOG_ID);
      String user = rs.getString(COLUMN_INDEX_VLOGIN);
      String sql = rs.getString(COLUMN_INDEX_SQL);
      String sqlParams = rs.getString(COLUMN_INDEX_SQL_PARAMS);
      String autoConnTran = rs.getString(COLUMN_INDEX_AUTO_CONN_TRAN);
      long transactionId = rs.getLong(COLUMN_INDEX_TRANSACTION_ID);
      long requestId = rs.getLong(COLUMN_INDEX_REQUEST_ID);
      long execTime = rs.getLong(COLUMN_INDEX_EXEC_TIME);
      int updateCount = rs.getInt(COLUMN_INDEX_UPDATE_COUNT);
      String status = rs.getString(COLUMN_INDEX_EXEC_STATUS);
      // Note that booleanProcessing = true is the default value in
      // AbstractRequest
      return new LogEntry(id, user, sql, sqlParams, autoConnTran,
          transactionId, false, requestId, execTime, updateCount, status);
    }
    catch (SQLException e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get("recovery.jdbc.recover.failed", e));
    }
    finally
    {
      try
      {
        if (rs != null)
          rs.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Return the real number of log entries between 2 log ids (usually matching
   * checkpoint indices). The SELECT includes both boundaries. The value is
   * retrieved through an event posted in the logger thread queue (makes sure
   * that all previous entries have been flushed to the log).
   * 
   * @param lowerLogId the lower log id
   * @param upperLogId the upper log id
   * @return the number of entries between the 2 ids
   * @throws SQLException if an error occurs querying the recovery log
   */
  public long getNumberOfLogEntries(long lowerLogId, long upperLogId)
      throws SQLException
  {
    checkIfShuttingDown();

    GetNumberOfLogEntriesEvent event = new GetNumberOfLogEntriesEvent(
        lowerLogId, upperLogId);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for number of log entries in ["
                + lowerLogId + "," + upperLogId + "]");
      }
    }
    return event.getNbOfLogEntries();
  }

  /**
   * Returns the number of log entries currently in the log table. The method is
   * not accurate since this number can change very quickly but it is correct
   * enough for management purposes.
   * 
   * @return the number of log entries currently in the log table
   * @throws SQLException if an error occurs while counting the number of log
   *           entries in the log table
   */
  long getNumberOfLogEntries() throws SQLException
  {
    Statement stmt;
    stmt = getDatabaseConnection().createStatement();
    ResultSet rs = null;
    try
    {
      rs = stmt.executeQuery("select count(*) from " + logTableName);
      rs.next();
      return rs.getLong(1);
    }
    finally
    {
      if (rs != null)
      {
        rs.close();
      }
      if (stmt != null)
      {
        stmt.close();
      }
    }
  }

  /**
   * Get the update count result for the execution of the query that has the
   * provided unique id.
   * 
   * @param requestId request result to look for
   * @return update count or -1 if not found
   * @throws SQLException if an error occured
   */
  public int getUpdateCountResultForQuery(long requestId) throws SQLException
  {
    checkIfShuttingDown();

    GetUpdateCountEvent event = new GetUpdateCountEvent(this, requestId);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for update count of request ID "
                + requestId + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() instanceof SQLException)
      throw (SQLException) event.getCatchedException();

    // Result might not have been found (NoResultAvailableException thrown) in
    // which case we just return the default update count (-1) that will cause
    // the driver to re-execute the query. If result was found,
    // event.getUpdateCount() contains the right value.
    return event.getUpdateCount();
  }

  /**
   * Tries to find a commit for a given transaction ID.
   * 
   * @param transactionId the transaction id to look for
   * @return true if commit was found in recovery log, false otherwise
   * @throws SQLException if there was a problem while searching the recovery
   *           log
   */
  public boolean findCommitForTransaction(long transactionId)
      throws SQLException
  {
    checkIfShuttingDown();

    FindCommitEvent event = new FindCommitEvent(this, transactionId);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for commit of transaction ID "
                + transactionId + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.wasFound();
  }

  /**
   * Tries to find a commit for a given transaction ID.
   * 
   * @param transactionId the transaction id to look for
   * @return commit status if found in recovery log, "" otherwise
   * @throws SQLException if there was a problem while searching the recovery
   *           log
   */
  public String getCommitStatusForTransaction(long transactionId)
      throws SQLException
  {
    checkIfShuttingDown();

    FindCommitEvent event = new FindCommitEvent(this, transactionId);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for commit of transaction ID "
                + transactionId + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.getStatus();
  }

  /**
   * Tries to find a rollback for a given transaction ID.
   * 
   * @param transactionId the transaction id to look for
   * @return true if rollback was found in recovery log, false otherwise
   * @throws SQLException if there was a problem while searching the recovery
   *           log
   */
  public boolean findRollbackForTransaction(long transactionId)
      throws SQLException
  {
    checkIfShuttingDown();

    FindRollbackEvent event = new FindRollbackEvent(this, transactionId);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for rollback of transaction ID "
                + transactionId + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.wasFound();
  }

  /**
   * Tries to find a rollback for a given transaction ID and returns its status
   * if found.
   * 
   * @param transactionId the transaction id to look for
   * @return commit status if found in recovery log, "" otherwise
   * @throws SQLException if there was a problem while searching the recovery
   *           log
   */
  public String getRollbackStatusForTransaction(long transactionId)
      throws SQLException
  {
    checkIfShuttingDown();

    FindRollbackEvent event = new FindRollbackEvent(this, transactionId);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for commit of transaction ID "
                + transactionId + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.getStatus();
  }

  /**
   * Tries to find a close for a given persistent connection ID.
   * 
   * @param persistentConnectionId the persistent connection ID to look for
   * @return true if close was found in recovery log, false otherwise
   * @throws SQLException if there was a problem while searching the recovery
   *           log
   */
  public boolean findCloseForPersistentConnection(long persistentConnectionId)
      throws SQLException
  {
    checkIfShuttingDown();

    FindClosePersistentConnectionEvent event = new FindClosePersistentConnectionEvent(
        this, persistentConnectionId);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for close of persistent connection ID "
                + persistentConnectionId + " (" + event.getCatchedException()
                + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.wasFound();
  }

  /**
   * Get the next request (begin/commit/rollback or WriteRequest) from the
   * recovery log given the id of the previously recovered request.
   * <p>
   * The id of the request before the first one to recover is given by
   * getCheckpointRequestId.
   * 
   * @param previousRequestId id of the previously recovered request
   * @param scheduler Scheduler that will be used to generate fake TransactionId
   *          when recovering requests in autocommit mode
   * @return AbstractTask task corresponding to the next request to recover or
   *         null if no such request exists
   * @exception SQLException if an error occurs
   * @see #getCheckpointLogId(String)
   */
  public RecoveryTask recoverNextRequest(long previousRequestId,
      AbstractScheduler scheduler) throws SQLException
  {
    RecoveryTask task = null;

    // Get the request with the id after previousRequestId.
    LogEntry logEntry = getNextLogEntry(previousRequestId);
    if (logEntry == null)
      return null;

    // Construct the request object according to its type
    long transactionId = logEntry.getTid();
    long id = logEntry.getLogId();
    String user = logEntry.getLogin();
    String sql = logEntry.getQuery().trim();
    String status = logEntry.getExecutionStatus();

    boolean escapeProcessing = true;

    if (CLOSE_PERSISTENT_CONNECTION.equals(sql))
    {
      if (logger.isDebugEnabled())
        logger.debug("closing persistent connection: " + transactionId);
      task = new RecoveryTask(transactionId, id,
          new ClosePersistentConnectionTask(1, 1, user, transactionId), status);
    }
    else if (OPEN_PERSISTENT_CONNECTION.equals(sql))
    {
      if (logger.isDebugEnabled())
        logger.debug("opening persistent connection: " + transactionId);
      task = new RecoveryTask(transactionId, id,
          new OpenPersistentConnectionTask(1, 1, user, transactionId), status);
    }
    else if (BEGIN.equals(sql))
    {
      // DO NOT SWITCH THIS BLOCK WITH THE NEXT if BLOCK
      // begin on a non-persistent connection
      task = new RecoveryTask(transactionId, id, new BeginTask(1, 1,
          new TransactionMetaData(transactionId, (long) timeout * 1000, user,
              false, 0)), status);
      if (logger.isDebugEnabled())
        logger.debug("begin transaction: " + transactionId);
    }
    else if (sql.startsWith(BEGIN))
    {
      // DO NOT SWITCH THIS BLOCK WITH THE PREVIOUS if BLOCK
      // begin on a persistent connection, extract the persistent connection id
      long persistentConnectionId = Long.parseLong(sql
          .substring(BEGIN.length()).trim());
      task = new RecoveryTask(transactionId, id, new BeginTask(1, 1,
          new TransactionMetaData(transactionId, (long) timeout * 1000, user,
              true, persistentConnectionId)), status);
      if (logger.isDebugEnabled())
        logger.debug("begin transaction: " + transactionId
            + " on persistent connection " + persistentConnectionId);
    }
    else if (COMMIT.equals(sql))
    { // commit (we do not care about the persistent connection id here)
      task = new RecoveryTask(transactionId, id, new CommitTask(1, 1,
          new TransactionMetaData(transactionId, (long) timeout * 1000, user,
              false, 0)), status);
      if (logger.isDebugEnabled())
        logger.debug("commit transaction: " + transactionId);
    }
    else if (ROLLBACK.equals(sql))
    { // rollback (we do not care about the persistent connection id here)
      int index = sql.indexOf(' ');
      if (index == -1)
      {
        task = new RecoveryTask(transactionId, id, new RollbackTask(1, 1,
            new TransactionMetaData(transactionId, (long) timeout * 1000, user,
                false, 0)), status);
        if (logger.isDebugEnabled())
          logger.debug("rollback transaction: " + transactionId);
      }
      else
      { // Rollback to savepoint
        String savepointName = sql.substring(index).trim();
        task = new RecoveryTask(transactionId, id, new RollbackToSavepointTask(
            1, 1, new TransactionMetaData(transactionId, (long) timeout * 1000,
                user, false, 0), savepointName), status);
        if (logger.isDebugEnabled())
          logger.debug("rollback transaction to savepoint: " + transactionId);
      }
    }
    else if (sql.startsWith("savepoint "))
    { // set savepoint (we do not care about the persistent connection id here)
      String savepointName = sql.substring(sql.indexOf(' ')).trim();
      task = new RecoveryTask(transactionId, id, new SavepointTask(1, 1,
          new TransactionMetaData(transactionId, (long) timeout * 1000, user,
              false, 0), savepointName), status);
      if (logger.isDebugEnabled())
        logger.debug("transaction set savepoint: " + transactionId);
    }
    else if (sql.startsWith("release "))
    { // release savepoint (we do not care about the persistent connection id
      // here)
      String savepointName = sql.substring(sql.indexOf(' '));
      task = new RecoveryTask(transactionId, id, new ReleaseSavepointTask(1, 1,
          new TransactionMetaData(transactionId, (long) timeout * 1000, user,
              false, 0), savepointName), status);
      if (logger.isDebugEnabled())
        logger.debug("transaction release savepoint: " + transactionId);
    }
    else
    {
      // Use regular expressions to determine object to rebuild
      AbstractRequest decodedRequest = requestFactory.requestFromString(sql,
          false, escapeProcessing, timeout, "\n");
      if (decodedRequest != null)
      {
        setRequestParameters(logEntry, decodedRequest, scheduler);
        if (logger.isDebugEnabled())
          logger.debug("recovering " + decodedRequest.getType()
              + " - request id : " + decodedRequest.getId()
              + " / transaction id : " + decodedRequest.getTransactionId());

        if (decodedRequest instanceof AbstractWriteRequest)
        {
          task = new RecoveryTask(transactionId, id,
              new StatementExecuteUpdateTask(1, 1,
                  (AbstractWriteRequest) decodedRequest), status);
        }
        else if (decodedRequest instanceof StoredProcedure)
        {
          task = new RecoveryTask(transactionId, id,
              new CallableStatementExecuteTask(1, 1,
                  (StoredProcedure) decodedRequest, null), status);
        }
        else
        {
          if (decodedRequest instanceof UnknownWriteRequest)
          {
            task = new RecoveryTask(transactionId, id,
                new StatementExecuteTask(1, 1,
                    (AbstractWriteRequest) decodedRequest, null), status);
          }
          else
          // read request
          {
            if (decodedRequest instanceof SelectRequest)
            {
              // Set a fake cursor name in all cases (addresses SEQUOIA-396)
              decodedRequest.setCursorName("replay_cursor");
            }
            /*
             * For unknown read requests, we do not care about "select for
             * update" pattern here since no broadcast will occur anyway
             */
            task = new RecoveryTask(transactionId, id,
                new StatementExecuteQueryTask(1, 1,
                    (SelectRequest) decodedRequest, null), status);
          }
        }
      }
    }

    // At this point the task should never be null or recovery may terminate
    // without warning. Verify that this safety condition is satisfied before
    // returning
    if (task == null)
    {
      String msg = ("Unable to generate task from recovery log record: log_id="
          + id + " transaction_id=" + transactionId + " sql=" + sql);
      logger.error(msg);
      logger.error("Backend being recovered is suspect!");
      throw new SQLException(msg);
    }
    else
      return task;
  }

  private void setRequestParameters(LogEntry entry,
      AbstractRequest decodedRequest, AbstractScheduler scheduler)
  {
    decodedRequest.setLogin(entry.getLogin());
    decodedRequest.setSqlOrTemplate(entry.getQuery());
    decodedRequest.setPreparedStatementParameters(entry.getQueryParams());
    decodedRequest.setId(entry.getRequestId());
    if (LogEntry.TRANSACTION.equals(entry.getAutoConnTrans()))
    {
      decodedRequest.setIsAutoCommit(false);
      decodedRequest.setTransactionId(entry.getTid());
      decodedRequest
          .setTransactionIsolation(org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL);
    }
    else
    {
      decodedRequest.setIsAutoCommit(true);
      decodedRequest.setTransactionId(scheduler.getNextTransactionId());
      if (LogEntry.PERSISTENT_CONNECTION.equals(entry.getAutoConnTrans()))
      {
        decodedRequest.setPersistentConnection(true);
        decodedRequest.setPersistentConnectionId(entry.getTid());
      }
    }
  }

  //
  //
  // Checkpoint Management
  //
  //

  /**
   * Deletes recovery log entries that are older than specified checkpoint. The
   * log entry pointed to by 'checkpointName' is kept in the recovery log to
   * make sure that logId numbering is not reset in case of full clearance.
   * (upon vdb restart, member logTableId is initialized with max(log_id) found
   * in the database, see code in intializeDatabase()).
   * 
   * @param checkpointName the name of the checkpoint uptil which log entries
   *          should be removed
   * @throws SQLException in case of error.
   */
  public void deleteLogEntriesBeforeCheckpoint(String checkpointName)
      throws SQLException
  {
    checkIfShuttingDown();

    long id = getCheckpointLogId(checkpointName);
    loggerThread.log(new DeleteLogEntriesAndCheckpointBetweenEvent(-1, id));
  }

  /**
   * Returns a time-ordered odered (most recent first) array of names of all the
   * checkpoint available in the recovery log. <strong>this method may not
   * return the exact list of checkpoint names (since it is called concurrently
   * to events posted to the recovery log queue).</strong>
   * 
   * @return a time-ordered odered (most recent first) <code>ArrayList</code>
   *         of <code>String</code> checkpoint names
   * @throws SQLException if fails
   * @deprecated use getCheckpoint().values() instead
   */
  public ArrayList getCheckpointNames() throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;

    try
    {
      // Ordering is not quite newest first if two checkpoints happen to have
      // the same log_id. We sort by name to make improper ordering less
      // likely. TODO: Add timestamp or reformat checkpoint names so that they
      // sort by date.
      if (logger.isDebugEnabled())
        logger.debug("Retrieving checkpoint names list");
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT name from " + checkpointTableName
              + " ORDER BY log_id DESC, name DESC");
      ResultSet rs = stmt.executeQuery();
      ArrayList list = new ArrayList();
      while (rs.next())
      {
        list.add(rs.getString(1));
      }
      rs.close();
      return list;
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.list.failed", e));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Returns a <code>Map&lt;String, String&gt;</code> of checkpoints where the
   * keys are the checkpoint names and the values are the corresponding log IDs.
   * The Map is orderered by log IDs (newest first).
   * 
   * @return a <code>Map&lt;String, String&gt;</code> of checkpoints (key=
   *         checkpoint name, value = log id)
   * @throws SQLException if an error occurs while retrieving the checkpoints
   *           information from the checkpoint table
   */
  Map/* <String, String> */getCheckpoints() throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;

    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Retrieving checkpoint names list");
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT log_id, name from " + checkpointTableName
              + " ORDER BY log_id DESC");
      ResultSet rs = stmt.executeQuery();
      Map checkpoints = new TreeMap();
      while (rs.next())
      {
        String name = rs.getString("name");
        String id = rs.getString("log_id");
        checkpoints.put(name, id);
      }
      rs.close();
      return checkpoints;
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.list.failed", e));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Returns a list of checkpoint names (strings) that all represent the same
   * point in time (same log id) as specified 'checkpointName'. The list is
   * never null and includes the specified 'checkpointName'.
   * 
   * @param checkpointName the checkpoint name
   * @return a list of checkpoint names (strings) that all represent the same
   *         point in time (same log id)
   * @throws SQLException if no such checkpoint exists of if there is an error
   */
  public String[] getCheckpointNameAliases(String checkpointName)
      throws SQLException
  {
    long logId = getCheckpointLogId(checkpointName);
    Object ret = executeQuery("SELECT name from " + checkpointTableName
        + " where log_id=" + logId + " ORDER BY log_id DESC",
        new QueryHandler()
        {
          public Object run(ResultSet rs) throws SQLException
          {
            ArrayList aliases = new ArrayList();
            while (rs.next())
              aliases.add(rs.getString("name"));
            return aliases.toArray(new String[aliases.size()]);
          }
        });
    return (String[]) ret;
  }

  abstract class QueryHandler
  {
    /**
     * To be defined by query handler. This is supposed to contain the
     * processing of the result set that is returned by the query execution.
     * 
     * @param rs the ResultSet returned by the query execution
     * @return an object containing the result of the processing of the handler.
     * @throws SQLException
     */
    abstract public Object run(ResultSet rs) throws SQLException;
  };

  Object executeQuery(String query, QueryHandler handler) throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;
    try
    {
      stmt = getDatabaseConnection().prepareStatement(query);
      ResultSet rs = stmt.executeQuery();
      return handler.run(rs);
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get("recovery.jdbc.query.failed", e));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close(); // this also closes the resultSet
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Get the log id corresponding to a given checkpoint. This is the first step
   * in a recovery process. Following steps consist in calling
   * recoverNextRequest.
   * 
   * @param checkpointName Name of the checkpoint
   * @return long the log identifier corresponding to this checkpoint.
   * @exception SQLException if an error occurs or the checkpoint does not exist
   * @see #recoverNextRequest(long)
   */
  public long getCheckpointLogId(String checkpointName) throws SQLException
  {
    checkIfShuttingDown();

    GetCheckpointLogIdEvent event = new GetCheckpointLogIdEvent(
        getCheckpointTableName(), checkpointName);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for id of checkpoint " + checkpointName
                + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.getCheckpointLogId();
  }

  /**
   * Get the log entry corresponding to a given checkpoint. This is used to set
   * a checkpoint globally cluster-wide using the unique log entry.
   * 
   * @param checkpointName Name of the checkpoint
   * @return a CheckpointLogEntry corresponding to this checkpoint.
   * @exception SQLException if an error occurs or the checkpoint does not exist
   * @see #recoverNextRequest(long)
   */
  public CheckpointLogEntry getCheckpointLogEntry(String checkpointName)
      throws SQLException
  {
    checkIfShuttingDown();

    GetCheckpointLogEntryEvent event = new GetCheckpointLogEntryEvent(this,
        checkpointName);
    synchronized (event)
    {
      loggerThread.log(event);
      try
      {
        event.wait();
      }
      catch (InterruptedException e)
      {
        throw new SQLException(
            "Interrupted while waiting for id of checkpoint " + checkpointName
                + " (" + event.getCatchedException() + ")");
      }
    }
    if (event.getCatchedException() != null)
      throw event.getCatchedException();
    return event.getCheckpointLogEntry();
  }

  /**
   * Shift the log entries from the given checkpoint id by the shift factor
   * provided. All entries with an id below nowCheckpointId and all the others
   * will have their id increased by shift.
   * 
   * @param fromId id where to start the move
   * @param shift increment to add to log entries id
   */
  public void moveEntries(long fromId, long shift)
  {
    synchronized (this)
    {
      // Move current entries in the database log
      loggerThread.log(new ShiftLogEntriesEvent(fromId, shift));
      // Move current id forward so that next entries do not have to be
      // relocated
      logTableId = logTableId + shift;
    }
  }

  /**
   * Delete the log entries from the fromId id to toId.
   * 
   * @param fromId id where to start the move
   * @param toId increment to add to log entries id
   */
  public void deleteLogEntriesAndCheckpointBetween(long fromId, long toId)
  {
    synchronized (this)
    {
      // Delete entries in the database log
      loggerThread.log(new DeleteLogEntriesAndCheckpointBetweenEvent(fromId,
          toId));
    }
  }

  /**
   * Remove a checkpoint from the recovery. This is useful for recovery
   * maintenance
   * 
   * @param checkpointName to remove
   */
  public void removeCheckpoint(String checkpointName)
  {
    RemoveCheckpointEvent removeCheckpointEvent = new RemoveCheckpointEvent(
        checkpointName);
    synchronized (removeCheckpointEvent)
    {
      loggerThread.log(removeCheckpointEvent);
      try
      {
        removeCheckpointEvent.wait();
      }
      catch (InterruptedException ignore)
      {
      }
    }
  }

  /**
   * Store the state of the backend in the recovery log
   * 
   * @param databaseName the virtual database name
   * @param backendRecoveryInfo the backend recovery information to store
   * @throws SQLException if cannot proceed
   */
  public void storeBackendRecoveryInfo(String databaseName,
      BackendRecoveryInfo backendRecoveryInfo) throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;
    PreparedStatement stmt2 = null;
    if ((backendRecoveryInfo.getCheckpoint() == null)
        || (backendRecoveryInfo.getCheckpoint().length() == 0)
        || (backendRecoveryInfo.getBackendState() != BackendState.DISABLED))
      backendRecoveryInfo.setCheckpoint(""); // No checkpoint
    else
    { // Check checkpoint name validity
      getCheckpointLogId(backendRecoveryInfo.getCheckpoint());
    }

    try
    {
      // 1. Get the reference point to delete
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT * FROM " + backendTableName
              + " WHERE backend_name LIKE ? and database_name LIKE ?");
      stmt.setString(1, backendRecoveryInfo.getBackendName());
      stmt.setString(2, databaseName);
      ResultSet rs = stmt.executeQuery();
      boolean mustUpdate = rs.next();
      rs.close();
      if (!mustUpdate)
      {
        stmt2 = getDatabaseConnection().prepareStatement(
            "INSERT INTO " + backendTableName + " values(?,?,?,?)");
        stmt2.setString(1, databaseName);
        stmt2.setString(2, backendRecoveryInfo.getBackendName());
        stmt2.setInt(3, backendRecoveryInfo.getBackendState());
        stmt2.setString(4, backendRecoveryInfo.getCheckpoint());
        if (stmt2.executeUpdate() != 1)
          throw new SQLException(
              "Error while inserting new backend reference. Incorrect number of rows");
      }
      else
      {
        stmt2 = getDatabaseConnection()
            .prepareStatement(
                "UPDATE "
                    + backendTableName
                    + " set backend_state=?,checkpoint_name=? where backend_name=? and database_name=?");
        stmt2.setInt(1, backendRecoveryInfo.getBackendState());
        stmt2.setString(2, backendRecoveryInfo.getCheckpoint());
        stmt2.setString(3, backendRecoveryInfo.getBackendName());
        stmt2.setString(4, databaseName);
        if (stmt2.executeUpdate() != 1)
          throw new SQLException(
              "Error while updating backend reference. Incorrect number of rows");
      }
    }
    catch (SQLException e)
    {
      invalidateInternalConnection();

      logger.warn("Failed to store backend recovery info", e);

      throw new SQLException("Unable to update checkpoint '"
          + backendRecoveryInfo.getCheckpoint() + "' for backend:"
          + backendRecoveryInfo.getBackendName());
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (stmt2 != null)
          stmt2.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Store a Checkpoint using the current log state.
   * 
   * @param checkpointName Name of the checkpoint
   * @exception SQLException if an error occurs
   */
  public void storeCheckpoint(String checkpointName) throws SQLException
  {
    // Check if a checkpoint with the name checkpointName already exists
    if (!validCheckpointName(checkpointName))
    {
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.duplicate", checkpointName));
    }

    loggerThread.log(new StoreCheckpointWithLogIdEvent(checkpointName,
        logTableId));
  }

  /**
   * Store a Checkpoint with the given log id.
   * 
   * @param checkpointName Name of the checkpoint
   * @param logId log id this checkpoint points to
   * @exception SQLException if an error occurs
   */
  public void storeCheckpoint(String checkpointName, long logId)
      throws SQLException
  {
    // Check if a checkpoint with the name checkpointName already exists
    if (!validCheckpointName(checkpointName))
    {
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.duplicate", checkpointName));
    }

    loggerThread.log(new StoreCheckpointWithLogIdEvent(checkpointName, logId));
  }

  //
  // 
  // Dump management
  //
  //

  /**
   * Get the DumpInfo element corresponding to the given dump name. Returns null
   * if no information is found for this dump name.
   * 
   * @param dumpName the name of the dump to look for
   * @return a <code>DumpInfo</code> object or null if not found in the table
   * @throws SQLException if a recovery log database access error occurs
   */
  public DumpInfo getDumpInfo(String dumpName) throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;

    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Retrieving dump " + dumpName + " information");
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT * from " + dumpTableName + " WHERE dump_name LIKE ?");
      stmt.setString(1, dumpName);

      ResultSet rs = stmt.executeQuery();
      DumpInfo dumpInfo = null;
      if (rs.next())
      {
        dumpInfo = new DumpInfo(rs.getString("dump_name"), rs
            .getTimestamp("dump_date"), rs.getString("dump_path"), rs
            .getString("dump_format"), rs.getString("checkpoint_name"), rs
            .getString("backend_name"), rs.getString(dumpTableTablesColumnName));
      }
      // else not found, return dumpInfo=null;

      rs.close();
      return dumpInfo;
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get("recovery.jdbc.dump.info.failed",
          new String[]{dumpName, e.getMessage()}));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Retrieve the list of available dumps.
   * 
   * @return an <code>ArrayList</code> of <code>DumpInfo</code> objects
   * @throws SQLException if a recovery log database access error occurs
   */
  public ArrayList getDumpList() throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;

    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Retrieving dump list");
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT * FROM " + dumpTableName + " ORDER BY dump_date DESC");
      ResultSet rs = stmt.executeQuery();
      ArrayList list = new ArrayList();
      while (rs.next())
      {
        list
            .add(new DumpInfo(rs.getString("dump_name"), rs
                .getTimestamp("dump_date"), rs.getString("dump_path"), rs
                .getString("dump_format"), rs.getString("checkpoint_name"), rs
                .getString("backend_name"), rs
                .getString(dumpTableTablesColumnName)));
      }
      rs.close();
      return list;
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get("recovery.jdbc.dump.list.failed", e));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Remove a dump information from the dump table base.
   * 
   * @param dumpInfo the <code>DumpInfo</code> to remove
   * @throws SQLException if the dump has has not been removed from the dump
   *           table
   */
  public void removeDump(DumpInfo dumpInfo) throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;

    try
    {
      if (logger.isDebugEnabled())
      {
        logger.debug("removing dump " + dumpInfo.getDumpName());
      }
      stmt = getDatabaseConnection().prepareStatement(
          "DELETE FROM " + dumpTableName + " WHERE dump_name=?");
      stmt.setString(1, dumpInfo.getDumpName());

      stmt.executeUpdate();
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get("recovery.jdbc.dump.remove.failed",
          new String[]{dumpInfo.getDumpName(), e.getMessage()}));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Set DumpInfo, thereby making a new dump available for restore.
   * 
   * @param dumpInfo the dump info to create.
   * @throws VirtualDatabaseException if an error occurs
   */
  public void setDumpInfo(DumpInfo dumpInfo) throws VirtualDatabaseException
  {
    try
    {
      storeDump(dumpInfo);
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(e);
    }
  }

  /**
   * Store the given dump information in the dump table
   * 
   * @param dump the <code>DumpInfo</code> to store
   * @throws SQLException if a recovery log database access error occurs
   */
  public void storeDump(DumpInfo dump) throws SQLException
  {
    checkIfShuttingDown();

    PreparedStatement stmt = null;

    if (dump == null)
      throw new NullPointerException(
          "Invalid null dump in JDBCRecoverylog.storeDump");

    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Storing dump " + dump.getDumpName());
      stmt = getDatabaseConnection().prepareStatement(
          "INSERT INTO " + dumpTableName + " VALUES (?,?,?,?,?,?,?)");
      stmt.setString(1, dump.getDumpName());
      stmt.setTimestamp(2, new Timestamp(dump.getDumpDate().getTime()));
      stmt.setString(3, dump.getDumpPath());
      stmt.setString(4, dump.getDumpFormat());
      stmt.setString(5, dump.getCheckpointName());
      stmt.setString(6, dump.getBackendName());
      stmt.setString(7, dump.getTables());

      stmt.executeUpdate();
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get("recovery.jdbc.dump.store.failed",
          new String[]{dump.getDumpName(), e.getMessage()}));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * @see StoreCheckpointWithLogIdEvent
   * @param dumpCheckpointName name of the checkpoint to store
   * @param checkpointId id of the checkpoint
   */
  public void storeDumpCheckpointName(String dumpCheckpointName,
      long checkpointId)
  {
    loggerThread.log(new StoreCheckpointWithLogIdEvent(dumpCheckpointName,
        checkpointId));
  }

  /**
   * Updates specified DumpTable entry column with specified value.
   * 
   * @param dumpName the name of the dump record to update
   * @param columnName the name of the column to update
   * @param value the value to set
   * @throws SQLException if any error occurs (invalid dump name, sql query
   *           syntax errors, hsqldb access errors, ...)
   */
  void updateDumpTableColumn(String dumpName, String columnName, String value)
      throws SQLException
  {
    checkIfShuttingDown();

    DumpInfo dumpInfo = getDumpInfo(dumpName);
    if (dumpInfo == null)
      throw new SQLException("No such dump name: " + dumpName);

    PreparedStatement stmt = null;
    int updateCount = 0;
    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Updating '" + columnName + "' for dump '"
            + dumpInfo.getDumpName() + "' to " + value);
      stmt = getDatabaseConnection().prepareStatement(
          "UPDATE " + dumpTableName + " SET " + columnName
              + "=? WHERE dump_name=?");
      stmt.setString(1, value);
      stmt.setString(2, dumpName);
      updateCount = stmt.executeUpdate();
    }
    catch (Exception e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get(
          "recovery.jdbc.dump.update.column.failed", new String[]{dumpName,
              e.getMessage()}));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }

    if (updateCount != 1)
    {
      String msg = "Invalid update count after dumpTable update.";
      logger.error(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * Update the path name for a given checkpoint.
   * 
   * @param dumpName the dump name
   * @param newPath the new path to set
   * @throws SQLException if a recovery log database access error occurs
   */
  public void updateDumpPath(String dumpName, String newPath)
      throws SQLException
  {
    updateDumpTableColumn(dumpName, "dump_path", newPath);
  }

  /**
   * Update the checkpoint name for specified dump, making it available for
   * restore operations.
   * 
   * @param dumpName the dump name
   * @param checkpointName the new chekpoint to set
   * @throws SQLException if a recovery log database access error occurs
   */
  public void updateDumpCheckpoint(String dumpName, String checkpointName)
      throws SQLException
  {
    updateDumpTableColumn(dumpName, "checkpoint_name", checkpointName);
  }

  //
  // 
  // Recovery log database tables management
  //
  //

  /**
   * Checks if the recovery log and checkpoint tables exist, and create them if
   * they do not exist. This method also starts the logger thread.
   */
  public void checkRecoveryLogTables()
  {
    try
    {
      intializeDatabase();
    }
    catch (SQLException e)
    {
      throw new RuntimeException("Unable to initialize the database: " + e);
    }

    // Start the logger thread
    loggerThread = new LoggerThread(this);
    loggerThread.start();
  }

  /**
   * Returns the backendTableName value.
   * 
   * @return Returns the backendTableName.
   */
  public String getBackendTableName()
  {
    return backendTableName;
  }

  /**
   * Returns the checkpointTableName value.
   * 
   * @return Returns the checkpointTableName.
   */
  public String getCheckpointTableName()
  {
    return checkpointTableName;
  }

  /**
   * Returns the logTableName value.
   * 
   * @return Returns the logTableName.
   */
  public String getLogTableName()
  {
    return logTableName;
  }

  /**
   * Returns the logTableSqlColumnName value.
   * 
   * @return Returns the logTableSqlColumnName.
   */
  public String getLogTableSqlColumnName()
  {
    return logTableSqlColumnName;
  }

  /**
   * Sets the backend table create statement
   * 
   * @param createTable statement to create the table
   * @param tableName the backend table name
   * @param checkpointNameType type for the checkpointName column
   * @param backendNameType type for the backendName column
   * @param backendStateType type for the backendState column
   * @param databaseNameType type for the databaseName column
   * @param extraStatement like primary keys
   */
  public void setBackendTableCreateStatement(String createTable,
      String tableName, String checkpointNameType, String backendNameType,
      String backendStateType, String databaseNameType, String extraStatement)
  {
    this.backendTableCreateTable = createTable;
    this.backendTableName = tableName;
    this.backendTableDatabaseName = databaseNameType;
    this.backendTableBackendName = backendNameType;
    this.backendTableBackendState = backendStateType;
    this.backendTableCheckpointName = checkpointNameType;
    this.backendTableExtraStatement = extraStatement;
    this.backendTableCreateStatement = createTable + " " + backendTableName
        + " (database_name " + databaseNameType + ", backend_name "
        + backendNameType + ",backend_state " + backendStateType
        + ", checkpoint_name " + checkpointNameType + " " + extraStatement
        + ")";

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("recovery.jdbc.backendtable.statement",
          backendTableCreateStatement));
  }

  /**
   * Sets the checkpoint table name and create statement.
   * 
   * @param createTable statement to create the table
   * @param tableName name of the checkpoint table.
   * @param nameType type for the name column
   * @param logIdType type for the log_id column
   * @param extraStatement like primary keys
   */
  public void setCheckpointTableCreateStatement(String createTable,
      String tableName, String nameType, String logIdType, String extraStatement)
  {
    this.checkpointTableCreateTable = createTable;
    this.checkpointTableName = tableName;
    this.checkpointTableNameType = nameType;
    this.checkpointTableLogIdType = logIdType;
    this.checkpointTableExtraStatement = extraStatement;
    // CREATE TABLE tableName (
    // name checkpointNameColumnType,
    // log_id logIdColumnType,
    // extraStatement)

    checkpointTableCreateStatement = createTable + " " + tableName + " (name "
        + nameType + ",log_id " + logIdType + extraStatement + ")";
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("recovery.jdbc.checkpointtable.statement",
          checkpointTableCreateStatement));
  }

  /**
   * Sets the dump table name and create statement.
   * 
   * @param createTable statement to create the table
   * @param tableName name of the checkpoint table.
   * @param dumpNameColumnType the dump name column type
   * @param dumpDateColumnType the dump data column type
   * @param dumpPathColumnType the dump path column type
   * @param dumpFormatColumnType the dump tpe column type
   * @param checkpointNameColumnType the checkpoint name column type
   * @param backendNameColumnType the backend name column type
   * @param tablesColumnName the database tables column name
   * @param tablesColumnType the database tables column type
   * @param extraStatement like primary keys
   */
  public void setDumpTableCreateStatement(String createTable, String tableName,
      String dumpNameColumnType, String dumpDateColumnType,
      String dumpPathColumnType, String dumpFormatColumnType,
      String checkpointNameColumnType, String backendNameColumnType,
      String tablesColumnName, String tablesColumnType, String extraStatement)
  {
    this.dumpTableCreateTable = createTable;
    this.dumpTableName = tableName;
    this.dumpTableDumpNameColumnType = dumpNameColumnType;
    this.dumpTableDumpDateColumnType = dumpDateColumnType;
    this.dumpTableDumpPathColumnType = dumpPathColumnType;
    this.dumpTableDumpFormatColumnType = dumpFormatColumnType;
    this.dumpTableCheckpointNameColumnType = checkpointNameColumnType;
    this.dumpTableBackendNameColumnType = backendNameColumnType;
    this.dumpTableTablesColumnName = tablesColumnName;
    this.dumpTableTablesColumnType = tablesColumnType;
    this.dumpTableExtraStatementDefinition = extraStatement;

    // CREATE TABLE DumpTable (
    // dump_name TEXT NOT NULL,
    // dump_date DATE,
    // dump_path TEXT NOT NULL,
    // dump_type TEXT NOT NULL,
    // checkpoint_name TEXT NOT NULL,
    // backend_name TEXT NOT NULL,
    // tables TEXT NOT NULL
    // )

    dumpTableCreateStatement = dumpTableCreateTable + " " + dumpTableName
        + " (dump_name " + dumpTableDumpNameColumnType + ",dump_date "
        + dumpDateColumnType + ",dump_path " + dumpPathColumnType
        + ",dump_format " + dumpFormatColumnType + ",checkpoint_name "
        + checkpointNameColumnType + ",backend_name " + backendNameColumnType
        + "," + dumpTableTablesColumnName + " " + tablesColumnType
        + extraStatement + ")";
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("recovery.jdbc.dumptable.statement",
          dumpTableCreateStatement));
  }

  /**
   * Sets the log table name and create statement.
   * 
   * @param createTable statement to create the table
   * @param tableName name of the log table
   * @param idType type of the id column
   * @param vloginType type of the login column
   * @param sqlName name of the sql statement column
   * @param sqlType type of the sql column
   * @param sqlParamsType type of the sql_param column
   * @param autoConnTranColumnType type of the auto_conn_tran column
   * @param transactionIdType type of the transaction column
   * @param extraStatement extra statement like primary keys ...
   * @param requestIdType request id column type
   * @param execTimeType execution time in ms column type
   * @param updateCountType update count column type
   */
  public void setLogTableCreateStatement(String createTable, String tableName,
      String idType, String vloginType, String sqlName, String sqlType,
      String sqlParamsType, String autoConnTranColumnType,
      String transactionIdType, String requestIdType, String execTimeType,
      String updateCountType, String extraStatement)
  {
    this.logTableCreateTable = createTable;
    this.logTableName = tableName;
    this.logTableLogIdType = idType;
    this.logTableVloginType = vloginType;
    this.logTableSqlColumnName = sqlName;
    this.logTableSqlType = sqlType;
    this.logTableAutoConnTranColumnType = autoConnTranColumnType;
    this.logTableTransactionIdType = transactionIdType;
    this.logTableRequestIdType = requestIdType;
    this.logTableExecTimeType = execTimeType;
    this.logTableUpdateCountType = updateCountType;
    this.logTableExtraStatement = extraStatement;
    logTableCreateStatement = createTable + " " + tableName + " (log_id "
        + idType + ",vlogin " + vloginType + "," + logTableSqlColumnName + " "
        + sqlType + "," + logTableSqlColumnName + "_param " + sqlParamsType
        + ",auto_conn_tran " + autoConnTranColumnType + ",transaction_id "
        + transactionIdType + ",request_id " + requestIdType + ",exec_status "
        + autoConnTranColumnType + ",exec_time " + execTimeType
        + ",update_count " + updateCountType + extraStatement + ")";
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("recovery.jdbc.logtable.statement",
          logTableCreateStatement));
  }

  //
  //
  // Log utility functions
  //
  //

  /**
   * Checks if a checkpoint with the name checkpointName is already stored in
   * the database.
   * 
   * @param checkpointName name of the checkpoint.
   * @return true if no checkpoint was found.
   */
  private boolean validCheckpointName(String checkpointName)
      throws SQLException
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = getDatabaseConnection().prepareStatement(
          "SELECT * FROM " + checkpointTableName + " WHERE name LIKE ?");
      stmt.setString(1, checkpointName);
      rs = stmt.executeQuery();

      // If the query returned any rows, the checkpoint name is already
      // in use and therefore invalid.
      boolean checkpointExists = rs.next();
      rs.close();
      return !checkpointExists;
    }
    catch (SQLException e)
    {
      invalidateInternalConnection();
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.check.failed", e));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  //
  //
  // Info/Monitoring/Debug related functions
  //
  //

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "jdbcrecoverylog";
  }

  /**
   * Allow to get the content of the recovery log for viewing
   * 
   * @return <code>String[][]</code>
   * @see org.continuent.sequoia.controller.monitoring.datacollector.DataCollector#retrieveRecoveryLogData(String)
   */
  public String[][] getData()
  {
    Statement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = getDatabaseConnection().createStatement();
      rs = stmt.executeQuery("select * from " + logTableName);
      ArrayList list = new ArrayList();
      while (rs.next())
      {
        // 3: Query 2: User 1: ID 4: TID
        list.add(new String[]{rs.getString(3), rs.getString(2),
            rs.getString(1), rs.getString(4)});
      }
      String[][] result = new String[list.size()][4];
      for (int i = 0; i < list.size(); i++)
        result[i] = (String[]) list.get(i);
      return result;
    }
    catch (SQLException e)
    {
      return null;
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (SQLException ignore)
      {
      }
      try
      {
        stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  String[] getColumnNames()
  {
    Statement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = getDatabaseConnection().createStatement();
      rs = stmt.executeQuery("select * from " + logTableName
          + " where log_id=0");
      int columnCount = rs.getMetaData().getColumnCount();
      String[] columnNames = new String[columnCount];
      for (int i = 0; i < columnCount; i++)
      {
        columnNames[i] = rs.getMetaData().getColumnName(i + 1);
      }
      return columnNames;
    }
    catch (SQLException e)
    {
      return new String[0];
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (SQLException ignore)
      {
      }
      try
      {
        stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * returns an long[2] for min and max log_id in the log table
   * 
   * @return an long[2] for min and max log_id in the log table
   * @throws SQLException if an error occurs while computing the index of the
   *           log table
   */
  long[] getIndexes() throws SQLException
  {
    Statement stmt;
    stmt = getDatabaseConnection().createStatement();
    ResultSet rs = null;
    try
    {
      rs = stmt.executeQuery("select min(log_id),max(log_id) from "
          + logTableName);
      rs.next();
      long min = rs.getLong(1);
      long max = rs.getLong(2);
      return new long[]{min, max};
    }
    finally
    {
      if (rs != null)
      {
        rs.close();
      }
      if (stmt != null)
      {
        stmt.close();
      }
    }
  }

  /**
   * Exposes the contents of the recovery log table as a matrix of String.<br />
   * <em>this method should be only used for management/debugging purpose</em>
   * 
   * @param from the starting index from which the log entries are retrieved
   *          (corresponds to the log_id column)
   * @param maxrows the maximum number of rows to retrieve
   * @return a matrix of String representing the content of the recovery log
   *         table
   */
  String[][] getLogEntries(long from, int maxrows)
  {
    Statement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = getDatabaseConnection().createStatement();
      stmt.setMaxRows(maxrows);
      rs = stmt.executeQuery("select * from " + logTableName
          + " where log_id >= " + from);
      int columnCount = rs.getMetaData().getColumnCount();
      List logEntries = new ArrayList();
      while (rs.next())
      {
        String[] logEntry = new String[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
          logEntry[i] = rs.getString(i + 1);
        }
        logEntries.add(logEntry);
      }
      String[][] result = new String[logEntries.size()][columnCount];
      for (int i = 0; i < logEntries.size(); i++)
        result[i] = (String[]) logEntries.get(i);
      return result;
    }
    catch (SQLException e)
    {
      return null;
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (SQLException ignore)
      {
      }
      try
      {
        stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  String[][] getLogEntriesInTransaction(long tid)
  {
    Statement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = getDatabaseConnection().createStatement();
      rs = stmt.executeQuery("select * from " + logTableName
          + " where transaction_id = " + tid);
      int columnCount = rs.getMetaData().getColumnCount();
      List logEntries = new ArrayList();
      while (rs.next())
      {
        String[] logEntry = new String[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
          logEntry[i] = rs.getString(i + 1);
        }
        logEntries.add(logEntry);
      }
      String[][] result = new String[logEntries.size()][columnCount];
      for (int i = 0; i < logEntries.size(); i++)
        result[i] = (String[]) logEntries.get(i);
      return result;
    }
    catch (SQLException e)
    {
      return null;
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (SQLException ignore)
      {
      }
      try
      {
        stmt.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Gives the log as an XML String
   * 
   * @return the XML representation of the log
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RecoveryLog + " "
        + DatabasesXmlTags.ATT_driver + "=\"" + driverClassName + "\" "
        + DatabasesXmlTags.ATT_url + "=\"" + url + "\" ");
    if (driverName != null)
    {
      info.append(DatabasesXmlTags.ATT_driverPath + "=\"" + driverName + "\" ");
    }
    info.append(DatabasesXmlTags.ATT_login + "=\"" + login + "\" "
        + DatabasesXmlTags.ATT_password + "=\"" + password + "\" "
        + DatabasesXmlTags.ATT_requestTimeout + "=\"" + (timeout / 1000)
        + "\" " + DatabasesXmlTags.ATT_recoveryBatchSize + "=\""
        + recoveryBatchSize + "\">");
    // Recovery Log table
    info.append("<" + DatabasesXmlTags.ELT_RecoveryLogTable + " "
        + DatabasesXmlTags.ATT_createTable + "=\"" + logTableCreateTable
        + "\" " + DatabasesXmlTags.ATT_tableName + "=\"" + logTableName + "\" "
        + DatabasesXmlTags.ATT_logIdColumnType + "=\"" + logTableLogIdType
        + "\" " + DatabasesXmlTags.ATT_vloginColumnType + "=\""
        + logTableVloginType + "\" " + DatabasesXmlTags.ATT_sqlColumnType
        + "=\"" + logTableSqlType + "\" "
        + DatabasesXmlTags.ATT_autoConnTranColumnType + "=\""
        + logTableAutoConnTranColumnType + "\" "
        + DatabasesXmlTags.ATT_transactionIdColumnType + "=\""
        + logTableTransactionIdType + "\" "
        + DatabasesXmlTags.ATT_requestIdColumnType + "=\""
        + logTableRequestIdType + "\" "
        + DatabasesXmlTags.ATT_execTimeColumnType + "=\""
        + logTableExecTimeType + "\" "
        + DatabasesXmlTags.ATT_updateCountColumnType + "=\""
        + logTableUpdateCountType + "\" "
        + DatabasesXmlTags.ATT_extraStatementDefinition + "=\""
        + logTableExtraStatement + "\"/>");
    // Checkpoint table
    info.append("<" + DatabasesXmlTags.ELT_CheckpointTable + " "
        + DatabasesXmlTags.ATT_createTable + "=\"" + checkpointTableCreateTable
        + "\" " + DatabasesXmlTags.ATT_tableName + "=\"" + checkpointTableName
        + "\" " + DatabasesXmlTags.ATT_checkpointNameColumnType + "=\""
        + checkpointTableNameType + "\" "
        + DatabasesXmlTags.ATT_logIdColumnType + "=\""
        + checkpointTableLogIdType + "\" "
        + DatabasesXmlTags.ATT_extraStatementDefinition + "=\""
        + checkpointTableExtraStatement + "\"" + "/>");
    // BackendLog table
    info.append("<" + DatabasesXmlTags.ELT_BackendTable + " "
        + DatabasesXmlTags.ATT_createTable + "=\"" + backendTableCreateTable
        + "\" " + DatabasesXmlTags.ATT_tableName + "=\"" + backendTableName
        + "\" " + DatabasesXmlTags.ATT_databaseNameColumnType + "=\""
        + backendTableDatabaseName + "\" "
        + DatabasesXmlTags.ATT_backendNameColumnType + "=\""
        + backendTableBackendName + "\" "
        + DatabasesXmlTags.ATT_backendStateColumnType + "=\""
        + backendTableBackendState + "\" "
        + DatabasesXmlTags.ATT_checkpointNameColumnType + "=\""
        + backendTableCheckpointName + "\" "
        + DatabasesXmlTags.ATT_extraStatementDefinition + "=\""
        + backendTableExtraStatement + "\"" + "/>");
    // Dump table
    info.append("<" + DatabasesXmlTags.ELT_DumpTable + " "
        + DatabasesXmlTags.ATT_createTable + "=\"" + dumpTableCreateTable
        + "\" " + DatabasesXmlTags.ATT_tableName + "=\"" + dumpTableName
        + "\" " + DatabasesXmlTags.ATT_dumpNameColumnType + "=\""
        + dumpTableDumpNameColumnType + "\" "
        + DatabasesXmlTags.ATT_dumpDateColumnType + "=\""
        + dumpTableDumpDateColumnType + "\" "
        + DatabasesXmlTags.ATT_dumpPathColumnType + "=\""
        + dumpTableDumpPathColumnType + "\" "
        + DatabasesXmlTags.ATT_dumpFormatColumnType + "=\""
        + dumpTableDumpFormatColumnType + "\" "
        + DatabasesXmlTags.ATT_checkpointNameColumnType + "=\""
        + dumpTableCheckpointNameColumnType + "\" "
        + DatabasesXmlTags.ATT_backendNameColumnType + "=\""
        + dumpTableBackendNameColumnType + "\" "
        + DatabasesXmlTags.ATT_tablesColumnName + "=\""
        + dumpTableTablesColumnName + "\" "
        + DatabasesXmlTags.ATT_tablesColumnType + "=\""
        + dumpTableTablesColumnType + "\" "
        + DatabasesXmlTags.ATT_extraStatementDefinition + "=\""
        + dumpTableExtraStatementDefinition + "\"" + "/>");
    info.append("</" + DatabasesXmlTags.ELT_RecoveryLog + ">");

    return info.toString();
  }

  //
  // VDB restart order management
  //

  /**
   * Check if a special checkpoint called "last-man-down" exists in the recovery
   * log
   * 
   * @throws SQLException if an error occurs while accessing the recovery log
   */
  public boolean isLastManDown() throws SQLException
  {
    Statement s = null;
    ResultSet rs = null;
    try
    {
      s = getDatabaseConnection().createStatement();
      rs = s.executeQuery("select * from " + checkpointTableName
          + " where name ='last-man-down'");
      return rs.next();
    }
    finally
    {
      if (s != null)
        try
        {
          s.close();
        }
        catch (Exception ignore)
        {
        }
      if (rs != null)
        try
        {
          rs.close();
        }
        catch (Exception ignore)
        {
        }
    }
  }

  /**
   * Insert a special checkpoint called "last-man-down" in the recovery log
   * 
   * @throws SQLException if an error occurs while updating the recovery log
   */
  public void setLastManDown() throws SQLException
  {
    execDatabaseUpdate("insert into " + checkpointTableName
        + " values ('last-man-down', 0)");
  }

  /**
   * Clear special checkpoint "last-man-down" if any in the recovery log
   * 
   * @throws SQLException if an error occurs while updating the recovery log
   */
  public void clearLastManDown() throws SQLException
  {
    execDatabaseUpdate("delete from " + checkpointTableName
        + " where name='last-man-down'");
  }
}
