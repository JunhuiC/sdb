/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2007 Continuent, Inc.
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
 * Contributor(s): Mathieu Peltier, Nicolas Modrzyk, Vadim Kassin, Olivier Fambon, Jean-Bernard van Zuylen, Stephane Giron.
 */

package org.continuent.sequoia.controller.virtualdatabase;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import org.continuent.sequoia.common.authentication.AuthenticationManager;
import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.jmx.management.BackendState;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.jmx.monitoring.backend.BackendStatistics;
import org.continuent.sequoia.common.jmx.notifications.SequoiaNotificationList;
import org.continuent.sequoia.common.locks.ReadPrioritaryFIFOWriteLock;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.users.AdminUser;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.common.xml.XmlTools;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.backup.Backuper;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.core.shutdown.VirtualDatabaseForceShutdownThread;
import org.continuent.sequoia.controller.core.shutdown.VirtualDatabaseSafeShutdownThread;
import org.continuent.sequoia.controller.core.shutdown.VirtualDatabaseShutdownThread;
import org.continuent.sequoia.controller.core.shutdown.VirtualDatabaseWaitShutdownThread;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.monitoring.SQLMonitoring;
import org.continuent.sequoia.controller.recoverylog.BackendRecoveryInfo;
import org.continuent.sequoia.controller.recoverylog.RecoverThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.management.AbstractAdminOperation;
import org.continuent.sequoia.controller.virtualdatabase.management.BackupBackendOperation;
import org.continuent.sequoia.controller.virtualdatabase.management.EnableBackendOperation;
import org.continuent.sequoia.controller.virtualdatabase.management.RestoreDumpOperation;

/**
 * A <code>VirtualDatabase</code> represents a database from client point of
 * view and hide the complexity of the cluster distribution to the client. The
 * client always uses the virtual database name and the Sequoia Controller will
 * use the real connections when an SQL request comes in.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:vadim@kase.kz">Vadim Kassin </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:Stephane.Giron@continuent.com>Stephane Giron </a>
 * @version 1.0
 */
public class VirtualDatabase implements XmlComponent
{
  private static final long                serialVersionUID                = 1399418136380336827L;

  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Transaction handling
  // 5. Database backend management
  // 6. Checkpoint management
  // 7. Getter/Setter (possibly in alphabetical order)
  // 8. Shutdown
  //

  /** Virtual database name */
  protected String                         name;

  /**
   * Authentification manager matching virtual database login/password to
   * backends login/password
   */
  protected AuthenticationManager          authenticationManager;

  /** <code>ArrayList</code> of <code>DatabaseBackend</code> objects */
  protected ArrayList                      backends;

  /** Read/Write lock for backend list */
  protected ReadPrioritaryFIFOWriteLock    rwLock;

  private IdleConnectionChecker            idleConnectionChecker;
  private long                             idleConnectionTimeout;

  /** The request manager to use for this database */
  protected RequestManager                 requestManager;

  /** ArrayList to store the order of requests */
  protected LinkedList                     totalOrderQueue                 = null;

  /** Virtual database logger */
  protected Trace                          logger                          = null;
  protected Trace                          requestLogger                   = null;

  /** end user logger */
  static Trace                             endUserLogger                   = Trace
                                                                               .getLogger("org.continuent.sequoia.enduser");

  // List of current active Worker Threads
  private ArrayList                        activeThreads                   = new ArrayList();
  // List of current idle Worker Threads
  private int                              idleThreads                     = 0;
  // List of current pending connections (Socket objects)
  private ArrayList                        pendingConnections              = new ArrayList();

  /** Maximum number of concurrent accepted for this virtual database */
  protected int                            maxNbOfConnections;

  /** If false one worker thread is forked per connection else */
  protected boolean                        poolConnectionThreads;

  /** Maximum time a worker thread can remain idle before dying */
  protected long                           maxThreadIdleTime;

  /**
   * Minimum number of worker threads to keep in the pool if
   * poolConnectionThreads is true
   */
  protected int                            minNbOfThreads;

  /** Maximum number of worker threads to fork */
  protected int                            maxNbOfThreads;

  /** Current number of worker threads */
  protected int                            currentNbOfThreads;

  /** Virtual Database MetaData */
  protected VirtualDatabaseDynamicMetaData metadata;
  private boolean                          useStaticResultSetMetaData      = true;
  protected VirtualDatabaseStaticMetaData  staticMetadata;

  private SQLMonitoring                    sqlMonitor                      = null;

  /** Use for method getAndCheck */
  public static final int                  CHECK_BACKEND_ENABLE            = 1;
  /** Use for method getAndCheck */
  public static final int                  CHECK_BACKEND_DISABLE           = 0;
  /** Use for method getAndCheck */
  public static final int                  NO_CHECK_BACKEND                = -1;

  /** Short form of SQL statements to include in traces and exceptions */
  private int                              sqlShortFormLength;

  /** The controller we belong to */
  Controller                               controller;

  /** Comma separated list of database product names (one instance per name) */
  private String                           databaseProductNames            = "Sequoia";

  /** Marker to see if the database is shutting down */
  protected boolean                        shuttingDown                    = false;
  private boolean                          refusingNewTransaction          = false;
  /** List of currently executing admin operations (preventing shutdown) */
  private List                             currentAdminOperations;

  protected NotificationBroadcasterSupport notificationBroadcasterSupport;

  protected int                            notificationSequence            = 0;

  protected long                           connectionId                    = 0;

  private boolean                          enforceTableExistenceIntoSchema = false;

  private List                             functionsToBroadcastList;

  protected List                           recoverThreads;

  private int                              ongoingSuspendOperationFromLocalController;
  private Object                           syncObject                      = new Object();

  public static final int                  SUSPENDING                      = 1;
  public static final int                  SUSPENDED                       = 2;
  public static final int                  RESUMING                        = 3;
  public static final int                  RUNNING                         = 4;

  private int                              suspendedState;
  private int                              suspendingState;
  private int                              resumingState;

  /**
   * Creates a new <code>VirtualDatabase</code> instance.
   * 
   * @param name the virtual database name.
   * @param maxConnections maximum number of concurrent connections.
   * @param pool should we use a pool of threads for handling connections?
   * @param minThreads minimum number of threads in the pool
   * @param maxThreads maximum number of threads in the pool
   * @param maxThreadIdleTime maximum time a thread can remain idle before being
   *            removed from the pool.
   * @param idleConnectionTimeout maximum time an established connection can
   *          remain idle before the associated thread is cleaned up and shut
   *          down.
   * @param sqlShortFormLength maximum number of characters of an SQL statement
   *          to diplay in traces or exceptions
   * @param useStaticResultSetMetaData true if DatabaseResultSetMetaData should
   *            use static fields or try to fetch the metadata from the
   *            underlying database
   * @param controller the controller we belong to
   */
  public VirtualDatabase(Controller controller, String name,
      int maxConnections, boolean pool, int minThreads, int maxThreads,
      long maxThreadIdleTime, long idleConnectionTimeout,
      int sqlShortFormLength, boolean useStaticResultSetMetaData,
      boolean enforceTableExistenceIntoSchema)
  {
    this.controller = controller;
    this.name = name;
    this.maxNbOfConnections = maxConnections;
    this.poolConnectionThreads = pool;
    this.minNbOfThreads = minThreads;
    this.maxNbOfThreads = maxThreads;
    this.maxThreadIdleTime = maxThreadIdleTime;
    this.idleConnectionTimeout = idleConnectionTimeout;
    this.sqlShortFormLength = sqlShortFormLength;
    this.useStaticResultSetMetaData = useStaticResultSetMetaData;
    this.enforceTableExistenceIntoSchema = enforceTableExistenceIntoSchema;
    backends = new ArrayList();
    currentAdminOperations = new LinkedList();
    recoverThreads = new LinkedList();

    rwLock = new ReadPrioritaryFIFOWriteLock();
    logger = Trace
        .getLogger("org.continuent.sequoia.controller.virtualdatabase." + name);
    requestLogger = Trace
        .getLogger("org.continuent.sequoia.controller.virtualdatabase.request."
            + name);
    ongoingSuspendOperationFromLocalController = 0;
    suspendedState = 0;
    suspendingState = 0;
    resumingState = 0;
  }

  /**
   * Starts the idle connection checker thread. The thread is killed by the
   * {@link #shutdown(int)} method.
   */
  public void start()
  {
    if (idleConnectionTimeout != 0)
    {
      idleConnectionChecker = new IdleConnectionChecker(this);
      idleConnectionChecker.start();
    }
  }

  /**
   * Sets the NotificationBroadcasterSupport associated with the MBean managing
   * this virtual database.
   * 
   * @param notificationBroadcasterSupport the notificationBroadcasterSuppor
   *            associated with the mbean managing this virtual database
   */
  public void setNotificationBroadcasterSupport(
      NotificationBroadcasterSupport notificationBroadcasterSupport)
  {
    this.notificationBroadcasterSupport = notificationBroadcasterSupport;
  }

  /**
   * Sends a JMX Notification on behalf of the MBean associated with this
   * virtual database
   * 
   * @param type type of the JMX notification
   * @param message message associated with the notification
   * @see SequoiaNotificationList
   */
  protected void sendJmxNotification(String type, String message)
  {
    if (!MBeanServerManager.isJmxEnabled())
    {
      // do not send jmx notification if jmx is not enabled
      return;
    }
    try
    {
      notificationBroadcasterSupport.sendNotification(new Notification(type,
          JmxConstants.getVirtualDataBaseObjectName(name),
          notificationSequence++, message));
    }
    catch (MalformedObjectNameException e)
    {
      // unable to get a correct vdb object name: do nothing
      logger.warn("Unable to send JMX notification", e);
    }
  }

  /**
   * Acquires a read lock on the backend lists (both enabled and disabled
   * backends). This should be called prior traversing the backend
   * <code>ArrayList</code>.
   * 
   * @throws InterruptedException if an error occurs
   */
  public final void acquireReadLockBackendLists() throws InterruptedException
  {
    rwLock.acquireRead();
  }

  /**
   * Releases the read lock on the backend lists (both enabled and disabled
   * backends). This should be called after traversing the backend
   * <code>ArrayList</code>.
   */
  public final void releaseReadLockBackendLists()
  {
    rwLock.releaseRead();
  }

  /**
   * Is this virtual database distributed ?
   * 
   * @return false
   */
  public boolean isDistributed()
  {
    return false;
  }

  /* Request Handling */

  /**
   * Checks if a given virtual login/password is ok.
   * 
   * @param virtualLogin the virtual user login
   * @param virtualPassword the virtual user password
   * @return <code>true</code> if the login/password is known from the
   *         <code>AuthenticationManager</code>. Returns <code>false</code>
   *         if no <code>AuthenticationManager</code> is defined.
   */
  public boolean checkUserAuthentication(String virtualLogin,
      String virtualPassword)
  {
    if (authenticationManager == null)
    {
      logger.error("No authentification manager defined to check login '"
          + virtualLogin + "'");
      return false;
    }
    else
    {
      boolean result = authenticationManager
          .isValidVirtualUser(new VirtualDatabaseUser(virtualLogin,
              virtualPassword));
      if (!result)
        endUserLogger.error(Translate.get(
            "virtualdatabase.authentication.failed", virtualLogin));
      return result;
    }
  }

  /**
   * Checks if a given admin login/password is ok.
   * 
   * @param adminLogin admin user login
   * @param adminPassword admin user password
   * @return <code>true</code> if the login/password is known from the
   *         <code>AuthenticationManager</code>. Returns <code>false</code>
   *         if no <code>AuthenticationManager</code> is defined.
   */
  public boolean checkAdminAuthentication(String adminLogin,
      String adminPassword)
  {
    if (authenticationManager == null)
    {
      endUserLogger.info(Translate.get(
          "virtualdatabase.checking.authentication", new String[]{adminLogin,
              this.getVirtualDatabaseName()}));
      String msg = "No authentification manager defined to check admin login '"
          + adminLogin + "'";
      logger.error(msg);
      endUserLogger.error(Translate.get(
          "virtualdatabase.check.authentication.failed", new String[]{
              adminLogin, this.getVirtualDatabaseName(), msg}));
      return false;
    }
    else
    {
      boolean result = authenticationManager.isValidAdminUser(new AdminUser(
          adminLogin, adminPassword));
      if (!result)
        endUserLogger.error(Translate.get(
            "virtualdatabase.authentication.failed", adminLogin));
      return result;
    }
  }

  /**
   * Adds a new vdb user. Uses the vdb login/password as real user
   * login/password. Only creates the user if vdb login/password are valid for
   * all backends hosting the vdb.
   * 
   * @param vdbUser vdb user to be added.
   */
  public void checkAndAddVirtualDatabaseUser(VirtualDatabaseUser vdbUser)
  {
    // If user does not exist in all backends leave
    if (!isValidUserForAllBackends(vdbUser))
    {
      if (logger.isWarnEnabled())
      {
        logger.warn("Could not create new vdb user " + vdbUser.getLogin()
            + " because it does not exist on all backends");
      }
      return;
    }

    // Add user
    try
    {
      performAddVirtualDatabaseUser(vdbUser);
      if (logger.isInfoEnabled())
      {
        logger.info("Added new vdb user " + vdbUser.getLogin());
      }
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
      {
        logger.warn("Problem when adding default connection manager for user "
            + vdbUser.getLogin() + ", trying to clean-up...");
        removeVirtualDatabaseUser(vdbUser);
      }
    }
  }

  /**
   * Removes vdb user.
   * 
   * @param vdbUser vdb user to be removed.
   */
  private void removeVirtualDatabaseUser(VirtualDatabaseUser vdbUser)
  {
    performRemoveVirtualDatabaseUser(vdbUser);
  }

  /**
   * Adds new vdb user and its corresponding connection managers.
   * 
   * @param vdbUser vdb user to be added.
   * @throws SQLException thrown if problem occurred when adding connection
   *             manager
   */
  public void performAddVirtualDatabaseUser(VirtualDatabaseUser vdbUser)
      throws SQLException
  {
    for (Iterator iter = backends.iterator(); iter.hasNext();)
    {
      DatabaseBackend backend = (DatabaseBackend) iter.next();
      backend.addDefaultConnectionManager(vdbUser);
    }
    authenticationManager.addVirtualUser(vdbUser);
    // Should we invoke authenticationManager.addRealUser() here?
  }

  /**
   * Removes vdb user and its corresponding connection managers.
   * 
   * @param vdbUser vdb user to be removed.
   */
  public void performRemoveVirtualDatabaseUser(VirtualDatabaseUser vdbUser)
  {
    authenticationManager.removeVirtualUser(vdbUser);
    for (Iterator iter = backends.iterator(); iter.hasNext();)
    {
      DatabaseBackend backend = (DatabaseBackend) iter.next();
      try
      {
        backend.removeConnectionManager(vdbUser);
      }
      catch (SQLException e)
      {
        if (logger.isWarnEnabled())
        {
          logger
              .warn("Problem when removing default connection manager for user "
                  + vdbUser.getLogin());
        }
      }
    }
    if (logger.isInfoEnabled())
    {
      logger.info("Removed vdb user " + vdbUser.getLogin());
    }
  }

  /**
   * Checks if a vdb user is valid as a user for allbackends.
   * 
   * @param vdbUser vdb user to be checked.
   * @return true if vdb user is valid for all backends, false otherwise.
   */
  public boolean isValidUserForAllBackends(VirtualDatabaseUser vdbUser)
  {
    boolean result = true;
    for (Iterator iter = backends.iterator(); iter.hasNext();)
    {
      DatabaseBackend backend = (DatabaseBackend) iter.next();
      if (!backend.isValidBackendUser(vdbUser))
      {
        result = false;
        break;
      }
    }
    return result;
  }

  /**
   * Performs a read request and returns the reply.
   * 
   * @param request the request to execute
   * @return a <code>ControllerResultSet</code> value
   * @exception SQLException if the request fails
   */
  protected ControllerResultSet statementExecuteQuery(SelectRequest request)
      throws SQLException
  {
    if (request == null)
    {
      String msg = "Request failed (null read request received)";
      logger.warn(msg);
      throw new SQLException(msg);
    }

    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("S " + request.getId() + " "
            + request.getTransactionId() + " " + request.getUniqueKey());

      request.setStartTime(System.currentTimeMillis());

      ControllerResultSet rs = requestManager.statementExecuteQuery(request);

      request.setEndTime(System.currentTimeMillis());
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logRequestTime(request);

      return rs;
    }
    catch (SQLException e)
    {
      String msg = "Request '" + request.getId() + "' failed ("
          + e.getMessage() + ")";
      if (!request.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        try
        {
          abort(request.getTransactionId(), true, false);
        }
        catch (SQLException e1)
        {
          e1.initCause(e);
          if (logger.isInfoEnabled())
          {
            msg = Translate.get("loadbalancer.request.failed.and.abort",
                new String[]{request.getSqlShortForm(getSqlShortFormLength()),
                    e.getMessage()});
            logger.info(msg);
            logger
                .info(
                    "Abort after request failure in transaction did not succeed probably because the transaction has already been aborted",
                    e1);
          }
        }
      }
      logger.debug(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(request);
      throw e;
    }
  }

  /**
   * Performs a write request and returns the number of rows affected.
   * 
   * @param request the request to execute
   * @return number of rows affected
   * @exception SQLException if the request fails
   */
  protected ExecuteUpdateResult statementExecuteUpdate(
      AbstractWriteRequest request) throws SQLException
  {
    if (request == null)
    {
      String msg = "Request failed (null write request received)";
      logger.warn(msg);
      throw new SQLException(msg);
    }

    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("W " + request.getId() + " "
            + request.getTransactionId() + " " + request.getUniqueKey());

      request.setStartTime(System.currentTimeMillis());

      ExecuteUpdateResult result = requestManager
          .statementExecuteUpdate(request);

      request.setEndTime(System.currentTimeMillis());
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logRequestTime(request);

      return result;
    }
    catch (SQLException e)
    {
      String msg = "Request '" + request.getId() + "' failed ("
          + e.getMessage() + ")";

      if (!request.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{request.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
        try
        {
          if (requestManager.getTransactionMetaData(new Long(request
              .getTransactionId())) != null)
            abort(request.getTransactionId(), true, false);
        }
        catch (SQLException e1)
        {
          logger.warn(msg);
          e1.initCause(e);
          if (logger.isInfoEnabled())
            logger
                .info(
                    "Abort after request failure in transaction did not succeed probably because the transaction has already been aborted",
                    e1);
        }
      }

      logger.debug(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(request);
      throw e;
    }
  }

  /**
   * Performs a write request and returns the auto generated keys.
   * 
   * @param request the request to execute
   * @return auto generated keys
   * @exception SQLException if the request fails
   */
  protected GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request) throws SQLException
  {
    if (request == null)
    {
      String msg = "Request failed (null write request received)";
      logger.warn(msg);
      throw new SQLException(msg);
    }

    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("W " + request.getId() + " "
            + request.getTransactionId() + " " + request.getUniqueKey());

      request.setStartTime(System.currentTimeMillis());

      GeneratedKeysResult result = requestManager
          .statementExecuteUpdateWithKeys(request);

      request.setEndTime(System.currentTimeMillis());
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logRequestTime(request);

      return result;
    }
    catch (SQLException e)
    {
      String msg = "Request '" + request.getId() + "' failed ("
          + e.getMessage() + ")";
      if (!request.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(request.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{request.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(request);
      throw e;
    }
  }

  /**
   * Execute a request using Statement.execute(). Handle this as a stored
   * procedure for which we have no metadata information.
   * 
   * @param request the request to execute
   * @return an <code>ExecuteResult</code> object
   * @exception SQLException if an error occurs
   */
  protected ExecuteResult statementExecute(AbstractRequest request)
      throws SQLException
  {
    if (request == null)
    {
      String msg = "Statement.execute() failed (null request received)";
      logger.warn(msg);
      throw new SQLException(msg);
    }

    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("E " + request.getId() + " "
            + request.getTransactionId() + " " + request.getUniqueKey());

      request.setStartTime(System.currentTimeMillis());

      ExecuteResult result = requestManager.statementExecute(request);

      request.setEndTime(System.currentTimeMillis());
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logRequestTime(request);

      return result;
    }
    catch (AllBackendsFailedException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.all.backends", new String[]{
              String.valueOf(request.getId()), e.getMessage()});
      if (!request.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(request.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{request.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(request);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(request.getId()), e.getMessage()});
      if (!request.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(request.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{request.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(request);
      throw e;
    }
  }

  /**
   * Call a stored procedure that returns a ResultSet.
   * 
   * @param proc the stored procedure call
   * @return a <code>java.sql.ResultSet</code> value
   * @exception SQLException if an error occurs
   */
  protected ControllerResultSet callableStatementExecuteQuery(
      StoredProcedure proc) throws SQLException
  {
    if (proc == null)
    {
      String msg = "Request failed (null stored procedure received)";
      logger.warn(msg);
      throw new SQLException(msg);
    }

    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("S " + proc.getId() + " " + proc.getTransactionId()
            + " " + proc.getUniqueKey());

      proc.setStartTime(System.currentTimeMillis());

      ControllerResultSet rs = requestManager
          .callableStatementExecuteQuery(proc);

      proc.setEndTime(System.currentTimeMillis());
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logRequestTime(proc);

      return rs;
    }
    catch (AllBackendsFailedException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.all.backends", new String[]{
              String.valueOf(proc.getId()), e.getMessage()});
      if (!proc.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(proc.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{proc.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(proc);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(proc.getId()), e.getMessage()});
      if (!proc.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(proc.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{proc.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(proc);
      throw e;
    }
  }

  /**
   * Call a stored procedure that performs an update.
   * 
   * @param proc the stored procedure call
   * @return number of rows affected
   * @exception SQLException if an error occurs
   */
  protected ExecuteUpdateResult callableStatementExecuteUpdate(
      StoredProcedure proc) throws SQLException
  {
    if (proc == null)
    {
      String msg = "Request failed (null stored procedure received)";
      logger.warn(msg);
      throw new SQLException(msg);
    }

    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("W " + proc.getId() + " " + proc.getTransactionId()
            + " " + proc.getUniqueKey());

      proc.setStartTime(System.currentTimeMillis());

      ExecuteUpdateResult result = requestManager
          .callableStatementExecuteUpdate(proc);

      proc.setEndTime(System.currentTimeMillis());
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logRequestTime(proc);

      return result;
    }
    catch (AllBackendsFailedException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.all.backends", new String[]{
              String.valueOf(proc.getId()), e.getMessage()});
      if (!proc.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(proc.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{proc.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(proc);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(proc.getId()), e.getMessage()});
      if (!proc.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(proc.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{proc.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(proc);
      throw e;
    }
  }

  /**
   * Execute a call to CallableStatement.execute() and returns a suite of
   * updateCount and/or ResultSets.
   * 
   * @param proc the stored procedure to execute
   * @return an <code>ExecuteResult</code> object
   * @exception SQLException if an error occurs
   */
  protected ExecuteResult callableStatementExecute(StoredProcedure proc)
      throws SQLException
  {
    if (proc == null)
    {
      String msg = "Request failed (null stored procedure received)";
      logger.warn(msg);
      throw new SQLException(msg);
    }

    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("E " + proc.getId() + " " + proc.getTransactionId()
            + " " + proc.getUniqueKey());

      proc.setStartTime(System.currentTimeMillis());

      ExecuteResult result = requestManager.callableStatementExecute(proc);

      proc.setEndTime(System.currentTimeMillis());
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logRequestTime(proc);

      return result;
    }
    catch (AllBackendsFailedException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.all.backends", new String[]{
              String.valueOf(proc.getId()), e.getMessage()});
      if (!proc.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(proc.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{proc.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(proc);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(proc.getId()), e.getMessage()});
      if (!proc.isAutoCommit())
      {
        // If the request fails in a transaction, the transaction is likely
        // to be rollbacked by the underlying database. Then we have to abort
        // the transaction.
        abort(proc.getTransactionId(), true, false);
        msg = Translate.get("loadbalancer.request.failed.and.abort",
            new String[]{proc.getSqlShortForm(getSqlShortFormLength()),
                e.getMessage()});
      }
      logger.warn(msg);
      if (sqlMonitor != null && sqlMonitor.isActive())
        sqlMonitor.logError(proc);
      throw e;
    }
  }

  /**
   * Close the given persistent connection.
   * 
   * @param login login to use to retrieve the right connection pool
   * @param persistentConnectionId id of the persistent connection to close
   */
  public void closePersistentConnection(String login,
      long persistentConnectionId)
  {
    requestManager.closePersistentConnection(login, persistentConnectionId);
  }

  /**
   * Returns true if the virtual database has opened the given persistent
   * connection.
   * 
   * @param persistentConnectionId id of the persistent connection to check
   * @return true if the connection is open
   */
  public boolean hasPersistentConnection(long persistentConnectionId)
  {
    return requestManager.hasPersistentConnection(persistentConnectionId);
  }

  /**
   * Open the given persistent connection.
   * 
   * @param login login to use to retrieve the right connection pool
   * @param persistentConnectionId id of the persistent connection to open
   * @throws SQLException if an error occurs while opening the connection
   */
  public void openPersistentConnection(String login, long persistentConnectionId)
      throws SQLException
  {
    requestManager
        .openPersistentConnection(login, persistentConnectionId, null);
  }

  /**
   * Notify the failover of the given transaction (really useful for
   * DistributedVirtualDatabase)
   * 
   * @param currentTid transaction id
   */
  public void failoverForTransaction(long currentTid)
  {
    logger.info("Transparent client failover operated for transaction "
        + currentTid);
  }

  /**
   * Notify the failover of the given persistent connection (really useful for
   * DistributedVirtualDatabase)
   * 
   * @param persistentConnectionId the persistent connection id
   */
  public void failoverForPersistentConnection(long persistentConnectionId)
  {
    // This should never happen when we have a single controller since when we
    // die there is no one to fail over.
    logger
        .info("Unexpected transparent client failover operated for persistent connection "
            + persistentConnectionId);
  }

  protected final Object CONNECTION_ID_SYNC_OBJECT = new Object();

  /**
   * Return the next connection identifier (monotically increasing number).
   * 
   * @return a connection identifier
   */
  public long getNextConnectionId()
  {
    synchronized (CONNECTION_ID_SYNC_OBJECT)
    {
      return connectionId++;
    }
  }

  /**
   * Return the next request identifier (monotically increasing number).
   * 
   * @return a request identifier
   */
  public long getNextRequestId()
  {
    return requestManager.getNextRequestId();
  }

  /**
   * Return a ControllerResultSet containing the PreparedStatement metaData of
   * the given sql template
   * 
   * @param request the request containing the sql template
   * @return an empty ControllerResultSet with the metadata
   * @throws SQLException if a database error occurs
   */
  public ControllerResultSet getPreparedStatementGetMetaData(
      AbstractRequest request) throws SQLException
  {
    try
    {
      return requestManager.getPreparedStatementGetMetaData(request);
    }
    catch (NoMoreBackendException e)
    {
      throw e;
    }
  }

  /**
   * Returns a <code>ParameterMetaData</code> containing the metadata of the
   * prepared statement parameters for the given request
   * 
   * @param request the request containing the sql template
   * @return the metadata of the given prepared statement parameters
   * @throws SQLException if a database error occurs
   */
  public ParameterMetaData getPreparedStatementGetParameterMetaData(
      AbstractRequest request) throws SQLException
  {
    return requestManager.getPreparedStatementGetParameterMetaData(request);
  }

  /*
   * Transaction management
   */

  /**
   * Begins a new transaction and returns the corresponding transaction
   * identifier. This method is called from the driver when
   * {@link org.continuent.sequoia.driver.Connection#setAutoCommit(boolean)}is
   * called with <code>false</code> argument.
   * <p>
   * Note that the transaction begin is not logged in the recovery log by this
   * method, you will have to call logLazyTransactionBegin.
   * 
   * @param login the login used by the connection
   * @param isPersistentConnection true if the transaction is started on a
   *            persistent connection
   * @param persistentConnectionId persistent connection id if the transaction
   *            must be started on a persistent connection
   * @return an unique transaction identifier
   * @exception SQLException if an error occurs
   * @see RequestManager#logLazyTransactionBegin(long)
   */
  public long begin(String login, boolean isPersistentConnection,
      long persistentConnectionId) throws SQLException
  {
    try
    {
      long tid = requestManager.begin(login, isPersistentConnection,
          persistentConnectionId);
      if (requestLogger.isInfoEnabled())
        requestLogger.info("B " + tid);
      return tid;
    }
    catch (SQLException e)
    {
      String msg = "Begin failed (" + e.getMessage() + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Abort a transaction that has been started but in which no query was
   * executed. As we use lazy transaction begin, there is no need to rollback
   * such transaction but just to cleanup the metadata associated with this not
   * effectively started transaction.
   * 
   * @param transactionId id of the transaction to abort
   * @param logAbort true if the abort (in fact rollback) should be logged in
   *            the recovery log
   * @param forceAbort true if the abort will be forced. Actually, abort will do
   *            nothing when a transaction has savepoints (we do not abort the
   *            whole transaction, so that the user can rollback to a previous
   *            savepoint), except when the connection is closed. In this last
   *            case, if the transaction is not aborted, it prevents future
   *            maintenance operations such as shutdowns, enable/disable from
   *            completing, so we have to force this abort operation. It also
   *            applies to the DeadlockDetectionThread and the cleanup of the
   *            VirtualDatabaseWorkerThread.
   * @throws SQLException if an error occurs
   */
  public void abort(long transactionId, boolean logAbort, boolean forceAbort)
      throws SQLException
  {
    requestManager.abort(transactionId, logAbort, forceAbort);
    // Emulate this as a rollback for the RequestPlayer
    if (requestLogger.isInfoEnabled())
      requestLogger.info("R " + transactionId);
  }

  /**
   * Commits a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param logCommit true if the commit should be logged in the recovery log
   * @param emptyTransaction true if this transaction has not executed any
   *            request
   * @exception SQLException if an error occurs
   */
  public void commit(long transactionId, boolean logCommit,
      boolean emptyTransaction) throws SQLException
  {
    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("C " + transactionId);
      requestManager.commit(transactionId, logCommit, emptyTransaction);
    }
    catch (SQLException e)
    {

      String msg = "Commit of transaction '" + transactionId + "' failed ("
          + e.getMessage() + ")";

      // If the commit fails in a transaction, the transaction is likely
      // to be rollbacked by the underlying database. Then we have to abort
      // the transaction.
      msg = Translate.get("loadbalancer.commit.failed.and.abort", new String[]{
          Long.toString(transactionId), e.getMessage()});
      try
      {
        abort(transactionId, true, false);
      }
      catch (SQLException e1)
      {
        if (logger.isInfoEnabled())
          logger
              .info(
                  "Abort after commit failure in transaction did not succeed probably because the transaction has already been aborted",
                  e);
      }

      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Rollbacks a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param logRollback true if the rollback should be logged in the recovery
   *            log
   * @exception SQLException if an error occurs
   */
  public void rollback(long transactionId, boolean logRollback)
      throws SQLException
  {
    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("R " + transactionId);
      requestManager.rollback(transactionId, logRollback);
    }
    catch (SQLException e)
    {
      String msg = "Rollback of transaction '" + transactionId + "' failed ("
          + e.getMessage() + ")";

      // If the rollback fails in a transaction, the transaction is likely
      // to be rollbacked by the underlying database. Then we have to abort
      // the transaction.
      msg = Translate.get("loadbalancer.rollback.failed.and.abort",
          new String[]{Long.toString(transactionId), e.getMessage()});
      try
      {
        abort(transactionId, true, false);
      }
      catch (SQLException e1)
      {
        if (logger.isInfoEnabled())
          logger
              .info(
                  "Abort after rollback failure in transaction did not succeed probably because the transaction has already been aborted",
                  e);
      }

      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Rollbacks a transaction given its id to a savepoint given its name
   * 
   * @param transactionId the transaction id
   * @param savepointName the name of the savepoint
   * @exception SQLException if an error occurs
   */
  public void rollback(long transactionId, String savepointName)
      throws SQLException
  {
    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("R " + transactionId + " " + savepointName);
      requestManager.rollback(transactionId, savepointName);
    }
    catch (SQLException e)
    {
      String msg = "Rollback to savepoint '" + savepointName + "' for "
          + "transaction '" + transactionId + "' failed (" + e.getMessage()
          + ")";

      // If the rollback fails in a transaction, the transaction is likely
      // to be rollbacked by the underlying database. Then we have to abort
      // the transaction.
      msg = Translate.get("loadbalancer.rollback.failed.and.abort",
          new String[]{Long.toString(transactionId), e.getMessage()});
      try
      {
        abort(transactionId, true, false);
      }
      catch (SQLException e1)
      {
        if (logger.isInfoEnabled())
          logger
              .info(
                  "Abort after rollback failure in transaction did not succeed probably because the transaction has already been aborted",
                  e);
      }

      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Sets a unnamed savepoint to a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @return the savepoint id
   * @exception SQLException if an error occurs
   */
  public int setSavepoint(long transactionId) throws SQLException
  {
    try
    {
      int savepointId = requestManager.setSavepoint(transactionId);
      if (requestLogger.isInfoEnabled())
        requestLogger.info("P " + transactionId + " " + savepointId);
      return savepointId;
    }
    catch (SQLException e)
    {
      String msg = "Setting unnamed savepoint to transaction '" + transactionId
          + "' failed (" + e.getMessage() + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Sets a savepoint given its desired name to a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param name the desired name of the savepoint
   * @exception SQLException if an error occurs
   */
  public void setSavepoint(long transactionId, String name) throws SQLException
  {
    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("P " + transactionId + " " + name);
      requestManager.setSavepoint(transactionId, name);
    }
    catch (SQLException e)
    {
      String msg = "Setting savepoint with name '" + name + "' to transaction "
          + "'" + transactionId + "' failed (" + e.getMessage() + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Releases a savepoint given its name from a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param savepointName the name of the savepoint
   * @exception SQLException if an error occurs
   */
  public void releaseSavepoint(long transactionId, String savepointName)
      throws SQLException
  {
    try
    {
      if (requestLogger.isInfoEnabled())
        requestLogger.info("F " + transactionId + " " + savepointName);
      requestManager.releaseSavepoint(transactionId, savepointName);
    }
    catch (SQLException e)
    {
      String msg = "Releasing savepoint with name '" + savepointName
          + "' from " + "transaction '" + transactionId + "' failed ("
          + e.getMessage() + ")";
      logger.warn(msg);
      throw e;
    }
  }

  //
  // Database backends management
  //

  /**
   * Add a backend to this virtual database.
   * 
   * @param db the database backend to add
   * @throws VirtualDatabaseException if an error occurs
   */
  public void addBackend(DatabaseBackend db) throws VirtualDatabaseException
  {
    this.addBackend(db, true);
  }

  /**
   * Add a backend to this virtual database.
   * 
   * @param db the database backend to add
   * @param checkForCompliance should load the driver ?
   * @throws VirtualDatabaseException if an error occurs
   */
  public void addBackend(DatabaseBackend db, boolean checkForCompliance)
      throws VirtualDatabaseException
  {
    if (db == null)
    {
      String msg = "Illegal null database backend in addBackend(DatabaseBackend) method";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    if (db.isReadEnabled())
    {
      String msg = "It is not allowed to add an enabled database.";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    // Get the lock on the list of backends
    try
    {
      rwLock.acquireWrite();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.writelock.failed", e);
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    // Check that the backend is not already up
    if (backends.indexOf(db) != -1)
    {
      rwLock.releaseWrite();
      String msg = "Duplicate backend " + db.getURL();
      logger.warn(msg);
      throw new VirtualDatabaseException(msg);
    }

    // Check the authentication manager has all virtual logins defined
    ArrayList logins = authenticationManager.getVirtualLogins();
    VirtualDatabaseUser vdu;
    String login;
    for (int i = 0; i < logins.size(); i++)
    {
      vdu = (VirtualDatabaseUser) logins.get(i);
      login = vdu.getLogin();
      if (db.getConnectionManager(login) == null)
      {
        rwLock.releaseWrite();
        throw new VirtualDatabaseException(Translate.get(
            "backend.missing.connection.manager", login));
      }
    }

    // Initialize the driver and check the compliance
    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Checking driver compliance");
      if (checkForCompliance)
        db.checkDriverCompliance(); // Also loads the driver
    }
    catch (Exception e)
    {
      rwLock.releaseWrite();
      String msg = "Error while adding database backend " + db.getName() + " ("
          + e + ")";
      logger.warn(msg);
      throw new VirtualDatabaseException(msg);
    }

    db.setSqlShortFormLength(getSqlShortFormLength());

    // Add the backend to the list
    backends.add(db);
    if (logger.isDebugEnabled())
      logger.debug("Backend " + db.getName() + " added successfully");

    // Set the backend state listener so that the state is logged into the
    // recovery log - if any. When there is no recovery log,
    // getBackendStateListener returns null, and stateListener is consequently
    // set to null. Looks like the only state listner ever is the recoveryLog.
    /*
     * Note: getRequestManager() is null, at load time, thus the test. At load
     * time, if there is a recovery log, the state listner eventually gets set,
     * but in a different fashion: it is set by RequestManager c'tor, when
     * calling setRecoveryLog().
     */
    if (getRequestManager() != null)
    {
      db.setStateListener(getRequestManager().getBackendStateListener());
    }

    // Release the lock
    rwLock.releaseWrite();

    // Notify Jmx listeners of the backend addition
    sendJmxNotification(SequoiaNotificationList.VIRTUALDATABASE_BACKEND_ADDED,
        Translate.get("notification.backend.added", db.getName()));

    // Add backend mbean to jmx server
    if (MBeanServerManager.isJmxEnabled())
    {
      try
      {
        ObjectName objectName = JmxConstants.getDatabaseBackendObjectName(name,
            db.getName());
        org.continuent.sequoia.controller.backend.management.DatabaseBackend managingBackend = new org.continuent.sequoia.controller.backend.management.DatabaseBackend(
            db);
        db.setNotificationBroadcaster(managingBackend.getBroadcaster());
        MBeanServerManager.registerMBean(managingBackend, objectName);
      }
      catch (Exception e)
      {
        logger.error(Translate.get(
            "virtualdatabase.fail.register.backend.mbean", db.getName()), e);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#forceDisableBackend(String)
   */
  public void forceDisableBackend(String backendName)
      throws VirtualDatabaseException
  {
    try
    {
      DatabaseBackend db = getAndCheckBackend(backendName,
          CHECK_BACKEND_DISABLE);
      requestManager.disableBackend(db, true);
      requestManager.setDatabaseSchema(
          getDatabaseSchemaFromActiveBackendsAndRefreshDatabaseProductNames(),
          false);

      sendJmxNotification(
          SequoiaNotificationList.VIRTUALDATABASE_BACKEND_DISABLED, Translate
              .get("notification.backend.disabled", db.getName()));
    }
    catch (Exception e)
    {
      logger.error("An error occured while disabling backend " + backendName
          + " (" + e + ")");
      throw new VirtualDatabaseException(e.getMessage(), e);
    }
  }

  /**
   * Prepare this virtual database for shutdown. This turns off all the backends
   * by cutting communication from this database. This does not prevents other
   * virtual database to use shared backends. This doesn't create checkpoints
   * either.
   * 
   * @param forceEnable true if backend disabling must be forced, false for
   *            regular/clean disabling
   * @throws VirtualDatabaseException if an error occurs
   */
  public void disableAllBackends(boolean forceEnable)
      throws VirtualDatabaseException
  {
    try
    {
      int size = this.backends.size();
      DatabaseBackend dbe;
      for (int i = 0; i < size; i++)
      {
        dbe = (DatabaseBackend) backends.get(i);
        if (dbe.isReadEnabled())
          requestManager.disableBackend(getAndCheckBackend(dbe.getName(),
              CHECK_BACKEND_DISABLE), forceEnable);
      }
    }
    catch (Exception e)
    {
      throw new VirtualDatabaseException(e.getMessage(), e);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#disableBackendWithCheckpoint(String)
   */
  public void disableBackendWithCheckpoint(String backendName)
      throws VirtualDatabaseException
  {
    try
    {
      DatabaseBackend backend = getAndCheckBackend(backendName,
          NO_CHECK_BACKEND);
      if (backend.isDisabled())
      {
        logger.info("Backend " + backendName + " is already disabled.");
        // disabling a disabled backend is a no-op
        return;
      }
      requestManager.disableBackendWithCheckpoint(backend,
          buildCheckpointName("disable " + backendName));
      // Force a schema refresh if we are not in RAIDb-1
      if (requestManager.getLoadBalancer().getRAIDbLevel() != RAIDbLevels.RAIDb1)
        requestManager
            .setDatabaseSchema(
                getDatabaseSchemaFromActiveBackendsAndRefreshDatabaseProductNames(),
                false);
    }
    catch (Exception e)
    {
      logger.error("An error occured while disabling backend " + backendName
          + " (" + e + ")");
      throw new VirtualDatabaseException(e.getMessage(), e);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#disableAllBackendsWithCheckpoint(java.lang.String)
   */
  public void disableAllBackendsWithCheckpoint(String checkpoint)
      throws VirtualDatabaseException
  {
    if (checkpoint == null)
    {
      disableAllBackends(false);
      return;
    }

    try
    {
      this.acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      throw new VirtualDatabaseException(e.getMessage(), e);
    }

    try
    {
      ArrayList backendInfos = new ArrayList();
      Iterator iter = backends.iterator();
      while (iter.hasNext())
      {
        DatabaseBackend backend = (DatabaseBackend) iter.next();
        backendInfos.add(new BackendInfo(backend));
      }
      requestManager.disableBackendsWithCheckpoint(backendInfos, checkpoint);
    }
    catch (Exception e)
    {
      throw new VirtualDatabaseException(e.getMessage(), e);
    }
    finally
    {
      this.releaseReadLockBackendLists();
    }
  }

  /**
   * Check that the virtual database is not resyncing its recovery log or
   * shutting down. This would prevent enable operations to take place.
   * 
   * @throws VirtualDatabaseException if virtual database is resyncing or
   *             shutting down.
   */
  private void enableBackendSanityChecks() throws VirtualDatabaseException
  {
    if (isResyncing())
    {
      String msg = Translate.get("virtualdatabase.fail.enable.cause.resyncing");
      logger.warn(msg);
      throw new VirtualDatabaseException(msg);
    }

    if (isShuttingDown())
    {
      String msg = Translate.get("virtualdatabase.fail.enable.cause.shutdown");
      logger.warn(msg);
      throw new VirtualDatabaseException(msg);
    }
  }

  /**
   * Enable the given backend from the given checkpoint. This method returns
   * once the recovery is complete.
   * 
   * @param backendName backend to enable
   * @param checkpointName checkpoint to enable from
   * @throws VirtualDatabaseException if an error occurs
   */
  public void enableBackendFromCheckpoint(String backendName,
      String checkpointName, boolean isWrite) throws VirtualDatabaseException
  {
    enableBackendSanityChecks();
    RecoverThread recoverThread = null;

    EnableBackendOperation enableOperation = new EnableBackendOperation(
        backendName);
    addAdminOperation(enableOperation);

    // Call the Request Manager
    try
    {
      DatabaseBackend backend = getAndCheckBackend(backendName,
          CHECK_BACKEND_ENABLE);
      recoverThread = requestManager.enableBackendFromCheckpoint(backend,
          checkpointName, isWrite);
      synchronized (this)
      {
        recoverThreads.add(recoverThread);
      }

      // Wait for recovery to complete
      recoverThread.join();
      if (recoverThread.getException() != null)
      {
        throw recoverThread.getException();
      }
      synchronized (this)
      {
        recoverThreads.remove(recoverThread);
      }
      requestManager.setSchemaIsDirty(true);

      // Update the static metadata
      getStaticMetaData().gatherStaticMetadata(backend);

      // Update the list of database product names
      if (databaseProductNames.indexOf(backend.getDatabaseProductName()) == -1)
        databaseProductNames += "," + backend.getDatabaseProductName();
    }
    catch (Exception e)
    {
      String msg = Translate.get(
          "virtualdatabase.enable.from.checkpoint.failed", new Object[]{name,
              backendName, e});
      logger.warn(msg, e);
      throw new VirtualDatabaseException(msg, e);
    }
    finally
    {
      // Remove the thread only if it was not already removed (this can happen
      // when an exception occurs inside the recover thread itself)
      if (recoverThread != null && recoverThreads.contains(recoverThread))
        synchronized (this)
        {
          recoverThreads.remove(recoverThread);
        }
      removeAdminOperation(enableOperation);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#enableBackendFromCheckpoint(java.lang.String)
   */
  public void enableBackendFromCheckpoint(String backendName, boolean isWrite)
      throws VirtualDatabaseException
  {
    enableBackendSanityChecks();

    DatabaseBackend backend = getAndCheckBackend(backendName, NO_CHECK_BACKEND);
    String checkpoint = backend.getLastKnownCheckpoint();
    if ((checkpoint == null) || ("".equals(checkpoint)))
      throw new VirtualDatabaseException(
          "Cannot enable backend "
              + backendName
              + " from a known state. Resynchronize this backend by restoring a dump.");
    else
    {
      if (logger.isDebugEnabled())
        logger.debug("Enabling backend " + backendName
            + " from its last checkpoint " + backend.getLastKnownCheckpoint());
    }
    enableBackendFromCheckpoint(backendName, backend.getLastKnownCheckpoint(),
        isWrite);
  }

  /**
   * Enable all the backends without any check.
   * 
   * @throws VirtualDatabaseException if fails
   */
  public void enableAllBackends() throws VirtualDatabaseException
  {
    enableBackendSanityChecks();

    try
    {
      int size = this.backends.size();
      DatabaseBackend dbe;
      for (int i = 0; i < size; i++)
      {
        dbe = (DatabaseBackend) backends.get(i);
        if (!dbe.isReadEnabled())
          forceEnableBackend(((DatabaseBackend) backends.get(i)).getName());
      }
    }
    catch (RuntimeException e)
    {
      logger.error("Runtime error in enableAllBackends", e);
      throw new VirtualDatabaseException(e.getMessage(), e);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#enableAllBackendsFromCheckpoint()
   */
  public void enableAllBackendsFromCheckpoint() throws VirtualDatabaseException
  {
    RecoveryLog log = requestManager.getRecoveryLog();
    if (log == null)
    {// If no recovery log is defined ignore fallback to a forced enable
      logger
          .warn("No recovery log has been configured, enabling backend without checkpoint.");
      enableAllBackends();
    }
    else
    {
      enableBackendSanityChecks();

      try
      {
        int size = this.backends.size();
        DatabaseBackend dbe;
        String backendName;
        BackendRecoveryInfo info;
        for (int i = 0; i < size; i++)
        {
          dbe = (DatabaseBackend) backends.get(i);
          backendName = dbe.getName();
          info = log.getBackendRecoveryInfo(name, backendName);
          switch (info.getBackendState())
          {
            case BackendState.DISABLED :
              String checkpoint = info.getCheckpoint();
              if (checkpoint == null || checkpoint.equals(""))
              {
                logger
                    .warn("Cannot enable backend "
                        + backendName
                        + " from a known state. Resynchronize this backend by restoring a dump.");
              }
              else
              {
                logger.info("Enabling backend " + backendName
                    + " from checkpoint " + checkpoint);
                enableBackendFromCheckpoint(dbe.getName(), checkpoint, true);
              }
              continue;
            case BackendState.UNKNOWN :
              logger.info("Unknown last state for backend " + backendName
                  + ". Leaving node in "
                  + (dbe.isReadEnabled() ? "enabled" : "disabled") + " state.");
              continue;
            case BackendState.BACKUPING :
            case BackendState.DISABLING :
            case BackendState.RESTORING :
            case BackendState.REPLAYING :
              if (!dbe.isReadEnabled())
              {
                logger.info("Unexpected transition state ("
                    + info.getBackendState() + ") for backend " + backendName
                    + ". Forcing backend to disabled state.");
                info.setBackendState(BackendState.DISABLED);
                log.storeBackendRecoveryInfo(name, info);
              }
              else
                logger.info("Unexpected transition state ("
                    + info.getBackendState() + ") for backend " + backendName
                    + ". Leaving backend in its current state.");
              continue;
            default :
              if (!dbe.isReadEnabled())
              {
                logger.info("Unexpected enabled state ("
                    + info.getBackendState() + ") for backend " + backendName
                    + ". Forcing backend to disabled state.");
                info.setBackendState(BackendState.DISABLED);
                log.storeBackendRecoveryInfo(name, info);
              }
              else
                logger.info("Unexpected enabled state ("
                    + info.getBackendState() + ") for backend " + backendName
                    + ". Leaving backend in its current state.");
              continue;
          }
        }
      }
      catch (Exception e)
      {
        throw new VirtualDatabaseException(e.getMessage(), e);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#forceEnableBackend(String)
   */
  public void forceEnableBackend(String backendName)
      throws VirtualDatabaseException
  {
    enableBackendSanityChecks();

    EnableBackendOperation enableOperation = new EnableBackendOperation(
        backendName + " (force)");
    addAdminOperation(enableOperation);

    // Call the Request Manager
    try
    {
      DatabaseBackend backend = getAndCheckBackend(backendName,
          CHECK_BACKEND_ENABLE);

      requestManager.enableBackend(backend);
      requestManager.setSchemaIsDirty(true);

      // Update the list of database product names
      if (databaseProductNames.indexOf(backend.getDatabaseProductName()) == -1)
        databaseProductNames += "," + backend.getDatabaseProductName();

      // Update the static metadata
      getStaticMetaData().gatherStaticMetadata(backend);

      sendJmxNotification(
          SequoiaNotificationList.VIRTUALDATABASE_BACKEND_ENABLED, Translate
              .get("notification.backend.enabled"));
    }
    catch (Exception e)
    {
      throw new VirtualDatabaseException(e.getMessage(), e);
    }
    finally
    {
      removeAdminOperation(enableOperation);
    }
  }

  /**
   * Prepare this virtual database for startup. This turns on all the backends
   * from the given checkpoint. If the checkpoint is null or an empty String,
   * the backends are enabled without further check else the backend states are
   * overriden to use the provided checkpoint.
   * 
   * @param checkpoint checkpoint for recovery log
   * @throws VirtualDatabaseException if fails
   */
  public void forceEnableAllBackendsFromCheckpoint(String checkpoint)
      throws VirtualDatabaseException
  {
    enableBackendSanityChecks();

    if (checkpoint == null || checkpoint.equals(""))
      enableAllBackends();
    else
    {
      try
      {
        int size = this.backends.size();
        DatabaseBackend backend;
        for (int i = 0; i < size; i++)
        {
          backend = (DatabaseBackend) backends.get(i);
          if (!backend.isReadEnabled())
          {
            backend.setLastKnownCheckpoint(checkpoint);
            enableBackendFromCheckpoint(backend.getName(), checkpoint, true);
          }
        }
      }
      catch (RuntimeException e)
      {
        logger
            .error("Runtime error in forceEnableAllBackendsFromCheckpoint", e);
        throw new VirtualDatabaseException(e.getMessage(), e);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getAllBackendNames()
   */
  public ArrayList getAllBackendNames() throws VirtualDatabaseException
  {
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = "Unable to acquire read lock on backend list in getAllBackendNames ("
          + e + ")";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    int size = backends.size();
    ArrayList result = new ArrayList();
    for (int i = 0; i < size; i++)
    {
      result.add(((DatabaseBackend) backends.get(i)).getName());
    }

    releaseReadLockBackendLists();
    return result;
  }

  /**
   * Find the DatabaseBackend corresponding to the given backend name and check
   * if it is possible to disable this backend. In the case enable, this method
   * also updates the virtual database schema by merging it with the one
   * provided by this backend.
   * 
   * @param backendName backend to look for
   * @param testEnable NO_CHECK_BACKEND no check is done, CHECK_BACKEND_DISABLE
   *            check if it is possible to disable the backend,
   *            CHECK_BACKEND_ENABLE check if it is possible to enable the
   *            backend
   * @return the backend to disable
   * @throws VirtualDatabaseException if an error occurs
   */
  public DatabaseBackend getAndCheckBackend(String backendName, int testEnable)
      throws VirtualDatabaseException
  {
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = "Unable to acquire read lock on backend list in getAndCheckBackend ("
          + e + ")";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    DatabaseBackend b;
    try
    {
      // Find the backend
      int size = backends.size();
      b = null;
      for (int i = 0; i < size; i++)
      {
        b = (DatabaseBackend) backends.get(i);
        if (b.getName().equals(backendName))
          break;
        else
          b = null;
      }

      // Check not null
      if (b == null)
      {
        String msg = "Trying to access a non-existing backend " + backendName;
        logger.warn(msg);
        throw new VirtualDatabaseException(msg);
      }

      // Check enable/disable
      switch (testEnable)
      {
        case NO_CHECK_BACKEND :
          break;
        case CHECK_BACKEND_DISABLE :
          if (!b.isReadEnabled())
          {
            String msg = "Backend " + backendName + " is already disabled";
            logger.warn(msg);
            throw new VirtualDatabaseException(msg);
          }
          break;
        case CHECK_BACKEND_ENABLE :
          if (b.isReadEnabled())
          {
            String msg = "Backend " + backendName + " is already enabled";
            logger.warn(msg);
            throw new VirtualDatabaseException(msg);
          }
          break;
        default :
          String msg = "Unexpected parameter in getAndCheckBackend(...)";
          logger.error(msg);
          throw new VirtualDatabaseException(msg);
      }
    }
    finally
    {
      releaseReadLockBackendLists();
    }

    if (testEnable == CHECK_BACKEND_ENABLE)
    {
      // Initialize backend for enable
      try
      {
        if (logger.isDebugEnabled())
          logger.debug("Initializing connections for backend " + b.getName());
        b.initializeConnections();

        b.checkDriverCompliance();

        if (logger.isDebugEnabled())
          logger.debug("Checking schema for backend " + b.getName());
        b.checkDatabaseSchema(null);

        DatabaseSchema backendSchema = b.getDatabaseSchema();

        if (backendSchema != null)
          requestManager.mergeDatabaseSchema(backendSchema);
        else
          logger.warn("Backend " + b.getName() + " has no defined schema.");
      }
      catch (SQLException e)
      {
        String msg = "Error while initalizing database backend " + b.getName()
            + " (" + e + ")";
        logger.warn(msg, e);
        throw new VirtualDatabaseException(msg, e);
      }
    }

    return b;
  }

  /**
   * Returns true if this vdb is resynching. This can only happen for
   * distributed vdbs that have recovery logs.
   * 
   * @return true if db is resynching
   */
  protected boolean isResyncing()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#replicateBackend(java.lang.String,
   *      java.lang.String, java.util.Map)
   */
  public void replicateBackend(String backendName, String newBackendName,
      Map parameters) throws VirtualDatabaseException
  {
    // Access the backend we want to replicate
    DatabaseBackend backend = getAndCheckBackend(backendName, NO_CHECK_BACKEND);
    DatabaseBackend newBackend = null;

    // Create a clone of the backend with additionnal parameters
    try
    {
      newBackend = backend.copy(newBackendName, parameters);
    }
    catch (Exception e)
    {
      String msg = Translate.get("virtualdatabase.fail.backend.copy", e);
      logger.warn(msg, e);
      throw new VirtualDatabaseException(msg, e);
    }

    // Add the backend to the virtual database.
    addBackend(newBackend);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#removeBackend(java.lang.String)
   */
  public void removeBackend(String backend) throws VirtualDatabaseException
  {
    removeBackend(getAndCheckBackend(backend, NO_CHECK_BACKEND));
  }

  /**
   * Remove a backend from this virtual database.
   * 
   * @param db the database backend to remove
   * @throws VirtualDatabaseException if an error occurs
   */
  public void removeBackend(DatabaseBackend db) throws VirtualDatabaseException
  {
    if (db == null)
    {
      String msg = "Illegal null database backend in removeBackend(DatabaseBackend) method";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    try
    {
      rwLock.acquireWrite();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.writelock.failed", e);
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    // Sanity checks
    int idx = backends.indexOf(db);
    if (idx == -1)
    {
      rwLock.releaseWrite(); // Release the lock
      String msg = "Trying to remove a non-existing backend " + db.getName();
      logger.warn(msg);
      throw new VirtualDatabaseException(msg);
    }

    if (((DatabaseBackend) backends.get(idx)).isReadEnabled())
    {
      rwLock.releaseWrite(); // Release the lock
      String msg = "Trying to remove an enabled backend " + db.getName();
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    // Remove it
    backends.remove(idx);
    rwLock.releaseWrite(); // Relase the lock

    sendJmxNotification(
        SequoiaNotificationList.VIRTUALDATABASE_BACKEND_REMOVED, Translate
            .get("notification.backend.removed"));

    // Remove backend mbean to jmx server
    if (MBeanServerManager.isJmxEnabled())
    {
      try
      {
        ObjectName objectName = JmxConstants.getDatabaseBackendObjectName(name,
            db.getName());
        MBeanServerManager.unregister(objectName);
      }
      catch (Exception e)
      {
        logger.error(Translate.get(
            "virtualdatabase.fail.unregister.backend.mbean", db.getName()), e);
      }
    }

    if (logger.isDebugEnabled())
      logger.debug("Backend " + db.getName() + " removed successfully");
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#transferBackend(java.lang.String,
   *      java.lang.String)
   */
  public void transferBackend(String backend, String controllerDestination)
      throws VirtualDatabaseException
  {
    throw new VirtualDatabaseException("Cannot transfer backend to controller:"
        + controllerDestination + " because database is not distributed");
  }

  //
  // Backup & Checkpoint management
  //

  /**
   * Returns a cluster-wide unique checkpoint name.
   * <p>
   * Unicity is needed since checkpoints are always set cluster-wide.
   * 
   * @param event the reason why this checkpoint is being set
   * @return a cluster-wide unique checkpoint name
   */
  public String buildCheckpointName(String event)
  {
    /*
     * Checkpoints name are now built such as they can be sorted alphabetically
     */
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSSZ");
    return (event + "-" + controller.getControllerName() + "-" + dateFormat
        .format(new Date(System.currentTimeMillis())));
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#backupBackend(String,
   *      String, String, String, String, String, boolean, ArrayList)
   */
  public void backupBackend(String backendName, String login, String password,
      String dumpName, String backuperName, String path, boolean force,
      ArrayList tables) throws VirtualDatabaseException
  {
    // Sanity checks
    if (!isDumpNameAvailable(dumpName))
    {
      throw new VirtualDatabaseException(Translate.get(
          "virtualdatabase.backup.dumpNameError", dumpName));
    }
    if (!force && (getNumberOfEnabledBackends() == 1))
    {
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.backup.onlyOneBackendLeft"));
    }

    // Perform the backup
    BackupBackendOperation backupOperation = new BackupBackendOperation(
        backendName, dumpName);
    try
    {
      addAdminOperation(backupOperation);
      DatabaseBackend db = getAndCheckBackend(backendName, NO_CHECK_BACKEND);
      requestManager.backupBackend(db, login, password, dumpName, backuperName,
          path, tables);
    }
    catch (SQLException sql)
    {
      throw new VirtualDatabaseException(sql);
    }
    finally
    {
      removeAdminOperation(backupOperation);
    }
  }

  protected int getNumberOfEnabledBackends() throws VirtualDatabaseException
  {
    // This check is not sufficient. Disable functionality. (see SEQUOIA-556)
    if (true)
      return -1;

    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get("virtualdatabase.fail.read.lock");
      logger.error(msg, e);
      throw new VirtualDatabaseException(msg, e);
    }

    int nbActive = 0;
    DatabaseBackend b;
    int size = backends.size();
    b = null;
    for (int i = 0; i < size; i++)
    {
      b = (DatabaseBackend) backends.get(i);
      if (b.isReadEnabled() || b.isWriteEnabled())
        // test symetrical to RequestManager.backupBackend()
        nbActive++;
    }

    releaseReadLockBackendLists();

    return nbActive;
  }

  /**
   * Checks if the dump name is available. If the <code>dumpName</code> is
   * already taken by an existing dump, return <code>false</code>; else
   * return <code>true</code>
   * 
   * @param tentativeDumpName tentative dump name we want to check availability
   * @return <code>true</code> is the name is available, <code>false</code>
   *         else.
   */
  public boolean isDumpNameAvailable(String tentativeDumpName)
  {
    DumpInfo[] dumps;
    try
    {
      dumps = getAvailableDumps();
    }
    catch (VirtualDatabaseException e)
    {
      return true;
    }
    for (int i = 0; i < dumps.length; i++)
    {
      DumpInfo dump = dumps[i];
      if (dump.getDumpName().equals(tentativeDumpName))
      {
        return false;
      }
    }
    return true;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#copyDump(java.lang.String,
   *      java.lang.String)
   */
  public void copyDump(String dumpName, String remoteControllerName)
      throws VirtualDatabaseException
  {
    if (!isDistributed())
      throw new VirtualDatabaseException(
          "can not copy dumps on non-distributed virtual database");
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#copyLogFromCheckpoint(java.lang.String,
   *      java.lang.String)
   */
  public void copyLogFromCheckpoint(String dumpName, String controllerName)
      throws VirtualDatabaseException
  {
    if (!hasRecoveryLog())
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));
    if (!isDistributed())
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.not.distributed"));

    /**
     * Implemented in the distributed incarnation of the vdb.
     * 
     * @see org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase#copyLogFromCheckpoint(String,
     *      String)
     */
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#deleteLogUpToCheckpoint(java.lang.String)
   */
  public void deleteLogUpToCheckpoint(String checkpointName)
      throws VirtualDatabaseException
  {
    if (!hasRecoveryLog())
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));

    try
    {
      getRequestManager().getRecoveryLog().deleteLogEntriesBeforeCheckpoint(
          checkpointName);
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(e);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackuperNames()
   */
  public String[] getBackuperNames()
  {
    return requestManager.getBackupManager().getBackuperNames();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getAvailableDumps()
   */
  public DumpInfo[] getAvailableDumps() throws VirtualDatabaseException
  {
    try
    {
      RecoveryLog recoveryLog = requestManager.getRecoveryLog();
      if (recoveryLog == null)
      {
        return new DumpInfo[0];
      }
      else
      {
        ArrayList dumps = recoveryLog.getDumpList();
        return (DumpInfo[]) dumps.toArray(new DumpInfo[dumps.size()]);
      }
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(e);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getDumpFormatForBackuper(java.lang.String)
   */
  public String getDumpFormatForBackuper(String backuperName)
  {
    Backuper backuper = requestManager.getBackupManager().getBackuperByName(
        backuperName);
    if (backuper == null)
    {
      return null;
    }
    return backuper.getDumpFormat();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#initializeFromBackend(java.lang.String,
   *      boolean)
   */
  public void initializeFromBackend(String databaseBackendName, boolean force)
      throws VirtualDatabaseException
  {
    RecoveryLog log = requestManager.getRecoveryLog();
    if (log == null)
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));

    try
    {
      // Check that all backends are in a disabled state without any last known
      // checkpoint
      int size = this.backends.size();
      DatabaseBackend backendToInitializeFrom = null;
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend dbe = (DatabaseBackend) backends.get(i);
        String backendName = dbe.getName();
        if (backendName.equals(databaseBackendName))
        {
          backendToInitializeFrom = dbe;
        }
        if (force)
          continue;
        BackendRecoveryInfo info = log
            .getBackendRecoveryInfo(name, backendName);
        int recoveryLogBackendState = info.getBackendState();
        if ((recoveryLogBackendState != BackendState.DISABLED)
            && (recoveryLogBackendState != BackendState.UNKNOWN)
            // If recovery log does not match runtime state, it means that the
            // log is dead and out of sync, so we ignore.
            && (recoveryLogBackendState == dbe.getStateValue()))
          throw new VirtualDatabaseException("Backend " + backendName
              + " is not in a disabled state (current state is "
              + BackendState.description(recoveryLogBackendState) + ")");
        String checkpoint = info.getCheckpoint();
        if ((checkpoint != null) && !checkpoint.equals(""))
          throw new VirtualDatabaseException("Backend " + backendName
              + " has a last known checkpoint (" + checkpoint + ")");
      }
      if (backendToInitializeFrom == null)
      {
        throw new VirtualDatabaseException("backend " + databaseBackendName
            + " does not exist");
      }
      // Ok, the backends are in a clean state, clean the recovery log
      log.resetRecoveryLog(true);
      // set the last known checkpoint to Initial_empty_recovery_log
      BackendRecoveryInfo info = log.getBackendRecoveryInfo(name,
          databaseBackendName);
      backendToInitializeFrom
          .setLastKnownCheckpoint("Initial_empty_recovery_log");
      backendToInitializeFrom.setState(BackendState.DISABLED);
      info.setCheckpoint("Initial_empty_recovery_log");
      info.setBackendState(BackendState.DISABLED);
      log.storeBackendRecoveryInfo(name, info);
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(e.getMessage());
    }
  }

  /**
   * Remove a checkpoint from the recovery log of this virtual database
   * 
   * @param checkpointName to remove
   * @throws VirtualDatabaseException if an error occurs
   */
  public void removeCheckpoint(String checkpointName)
      throws VirtualDatabaseException
  {
    try
    {
      requestManager.removeCheckpoint(checkpointName);
    }
    catch (Exception e)
    {
      throw new VirtualDatabaseException(e.getMessage());
    }
  }

  /**
   * Delete the dump entry associated to the <code>dumpName</code>.<br />
   * If <code>keepsFile</code> is false, the dump file is also removed from
   * the file system.
   * 
   * @param dumpName name of the dump entry to remove
   * @param keepsFile <code>true</code> if the dump should be also removed
   *            from the file system, <code>false</code> else
   * @throws VirtualDatabaseException if an exception occured while removing the
   *             dump entry or the dump file
   */
  public void deleteDump(String dumpName, boolean keepsFile)
      throws VirtualDatabaseException
  {
    if (dumpName == null)
    {
      throw new VirtualDatabaseException("dump name can not be null");
    }
    RecoveryLog recoveryLog = requestManager.getRecoveryLog();
    if (recoveryLog == null)
    {
      throw new VirtualDatabaseException(
          "no recovery log for the virtual database" + getVirtualDatabaseName());
    }
    DumpInfo dumpInfo;
    try
    {
      dumpInfo = recoveryLog.getDumpInfo(dumpName);
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(e);
    }
    if (dumpInfo == null)
    {
      throw new VirtualDatabaseException("Dump of name " + dumpName
          + " not found in the recovery log of virtual database "
          + getVirtualDatabaseName());
    }
    Backuper backuper = requestManager.getBackupManager().getBackuperByFormat(
        dumpInfo.getDumpFormat());
    if (backuper == null)
    {
      throw new VirtualDatabaseException("No backuper found for format "
          + dumpInfo.getDumpFormat() + " for the virtual database "
          + getVirtualDatabaseName());
    }
    try
    {
      recoveryLog.removeDump(dumpInfo);
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(e);
    }
    if (!keepsFile)
    {
      try
      {
        backuper.deleteDump(dumpInfo.getDumpPath(), dumpInfo.getDumpName());
      }
      catch (BackupException e)
      {
        throw new VirtualDatabaseException(e);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#restoreDumpOnBackend(String,
   *      String, String, String, ArrayList)
   */
  public void restoreDumpOnBackend(String databaseBackendName, String login,
      String password, String dumpName, ArrayList tables)
      throws VirtualDatabaseException
  {
    DatabaseBackend backend = getAndCheckBackend(databaseBackendName,
        NO_CHECK_BACKEND);
    // Backend cannot be null, otherwise the above throws a
    // VirtualDatabaseException

    RestoreDumpOperation restoreOperation = new RestoreDumpOperation(
        databaseBackendName, dumpName);
    try
    {
      addAdminOperation(restoreOperation);
      requestManager.restoreBackendFromBackupCheckpoint(backend, login,
          password, dumpName, tables);
    }
    catch (BackupException e)
    {
      throw new VirtualDatabaseException(e);
    }
    finally
    {
      removeAdminOperation(restoreOperation);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#setBackendLastKnownCheckpoint
   */
  public void setBackendLastKnownCheckpoint(String backendName,
      String checkpoint) throws VirtualDatabaseException
  {
    RecoveryLog log = requestManager.getRecoveryLog();
    DatabaseBackend backend = getAndCheckBackend(backendName, NO_CHECK_BACKEND);
    if (log == null)
      throw new VirtualDatabaseException("No recovery log has been defined");
    else
    {
      if (!backend.isDisabled())
        throw new VirtualDatabaseException(
            "Cannot setLastKnownCheckpoint on a non-disabled backend");
      else
      {
        try
        {
          log.storeBackendRecoveryInfo(this.name,
              new BackendRecoveryInfo(backend.getName(), checkpoint, backend
                  .getStateValue(), this.name));

          backend.setLastKnownCheckpoint(checkpoint);
        }
        catch (SQLException e)
        {
          throw new VirtualDatabaseException(
              "Failed to store recovery info for backend '" + backendName
                  + "' (" + e + ")");
        }
      }
    }
  }

  /**
   * Sets a checkpoint indicating that this vdb has shutdown.
   */
  public void setShutdownCheckpoint()
  {
    RecoveryLog recoveryLog = requestManager.getRecoveryLog();
    if (recoveryLog != null)
      try
      {
        recoveryLog.storeCheckpoint(buildCheckpointName("shutdown"));
      }
      catch (SQLException e)
      {
        logger.warn("Error while setting shutdown checkpoint", e);
      }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#transferDump(java.lang.String,
   *      java.lang.String, boolean)
   */
  public void transferDump(String dumpName, String remoteControllerName,
      boolean noCopy) throws VirtualDatabaseException
  {
    if (!isDistributed())
      throw new VirtualDatabaseException(
          "can not transfer dumps on non-distributed virtual database");
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#updateDumpPath(java.lang.String,
   *      java.lang.String)
   */
  public void updateDumpPath(String dumpName, String newPath)
      throws VirtualDatabaseException
  {
    try
    {
      RecoveryLog recoveryLog = requestManager.getRecoveryLog();
      if (recoveryLog == null)
      {
        throw new VirtualDatabaseException("no recovery log"); // TODO I18N
      }
      else
      {
        recoveryLog.updateDumpPath(dumpName, newPath);
      }
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(e);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#viewCheckpointNames()
   */
  public ArrayList viewCheckpointNames()
  {
    try
    {
      RecoveryLog recoveryLog = requestManager.getRecoveryLog();
      if (recoveryLog == null)
        return new ArrayList();
      else
        return recoveryLog.getCheckpointNames();
    }
    catch (SQLException e)
    {
      return new ArrayList();
    }
  }

  //
  // Thread management mainly used by controller and monitoring
  //

  /**
   * Add a VirtualDatabaseWorkerThread to the list of active threads.
   * 
   * @param thread the VirtualDatabaseWorkerThread to add
   */
  public void addVirtualDatabaseWorkerThread(VirtualDatabaseWorkerThread thread)
  {
    synchronized (activeThreads)
    {
      activeThreads.add(thread);
      incrementCurrentNbOfThread();
    }
  }

  /**
   * Check connection idle times for active threads. Cleans up and shuts down
   * threads where idle time reached a preset timeout.
   */
  protected void checkActiveThreadsIdleConnectionTime()
  {
    List activeThreadsCopy;
    synchronized (activeThreads)
    {
      activeThreadsCopy = new ArrayList(activeThreads);
    }

    for (Iterator iter = activeThreadsCopy.iterator(); iter.hasNext();)
    {
      VirtualDatabaseWorkerThread vdwt = (VirtualDatabaseWorkerThread) iter
          .next();
      long idleTime = vdwt.getIdleTime();
      if (idleTime > idleConnectionTimeout)
      {
        if (logger.isWarnEnabled())
          logger.warn("Connection idle for " + idleTime
              + " ms, cleaning up and shutting down thread");
        vdwt.cleanup();
        vdwt.shutdown();
      }
    }
  }

  /**
   * Substract one to currentNbOfThreads. Warning! This method is not
   * synchronized.
   */
  protected void decreaseCurrentNbOfThread()
  {
    currentNbOfThreads--;
  }

  /**
   * Remove an idle thread. Warning! This method must be called in a
   * synchronized block on activeThreads.
   */
  protected void decreaseIdleThread()
  {
    idleThreads--;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getCurrentNbOfThreads()
   */
  public int getCurrentNbOfThreads()
  {
    return currentNbOfThreads;
  }

  /**
   * Returns the number of idle worker threads. Warning! This method must be
   * called in a synchronized block on activeThreads.
   * 
   * @return int number of idle worker threads
   */
  public int getIdleThreads()
  {
    return idleThreads;
  }

  /**
   * Returns the maxNbOfThreads.
   * 
   * @return int maximum number of threads
   */
  public int getMaxNbOfThreads()
  {
    return maxNbOfThreads;
  }

  /**
   * Returns the maxThreadIdleTime.
   * 
   * @return long maximum thread idle time in ms
   */
  public long getMaxThreadIdleTime()
  {
    return maxThreadIdleTime;
  }

  /**
   * Returns the idleConnectionTimeout.
   * 
   * @return long idle connection timeout in ms
   */
  public long getIdleConnectionTimeout()
  {
    return idleConnectionTimeout;
  }

  /**
   * Returns the minNbOfThreads.
   * 
   * @return int minimum number of threads
   */
  public int getMinNbOfThreads()
  {
    return minNbOfThreads;
  }

  //
  // Thread management mainly used by controller and monitoring
  //

  /**
   * Return the VirtualDatabaseWorkerThread that is currently executing the
   * transaction identified by the provided id.
   * 
   * @param transactionId the transaction id to look for
   * @return the corresponding VirtualDatabaseWorkerThread or null if none is
   *         found
   */
  public VirtualDatabaseWorkerThread getVirtualDatabaseWorkerThreadForTransaction(
      long transactionId)
  {
    synchronized (activeThreads)
    {
      for (Iterator iter = activeThreads.iterator(); iter.hasNext();)
      {
        VirtualDatabaseWorkerThread vdbwt = (VirtualDatabaseWorkerThread) iter
            .next();
        if (vdbwt.getCurrentTransactionId() == transactionId)
          return vdbwt;
      }
    }
    return null;
  }

  /**
   * Return the VirtualDatabaseWorkerThread that is currently executing the
   * persistent connection identified by the provided id.
   * 
   * @param persistentConnectionId the transaction id to look for
   * @return the corresponding VirtualDatabaseWorkerThread or null if none is
   *         found
   */
  public VirtualDatabaseWorkerThread getVirtualDatabaseWorkerThreadForPersistentConnection(
      long persistentConnectionId)
  {
    synchronized (activeThreads)
    {
      for (Iterator iter = activeThreads.iterator(); iter.hasNext();)
      {
        VirtualDatabaseWorkerThread vdbwt = (VirtualDatabaseWorkerThread) iter
            .next();
        if (vdbwt.getPersistentConnectionId() == persistentConnectionId)
          return vdbwt;
      }
    }
    return null;
  }

  /**
   * Adds one to currentNbOfThreads. Warning! This method is not synchronized.
   */
  protected void incrementCurrentNbOfThread()
  {
    currentNbOfThreads++;
  }

  /**
   * Method add an idle thread. Warning! This method must be called in a
   * synchronized block on activeThreads.
   */
  protected void incrementIdleThreadCount()
  {
    idleThreads++;
  }

  /**
   * Returns the poolConnectionThreads.
   * 
   * @return boolean true if threads are pooled
   */
  public boolean isPoolConnectionThreads()
  {
    return poolConnectionThreads;
  }

  /**
   * Sets the maxThreadIdleTime.
   * 
   * @param maxThreadIdleTime The maxThreadIdleTime to set
   */
  public void setMaxThreadIdleTime(long maxThreadIdleTime)
  {
    this.maxThreadIdleTime = maxThreadIdleTime;
  }

  /**
   * Sets the minNbOfThreads.
   * 
   * @param minNbOfThreads The minNbOfThreads to set
   */
  public void setMinNbOfThreads(int minNbOfThreads)
  {
    this.minNbOfThreads = minNbOfThreads;
  }

  /**
   * Sets the poolConnectionThreads.
   * 
   * @param poolConnectionThreads The poolConnectionThreads to set
   */
  public void setPoolConnectionThreads(boolean poolConnectionThreads)
  {
    this.poolConnectionThreads = poolConnectionThreads;
  }

  //
  // Getter/Setter and tools (equals, ...)
  //

  /**
   * Returns the activeThreads list.
   * 
   * @return ArrayList of <code>VirtualDatabaseWorkerThread</code>
   */
  public ArrayList getActiveThreads()
  {
    return activeThreads;
  }

  /**
   * Returns the authentication manager of this virtual database.
   * 
   * @return an <code>AuthenticationManager</code> instance
   */
  public AuthenticationManager getAuthenticationManager()
  {
    return authenticationManager;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackendInformation(String)
   */
  public String getBackendInformation(String backendName)
      throws VirtualDatabaseException
  {
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = "Unable to acquire read lock on backend list in getBackendInformation ("
          + e + ")";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    // Find the backend
    int size = backends.size();
    DatabaseBackend b = null;
    for (int i = 0; i < size; i++)
    {
      b = (DatabaseBackend) backends.get(i);
      if (b.getName().equals(backendName))
        break;
      else
        b = null;
    }

    if (b == null)
    {
      releaseReadLockBackendLists();
      String msg = "Backend " + backendName + " does not exists.";
      logger.warn(msg);
      throw new VirtualDatabaseException(msg);
    }

    releaseReadLockBackendLists();
    return b.getXml();
  }

  /**
   * Return the list of all backends
   * 
   * @return <code>ArrayList</code> of <code>DatabaseBackend</code> Objects
   */
  public ArrayList getBackends()
  {
    return backends;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackendSchema(java.lang.String)
   */
  public String getBackendSchema(String backendName)
      throws VirtualDatabaseException
  {
    DatabaseBackend backend = getAndCheckBackend(backendName, NO_CHECK_BACKEND);
    // we know the backend is not null, otherwise we have a
    // VirtualDatabaseException ...
    try
    {
      return XmlTools.prettyXml(backend.getSchemaXml(true));
    }
    catch (Exception e)
    {
      throw new VirtualDatabaseException(e.getMessage());
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackendState(java.lang.String)
   */
  public String getBackendState(String backendName)
      throws VirtualDatabaseException
  {
    DatabaseBackend backend = getAndCheckBackend(backendName, NO_CHECK_BACKEND);
    return backend.getState();
  }

  /**
   * Retrieves the warnings for the given connection on the first available
   * backend
   * 
   * @param connId the persistent connection id to retrieve warnings from
   * @exception SQLException if a database access error occurs or this method is
   *                called on a closed connection
   * @return connection SQL warnings or null
   */
  public SQLWarning getConnectionWarnings(long connId) throws SQLException
  {
    DatabaseBackend b = getFirstAvailableBackend();
    return b.getPersistentConnectionWarnings(connId);
  }

  /**
   * Forwards call to clearWarnings() to the given persistent connections on
   * each backend.
   * 
   * @param connId the persistent connection id to clear warnings from
   * @exception SQLException if a database access error occurs
   */
  public void clearConnectionWarnings(long connId) throws SQLException
  {
    // Loop round the list in a failfast manner
    if (backends == null)
      return;
    try
    {
      for (Iterator iter = backends.iterator(); iter.hasNext();)
      {
        DatabaseBackend b = (DatabaseBackend) iter.next();
        if (b.isWriteEnabled() && b.isJDBCConnected())
          b.clearPersistentConnectionWarnings(connId);
      }
    }
    catch (ConcurrentModificationException e)
    {
      // loop until available
      clearConnectionWarnings(connId);
    }
  }

  /**
   * Gets the virtual database name to be used by the client (Sequoia driver)
   * This method should be used for local references only (it is faster). For
   * remote RMI calls, use {@link #getVirtualDatabaseName()}.
   * 
   * @return the virtual database name
   * @see VirtualDatabase#getVirtualDatabaseName()
   */
  public String getDatabaseName()
  {
    return name;
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getDatabaseProductName()
   */
  public String getDatabaseProductName()
  {
    return databaseProductNames;
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData
   * @return associated metada for this database
   */
  public VirtualDatabaseDynamicMetaData getDynamicMetaData()
  {
    if (metadata == null)
    {
      metadata = new VirtualDatabaseDynamicMetaData(this);
    }
    return metadata;
  }

  /**
   * Get the current database schema from merging the schemas of all active
   * backends.
   * 
   * @return the current database schema dynamically gathered
   * @throws SQLException if an error occurs
   */
  public DatabaseSchema getDatabaseSchemaFromActiveBackends()
      throws SQLException
  {
    boolean isRaidb1 = requestManager.getLoadBalancer().getRAIDbLevel() == RAIDbLevels.RAIDb1;

    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = "Unable to acquire read lock on backend list in getDatabaseSchemaFromActiveBackends ("
          + e + ")";
      logger.error(msg);
      throw new SQLException(msg);
    }

    DatabaseSchema schema = null;
    try
    {
      // Build the new schema from all active backend's schemas
      int size = backends.size();
      DatabaseBackend b = null;
      for (int i = 0; i < size; i++)
      {
        b = (DatabaseBackend) backends.get(i);
        if (b.isReadEnabled())
        {
          DatabaseSchema backendSchema = b.getDatabaseSchema();
          if (backendSchema != null)
          { // The backend schema might be null during the recovery phase
            if (schema == null)
              schema = new DatabaseSchema(backendSchema);
            else
              schema.mergeSchema(backendSchema);

            // In RAIDb-1 all backends are the same so there is no need to merge
            // and we just take the schema of the first backend
            if (isRaidb1)
              break;
          }
        }
      }

      // Note that if the RecoveryLog points to the same database it will appear
      // in the database schema but this is a normal behavior.
    }
    finally
    {
      releaseReadLockBackendLists();
    }

    return schema;
  }

  /**
   * Get the current database schema from merging the schemas of all active
   * backends. This is needed when a backend is disabled.
   * 
   * @return the current database schema dynamically gathered
   * @throws SQLException if an error occurs
   */
  public DatabaseSchema getDatabaseSchemaFromActiveBackendsAndRefreshDatabaseProductNames()
      throws SQLException
  {
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = "Unable to acquire read lock on backend list in getDatabaseSchemaFromActiveBackendsAndRefreshDatabaseProductNames ("
          + e + ")";
      logger.error(msg);
      throw new SQLException(msg);
    }

    DatabaseSchema schema = null;
    String dbProductNames = "Sequoia";
    try
    {
      // Build the new schema from all active backend's schemas
      int size = backends.size();
      DatabaseBackend b = null;
      for (int i = 0; i < size; i++)
      {
        b = (DatabaseBackend) backends.get(i);
        if (b.isReadEnabled())
        {
          DatabaseSchema backendSchema = b.getDatabaseSchema();
          if (backendSchema != null)
          { // The backend schema might be null during the recovery phase
            if (schema == null)
              schema = new DatabaseSchema(backendSchema);
            else
              schema.mergeSchema(backendSchema);
          }
        }

        // Update the list of database product names
        if (dbProductNames.indexOf(b.getDatabaseProductName()) == -1)
          dbProductNames += "," + b.getDatabaseProductName();
      }
    }
    finally
    {
      releaseReadLockBackendLists();
    }
    databaseProductNames = dbProductNames;

    // Note that if the RecoveryLog points to the same database it will appear
    // in the database schema but this is a normal behavior.

    return schema;
  }

  /**
   * Retrieves the first available backend from this virtual database
   * 
   * @return the first available backend or null if no backend enabled is found
   */
  DatabaseBackend getFirstAvailableBackend()
  {
    // Parse the list in a failfast manner
    if (backends == null)
      return null;
    try
    {
      for (Iterator iter = backends.iterator(); iter.hasNext();)
      {
        DatabaseBackend b = (DatabaseBackend) iter.next();
        if (b.isReadEnabled() && b.isJDBCConnected())
          return b;
      }
    }
    catch (ConcurrentModificationException e)
    {
      return getFirstAvailableBackend();
    }

    return null;
  }

  /**
   * Returns the logger value.
   * 
   * @return Returns the logger.
   */
  public final Trace getLogger()
  {
    return logger;
  }

  /**
   * Returns the maxNbOfConnections.
   * 
   * @return int
   */
  public int getMaxNbOfConnections()
  {
    return maxNbOfConnections;
  }

  /**
   * Returns the number of savepoints that are defined for a given transaction
   * by asking to the request manager.
   * 
   * @param tId the transaction id
   * @return the number of savepoints that are defined in the transaction whose
   *         id is tId
   */
  public int getNumberOfSavepointsInTransaction(long tId)
  {
    return requestManager.getNumberOfSavepointsInTransaction(tId);
  }

  /**
   * Returns the pendingConnections.
   * 
   * @return ArrayList
   */
  public ArrayList getPendingConnections()
  {
    return pendingConnections;
  }

  /**
   * Gets the request manager associated to this database.
   * 
   * @return a <code>RequestManager</code> instance
   */
  public RequestManager getRequestManager()
  {
    return requestManager;
  }

  /**
   * Get the whole static metadata for this virtual database. A new empty
   * metadata object is created if there was none yet. It will be filled later
   * by gatherStaticMetadata() when the backend is enabled.
   * 
   * @return Virtual database static metadata
   */
  public VirtualDatabaseStaticMetaData getStaticMetaData()
  {
    return doGetStaticMetaData();
  }

  /**
   * @see #getStaticMetaData()
   */
  public VirtualDatabaseStaticMetaData doGetStaticMetaData()
  {
    if (staticMetadata == null)
    {
      staticMetadata = new VirtualDatabaseStaticMetaData(this);
    }
    return staticMetadata;
  }

  /**
   * Gets the virtual database name to be used by the client (Sequoia driver)
   * 
   * @return the virtual database name
   */
  public String getVirtualDatabaseName()
  {
    return name;
  }

  /**
   * Returns the current SQL monitor
   * 
   * @return a <code>SQLMonitoring</code> instance or null if no monitor is
   *         defined
   */
  public SQLMonitoring getSQLMonitor()
  {
    return sqlMonitor;
  }

  /**
   * Return the sql short form length to use when reporting an error.
   * 
   * @return sql short form length
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getSqlShortForm(int)
   */
  public int getSqlShortFormLength()
  {
    return sqlShortFormLength;
  }

  /**
   * Returns the totalOrderQueue value.
   * 
   * @return Returns the totalOrderQueue.
   */
  public LinkedList getTotalOrderQueue()
  {
    return totalOrderQueue;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#hasRecoveryLog()
   */
  public boolean hasRecoveryLog()
  {
    RecoveryLog log = requestManager.getRecoveryLog();
    if (log == null)
      return false;
    else
      return true;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#hasResultCache()
   */
  public boolean hasResultCache()
  {
    AbstractResultCache cache = requestManager.getResultCache();
    if (cache == null)
      return false;
    else
      return true;
  }

  /**
   * Sets the authentication manager for this virtual database.
   * 
   * @param authenticationManager the <code>AuthenticationManager</code> to
   *            set
   */
  public void setAuthenticationManager(
      AuthenticationManager authenticationManager)
  {
    this.authenticationManager = authenticationManager;
  }

  /**
   * Sets a new static database schema for this database if no one exist or
   * merge the given schema to the existing one. A static schema can only be
   * replaced by another static schema.
   * 
   * @param schema the new database shema
   */
  public void setStaticDatabaseSchema(DatabaseSchema schema)
  {
    if (requestManager != null)
      requestManager.setDatabaseSchema(schema, true);
    else
      logger
          .warn("Unable to set database schema, no request manager has been defined.");
  }

  /**
   * Sets the maxNbOfConnections.
   * 
   * @param maxNbOfConnections The maxNbOfConnections to set
   */
  public void setMaxNbOfConnections(int maxNbOfConnections)
  {
    this.maxNbOfConnections = maxNbOfConnections;
  }

  /**
   * Sets the maxNbOfThreads.
   * 
   * @param maxNbOfThreads The maxNbOfThreads to set
   */
  public void setMaxNbOfThreads(int maxNbOfThreads)
  {
    this.maxNbOfThreads = maxNbOfThreads;
  }

  /**
   * Sets a new request manager for this database.
   * 
   * @param requestManager the new request manager.
   */
  public void setRequestManager(RequestManager requestManager)
  {
    this.requestManager = requestManager;
  }

  /**
   * Sets a new SQL Monitor
   * 
   * @param sqlMonitor the new SQL monitor
   */
  public void setSQLMonitor(SQLMonitoring sqlMonitor)
  {
    this.sqlMonitor = sqlMonitor;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#setMonitoringToActive(boolean)
   */
  public void setMonitoringToActive(boolean active)
      throws VirtualDatabaseException
  {
    if (sqlMonitor == null)
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.monitoring.not.defined"));
    else
      sqlMonitor.setActive(active);
  }

  /**
   * Returns the useStaticResultSetMetaData value.
   * 
   * @return Returns the useStaticResultSetMetaData.
   */
  public final boolean useStaticResultSetMetaData()
  {
    return useStaticResultSetMetaData;
  }

  /**
   * Two virtual databases are equal if they have the same name and group.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the two virtual databases are equals
   */
  public boolean equals(Object other)
  {
    if ((other == null) || (!(other instanceof VirtualDatabase)))
      return false;
    else
    {
      VirtualDatabase db = (VirtualDatabase) other;
      return name.equals(db.getDatabaseName());
    }
  }

  // /////////////////////////////////////////
  // JMX
  // ////////////////////////////////////////

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#cleanMonitoringData()
   */
  public void cleanMonitoringData() throws VirtualDatabaseException
  {
    if (sqlMonitor == null)
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.monitoring.not.defined"));
    else
      sqlMonitor.cleanStats();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#retrieveBackendsData()
   */
  public String[][] retrieveBackendsData() throws VirtualDatabaseException
  {
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get("virtualdatabase.fail.read.lock", e);
      throw new VirtualDatabaseException(msg);
    }
    ArrayList localBackends = this.getBackends();
    int backendListSize = localBackends.size();
    String[][] data = new String[backendListSize][];
    for (int i = 0; i < backendListSize; i++)
    {
      data[i] = ((DatabaseBackend) localBackends.get(i)).getBackendData();
    }
    releaseReadLockBackendLists();
    return data;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackendStatistics(java.lang.String)
   */
  public BackendStatistics getBackendStatistics(String backendName)
      throws VirtualDatabaseException
  {
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get("virtualdatabase.fail.read.lock", e);
      throw new VirtualDatabaseException(msg);
    }
    BackendStatistics stat = null;
    ArrayList backendList = this.getBackends();
    for (Iterator iter = backendList.iterator(); iter.hasNext();)
    {
      DatabaseBackend backend = (DatabaseBackend) iter.next();
      if (backend.getName().equals(backendName))
      {
        stat = backend.getBackendStats();
      }
    }
    releaseReadLockBackendLists();
    return stat;
  }

  /**
   * Add an admin operation to the list of current admin operations
   * 
   * @param operation the operation to add
   */
  protected void addAdminOperation(AbstractAdminOperation operation)
  {
    synchronized (currentAdminOperations)
    {
      currentAdminOperations.add(operation);
    }
  }

  /**
   * Remove a currently executing admin operation. Returns true if the command
   * was successfully removed from the list.
   * 
   * @param operation the admin operation to remove.
   * @return true if operation was found and removed from the list
   */
  protected boolean removeAdminOperation(AbstractAdminOperation operation)
  {
    synchronized (currentAdminOperations)
    {
      currentAdminOperations.notify();
      return currentAdminOperations.remove(operation);
    }
  }

  /**
   * Wait for all current admin operations to complete.
   */
  private void waitForAdminOperationsToComplete()
  {
    synchronized (currentAdminOperations)
    {
      while (!currentAdminOperations.isEmpty())
      {
        for (Iterator iter = currentAdminOperations.iterator(); iter.hasNext();)
        {
          AbstractAdminOperation op = (AbstractAdminOperation) iter.next();
          logger.info("Waiting for command '" + op + "' to complete");
        }
        try
        {
          currentAdminOperations.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }
    }
  }

  //
  // Shutdown
  //

  /**
   * Return true if this database is shutting down.
   * 
   * @return true if this database is shutting down.
   */
  public boolean isShuttingDown()
  {
    return shuttingDown;
  }

  /**
   * Set the VDB shutting down state
   * 
   * @param shuttingDown TRUE when vdb is shutting down
   */
  public void setShuttingDown(boolean shuttingDown)
  {
    this.shuttingDown = shuttingDown;
  }

  /**
   * Returns true if the controller stops accepting new transaction
   * 
   * @return true if new transactions are rejected
   */
  public boolean isRejectingNewTransaction()
  {
    return refusingNewTransaction;
  }

  /**
   * Set to true if the controller should stop accepting new transactions.
   * 
   * @param canAcceptNewTransaction true to stop accepting new transactions
   */
  public void setRejectingNewTransaction(boolean canAcceptNewTransaction)
  {
    this.refusingNewTransaction = canAcceptNewTransaction;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#shutdown(int)
   */
  public void shutdown(int level)
  {
    if (idleConnectionTimeout != 0)
      idleConnectionChecker.kill();

    VirtualDatabaseShutdownThread vdst = null;
    String msg = Translate.get("virtualdatabase.shutting.down", this
        .getVirtualDatabaseName());
    synchronized (this)
    {
      if (shuttingDown && !(level == Constants.SHUTDOWN_FORCE))
        return;
      switch (level)
      {
        case Constants.SHUTDOWN_WAIT :
          vdst = new VirtualDatabaseWaitShutdownThread(this);
          msg = Translate.get("virtualdatabase.shutdown.type.wait", this
              .getVirtualDatabaseName());
          logger.info(msg);
          endUserLogger.info(msg);
          break;
        case Constants.SHUTDOWN_SAFE :
          shuttingDown = true;
          vdst = new VirtualDatabaseSafeShutdownThread(this);
          msg = Translate.get("virtualdatabase.shutdown.type.safe", this
              .getVirtualDatabaseName());
          logger.info(msg);
          endUserLogger.info(msg);
          break;
        case Constants.SHUTDOWN_FORCE :
          shuttingDown = true;
          vdst = new VirtualDatabaseForceShutdownThread(this);
          msg = Translate.get("virtualdatabase.shutdown.type.force", this
              .getVirtualDatabaseName());
          logger.warn(msg);
          endUserLogger.info(msg);
          break;
        default :
          msg = Translate.get("virtualdatabase.shutdown.unknown.level",
              new Object[]{new Integer(level), this.getVirtualDatabaseName()});
          logger.error(msg);
          endUserLogger.error(msg);
          throw new RuntimeException(msg);
      }
    }

    if (level != Constants.SHUTDOWN_FORCE)
    {
      // Wait for all blocking admin operations to complete
      waitForAdminOperationsToComplete();
    }

    Thread thread = new Thread(vdst.getShutdownGroup(), vdst,
        "VirtualDatabase Shutdown Thread");
    thread.start();
    try
    {
      logger.info("Waiting for virtual database " + name + " shutdown");
      thread.join();
      controller.removeVirtualDatabase(name);
      msg = Translate.get("notification.virtualdatabase.shutdown", name);
      logger.info(msg);
      endUserLogger.info(msg);

      if (MBeanServerManager.isJmxEnabled())
      {
        try
        {
          MBeanServerManager.unregister(JmxConstants
              .getVirtualDataBaseObjectName(name));
          if (MBeanServerManager.getInstance().isRegistered(
              JmxConstants.getRecoveryLogObjectName(name)))
          {
            MBeanServerManager.unregister(JmxConstants
                .getRecoveryLogObjectName(name));
          }
          MBeanServerManager.unregister(JmxConstants
              .getAbstractSchedulerObjectName(name));
          MBeanServerManager.unregister(JmxConstants
              .getLoadBalancerObjectName(name));
          MBeanServerManager.unregister(JmxConstants
              .getRequestManagerObjectName(name));
          ArrayList backendNames = getAllBackendNames();
          for (int i = 0; i < backendNames.size(); i++)
          {
            String backendName = (String) backendNames.get(i);
            MBeanServerManager.unregister(JmxConstants
                .getDatabaseBackendObjectName(name, backendName));
          }
        }
        catch (Exception e)
        {
          logger.error(Translate.get("virtualdatabase.fail.unregister.mbean",
              name), e);
        }
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  /**
   * Write the checkpoints for all backends on the recovery log
   */
  public void storeBackendsInfo()
  {
    requestManager.storeBackendsInfo(this.name, getBackends());
  }

  /**
   * Get all users connected to that database
   * 
   * @return an <code>ArrayList</code> of strings containing the clients
   *         username
   */
  public ArrayList viewAllClientNames()
  {
    ArrayList list = this.getActiveThreads();
    int size = list.size();
    ArrayList clients = new ArrayList(size);
    for (int i = 0; i < list.size(); i++)
      clients.add(((VirtualDatabaseWorkerThread) list.get(i)).getUser());
    return clients;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#viewBackendInformation(java.lang.String)
   */
  public String[] viewBackendInformation(String backendName)
      throws VirtualDatabaseException
  {
    DatabaseBackend backend = getAndCheckBackend(backendName, NO_CHECK_BACKEND);
    return backend.getBackendData();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#viewControllerList()
   */
  public String[] viewControllerList()
  {
    return new String[]{viewOwningController()};
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#viewGroupBackends()
   */
  public Hashtable viewGroupBackends() throws VirtualDatabaseException
  {
    Hashtable map = new Hashtable();
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = "Unable to acquire read lock on backend list in getAllBackendNames ("
          + e + ")";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    // Create an ArrayList<BackendInfo> from the backend list
    int size = backends.size();
    ArrayList backendInfos = new ArrayList(size);
    for (int i = 0; i < size; i++)
      backendInfos.add(new BackendInfo(((DatabaseBackend) backends.get(i))));

    releaseReadLockBackendLists();

    // Return a map with the controller JMX name and its ArrayList<BackendInfo>
    map.put(controller.getJmxName(), backendInfos);
    return map;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#viewOwningController()
   */
  public String viewOwningController()
  {
    return controller.getJmxName();
  }

  /**
   * Retrieves this <code>VirtualDatabase</code> object in xml format
   * 
   * @return xml formatted string that conforms to sequoia.dtd
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_VirtualDatabase + " "
        + DatabasesXmlTags.ATT_name + "=\"" + this.getVirtualDatabaseName()
        + "\" " + DatabasesXmlTags.ATT_maxNbOfConnections + "=\""
        + this.getMaxNbOfConnections() + "\" "
        + DatabasesXmlTags.ATT_poolThreads + "=\""
        + this.isPoolConnectionThreads() + "\" "
        + DatabasesXmlTags.ATT_minNbOfThreads + "=\""
        + this.getMinNbOfThreads() + "\" "
        + DatabasesXmlTags.ATT_maxNbOfThreads + "=\""
        + this.getMaxNbOfThreads() + "\" "
        + DatabasesXmlTags.ATT_maxThreadIdleTime + "=\""
        + this.getMaxThreadIdleTime() / 1000 + "\" "
        + DatabasesXmlTags.ATT_sqlDumpLength + "=\"" + this.sqlShortFormLength
        + "\">");

    info.append(getDistributionXml());

    if (this.getSQLMonitor() != null)
      info.append(sqlMonitor.getXml());

    info.append(requestManager.getBackupManager().getXml());

    if (this.getAuthenticationManager() != null)
      info.append(authenticationManager.getXml());

    try
    {
      acquireReadLockBackendLists();
      int size = backends.size();
      for (int i = 0; i < size; i++)
        info.append(((DatabaseBackend) backends.get(i)).getXml());
      releaseReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      logger.error(Translate.get("virtualdatabase.fail.read.lock", e));
    }
    if (requestManager != null)
      info.append(requestManager.getXml());
    info.append("</" + DatabasesXmlTags.ELT_VirtualDatabase + ">");
    return info.toString();
  }

  /**
   * Get the XML dump of the Distribution element if any.
   * 
   * @return ""
   */
  protected String getDistributionXml()
  {
    return "";
  }

  /**
   * Indicates if the virtual database requires that tables referenced in a
   * query exist into the schema. This concerns only write queries. If true,
   * queries containing references to tables that do not exist in the database
   * schema will be rejected. Otherwise, queries will be executed against the
   * backends using the conflicting queue. This is done to avoid unwanted
   * cross-database queries to be executed on the cluster.
   * 
   * @return enforceTableExistenceIntoSchema that is true if tables have to be
   *         found in the database schema to let a query to be executed against
   *         backends
   */
  public boolean enforceTableExistenceIntoSchema()
  {
    return enforceTableExistenceIntoSchema;
  }

  /**
   * Set the list of functions, if defined, that forces the cluster to broadcast
   * a select statement. 
   * TODO: Define format list
   * 
   * @param functionsToBroadcast List of functions to broadcast (list of
   *          Strings?)
   */
  public void setFunctionsToBroadcastList(List functionsToBroadcast)
  {
    this.functionsToBroadcastList = functionsToBroadcast;
  }

  /**
   * Returns the list of functions, if defined, that forces the cluster to
   * broadcast a select statement .
   * 
   * @return Returns functionsToBroadcastList.
   */
  public List getFunctionsToBroadcastList()
  {
    return functionsToBroadcastList;
  }

  /**
   * resumeActivity is used to restart interactivelly a virtual database that
   * would be blocked in a suspended state
   * 
   * @throws VirtualDatabaseException
   */
  public void resumeActivity() throws VirtualDatabaseException
  {
    if (this.isSuspendedFromLocalController())
    {
      boolean threadToInterrupt = false;
      synchronized (syncObject)
      {
        for (Iterator iterator = recoverThreads.iterator(); iterator.hasNext();)
        {
          threadToInterrupt = ((RecoverThread) iterator.next())
              .canBeInterrupted();
          if (threadToInterrupt)
            break;
        }
      }
      if (threadToInterrupt)
      {
        this.getRequestManager().resumeActivity(true);
        return;
      }

    }

    String errorMsg = "This command can only be used :\n"
        + "- on a suspended virtualdatabase\n"
        + "- from the controller where the suspend operation was done.";
    logger.warn("Interactively resuming activity :\n" + errorMsg);
    throw new VirtualDatabaseException(errorMsg);
  }

  /**
   * Sets the isSuspended value.
   * 
   * @param isSuspended The isSuspended value to set.
   */
  public void setSuspendedFromLocalController(boolean isSuspended)
  {
    synchronized (syncObject)
    {
      if (isSuspended)
        ongoingSuspendOperationFromLocalController++;
      else
        ongoingSuspendOperationFromLocalController--;
    }
  }

  /**
   * Returns the isSuspended value.
   * 
   * @return Returns current isSuspended state.
   */
  public boolean isSuspendedFromLocalController()
  {
    synchronized (syncObject)
    {
      return ongoingSuspendOperationFromLocalController > 0;
    }

  }

  /**
   * Return the activity status of the virtual database
   * 
   * @return the current activity status
   */
  public int getActivityStatus()
  {
    synchronized (syncObject)
    {
      if (suspendedState > 0)
        return SUSPENDED;
      else if (suspendingState > 0)
        return SUSPENDING;
      else if (resumingState > 0)
        return RESUMING;
      return RUNNING;
    }
  }

  /**
   * Set the activity status to the given value
   * 
   * @param activityStatus The activity status to set
   */
  public void setActivityStatus(int activityStatus)
  {
    synchronized (syncObject)
    {
      switch (activityStatus)
      {
        case SUSPENDING :
          suspendingState++;
          break;
        case SUSPENDED :
          if (suspendingState > 0)
            suspendingState--;
          suspendedState++;
          break;
        case RESUMING :
          if (suspendedState > 0)
            suspendedState--;
          resumingState++;
          break;
        case RUNNING :
          if (resumingState > 0)
            resumingState--;
          break;
      }
    }
  }
}