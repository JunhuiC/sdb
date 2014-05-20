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
 * Contributor(s): Julie Marguerite, Greg Ward, Nicolas Modrzyk, Vadim Kassin,
 *   Jean-Bernard van Zuylen, Peter Royal, Stephane Giron.
 */

package org.continuent.sequoia.controller.requestmanager;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.Vector;

import javax.management.NotCompliantMBeanException;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.RollbackException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.jmx.management.BackendState;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.jmx.mbeans.RequestManagerMBean;
import org.continuent.sequoia.common.jmx.notifications.SequoiaNotificationList;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.backend.BackendStateListener;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.backup.BackupManager;
import org.continuent.sequoia.controller.backup.Backuper;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.cache.parsing.ParsingCache;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.loadbalancer.LoadBalancerControl;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.recoverylog.BackendRecoveryInfo;
import org.continuent.sequoia.controller.recoverylog.RecoverThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.AlterRequest;
import org.continuent.sequoia.controller.requests.CreateRequest;
import org.continuent.sequoia.controller.requests.DropRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.RequestFactory;
import org.continuent.sequoia.controller.requests.RequestType;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.requests.UpdateRequest;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCommit;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedOpenPersistentConnection;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedReleaseSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollback;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollbackToSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedSetSavepoint;

/**
 * This class defines the Request Manager.
 * <p>
 * The RM is composed of a Request Scheduler, an optional Query Cache, and a
 * Load Balancer and an optional Recovery Log.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:vadim@kase.kz">Vadim Kassin </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:Stephane.Giron@continuent.com>Stephane Giron </a>
 * @version 1.0
 */
public class RequestManager extends AbstractStandardMBean
    implements
      XmlComponent,
      RequestManagerMBean
{

  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Transaction handling
  // 5. Database backend management
  // 6. Getter/Setter (possibly in alphabetical order)
  //

  /** begin timeout in ms */
  protected long                 beginTimeout;

  /** commit timeout in ms */
  protected long                 commitTimeout;

  /** rollback timeout in ms */
  protected long                 rollbackTimeout;

  /** The virtual database owning this Request Manager */
  protected VirtualDatabase      vdb;

  /** The request scheduler to order and schedule requests */
  protected AbstractScheduler    scheduler;

  /** An optional request cache to cache responses to SQL requests */
  protected AbstractResultCache  resultCache;

  /** The request load balancer to use to send requests to the databases */
  protected AbstractLoadBalancer loadBalancer;

  /** An optional recovery log */
  protected RecoveryLog          recoveryLog;

  /** The backup manager responsible for backup and restore of backends */
  protected BackupManager        backupManager;

  // The virtual dabase schema
  protected DatabaseSchema       dbs;

  /** <code>true</code> if schema is no more up-to-date and needs a refresh */
  private boolean                schemaIsDirty                 = false;

  private boolean                schemaIsStatic                = false;

  private boolean                isCaseSensitiveParsing        = false;

  protected ParsingCache         parsingCache                  = null;

  private MetadataCache          metadataCache                 = null;

  // SQL queries parsing granularity according to Scheduler, ResultCache and
  // LoadBalancer required granularity
  protected int                  schedulerParsingranularity    = ParsingGranularities.NO_PARSING;

  private int                    cacheParsingranularity        = ParsingGranularities.NO_PARSING;

  private int                    loadBalancerParsingranularity = ParsingGranularities.NO_PARSING;

  protected int                  requiredParsingGranularity    = ParsingGranularities.NO_PARSING;

  private long                   requestId                     = 0;

  /* end user logger */
  protected static Trace         endUserLogger                 = Trace
                                                                   .getLogger("org.continuent.sequoia.enduser");

  /**
   * Hashtable&lt;Long, TransactionMetaData&gt;, the <code>Long</code> key is
   * the same transaction ID as the <code>transactionId</code> field of its
   * corresponding <code>TransactionMetaData</code> value.
   */
  // <pedantic>This Hashtable should be called transactionMetaData and the
  // TransactionMetaData class should be renamed TransactionMetaDatum but
  // 1/ I don't care
  // 2/ I don't spell check my code :-)
  // Jeff.
  // </pedantic>
  protected Hashtable<Long, TransactionMetaData>            transactionMetaDatas;

  /**
   * Hashtable&lt;Long, String&gt;, the <code>Long</code> being a transaction
   * ID and its corresponding <code>String</code> being the name of a
   * savepoint.
   */
  protected Hashtable<Long, LinkedList<String>>            tidSavepoints;

  protected Trace                logger                        = null;

  private BackendStateListener   backendStateListener;

  //
  // Constructors
  //

  /**
   * Creates a new <code>RequestManager</code> instance.
   * 
   * @param vdb the virtual database this request manager belongs to
   * @param scheduler the Request Scheduler to use
   * @param cache a Query Cache implementation
   * @param loadBalancer the Request Load Balancer to use
   * @param recoveryLog the Log Recovery to use
   * @param beginTimeout timeout in seconds for begin
   * @param commitTimeout timeout in seconds for commit
   * @param rollbackTimeout timeout in seconds for rollback
   * @throws SQLException if an error occurs
   * @throws NotCompliantMBeanException if the MBean is not JMX compliant
   */
  public RequestManager(VirtualDatabase vdb, AbstractScheduler scheduler,
      AbstractResultCache cache, AbstractLoadBalancer loadBalancer,
      RecoveryLog recoveryLog, long beginTimeout, long commitTimeout,
      long rollbackTimeout) throws SQLException, NotCompliantMBeanException
  {
    super(RequestManagerMBean.class);
    this.vdb = vdb;
    assignAndCheckSchedulerLoadBalancerValidity(scheduler, loadBalancer);
    // requiredParsingGranularity is the maximum of each component granularity
    this.resultCache = cache;
    if (resultCache != null)
    {
      cacheParsingranularity = cache.getParsingGranularity();
      if (cacheParsingranularity > requiredParsingGranularity)
        requiredParsingGranularity = cacheParsingranularity;
    }
    setRecoveryLog(recoveryLog);
    initRequestManagerVariables(vdb, beginTimeout, commitTimeout,
        rollbackTimeout);
    logger.info(Translate.get("requestmanager.parsing.granularity",
        ParsingGranularities.getInformation(requiredParsingGranularity)));

    if (MBeanServerManager.isJmxEnabled())
    {
      try
      {
        MBeanServerManager.registerMBean(this, JmxConstants
            .getRequestManagerObjectName(vdb.getVirtualDatabaseName()));

      }
      catch (Exception e)
      {
        logger.error(Translate.get("jmx.failed.register.mbean.requestmanager"));
      }
    }
  }

  /**
   * Retrieve the last known checkpoint from the recovery log and set it for
   * each backend.
   */
  public void initBackendsLastKnownCheckpointFromRecoveryLog()
  {
    if (recoveryLog == null)
      return;
    String databaseName = vdb.getVirtualDatabaseName();
    ArrayList<?> backends = vdb.getBackends();
    int size = backends.size();
    DatabaseBackend backend;
    BackendRecoveryInfo info;
    for (int i = 0; i < size; i++)
    {
      backend = (DatabaseBackend) backends.get(i);
      try
      {
        info = recoveryLog.getBackendRecoveryInfo(databaseName, backend
            .getName());
        String checkpoint = info.getCheckpoint();
        if ((checkpoint == null) || ("".equals(checkpoint)))
        { // No last known checkpoint
          if (info.getBackendState() != BackendState.UNKNOWN)
          {
            String msg = "Backend " + backend.getName()
                + " was not properly stopped, manual recovery will be needed ("
                + info + ")";
            logger.warn(msg);
            throw new SQLException(msg);
          }
        }
        backend.setLastKnownCheckpoint(checkpoint);
      }
      catch (SQLException e)
      {
        if (logger.isDebugEnabled())
        {
          logger.debug(e.getMessage(), e);
        }
        backend.setState(BackendState.UNKNOWN);
        // Also sets backend.setLastKnownCheckpoint(null);
      }
    }
  }

  /**
   * Check that Scheduler and Load Balancer are not null and have compatible
   * RAIDb levels.
   * 
   * @param aScheduler the scheduler to set
   * @param aLoadBalancer the load balancer to set
   * @throws SQLException if an error occurs
   */
  private void assignAndCheckSchedulerLoadBalancerValidity(
      AbstractScheduler aScheduler, AbstractLoadBalancer aLoadBalancer)
      throws SQLException
  {
    if (aScheduler == null)
      throw new SQLException(Translate.get("requestmanager.null.scheduler"));

    if (aLoadBalancer == null)
      throw new SQLException(Translate.get("requestmanager.null.loadbalancer"));

    if (aScheduler.getRAIDbLevel() != aLoadBalancer.getRAIDbLevel())
      throw new SQLException(Translate.get(
          "requestmanager.incompatible.raidb.levels", new String[]{
              "" + aScheduler.getRAIDbLevel(),
              "" + aLoadBalancer.getRAIDbLevel()}));

    // requiredParsingGranularity is the maximum of each component granularity
    setScheduler(aScheduler);
    schedulerParsingranularity = aScheduler.getParsingGranularity();
    requiredParsingGranularity = schedulerParsingranularity;
    setLoadBalancer(aLoadBalancer);
    loadBalancerParsingranularity = aLoadBalancer.getParsingGranularity();
    if (loadBalancerParsingranularity > requiredParsingGranularity)
      requiredParsingGranularity = loadBalancerParsingranularity;
  }

  /**
   * Get the parsing from the parsing cache or parse the query.
   * 
   * @param request the request to be parsed (request will be parsed after this
   *          call)
   * @throws SQLException if an error occurs during parsing
   */
  public void getParsingFromCacheOrParse(AbstractRequest request)
      throws SQLException
  {
    /*
     * If we need to parse the request, try to get the parsing from the cache.
     * Note that if we have a cache miss but backgroundParsing has been turned
     * on, then this call will start a ParsedThread in background.
     */
    if ((requiredParsingGranularity != ParsingGranularities.NO_PARSING)
        && (!request.isParsed()))
    {
      if (parsingCache == null)
        request.parse(getDatabaseSchema(), requiredParsingGranularity,
            isCaseSensitiveParsing);
      else
        parsingCache.getParsingFromCacheAndParseIfMissing(request);
    }
  }

  /**
   * Method initRequestManagerVariables.
   * 
   * @param vdb the virtual database
   * @param beginTimeout timeout for begin operation
   * @param commitTimeout timeout for commit operation
   * @param rollbackTimeout timeout for rollback operation
   */
  private void initRequestManagerVariables(VirtualDatabase vdb,
      long beginTimeout, long commitTimeout, long rollbackTimeout)
  {
    this.transactionMetaDatas = new Hashtable<Long, TransactionMetaData>();
    this.tidSavepoints = new Hashtable<Long, LinkedList<String>>();
    this.beginTimeout = beginTimeout;
    this.commitTimeout = commitTimeout;
    this.rollbackTimeout = rollbackTimeout;
    this.vdb = vdb;
    logger = Trace
        .getLogger("org.continuent.sequoia.controller.RequestManager."
            + vdb.getDatabaseName());
  }

  //
  // Request Handling
  //

  /**
   * Close the given persistent connection.
   * 
   * @param login login to use to retrieve the right connection pool
   * @param persistentConnectionId id of the persistent connection to close
   */
  public void closePersistentConnection(String login,
      long persistentConnectionId)
  {
    VirtualDatabaseWorkerThread vdbwt = vdb
        .getVirtualDatabaseWorkerThreadForPersistentConnection(persistentConnectionId);
    if (vdbwt != null)
      vdbwt.notifyClose(persistentConnectionId);

    try
    {
      scheduler.scheduleClosePersistentConnection();

      // No need to wait for task completion (cannot fail), so log right away.
      if (recoveryLog != null)
        recoveryLog.logClosePersistentConnection(login, persistentConnectionId);

      loadBalancer.closePersistentConnection(login, persistentConnectionId);
    }
    catch (NoMoreBackendException ignore)
    {
      // Removes ugly trace as reported in SEQUOIA-390
    }
    catch (SQLException e)
    {
      logger.warn("Failed to close persistent connection "
          + persistentConnectionId, e);
    }
    finally
    {
      scheduler.closePersistentConnectionCompleted(persistentConnectionId);
    }
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
    return scheduler.hasPersistentConnection(persistentConnectionId);
  }

  /**
   * Open the given persistent connection.
   * 
   * @param login login to use to retrieve the right connection pool
   * @param persistentConnectionId id of the persistent connection to open
   * @param dmsg message in the total order queue if applicable (null otherwise)
   * @throws SQLException An exception can be thrown when it failed to open a
   *           connection
   */
  public void openPersistentConnection(String login,
      long persistentConnectionId, DistributedOpenPersistentConnection dmsg)
      throws SQLException
  {
    boolean success = false;
    long entryId = -1;
    try
    {
      scheduler.scheduleOpenPersistentConnection(dmsg);

      if (recoveryLog != null)
        entryId = recoveryLog.logOpenPersistentConnection(login,
            persistentConnectionId);

      loadBalancer.openPersistentConnection(login, persistentConnectionId);
      success = true;
    }
    catch (NoMoreBackendException e)
    {
      throw e;
    }
    catch (SQLException e)
    {
      logger.warn("Failed to open persistent connection "
          + persistentConnectionId, e);
      throw e;
    }
    finally
    {
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(entryId, success, 0);
      scheduler.openPersistentConnectionCompleted(persistentConnectionId,
          success);
    }
  }

  private final Object REQUEST_ID_SYNC_OBJECT = new Object();

  /**
   * Initialize the request id with the given value (usually retrieved from the
   * recovery log).
   * 
   * @param requestId new current request identifier
   */
  public void initializeRequestId(long requestId)
  {
    synchronized (REQUEST_ID_SYNC_OBJECT)
    {
      // Use the max operator as a safeguard: IDs may have been delivered but
      // not logged yet.
      this.requestId = Math.max(this.requestId + 1, requestId);
    }
  }

  /**
   * Return the next request identifier (monotically increasing number).
   * 
   * @return a request identifier
   */
  public long getNextRequestId()
  {
    synchronized (REQUEST_ID_SYNC_OBJECT)
    {
      return requestId++;
    }
  }

  /**
   * Perform a read request and return the reply. Call first the scheduler, then
   * the cache (if defined) and finally the load balancer.
   * 
   * @param request the request to execute
   * @return a <code>ControllerResultSet</code> value
   * @exception SQLException if an error occurs
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request)
      throws SQLException
  {
    // Sanity check
    TransactionMetaData tm = null;
    if (!request.isAutoCommit())
    { // Check that the transaction has been started
      Long tid = new Long(request.getTransactionId());
      try
      {
        tm = getTransactionMetaData(tid);
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get("transaction.not.started", tid));
      }

      if (request.isMustBroadcast() && tm.isReadOnly())
      {
        /*
         * If the transaction still shows as read-only, begin has not been
         * logged yet and we need to lazily log this transaction start (see
         * SEQUOIA-1063) -- Note: If this code is invoked from
         * DistributedStatementExecuteQuery, begin will already be logged (in
         * DistributedRequestManager.lazyTransactionStart) and the transaction
         * marker will not be set anymore to read-only which will prevent us
         * from logging twice in the distributed case.
         */
        request.setIsLazyTransactionStart(true);
      }
    }

    getParsingFromCacheOrParse(request);

    //
    // SCHEDULER
    //

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("requestmanager.read.request.schedule",
          new String[]{String.valueOf(request.getId()),
              request.getSqlShortForm(vdb.getSqlShortFormLength())}));

    // Wait for the scheduler to give us the authorization to execute
    scheduler.scheduleReadRequest(request);

    //
    // CACHE
    //

    ControllerResultSet result = null;
    try
    { // Check cache if any
      if ((resultCache != null) && (!request.isMustBroadcast()))
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("requestmanager.read.request.cache.get",
              new String[]{String.valueOf(request.getId()),
                  request.getSqlShortForm(vdb.getSqlShortFormLength())}));

        AbstractResultCacheEntry qce = resultCache.getFromCache(request, true);
        if (qce != null)
        {
          result = qce.getResult();
          if (result != null)
          { // Cache hit !
            if (vdb.getSQLMonitor() != null)
              vdb.getSQLMonitor().logCacheHit(request);

            scheduler.readCompleted(request);
            return result;
          }
        }
      }

      //
      // LOAD BALANCER
      //

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("requestmanager.read.request.balance",
            new String[]{String.valueOf(request.getId()),
                request.getSqlShortForm(vdb.getSqlShortFormLength())}));

      // At this point, we have a result cache miss.

      // Send the request to the load balancer
      result = loadBalancer.statementExecuteQuery(request, metadataCache);

      //
      // UPDATES & NOTIFICATIONS
      //

      // Update cache
      if ((resultCache != null)
          && (request.getCacheAbility() != RequestType.UNCACHEABLE))
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get(
              "requestmanager.read.request.cache.update", new String[]{
                  String.valueOf(request.getId()),
                  request.getSqlShortForm(vdb.getSqlShortFormLength())}));

        resultCache.addToCache(request, result);
        if (tm != null)
          tm.setAltersQueryResultCache(true);
      }
    }
    catch (Exception failed)
    {
      if (resultCache != null)
        resultCache.removeFromPendingQueries(request);
      if (failed instanceof NoMoreBackendException)
        throw (NoMoreBackendException) failed;
      String msg = Translate.get("requestmanager.request.failed", new String[]{
          request.getSqlShortForm(vdb.getSqlShortFormLength()),
          failed.getMessage()});
      if (failed instanceof RuntimeException)
        logger.warn(msg, failed);
      else if (failed instanceof SQLException)
      {
        logger.debug(msg);
        throw (SQLException) failed;
      }
      else
      {
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    finally
    {
      // Notify scheduler of completion
      scheduler.readCompleted(request);
    }
    return result;
  }

  /**
   * Perform a write request and return the number of rows affected Call first
   * the scheduler (if defined), then notify the cache (if defined) and finally
   * call the load balancer.
   * 
   * @param request the request to execute
   * @return number of rows affected
   * @exception SQLException if an error occurs
   */
  public ExecuteUpdateResult statementExecuteUpdate(AbstractWriteRequest request)
      throws SQLException
  {
    boolean hasBeenScheduled = false, schedulerHasBeenNotified = false;
    try
    {
      scheduleExecWriteRequest(request);
      hasBeenScheduled = true;
      ExecuteUpdateResult execWriteRequestResult = null;
      try
      {
        execWriteRequestResult = loadBalanceStatementExecuteUpdate(request);
      }
      catch (AllBackendsFailedException e)
      {
        String msg = Translate
            .get("requestmanager.write.request.failed.unexpected");
        logger.fatal(msg, e);
        endUserLogger.fatal(msg);
        throw new RuntimeException(msg, e);
      }
      updateAndNotifyExecWriteRequest(request, execWriteRequestResult
          .getUpdateCount());
      schedulerHasBeenNotified = true;
      return execWriteRequestResult;
    }
    finally
    {
      if (hasBeenScheduled && !schedulerHasBeenNotified)
        scheduler.writeCompleted(request);
    }
  }

  /**
   * Perform a write request and return the auto generated keys. Call first the
   * scheduler (if defined), then notify the cache (if defined) and finally call
   * the load balancer.
   * 
   * @param request the request to execute
   * @return auto generated keys.
   * @exception SQLException if an error occurs
   */
  public GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request) throws SQLException
  {
    boolean hasBeenScheduled = false, schedulerHasBeenNotified = false;
    try
    {
      scheduleExecWriteRequest(request);
      hasBeenScheduled = true;
      GeneratedKeysResult execWriteRequestWithKeysResult = null;
      try
      {
        execWriteRequestWithKeysResult = loadBalanceStatementExecuteUpdateWithKeys(request);
      }
      catch (AllBackendsFailedException e)
      {
        String msg = Translate
            .get("requestmanager.write.request.keys.failed.unexpected");
        logger.fatal(msg, e);
        endUserLogger.fatal(msg);
        throw new RuntimeException(msg, e);
      }
      updateAndNotifyExecWriteRequest(request, execWriteRequestWithKeysResult
          .getUpdateCount());
      schedulerHasBeenNotified = true;
      return execWriteRequestWithKeysResult;
    }
    finally
    {
      if (hasBeenScheduled && !schedulerHasBeenNotified)
        scheduler.writeCompleted(request);
    }
  }

  /**
   * Execute a call to CallableStatement.execute() and returns a suite of
   * updateCount and/or ResultSets.
   * 
   * @param request the stored procedure to execute
   * @return an <code>ExecuteResult</code> object
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @exception SQLException if an error occurs
   */
  public ExecuteResult statementExecute(AbstractRequest request)
      throws AllBackendsFailedException, SQLException
  {
    // Create a fake stored procedure
    StoredProcedure proc = new StoredProcedure(request.getSqlOrTemplate(),
        request.getEscapeProcessing(), request.getTimeout(), request
            .getLineSeparator());
    proc.setIsAutoCommit(request.isAutoCommit());
    proc.setTransactionId(request.getTransactionId());
    proc.setTransactionIsolation(request.getTransactionIsolation());
    proc.setId(request.getId());
    proc.setLogin(request.getLogin());
    proc.setPreparedStatementParameters(request
        .getPreparedStatementParameters());
    proc.setTimeout(request.getTimeout());
    proc.setMaxRows(request.getMaxRows());
    proc.setPersistentConnection(request.isPersistentConnection());
    proc.setPersistentConnectionId(request.getPersistentConnectionId());

    boolean hasBeenScheduled = false;
    try
    {
      ExecuteResult result;

      // Schedule as a stored procedure
      scheduleStoredProcedure(proc);
      hasBeenScheduled = true;

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("requestmanager.write.stored.procedure",
            new String[]{String.valueOf(request.getId()),
                request.getSqlShortForm(vdb.getSqlShortFormLength())}));

      result = loadBalanceStatementExecute(proc);

      updateRecoveryLogFlushCacheAndRefreshSchema(proc);

      // Notify scheduler of completion
      scheduler.storedProcedureCompleted(proc);

      return result;
    }
    catch (AllBackendsFailedException e)
    {
      throw e;
    }
    finally
    {
      if (hasBeenScheduled)
        scheduler.storedProcedureCompleted(proc);
    }
  }

  /**
   * Schedule a request for execution.
   * 
   * @param request the request to execute
   * @throws SQLException if an error occurs
   */
  public void scheduleExecWriteRequest(AbstractWriteRequest request)
      throws SQLException
  {
    // Sanity check
    if (!request.isAutoCommit())
    { // Check that the transaction has been
      // started
      long tid = request.getTransactionId();
      TransactionMetaData tm = transactionMetaDatas
          .get(new Long(tid));
      if (tm == null)
        throw new SQLException(Translate.get("transaction.not.started", tid));

      // If the transaction still shows as read-only, begin has not been logged
      // yet and we need to log this transaction start lazily (see SEQUOIA-1063)
      if (tm.isReadOnly())
        request.setIsLazyTransactionStart(true);
    }

    getParsingFromCacheOrParse(request);

    //
    // SCHEDULER
    //

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("requestmanager.write.request.schedule",
          new String[]{String.valueOf(request.getId()),
              request.getSqlShortForm(vdb.getSqlShortFormLength())}));

    // Wait for the scheduler to give us the authorization to execute
    try
    {
      scheduler.scheduleWriteRequest(request);
    }
    catch (RollbackException e)
    { // Something bad happened and we need to rollback this transaction
      rollback(request.getTransactionId(), true);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Send the given query to the load balancer. If the request fails, the
   * scheduler is properly notified.
   * 
   * @param request the request to execute
   * @return update count and auto-generated keys
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           query
   * @throws NoMoreBackendException if no backends are left to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public GeneratedKeysResult loadBalanceStatementExecuteUpdateWithKeys(
      AbstractWriteRequest request) throws AllBackendsFailedException,
      NoMoreBackendException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("requestmanager.write.request.balance",
          new String[]{String.valueOf(request.getId()),
              String.valueOf(request.getTransactionId()),
              request.getSqlShortForm(vdb.getSqlShortFormLength())}));

    try
    { // Send the request to the load balancer
      return loadBalancer
          .statementExecuteUpdateWithKeys(request, metadataCache);
    }
    catch (Exception failed)
    {
      if (!vdb.isDistributed())
      { // Notify log of failure
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(request.getLogId(), false, request
              .getExecTimeInMs());
      }

      String msg = Translate.get("requestmanager.request.failed", new String[]{
          request.getSqlShortForm(vdb.getSqlShortFormLength()),
          failed.getMessage()});
      if (failed instanceof RuntimeException)
        logger.warn(msg, failed);

      if (failed instanceof AllBackendsFailedException)
        throw (AllBackendsFailedException) failed;
      else if (failed instanceof SQLException)
        throw (SQLException) failed;
      else if (failed instanceof NoMoreBackendException)
        throw (NoMoreBackendException) failed;
      else
        throw new SQLException(msg);
    }
  }

  /**
   * Send the given query to the load balancer. If the request fails, the
   * scheduler is properly notified.
   * 
   * @param request the request to execute
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           query
   * @exception NoMoreBackendException if no backends are left to execute the
   *              request
   * @throws SQLException if an error occurs
   * @return number of modified lines
   */
  public ExecuteUpdateResult loadBalanceStatementExecuteUpdate(
      AbstractWriteRequest request) throws AllBackendsFailedException,
      NoMoreBackendException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("requestmanager.write.request.balance",
          new String[]{String.valueOf(request.getId()),
              String.valueOf(request.getTransactionId()),
              request.getSqlShortForm(vdb.getSqlShortFormLength())}));

    try
    { // Send the request to the load balancer
      if (request.isUpdate() && (resultCache != null))
      { // Try the optimization if we try to update values that are already
        // up-to-date. Warnings will be lost anyway so forget it
        if (!resultCache.isUpdateNecessary((UpdateRequest) request))
          return new ExecuteUpdateResult(0);
      }
      return loadBalancer.statementExecuteUpdate(request);
    }
    catch (Exception failed)
    {
      if (!vdb.isDistributed())
      { // Notify log of failure
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(request.getLogId(), false, request
              .getExecTimeInMs());
      }

      String msg = Translate.get("requestmanager.request.failed", new String[]{
          request.getSqlShortForm(vdb.getSqlShortFormLength()),
          failed.getMessage()});

      // Error logging
      if (failed instanceof RuntimeException)
        logger.warn(msg, failed);

      // Rethrow exception
      if (failed instanceof AllBackendsFailedException)
        throw (AllBackendsFailedException) failed;
      else if (failed instanceof SQLException)
        throw (SQLException) failed;
      else if (failed instanceof NoMoreBackendException)
        throw (NoMoreBackendException) failed;
      else
        throw new SQLException(msg);
    }
  }

  /**
   * Execute a request using Statement.execute() on the load balancer. Note that
   * we flush the cache before calling the load balancer.
   * 
   * @param request the request to execute.
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   */
  public ExecuteResult loadBalanceStatementExecute(AbstractRequest request)
      throws AllBackendsFailedException, SQLException
  {
    ExecuteResult result;
    //
    // CACHE
    //

    // Flush cache (if any) before
    if (resultCache != null)
    {
      resultCache.flushCache();
    }

    //
    // LOAD BALANCER
    //

    try
    { // Send the request to the load balancer
      result = loadBalancer.statementExecute(request, metadataCache);
      return result;
    }
    catch (Exception failed)
    {
      if (!vdb.isDistributed())
      { // Notify log of failure
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(request.getLogId(), false, request
              .getExecTimeInMs());
      }

      String msg = Translate.get("requestmanager.request.failed", new String[]{
          request.getSqlShortForm(vdb.getSqlShortFormLength()),
          failed.getMessage()});
      if (failed instanceof RuntimeException)
        logger.warn(msg, failed);

      if (failed instanceof AllBackendsFailedException)
        throw (AllBackendsFailedException) failed;
      else if (failed instanceof SQLException)
        throw (SQLException) failed;
      else if (failed instanceof NoMoreBackendException)
        throw (NoMoreBackendException) failed;
      else
        throw (SQLException) new SQLException(msg).initCause(failed);
    }
  }

  /**
   * Update the recovery log, cache, update the database schema if needed and
   * finally notify the scheduler. Note that if an error occurs, the scheduler
   * is always notified.
   * 
   * @param request the request to execute
   * @param updateCount the update count if query was executed with
   *          executeUpdate(), -1 otherwise
   * @throws SQLException if an error occurs
   */
  public void updateAndNotifyExecWriteRequest(AbstractWriteRequest request,
      int updateCount) throws SQLException
  {
    try
    {
      TransactionMetaData tm = null;
      if (!request.isAutoCommit())
      {
        /*
         * This is a write transaction, update the transactional context so that
         * commit/rollback for that transaction will be logged.
         */
        tm = getTransactionMetaData(new Long(request.getTransactionId()));
        tm.setReadOnly(false);
      }

      // Update the recovery log (if there is one and we are not using SingleDB)
      if ((recoveryLog != null)
          && (loadBalancer.getRAIDbLevel() != RAIDbLevels.SingleDB))
        recoveryLog.logRequestExecuteUpdateCompletion(request.getLogId(), true,
            updateCount, request.getExecTimeInMs());

      if (request.altersSomething())
      {
        // Start to update the query result cache first if needed (must be done
        // before altering the schema)
        if (request.altersQueryResultCache() && (resultCache != null))
        {
          if (logger.isDebugEnabled())
            logger.debug(Translate.get(
                "requestmanager.write.request.cache.update", new String[]{
                    String.valueOf(request.getId()),
                    request.getSqlShortForm(vdb.getSqlShortFormLength())}));

          if (tm != null)
            tm.setAltersQueryResultCache(true);
          resultCache.writeNotify(request);
        }

        // Update the schema if needed
        if (request.altersDatabaseSchema()
            && (requiredParsingGranularity != ParsingGranularities.NO_PARSING))
        {
          if (tm != null)
            tm.setAltersDatabaseSchema(true);

          DatabaseSchema currentSchema = getDatabaseSchema();
          if (currentSchema == null)
          {
            // the schema can be set to null during force disable backend.
            // Don't try to update it (see UNICLUSTER-246), just flag it as
            // to be refreshed
            setSchemaIsDirty(true);
          }
          else
          {
            if (request.isCreate())
            { // Add the table to the schema
              CreateRequest createRequest = (CreateRequest) request;
              if (createRequest.getDatabaseTable() != null)
              {
                currentSchema.addTable(new DatabaseTable(
                    ((CreateRequest) request).getDatabaseTable()));
                if (logger.isDebugEnabled())
                  logger.debug(Translate.get("requestmanager.schema.add.table",
                      request.getTableName()));
                // TODO : right now, we ask a complete schema refresh
                // Optimization, we should refresh only this table
                setSchemaIsDirty(true);
              }
              else
                // Some other create statement that modifies the schema, force
                // refresh
                setSchemaIsDirty(true);
            }
            else if (request.isDrop())
            { // Delete the table from the schema
              SortedSet<?> tables = ((DropRequest) request).getTablesToDrop();
              if (tables != null)
              { // Tables to drop !
                for (Iterator<?> iter = tables.iterator(); iter.hasNext();)
                {
                  String tableName = (String) iter.next();
                  DatabaseTable table = currentSchema.getTable(tableName);
                  if (table == null)
                  {
                    // Table not found, force refresh
                    setSchemaIsDirty(true);
                  }
                  else
                  { // Table found, let's try to update the schema
                    if (currentSchema.removeTable(table))
                    {
                      if (logger.isDebugEnabled())
                        logger.debug(Translate.get(
                            "requestmanager.schema.remove.table", table
                                .getName()));

                      // Remove table from depending tables
                      if (logger.isDebugEnabled())
                        logger
                            .debug("Removing table '"
                                + table.getName()
                                + "' from dependending tables in request manager database schema");
                      currentSchema.removeTableFromDependingTables(table);
                    }
                    else
                    {
                      // Table not found, force refresh
                      setSchemaIsDirty(true);
                    }
                  }
                }
              }
            }
            else if (request.isAlter()
                && (requiredParsingGranularity > ParsingGranularities.TABLE))
            { // Add or drop the column from the table
              AlterRequest req = (AlterRequest) request;
              DatabaseTable alteredTable = currentSchema.getTable(req
                  .getTableName());
              if ((alteredTable != null) && (req.getColumn() != null))
              {
                if (req.isDrop())
                  alteredTable.removeColumn(req.getColumn().getName());
                else if (req.isAdd())
                  alteredTable.addColumn(req.getColumn());
                else
                  // Unsupported, force refresh
                  setSchemaIsDirty(true);
              }
              else
                // Table not found, force refresh
                setSchemaIsDirty(true);
            }
            else
              // Unsupported, force refresh
              setSchemaIsDirty(true);
          }
        }
        if (request.altersMetadataCache() && (metadataCache != null))
        {
          if (tm != null)
            tm.setAltersMetadataCache(true);

          metadataCache.flushCache();
        }

        // The following modifications are not specifically handled since there
        // is no cache for such structure yet

        // if (request.altersAggregateList())
        // ;
        // if (request.altersDatabaseCatalog())
        // ;
        // if (request.altersStoredProcedureList())
        // ;
        // if (request.altersUserDefinedTypes())
        // ;
        // if (request.altersUsers())
        // ;

      }
    }
    catch (Exception failed)
    {
      logger.fatal("---");
      logger.fatal("UNEXPECTED ERROR", failed);
      logger.fatal("---");
      String msg = Translate.get("requestmanager.request.failed", new String[]{
          request.getSqlShortForm(vdb.getSqlShortFormLength()),
          failed.getMessage()});
      if (failed instanceof RuntimeException)
        logger.warn(msg, failed);
      else
        logger.warn(msg);
      throw new SQLException(msg);
    }
    finally
    {
      // Notify scheduler
      scheduler.writeCompleted(request);
    }
  }

  /**
   * Call a stored procedure that returns a ResultSet.
   * 
   * @param proc the stored procedure call
   * @return a <code>ControllerResultSet</code> value
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @exception SQLException if an error occurs
   */
  public ControllerResultSet callableStatementExecuteQuery(StoredProcedure proc)
      throws AllBackendsFailedException, SQLException
  {
    ControllerResultSet result = null;
    boolean hasBeenScheduled = false;
    boolean success = false;
    try
    {
      scheduleStoredProcedure(proc);
      hasBeenScheduled = true;

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("requestmanager.read.stored.procedure",
            new String[]{String.valueOf(proc.getId()),
                proc.getSqlShortForm(vdb.getSqlShortFormLength())}));

      result = loadBalanceCallableStatementExecuteQuery(proc);

      updateRecoveryLogFlushCacheAndRefreshSchema(proc);

      success = true;
      return result;
    }
    catch (AllBackendsFailedException e)
    {
      throw e;
    }
    catch (NoMoreBackendException e)
    {
      throw e;
    }
    catch (Exception failed)
    {
      String msg = Translate.get("requestmanager.stored.procedure.failed",
          new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              failed.getMessage()});
      if (failed instanceof SQLException)
      {
        logger.debug(msg);
        throw (SQLException) failed;
      }
      else
      {
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    finally
    {
      if (hasBeenScheduled)
        scheduler.storedProcedureCompleted(proc);
      if (!vdb.isDistributed() && !success)
      { // Notify log of failure
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
              .getExecTimeInMs());
      }
    }
  }

  /**
   * Call a stored procedure that performs an update.
   * 
   * @param proc the stored procedure call
   * @return number of rows affected
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @exception SQLException if an error occurs
   */
  public ExecuteUpdateResult callableStatementExecuteUpdate(StoredProcedure proc)
      throws AllBackendsFailedException, SQLException
  {
    ExecuteUpdateResult result;
    boolean hasBeenScheduled = false;
    boolean success = false;
    try
    {
      // Wait for the scheduler to give us the authorization to execute
      scheduleStoredProcedure(proc);
      hasBeenScheduled = true;

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("requestmanager.write.stored.procedure",
            new String[]{String.valueOf(proc.getId()),
                proc.getSqlShortForm(vdb.getSqlShortFormLength())}));

      result = loadBalanceCallableStatementExecuteUpdate(proc);

      updateRecoveryLogFlushCacheAndRefreshSchema(proc);

      // Notify scheduler of completion
      scheduler.storedProcedureCompleted(proc);

      success = true;
      return result;
    }
    catch (AllBackendsFailedException e)
    {
      throw e;
    }
    catch (Exception failed)
    {
      String msg = Translate.get("requestmanager.stored.procedure.failed",
          new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              failed.getMessage()});
      if (failed instanceof SQLException)
      {
        logger.debug(msg);
        throw (SQLException) failed;
      }
      {
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    finally
    {
      if (hasBeenScheduled)
        scheduler.storedProcedureCompleted(proc);
      if (!vdb.isDistributed() && !success)
      { // Notify log of failure
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
              .getExecTimeInMs());
      }
    }
  }

  /**
   * Execute a call to CallableStatement.execute() and returns a suite of
   * updateCount and/or ResultSets.
   * 
   * @param proc the stored procedure to execute
   * @return an <code>ExecuteResult</code> object
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @exception SQLException if an error occurs
   */
  public ExecuteResult callableStatementExecute(StoredProcedure proc)
      throws AllBackendsFailedException, SQLException
  {
    ExecuteResult result;
    boolean hasBeenScheduled = false;
    boolean success = false;
    try
    {
      scheduleStoredProcedure(proc);
      hasBeenScheduled = true;

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("requestmanager.write.stored.procedure",
            new String[]{String.valueOf(proc.getId()),
                proc.getSqlShortForm(vdb.getSqlShortFormLength())}));

      result = loadBalanceCallableStatementExecute(proc);

      updateRecoveryLogFlushCacheAndRefreshSchema(proc);

      success = true;
      return result;
    }
    catch (AllBackendsFailedException e)
    {
      throw e;
    }
    catch (NoMoreBackendException e)
    {
      throw e;
    }
    catch (Exception failed)
    {
      String msg = Translate.get("requestmanager.stored.procedure.failed",
          new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              failed.getMessage()});
      if (failed instanceof SQLException)
      {
        logger.debug(msg);
        throw (SQLException) failed;
      }
      else
      {
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    finally
    {
      if (hasBeenScheduled)
        scheduler.storedProcedureCompleted(proc);
      if (!vdb.isDistributed() && !success)
      { // Notify log of failure
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
              .getExecTimeInMs());
      }
    }
  }

  /**
   * This method does some sanity check on the given stored procedure and then
   * tries to schedule it. Note that it is more likely that on a stored
   * procedure the scheduler will lock in write the entire database as it does
   * not know which tables are accessed by the procedure.
   * 
   * @param proc the stored procedure to schedule
   * @throws SQLException if an error occurs
   */
  public void scheduleStoredProcedure(StoredProcedure proc) throws SQLException
  {
    // Sanity check
    if (!proc.isAutoCommit())
    { // Check that the transaction has been started
      long tid = proc.getTransactionId();
      TransactionMetaData tm = transactionMetaDatas
          .get(new Long(tid));
      if (tm == null)
        throw new SQLException(Translate.get("transaction.not.started", tid));

      // If the transaction still shows as read-only, begin has not been logged
      // yet and we need to log this transaction start lazily (see SEQUOIA-1063)
      if (tm.isReadOnly())
        proc.setIsLazyTransactionStart(true);
    }

    getParsingFromCacheOrParse(proc);

    //
    // SCHEDULER
    //

    // Wait for the scheduler to give us the authorization to execute
    try
    {
      scheduler.scheduleStoredProcedure(proc);
    }
    catch (RollbackException e)
    { // Something bad happened and we need to rollback this transaction
      rollback(proc.getTransactionId(), true);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Execute a read stored procedure on the load balancer. Note that we flush
   * the cache before calling the load balancer.
   * 
   * @param proc the stored procedure to call
   * @return the corresponding ControllerResultSet
   * @throws SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   */
  public ControllerResultSet loadBalanceCallableStatementExecuteQuery(
      StoredProcedure proc) throws SQLException, AllBackendsFailedException
  {
    DatabaseProcedureSemantic semantic = proc.getSemantic();
    ControllerResultSet result;

    //
    // CACHE
    //

    // Cache is always flushed unless the user has explicitely set the
    // connection to read-only mode in which case we assume that the
    // users deliberately forces the cache not to be flushed when calling
    // this stored procedure.
    if ((resultCache != null) && (!proc.isReadOnly()))
    {
      if ((semantic == null) || (semantic.isWrite()))
        resultCache.flushCache();
    }

    //
    // LOAD BALANCER
    //

    // Send the request to the load balancer
    if (proc.isReadOnly() || ((semantic != null) && (semantic.isReadOnly())))
      result = loadBalancer.readOnlyCallableStatementExecuteQuery(proc,
          metadataCache);
    else
    {
      // Disable fetch size with a distributed execution
      proc.setFetchSize(0);
      result = loadBalancer.callableStatementExecuteQuery(proc, metadataCache);
    }
    return result;
  }

  /**
   * Execute a write stored procedure on the load balancer. Note that we flush
   * the cache before calling the load balancer.
   * 
   * @param proc the stored procedure to call
   * @return the number of updated rows
   * @throws SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   */
  public ExecuteUpdateResult loadBalanceCallableStatementExecuteUpdate(
      StoredProcedure proc) throws AllBackendsFailedException, SQLException
  {
    ExecuteUpdateResult result;
    //
    // CACHE
    //

    // Flush cache (if any) before as we don't properly lock the tables
    if (resultCache != null)
    {
      DatabaseProcedureSemantic semantic = proc.getSemantic();
      if ((semantic == null) || (semantic.isWrite()))
        resultCache.flushCache();
    }

    //
    // LOAD BALANCER
    //

    // Send the request to the load balancer
    result = loadBalancer.callableStatementExecuteUpdate(proc);
    return result;
  }

  /**
   * Execute a write stored procedure on the load balancer. Note that we flush
   * the cache before calling the load balancer.
   * 
   * @param proc the stored procedure to call
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   */
  public ExecuteResult loadBalanceCallableStatementExecute(StoredProcedure proc)
      throws AllBackendsFailedException, SQLException
  {
    DatabaseProcedureSemantic semantic = proc.getSemantic();
    ExecuteResult result;

    //
    // CACHE
    //

    // Flush cache (if any) before as we don't properly lock the tables
    if (resultCache != null)
    {
      if ((semantic == null) || (semantic.isWrite()))
        resultCache.flushCache();
    }

    //
    // LOAD BALANCER
    //

    // Send the request to the load balancer
    if (proc.isReadOnly() || ((semantic != null) && (semantic.isReadOnly())))
      result = loadBalancer.readOnlyCallableStatementExecute(proc,
          metadataCache);
    else
      result = loadBalancer.callableStatementExecute(proc, metadataCache);
    return result;
  }

  /**
   * Update the recovery log with successful completion of the query, flush the
   * cache and force schema refresh if needed.
   * 
   * @param proc the stored procedure to log
   * @throws SQLException if the transaction context could not be updated
   */
  public void updateRecoveryLogFlushCacheAndRefreshSchema(StoredProcedure proc)
      throws SQLException
  {
    // Stored procedures executing on a read-only connection don't update
    // anything
    if (proc.isReadOnly())
      return;

    DatabaseProcedureSemantic semantic = proc.getSemantic();
    TransactionMetaData tm = null;
    if (!proc.isAutoCommit())
      tm = getTransactionMetaData(new Long(proc.getTransactionId()));

    if ((semantic == null) || (semantic.hasDDLWrite()))
    {
      // Schema might have been updated, force refresh
      if ((semantic == null) && (logger.isDebugEnabled()))
        logger.debug("No semantic found for stored procedure "
            + proc.getSqlShortForm(vdb.getSqlShortFormLength())
            + ". Forcing a schema refresh.");
      setSchemaIsDirty(true);

      if (tm != null)
        tm.setAltersDatabaseSchema(true);
    }

    if ((semantic == null) || (semantic.isWrite()))
    {
      // Update the recovery log (if there is one and we are not using SingleDB)
      if ((recoveryLog != null)
          && (loadBalancer.getRAIDbLevel() != RAIDbLevels.SingleDB))
        recoveryLog.logRequestCompletion(proc.getLogId(), true, proc
            .getExecTimeInMs());

      //
      // Update transaction context if needed to force commit to be logged
      // (occurs if callableStatement.executeQuery() is called without semantic
      // information.
      //
      if (tm != null)
      {
        tm.setReadOnly(false);
        tm.setAltersQueryResultCache(true);
      }

      //
      // CACHE
      //

      // Flush cache (if any) after for consistency (we don't know what has been
      // modified by the stored procedure)
      if (resultCache != null)
        resultCache.flushCache();

      if (metadataCache != null)
        metadataCache.flushCache();
    }
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
    return loadBalancer.getPreparedStatementGetMetaData(request);
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
    return loadBalancer.getPreparedStatementGetParameterMetaData(request);
  }

  //
  // Transaction management
  //

  /**
   * Begin a new transaction and return the corresponding transaction
   * identifier. This method is called from the driver when setAutoCommit(false)
   * is called.
   * <p>
   * Note that the transaction begin is not logged in the recovery log by this
   * method, you will have to call logLazyTransactionBegin.
   * 
   * @param login the login used by the connection
   * @param isPersistentConnection true if the transaction is started on a
   *          persistent connection
   * @param persistentConnectionId persistent connection id if the transaction
   *          must be started on a persistent connection
   * @return long a unique transaction identifier
   * @throws SQLException if an error occurs
   * @see #logLazyTransactionBegin(long)
   */
  public long begin(String login, boolean isPersistentConnection,
      long persistentConnectionId) throws SQLException
  {
    long tid = scheduler.getNextTransactionId();
    doBegin(login, tid, isPersistentConnection, persistentConnectionId);
    return tid;
  }

  /**
   * Begins a new transaction for the given <code>login</code> and
   * <code>tid</code>. Informs the loadbalancer and the scheduler about this
   * begun transaction.
   * 
   * @param login the login used by the connection
   * @param tid the tid associed with the transaction to begin
   * @param isPersistentConnection true if the transaction is started on a
   *          persistent connection
   * @param persistentConnectionId persistent connection id if the transaction
   *          must be started on a persistent connection
   * @throws SQLException if an error occurs
   */
  public void doBegin(String login, long tid, boolean isPersistentConnection,
      long persistentConnectionId) throws SQLException
  {
    try
    {
      TransactionMetaData tm = new TransactionMetaData(tid, beginTimeout,
          login, isPersistentConnection, persistentConnectionId);
      scheduler.begin(tm, false, null);

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("transaction.begin", String.valueOf(tid)));

      try
      {
        // Send to load balancer
        loadBalancer.begin(tm);
        // the tid is stored *now* before notifying the scheduler of begin
        // completion to avoid releasing pending write queries before
        // transaction list is updated
        transactionMetaDatas.put(new Long(tid), tm);
      }
      catch (SQLException e)
      {
        throw e;
      }
      finally
      {
        // Notify scheduler for completion in any case
        scheduler.beginCompleted(tid);
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.begin");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Log the begin of a transaction that is started lazily. In fact, we just log
   * the begin when we execute the first write request in a transaction to
   * prevent logging begin/commit for read-only transactions. This also prevents
   * a problem with backends that are disabled with a checkpoint when no request
   * has been played in the transaction but the begin statement has already been
   * logged. In that case, the transaction would not be properly replayed at
   * restore time.
   * 
   * @param transactionId the transaction id begin to log
   * @throws SQLException if an error occurs
   */
  public void logLazyTransactionBegin(long transactionId) throws SQLException
  {
    try
    {
      Long tid = new Long(transactionId);
      TransactionMetaData tm = getTransactionMetaData(tid);

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("transaction.begin.log", String
            .valueOf(transactionId)));

      // Log the begin
      if (recoveryLog != null)
        recoveryLog.logBegin(tm);
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.begin.log");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
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
   *          the recovery log
   * @param forceAbort true if the abort will be forced. Actually, abort will do
   *          nothing when a transaction has savepoints (we do not abort the
   *          whole transaction, so that the user can rollback to a previous
   *          savepoint), except when the connection is closed. In this last
   *          case, if the transaction is not aborted, it prevents future
   *          maintenance operations such as shutdowns, enable/disable from
   *          completing, so we have to force this abort operation. It also
   *          applies to the DeadlockDetectionThread and the cleanup of the
   *          VirtualDatabaseWorkerThread.
   * @throws SQLException if an error occurs
   */
  public void abort(long transactionId, boolean logAbort, boolean forceAbort)
      throws SQLException
  {
    TransactionMetaData tm;
    try
    {
      Long tid = new Long(transactionId);
      boolean tmIsFake = false;
      try
      {
        tm = getTransactionMetaData(tid);
        if (!forceAbort && tidSavepoints.get(tid) != null)
        {
          if (logger.isDebugEnabled())
            logger.debug("Transaction " + transactionId
                + " has savepoints, transaction will not be aborted");
          return;
        }
      }
      catch (SQLException e1)
      {
        logger.warn("No transaction metadata found to abort transaction "
            + transactionId + ". Creating a fake context for abort.");
        // Note that we ignore the persistent connection id (will be retrieved
        // by the connection manager)
        tm = new TransactionMetaData(transactionId, 0,
            RecoveryLog.UNKNOWN_USER, false, 0);
        tm.setReadOnly(!logAbort);
        tmIsFake = true;
        if (tidSavepoints.get(tid) != null)
        {
          if (logger.isDebugEnabled())
            logger.debug("Transaction " + transactionId
                + " has savepoints, transaction will not be aborted");
          return;
        }
      }

      VirtualDatabaseWorkerThread vdbwt = vdb
          .getVirtualDatabaseWorkerThreadForTransaction(transactionId);
      if (vdbwt != null)
        vdbwt.notifyAbort(transactionId);

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("transaction.aborting", String
            .valueOf(transactionId)));

      // Notify the scheduler to abort which is the same as a rollback
      // from a scheduler point of view.

      boolean abortScheduled = false;
      try
      {
        if (!tmIsFake)
        {
          scheduler.rollback(tm, null);
          abortScheduled = true;
        }

        loadBalancer.abort(tm);

        // Update recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(tm.getLogId(), true, 0);

        // Invalidate the query result cache if this transaction has updated the
        // cache or altered the schema
        if ((resultCache != null)
            && (tm.altersQueryResultCache() || tm.altersDatabaseSchema()))
          resultCache.rollback(transactionId);

        // Check for schema modifications that need to be rollbacked
        if (tm.altersDatabaseSchema())
        {
          if (metadataCache != null)
            metadataCache.flushCache();
          setSchemaIsDirty(true);
        }
      }

      // FIXME This whole catch block is ugly. In particular the Rollback task
      // is re-created. The original task is created and posted on the total
      // order queue by the call to loadBalancer.abort() above.
      // The aim is to ensure that we log the rollback when no backends are
      // enabled even for read only transactions. This is needed since a begin
      // could have been previously logged.
      catch (NoMoreBackendException e)
      {
        // Log the query in any case for later recovery (if the request really
        // failed, it will be unloged later)
        if (getRecoveryLog() != null)
        {
          if (logger.isDebugEnabled())
            logger.debug(Translate
                .get("virtualdatabase.distributed.abort.logging.only",
                    transactionId));

          // Wait to be sure that we log in the proper order
          DistributedRollback totalOrderAbort = new DistributedRollback(tm
              .getLogin(), transactionId);
          if (getLoadBalancer().waitForTotalOrder(totalOrderAbort, false))
            getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(
                totalOrderAbort);

          // Update recovery log
          this.getRecoveryLog().logRequestCompletion(tm.getLogId(), false, 0);
          e.setRecoveryLogId(tm.getLogId());
          e.setLogin(tm.getLogin());
        }
        throw e;
      }

      catch (SQLException e)
      {
        /*
         * Check if the we have to remove the entry from the total order queue
         * (wait to be sure that we do it in the proper order)
         */
        DistributedRollback totalOrderAbort = new DistributedRollback(tm
            .getLogin(), transactionId);
        if (loadBalancer.waitForTotalOrder(totalOrderAbort, false))
          loadBalancer
              .removeObjectFromAndNotifyTotalOrderQueue(totalOrderAbort);

        // Update recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(tm.getLogId(), false, 0);

        throw e;
      }
      finally
      {
        if (abortScheduled) // is not set if tm is fake
        {
          // Notify scheduler for completion
          scheduler.rollbackCompleted(tm, true);
        }

        completeTransaction(tid);

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("transaction.aborted", String
              .valueOf(transactionId)));
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.abort");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Get the TransactionMetaData for the given transaction id.
   * 
   * @param tid transaction id
   * @return the TransactionMetaData
   * @throws SQLException if no marker has been found for this transaction
   */
  public TransactionMetaData getTransactionMetaData(Long tid)
      throws SQLException
  {
    TransactionMetaData tm = transactionMetaDatas
        .get(tid);

    if (tm == null)
      throw new SQLException(Translate.get("transaction.marker.not.found",
          String.valueOf(tid)));

    return tm;
  }

  /**
   * Complete the transaction by removing it from the transactionMetaDatas.
   * 
   * @param tid transaction id
   */
  public void completeTransaction(Long tid)
  {
    if (loadBalancer.waitForCompletionPolicy.getPolicy() == WaitForCompletionPolicy.ALL)
    {
      transactionMetaDatas.remove(tid);
      tidSavepoints.remove(tid);
    }
    else
    { // Asynchronous execution, wait for the last backend to complete the
      // transaction
      TransactionMetaData tm = transactionMetaDatas
          .get(tid);
      if (tm != null)
      { // Check that everyone has released its locks
        if (tm.isLockedByBackends())
        {
          return;
        }
        transactionMetaDatas.remove(tid);
      }
      tidSavepoints.remove(tid);
    }
  }

  /**
   * Commit a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param logCommit true if the commit should be logged in the recovery log
   * @param emptyTransaction true if this transaction has not executed any
   *          request
   * @throws SQLException if an error occurs
   */
  public void commit(long transactionId, boolean logCommit,
      boolean emptyTransaction) throws SQLException
  {
    try
    {
      Long tid = new Long(transactionId);
      TransactionMetaData tm = getTransactionMetaData(tid);

      boolean commitScheduled = false;
      boolean success = false;
      try
      {
        scheduler.commit(tm, emptyTransaction, null);
        commitScheduled = true;
        if (!emptyTransaction)
        {
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("transaction.commit", String
                .valueOf(tid)));

          // Send to load balancer
          loadBalancer.commit(tm);

          // Notify the cache
          if (resultCache != null)
            resultCache.commit(tm.getTransactionId());
        }
        success = true;
      }
      catch (SQLException e)
      {
        /*
         * Check if the we have to remove the entry from the total order queue
         * (wait to be sure that we do it in the proper order)
         */
        DistributedCommit totalOrderCommit = new DistributedCommit(tm
            .getLogin(), transactionId);
        if (loadBalancer.waitForTotalOrder(totalOrderCommit, false))
          loadBalancer
              .removeObjectFromAndNotifyTotalOrderQueue(totalOrderCommit);

        throw e;
      }
      catch (AllBackendsFailedException e)
      {
        String msg = "All backends failed to commit transaction "
            + transactionId + " (" + e + ")";
        logger.error(msg);
        throw new SQLException(msg);
      }
      finally
      {
        // Update the recovery log
        // The recovery log will take care of read-only transactions to log
        // only what is required.
        if (recoveryLog != null && tm.getLogId() != 0)
          recoveryLog.logRequestCompletion(tm.getLogId(), success, 0);
        if (commitScheduled)
        {
          // Notify scheduler for completion
          scheduler.commitCompleted(tm, success);
        }
        if (success)
        {
          completeTransaction(tid);
        }
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.commit");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Rollback a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param logRollback true if the rollback should be logged in the recovery
   *          log
   * @throws SQLException if an error occurs
   */
  public void rollback(long transactionId, boolean logRollback)
      throws SQLException
  {
    try
    {
      Long tid = new Long(transactionId);
      TransactionMetaData tm = getTransactionMetaData(tid);

      // Wait for the scheduler to give us the authorization to execute

      boolean rollbackScheduled = false;
      boolean success = false;
      try
      {
        scheduler.rollback(tm, null);
        rollbackScheduled = true;

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("transaction.rollback", String
              .valueOf(transactionId)));

        // Send to load balancer
        loadBalancer.rollback(tm);

        // Invalidate the query result cache if this transaction has updated the
        // cache or altered the schema
        if ((resultCache != null)
            && (tm.altersQueryResultCache() || tm.altersDatabaseSchema()))
          resultCache.rollback(transactionId);

        // Check for schema modifications that need to be rollbacked
        if (tm.altersDatabaseSchema())
        {
          if (metadataCache != null)
            metadataCache.flushCache();
          setSchemaIsDirty(true);
        }

        success = true;
      }
      catch (SQLException e)
      {
        /*
         * Check if the we have to remove the entry from the total order queue
         * (wait to be sure that we do it in the proper order)
         */
        DistributedRollback totalOrderRollback = new DistributedRollback(tm
            .getLogin(), transactionId);
        if (loadBalancer.waitForTotalOrder(totalOrderRollback, false))
        {
          loadBalancer
              .removeObjectFromAndNotifyTotalOrderQueue(totalOrderRollback);
        }
        else if (recoveryLog != null)
        { // Force rollback logging even in the case of failure. The recovery
          // log will take care of empty or read-only transactions that do not
          // need to log the rollback.
          if (!recoveryLog.findRollbackForTransaction(tm.getTransactionId()))
            recoveryLog.logRollback(tm);
          if (!rollbackScheduled)
            recoveryLog.logRequestCompletion(tm.getLogId(), true, 0);
        }

        throw e;
      }
      catch (AllBackendsFailedException e)
      {
        String msg = Translate.get("requestmanager.rollback.failed.all",
            new String[]{String.valueOf(transactionId), e.getMessage()});
        logger.error(msg);
        throw new SQLException(msg);
      }
      finally
      {
        if (rollbackScheduled)
        {
          // Update the recovery log
          if (recoveryLog != null)
            recoveryLog.logRequestCompletion(tm.getLogId(), true, 0);

          // Notify scheduler for completion
          scheduler.rollbackCompleted(tm, success);
        }

        completeTransaction(tid);
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.rollback");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Rollback a transaction given its id to a savepoint given its name.
   * 
   * @param transactionId the transaction id
   * @param savepointName the name of the savepoint
   * @throws SQLException if an error occurs
   */
  public void rollback(long transactionId, String savepointName)
      throws SQLException
  {
    try
    {
      Long tid = new Long(transactionId);
      TransactionMetaData tm = getTransactionMetaData(tid);

      boolean rollbackScheduled = false;
      boolean validSavepoint = false;

      try
      {
        // Check that a savepoint with given name has been set
        if (!hasSavepoint(tid, savepointName))
          throw new SQLException(Translate.get(
              "transaction.savepoint.not.found", new String[]{savepointName,
                  String.valueOf(transactionId)}));

        validSavepoint = true;

        // Wait for the scheduler to give us the authorization to execute
        scheduler.rollback(tm, savepointName, null);
        rollbackScheduled = true;

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("transaction.rollbacksavepoint",
              new String[]{String.valueOf(transactionId), savepointName}));

        // Send to loadbalancer
        loadBalancer.rollbackToSavepoint(tm, savepointName);

        // Update the recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(tm.getLogId(), true, 0);

        // Invalidate the query result cache if this transaction has updated the
        // cache or altered the schema
        if ((resultCache != null)
            && (tm.altersQueryResultCache() || tm.altersDatabaseSchema()))
          resultCache.rollback(transactionId);

        // Check for schema modifications that need to be rollbacked
        if (tm.altersDatabaseSchema())
        {
          if (metadataCache != null)
            metadataCache.flushCache();
          setSchemaIsDirty(true);
        }
      }
      catch (SQLException e)
      {
        /*
         * Check if the we have to remove the entry from the total order queue
         * (wait to be sure that we do it in the proper order)
         */
        DistributedRollbackToSavepoint totalOrderRollbackToSavepoint = new DistributedRollbackToSavepoint(
            transactionId, savepointName);
        if (loadBalancer
            .waitForTotalOrder(totalOrderRollbackToSavepoint, false))
          loadBalancer
              .removeObjectFromAndNotifyTotalOrderQueue(totalOrderRollbackToSavepoint);

        throw e;
      }
      catch (AllBackendsFailedException e)
      {
        String msg = Translate.get(
            "requestmanager.rollbackavepoint.failed.all", new String[]{
                String.valueOf(transactionId), savepointName, e.getMessage()});
        logger.error(msg);
        throw new SQLException(msg);
      }
      finally
      {
        if (rollbackScheduled)
        {
          // Notify scheduler for completion
          scheduler.savepointCompleted(transactionId);
        }

        if (validSavepoint)
          // Remove all the savepoints set after the savepoint we rollback to
          removeSavepoints(tid, savepointName);
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.rollbacksavepoint");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Sets a unnamed savepoint to a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @return the generated id of the new savepoint
   * @throws SQLException if an error occurs
   */
  public int setSavepoint(long transactionId) throws SQLException
  {
    try
    {
      Long tid = new Long(transactionId);
      TransactionMetaData tm = getTransactionMetaData(tid);
      String savepointName = "unnamed savepoint";
      int savepointId;

      boolean setSavepointScheduled = false;
      try
      {
        // Wait for the scheduler to give us the authorization to execute
        savepointId = scheduler.setSavepoint(tm);
        setSavepointScheduled = true;

        savepointName = String.valueOf(savepointId);

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("transaction.setsavepoint", new String[]{
              savepointName, String.valueOf(transactionId)}));

        // Send to loadbalancer
        loadBalancer.setSavepoint(tm, savepointName);

        // Update the recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(tm.getLogId(), true, 0);
      }
      catch (AllBackendsFailedException e)
      {
        String msg = Translate.get("requestmanager.setsavepoint.failed.all",
            new String[]{savepointName, String.valueOf(transactionId),
                e.getMessage()});
        logger.error(msg);
        throw new SQLException(msg);
      }
      catch (SQLException e)
      {
        throw e;
      }
      finally
      {
        if (setSavepointScheduled)
        {
          // Notify scheduler for completion
          scheduler.savepointCompleted(transactionId);
        }
      }

      // Add savepoint name to list of savepoints for this transaction
      addSavepoint(tid, savepointName);
      return savepointId;
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.setsavepoint");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Sets a savepoint given its desired name to a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param name the desired name of the savepoint
   * @throws SQLException if an error occurs
   */
  public void setSavepoint(long transactionId, String name) throws SQLException
  {
    try
    {
      Long tid = new Long(transactionId);
      TransactionMetaData tm = getTransactionMetaData(tid);

      boolean setSavepointScheduled = false;
      try
      {
        // Wait for the scheduler to give us the authorization to execute
        scheduler.setSavepoint(tm, name, null);
        setSavepointScheduled = true;

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("transaction.setsavepoint", new String[]{
              name, String.valueOf(transactionId)}));

        // Send to loadbalancer
        loadBalancer.setSavepoint(tm, name);

        // Update the recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(tm.getLogId(), true, 0);
      }
      catch (AllBackendsFailedException e)
      {
        String msg = Translate.get("requestmanager.setsavepoint.failed.all",
            new String[]{name, String.valueOf(transactionId), e.getMessage()});
        logger.error(msg);
        throw new SQLException(msg);
      }
      catch (SQLException e)
      {
        /*
         * Check if the we have to remove the entry from the total order queue
         * (wait to be sure that we do it in the proper order)
         */
        DistributedSetSavepoint totalOrderSavePoint = new DistributedSetSavepoint(
            tm.getLogin(), transactionId, name);
        if (loadBalancer.waitForTotalOrder(totalOrderSavePoint, false))
          loadBalancer
              .removeObjectFromAndNotifyTotalOrderQueue(totalOrderSavePoint);

        throw e;
      }
      finally
      {
        if (setSavepointScheduled)
        {
          // Notify scheduler for completion
          scheduler.savepointCompleted(transactionId);
        }
      }

      // Add savepoint name to list of savepoints for this transaction
      addSavepoint(tid, name);
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.setsavepoint");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Releases a savepoint given its name from a transaction given its id.
   * 
   * @param transactionId the transaction id
   * @param name the name of the savepoint
   * @exception SQLException if an error occurs
   */
  public void releaseSavepoint(long transactionId, String name)
      throws SQLException
  {
    try
    {
      Long tid = new Long(transactionId);
      TransactionMetaData tm = getTransactionMetaData(tid);

      boolean releasedSavepointScheduled = false;
      try
      {
        // Check that a savepoint with given name has been set
        if (!hasSavepoint(tid, name))
          throw new SQLException(Translate.get(
              "transaction.savepoint.not.found", new String[]{name,
                  String.valueOf(transactionId)}));

        // Wait for the scheduler to give us the authorization to execute
        scheduler.releaseSavepoint(tm, name, null);
        releasedSavepointScheduled = true;

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("transaction.releasesavepoint",
              new String[]{name, String.valueOf(transactionId)}));

        // Send to loadbalancer
        loadBalancer.releaseSavepoint(tm, name);

        // Update the recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(tm.getLogId(), true, 0);
      }
      catch (SQLException e)
      {
        /*
         * Check if the we have to remove the entry from the total order queue
         * (wait to be sure that we do it in the proper order)
         */
        DistributedReleaseSavepoint totalOrderReleaseSavepoint = new DistributedReleaseSavepoint(
            transactionId, name);
        if (loadBalancer.waitForTotalOrder(totalOrderReleaseSavepoint, false))
          loadBalancer
              .removeObjectFromAndNotifyTotalOrderQueue(totalOrderReleaseSavepoint);

        throw e;
      }
      catch (AllBackendsFailedException e)
      {
        String msg = Translate.get(
            "requestmanager.releasesavepoint.failed.all", new String[]{name,
                String.valueOf(transactionId), e.getMessage()});
        logger.error(msg);
        throw new SQLException(msg);
      }
      finally
      {
        if (releasedSavepointScheduled)
        {
          // Notify scheduler for completion
          scheduler.savepointCompleted(transactionId);
        }

        // Remove savepoint for the transaction
        // Will log an error in case of an invalid savepoint
        removeSavepoint(tid, name);
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("fatal.runtime.exception.requestmanager.releasesavepoint");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(e.getMessage());
    }
  }

  /**
   * Adds a given savepoint to a given transaction
   * 
   * @param tid transaction id
   * @param savepointName name of the savepoint
   */
  public void addSavepoint(Long tid, String savepointName)
  {
    LinkedList<String> savepoints = tidSavepoints.get(tid);
    if (savepoints == null)
    { // Lazy list creation
      savepoints = new LinkedList<String>();
      tidSavepoints.put(tid, savepoints);
    }

    savepoints.addLast(savepointName);
  }

  /**
   * Removes a given savepoint for a given transaction
   * 
   * @param tid transaction id
   * @param savepointName name of the savepoint
   */
  public void removeSavepoint(Long tid, String savepointName)
  {
    LinkedList<?> savepoints = tidSavepoints.get(tid);
    if (savepoints == null)
      logger.error("No savepoints found for transaction " + tid);
    else
      savepoints.remove(savepointName);
  }

  /**
   * Removes all the savepoints set after a given savepoint for a given
   * transaction
   * 
   * @param tid transaction id
   * @param savepointName name of the savepoint
   */
  public void removeSavepoints(Long tid, String savepointName)
  {
    LinkedList<?> savepoints = tidSavepoints.get(tid);
    if (savepoints == null)
    {
      logger.error("No savepoints found for transaction " + tid);
      return;
    }

    int index = savepoints.indexOf(savepointName);
    if (index == -1)
      logger.error("No savepoint with name " + savepointName + " found "
          + "for transaction " + tid);
    else if (index < savepoints.size())
      savepoints.subList(index + 1, savepoints.size()).clear();
  }

  /**
   * Check if a given savepoint has been set for a given transaction
   * 
   * @param tid transaction id
   * @param savepointName name of the savepoint
   * @return true if the savepoint exists
   */
  public boolean hasSavepoint(Long tid, String savepointName)
  {
    LinkedList<?> savepoints = tidSavepoints.get(tid);
    if (savepoints == null)
      return false;

    return savepoints.contains(savepointName);
  }

  //
  // Database Backends management
  //

  /**
   * Enable a backend that has been previously added to this virtual database
   * and that is in the disabled state.
   * <p>
   * The backend is enabled without further check.
   * <p>
   * The enableBackend method of the load balancer is called.
   * 
   * @param db The database backend to enable
   * @throws SQLException if an error occurs
   */
  public void enableBackend(DatabaseBackend db) throws SQLException
  {
    enableBackend(db, true);
  }

  /**
   * Enable a backend that has been previously added to this virtual database
   * and that is in the disabled state.
   * <p>
   * The backend is enabled without further check.
   * <p>
   * The enableBackend method of the load balancer is called.
   * 
   * @param db The database backend to enable
   * @param writeEnabled true if backend should be write enabled, false if it
   *          should only be read enabled
   * @throws SQLException if an error occurs
   */
  public void enableBackend(DatabaseBackend db, boolean writeEnabled)
      throws SQLException
  {
    db
        .setSchemaIsNeededByVdb(requiredParsingGranularity != ParsingGranularities.NO_PARSING);
    loadBalancer.enableBackend(db, writeEnabled);
    logger.info(Translate.get("backend.state.enabled", db.getName()));
  }

  /**
   * The backend must have been previously added to this virtual database and be
   * in the disabled state.
   * <p>
   * All the queries since the given checkpoint are played and the backend state
   * is set to enabled when it is completely synchronized.
   * <p>
   * Note that the job is performed in background by a
   * <code>RecoverThread</code>. You can synchronize on thread termination if
   * you need to wait for completion of this task and listen to JMX
   * notifications for the success status.
   * 
   * @param db The database backend to enable
   * @param checkpointName The checkpoint name to restart from
   * @return the JDBC reocver thread synchronizing the backend
   * @throws SQLException if an error occurs
   */
  public RecoverThread enableBackendFromCheckpoint(DatabaseBackend db,
      String checkpointName, boolean isWrite) throws SQLException
  {
    // Sanity checks
    if (recoveryLog == null)
    {
      String msg = Translate.get(
          "recovery.restore.checkpoint.failed.cause.null", checkpointName);
      logger.error(msg);
      throw new SQLException(msg);
    }

    if (db.getStateValue() != BackendState.DISABLED)
      throw new SQLException(Translate.get(
          "recovery.restore.backend.state.invalid", db.getName()));

    db
        .setSchemaIsNeededByVdb(requiredParsingGranularity != ParsingGranularities.NO_PARSING);

    RecoverThread recoverThread = new RecoverThread(scheduler, recoveryLog, db,
        this, checkpointName, isWrite);

    // fire the thread and forget
    // exception will be reported in a jmx notification on the backend.
    recoverThread.start();
    return recoverThread;
  }

  /**
   * Disable a backend that is currently enabled on this virtual database.
   * <p>
   * The backend is disabled without further check.
   * <p>
   * The load balancer disabled method is called on the specified backend.
   * 
   * @param db The database backend to disable
   * @param forceDisable true if disabling must be forced on the backend
   * @throws SQLException if an error occurs
   */
  public void disableBackend(DatabaseBackend db, boolean forceDisable)
      throws SQLException
  {
    if (db.isReadEnabled() || db.isWriteEnabled())
    {
      loadBalancer.disableBackend(db, forceDisable);
      logger.info(Translate.get("backend.state.disabled", db.getName()));
    }
    else
    {
      throw new SQLException(Translate.get("backend.already.disabled", db
          .getName()));
    }
  }

  /**
   * The backend must belong to this virtual database and be in the enabled
   * state. A checkpoint is stored with the given name and the backend is set to
   * the disabling state. The method then wait for all in-flight transactions to
   * complete before disabling completely the backend and storing its last known
   * checkpoint (upon success only).
   * 
   * @param db The database backend to disable
   * @param checkpointName The checkpoint name to store
   * @throws SQLException if an error occurs
   */
  public void disableBackendWithCheckpoint(DatabaseBackend db,
      String checkpointName) throws SQLException
  {
    ArrayList<BackendInfo> backendsArrayList = new ArrayList<BackendInfo>();
    backendsArrayList.add(new BackendInfo(db));
    disableBackendsWithCheckpoint(backendsArrayList, checkpointName);
  }

  /**
   * Disable a list of backends. Only to store only one checkpoint, and to
   * disable all the backends at the same time so the the system is in a
   * coherent state. Consider only the backends that were enabled. The others
   * are left in the state they were before.
   * 
   * @param backendInfos a List of BackendInfo to disable
   * @param checkpointName to store
   * @throws SQLException if an error occurs
   */
  public void disableBackendsWithCheckpoint(
      final ArrayList/* <BackendInfo> */<BackendInfo> backendInfos, String checkpointName)
      throws SQLException
  {
    // Sanity checks
    if (recoveryLog == null)
    {
      String msg = Translate.get("recovery.store.checkpoint.failed.cause.null",
          checkpointName);
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Wait for all pending writes to finish
    logger.info(Translate.get("requestmanager.wait.pending.writes"));
    scheduler.suspendNewWrites();
    scheduler.waitForSuspendedWritesToComplete();

    // Store checkpoint
    recoveryLog.storeCheckpoint(checkpointName);
    logger.info(Translate.get("recovery.checkpoint.stored", checkpointName));

    // Copy the list and consider only the backends that are enabled
    List<DatabaseBackend> backendsToDisable = new ArrayList<DatabaseBackend>();
    Iterator<BackendInfo> iter = backendInfos.iterator();
    while (iter.hasNext())
    {
      BackendInfo backendInfo = iter.next();
      DatabaseBackend db;
      try
      {
        db = vdb.getAndCheckBackend(backendInfo.getName(),
            VirtualDatabase.NO_CHECK_BACKEND);
        if (db.isWriteEnabled() || db.isReadEnabled())
        {
          backendsToDisable.add(db);
        }
      }
      catch (VirtualDatabaseException e)
      {
        if (logger.isWarnEnabled())
        {
          logger.warn(e.getMessage(), e);
        }
      }
    }

    // Signal all backends that they should not begin any new transaction
    int size = backendsToDisable.size();
    for (int i = 0; i < size; i++)
    {
      DatabaseBackend db = backendsToDisable.get(i);
      db.setState(BackendState.DISABLING);
      logger.info(Translate.get("backend.state.disabling", db.getName()));
    }

    // Resume writes
    logger.info(Translate.get("requestmanager.resume.pending.writes"));
    scheduler.resumeWrites();

    // Wait for all current transactions on backends to finish
    for (int i = 0; i < size; i++)
    {
      DatabaseBackend db = backendsToDisable.get(i);
      db.waitForAllTransactionsAndPersistentConnectionsToComplete();
    }

    // Now we can safely disable all backends
    for (int i = 0; i < size; i++)
    {
      DatabaseBackend db = backendsToDisable.get(i);
      db.setLastKnownCheckpoint(checkpointName);
      loadBalancer.disableBackend(db, true);
      logger.info(Translate.get("backend.state.disabled", db.getName()));
    }
  }

  /**
   * Create a backup from the content of a backend.
   * 
   * @param backend the target backend to backup
   * @param login the login to use to connect to the database for the backup
   *          operation
   * @param password the password to use to connect to the database for the
   *          backup operation
   * @param dumpName the name of the dump to create
   * @param backuperName the logical name of the backuper to use
   * @param path the path where to store the dump
   * @param tables the list of tables to backup, null means all tables
   * @throws SQLException if the backup fails
   */
  public void backupBackend(DatabaseBackend backend, String login,
      String password, String dumpName, String backuperName, String path,
      ArrayList<?> tables) throws SQLException
  {
    Backuper backuper = backupManager.getBackuperByName(backuperName);
    if (backuper == null)
      throw new SQLException("No backuper named " + backuperName
          + " was found.");

    boolean enableAfter = false;
    String checkpointName = vdb.buildCheckpointName("backup " + dumpName);
    if (backend.isReadEnabled() || backend.isWriteEnabled())
    { // Disable backend and store checkpoint
      disableBackendWithCheckpoint(backend, checkpointName);
      logger.info(Translate.get("backend.state.disabled", backend.getName()));
      enableAfter = true;
    }
    else
    {
      if (backend.getLastKnownCheckpoint() == null)
      {
        throw new SQLException(Translate.get(
            "controller.backup.no.lastknown.checkpoint", backend.getName()));
      }
    }
    // else backend is already disabled, no checkpoint is stored here, it should
    // have been done at disable time.

    try
    {
      logger.info(Translate.get("controller.backup.start", backend.getName()));

      // Sanity check to be sure that no pending write request is in the pipe
      // for the
      // backend. This is probably excessive paranoia as we should have cleared
      // them
      // previously.
      Vector<?> pending = backend.getPendingRequests();
      if (pending.size() != 0)
      {
        boolean pendingWrites = false;
        int shortFormLength = this.getVirtualDatabase().getSqlShortFormLength();
        for (int i = 0; i < pending.size(); i++)
        {
          AbstractRequest req = (AbstractRequest) pending.get(i);
          if (req != null)
          {
            // For now we tolerate read requests.
            if (req instanceof SelectRequest
                && !((SelectRequest) req).isMustBroadcast())
            {
              logger.warn("Pending read:"
                  + req.toStringShortForm(shortFormLength));
            }
            else
            {
              logger.error("Pending write:"
                  + req.toStringShortForm(shortFormLength));
              pendingWrites = true;
            }
          }
        }

        logger.warn("Pending Requests:" + backend.getPendingRequests().size());
        logger.warn("Read enabled:" + backend.isReadEnabled());
        logger.warn("Write enabled:" + backend.isWriteEnabled());

        if (pendingWrites)
        {
          throw new BackupException(Translate.get(
              "backend.not.ready.for.backup", pending.size()));
        }
      }

      // Let's start the backup
      backend.setState(BackendState.BACKUPING);
      Date dumpDate = backuper.backup(backend, login, password, dumpName, path,
          tables);
      if (recoveryLog != null)
      {
        DumpInfo dump = new DumpInfo(dumpName, dumpDate, path, backuper
            .getDumpFormat(), backend.getLastKnownCheckpoint(), backend
            .getName(), "*");
        recoveryLog.storeDump(dump);
      }

      // Notify that a new dump is available
      backend.notifyJmx(SequoiaNotificationList.VIRTUALDATABASE_NEW_DUMP_LIST);
    }
    catch (BackupException be)
    {
      logger.error(Translate.get("controller.backup.failed"), be);
      throw new SQLException(be.getMessage());
    }
    catch (RuntimeException e)
    {
      logger.error(Translate.get("controller.backup.failed"), e);
      throw new SQLException(e.getMessage());
    }
    finally
    {
      // Switch from BACKUPING to DISABLED STATE
      backend.setState(BackendState.DISABLED);
    }

    logger.info(Translate.get("controller.backup.complete", backend.getName()));

    if (enableAfter)
    {
      RecoverThread thread = enableBackendFromCheckpoint(backend,
          checkpointName, true);
      try
      {
        thread.join();
        // Don't forget to set a new schema on the Request Manager as it was set
        // to null when backend was disabled
        setSchemaIsDirty(true);
      }
      catch (InterruptedException e)
      {
        logger.error("Recovery thread has been interrupted", e);
      }
    }

  }

  /**
   * Restore a dump on a specific backend. The proper Backuper is retrieved
   * automatically according to the dump format stored in the recovery log dump
   * table.
   * <p>
   * This method disables the backend and leave it disabled after recovery
   * process. The user has to call the <code>enableBackendFromCheckpoint</code>
   * after this.
   * 
   * @param backend the backend to restore
   * @param login the login to use to connect to the database for the restore
   *          operation
   * @param password the password to use to connect to the database for the
   *          restore operation
   * @param dumpName the name of the dump to restore
   * @param tables the list of tables to restore, null means all tables
   * @throws BackupException if the restore operation failed
   */
  public void restoreBackendFromBackupCheckpoint(DatabaseBackend backend,
      String login, String password, String dumpName, ArrayList<?> tables)
      throws BackupException
  {
    DumpInfo dumpInfo;
    try
    {
      dumpInfo = recoveryLog.getDumpInfo(dumpName);
    }
    catch (SQLException e)
    {
      throw new BackupException(
          "Recovery log error access occured while retrieving information for dump "
              + dumpName, e);
    }
    if (dumpInfo == null)
      throw new BackupException(
          "No information was found in the dump table for dump " + dumpName);

    Backuper backuper = backupManager.getBackuperByFormat(dumpInfo
        .getDumpFormat());
    if (backuper == null)
      throw new BackupException("No backuper was found to handle dump format "
          + dumpInfo.getDumpFormat());

    if (backend.isReadEnabled())
      throw new BackupException(
          "Unable to restore a dump on an active backend. Disable the backend first.");

    try
    {
      backend.setState(BackendState.RESTORING);

      backuper.restore(backend, login, password, dumpName, dumpInfo
          .getDumpPath(), tables);

      // Set the checkpoint name corresponding to this database dump
      backend.setLastKnownCheckpoint(dumpInfo.getCheckpointName());
      backend.setState(BackendState.DISABLED);
    }
    catch (BackupException be)
    {
      backend.setState(BackendState.UNKNOWN);
      logger.error(Translate.get("controller.backup.recovery.failed"), be);
      throw be;
    }
    finally
    {
      logger.info(Translate.get("controller.backup.recovery.done", backend
          .getName()));
    }
  }

  /**
   * Store all the backends checkpoint in the recoverylog
   * 
   * @param databaseName the virtual database name
   * @param backends the <code>Arraylist</code> of backends
   */
  public void storeBackendsInfo(String databaseName, ArrayList<?> backends)
  {
    if (recoveryLog == null)
      return;
    int size = backends.size();
    DatabaseBackend backend;
    for (int i = 0; i < size; i++)
    {
      backend = (DatabaseBackend) backends.get(i);
      try
      {
        recoveryLog.storeBackendRecoveryInfo(databaseName,
            new BackendRecoveryInfo(backend.getName(), backend
                .getLastKnownCheckpoint(), backend.getStateValue(),
                databaseName));
      }
      catch (SQLException e)
      {
        logger.error(Translate.get("recovery.store.checkpoint.failed",
            new String[]{backend.getName(), e.getMessage()}), e);
      }
    }
  }

  /**
   * Remove a checkpoint and corresponding entries from the log table
   * 
   * @param checkpointName to remove
   */
  public void removeCheckpoint(String checkpointName)
  {
    recoveryLog.removeCheckpoint(checkpointName);
  }

  //
  // Database schema management
  //

  /**
   * Get the <code>DatabaseSchema</code> used by this Request Manager.
   * 
   * @return a <code>DatabaseSchema</code> value
   */
  public synchronized DatabaseSchema getDatabaseSchema()
  {
    try
    {
      // Refresh schema if needed. Note that this will break static schemas if
      // any
      if (schemaIsDirty)
      {
        if (logger.isDebugEnabled())
          logger.debug("Database schema is dirty, refreshing it");
        DatabaseSchema activeSchema = vdb.getDatabaseSchemaFromActiveBackends();
        if (activeSchema != null)
        {
          if (dbs == null)
            dbs = activeSchema;
          else
            dbs.mergeSchema(activeSchema);
          setSchemaIsDirty(false);
        }
      }
    }
    catch (SQLException e)
    {
      logger.error("Unable to refresh schema", e);
    }
    return dbs;
  }

  /**
   * Merge the given schema with the existing database schema.
   * 
   * @param backendSchema The virtual database schema to merge.
   */
  public synchronized void mergeDatabaseSchema(DatabaseSchema backendSchema)
  {
    try
    {
      if (dbs == null)
        setDatabaseSchema(new DatabaseSchema(backendSchema), false);
      else
      {
        dbs.mergeSchema(backendSchema);
        logger.info(Translate
            .get("requestmanager.schema.virtualdatabase.merged.new"));

        if (schedulerParsingranularity != ParsingGranularities.NO_PARSING)
          scheduler.mergeDatabaseSchema(dbs);

        if (cacheParsingranularity != ParsingGranularities.NO_PARSING)
          resultCache.mergeDatabaseSchema(dbs);
      }
    }
    catch (SQLException e)
    {
      logger.error(Translate.get("requestmanager.schema.merge.failed", e
          .getMessage()), e);
    }
  }

  /**
   * Sets the <code>DatabaseSchema</code> to be able to parse the requests and
   * find dependencies.
   * 
   * @param schema a <code>DatabaseSchema</code> value
   * @param isStatic true if the given schema is static
   */
  public synchronized void setDatabaseSchema(DatabaseSchema schema,
      boolean isStatic)
  {
    if (schemaIsStatic)
    {
      if (isStatic)
      {
        logger.warn(Translate
            .get("requestmanager.schema.replace.static.with.new"));
        this.dbs = schema;
      }
      else
        logger.info(Translate.get("requestmanager.schema.ignore.new.dynamic"));
    }
    else
    {
      schemaIsStatic = isStatic;
      this.dbs = schema;
      logger.info(Translate
          .get("requestmanager.schema.set.new.virtualdatabase"));
    }

    if (schedulerParsingranularity != ParsingGranularities.NO_PARSING)
      scheduler.setDatabaseSchema(dbs);

    if (cacheParsingranularity != ParsingGranularities.NO_PARSING)
      resultCache.setDatabaseSchema(dbs);

    // Load balancers do not have a specific database schema to update
  }

  /**
   * Sets the schemaIsDirty value if the backend schema needs to be refreshed.
   * 
   * @param schemaIsDirty The schemaIsDirty to set.
   */
  public synchronized void setSchemaIsDirty(boolean schemaIsDirty)
  {
    this.schemaIsDirty = schemaIsDirty;
  }

  //
  // Getter/Setter methods
  //

  /**
   * Returns the vdb value.
   * 
   * @return Returns the vdb.
   */
  public VirtualDatabase getVirtualDatabase()
  {
    return vdb;
  }

  /**
   * Sets the backup manager for this recovery log
   * 
   * @param currentBackupManager an instance of <code>BackupManager</code>
   */
  public void setBackupManager(BackupManager currentBackupManager)
  {
    this.backupManager = currentBackupManager;
  }

  /**
   * Returns the backupManager value.
   * 
   * @return Returns the backupManager.
   */
  public BackupManager getBackupManager()
  {
    return backupManager;
  }

  /**
   * Get the Request Load Balancer used in this Request Controller.
   * 
   * @return an <code>AbstractLoadBalancer</code> value
   */
  public AbstractLoadBalancer getLoadBalancer()
  {
    return loadBalancer;
  }

  /**
   * Set the Request Load Balancer to use in this Request Controller.
   * 
   * @param loadBalancer a Request Load Balancer implementation
   */
  public void setLoadBalancer(AbstractLoadBalancer loadBalancer)
  {
    if (this.loadBalancer != null)
      throw new RuntimeException(
          "It is not possible to dynamically change a load balancer.");
    this.loadBalancer = loadBalancer;
    if (loadBalancer == null)
      return;
    loadBalancerParsingranularity = loadBalancer.getParsingGranularity();
    if (loadBalancerParsingranularity > requiredParsingGranularity)
      requiredParsingGranularity = loadBalancerParsingranularity;

    if (MBeanServerManager.isJmxEnabled())
    {
      try
      {
        MBeanServerManager.registerMBean(new LoadBalancerControl(loadBalancer),
            JmxConstants
                .getLoadBalancerObjectName(vdb.getVirtualDatabaseName()));
      }
      catch (Exception e)
      {
        logger.error(Translate.get("jmx.failed.register.mbean.loadbalancer"));
      }
    }

  }

  /**
   * Get the result cache (if any) used in this Request Manager.
   * 
   * @return an <code>AbstractResultCache</code> value or null if no Reqsult
   *         Cache has been defined
   */
  public AbstractResultCache getResultCache()
  {
    return resultCache;
  }

  /**
   * Returns the metadataCache value.
   * 
   * @return Returns the metadataCache.
   */
  public MetadataCache getMetadataCache()
  {
    return metadataCache;
  }

  /**
   * Sets the metadataCache value.
   * 
   * @param metadataCache The metadataCache to set.
   */
  public void setMetadataCache(MetadataCache metadataCache)
  {
    this.metadataCache = metadataCache;
  }

  /**
   * Sets the ParsingCache.
   * 
   * @param parsingCache The parsingCache to set.
   */
  public void setParsingCache(ParsingCache parsingCache)
  {
    parsingCache.setRequestManager(this);
    parsingCache.setGranularity(requiredParsingGranularity);
    parsingCache.setCaseSensitiveParsing(isCaseSensitiveParsing);
    this.parsingCache = parsingCache;
  }

  /**
   * Returns the Recovery Log Manager.
   * 
   * @return RecoveryLog the current recovery log (null if none)
   */
  public RecoveryLog getRecoveryLog()
  {
    return recoveryLog;
  }

  /**
   * Sets the Recovery Log Manager.
   * 
   * @param recoveryLog The log recovery to set
   */
  public void setRecoveryLog(RecoveryLog recoveryLog)
  {
    if (recoveryLog == null)
      return;
    this.recoveryLog = recoveryLog;
    loadBalancer.setRecoveryLog(recoveryLog);
    ArrayList<?> backends = vdb.getBackends();
    int size = backends.size();
    backendStateListener = new BackendStateListener(vdb
        .getVirtualDatabaseName(), recoveryLog);
    for (int i = 0; i < size; i++)
      ((DatabaseBackend) backends.get(i))
          .setStateListener(backendStateListener);
  }

  /**
   * Set the Request Cache to use in this Request Controller.
   * 
   * @param cache a Request Cache implementation
   */
  public void setResultCache(AbstractResultCache cache)
  {
    resultCache = cache;
    cacheParsingranularity = cache.getParsingGranularity();
    if (cacheParsingranularity > requiredParsingGranularity)
      requiredParsingGranularity = cacheParsingranularity;
  }

  /**
   * Get the Request Scheduler (if any) used in this Request Controller.
   * 
   * @return an <code>AbstractScheduler</code> value or null if no Request
   *         Scheduler has been defined
   */
  public AbstractScheduler getScheduler()
  {
    return scheduler;
  }

  /**
   * Set the Request Scheduler to use in this Request Controller.
   * 
   * @param scheduler a Request Scheduler implementation
   */
  public void setScheduler(AbstractScheduler scheduler)
  {
    this.scheduler = scheduler;
    schedulerParsingranularity = scheduler.getParsingGranularity();
    if (schedulerParsingranularity > requiredParsingGranularity)
      requiredParsingGranularity = schedulerParsingranularity;
  }

  /**
   * Sets the parsing case sensitivity. If true the request are parsed in a case
   * sensitive way (table/column name must match exactly the case of the names
   * fetched from the database or enforced by a static schema).
   * 
   * @param isCaseSensitiveParsing true if parsing is case sensitive
   */
  public void setCaseSensitiveParsing(boolean isCaseSensitiveParsing)
  {
    this.isCaseSensitiveParsing = isCaseSensitiveParsing;
    if (parsingCache != null)
      parsingCache.setCaseSensitiveParsing(isCaseSensitiveParsing);
  }

  //
  // Debug/Monitoring
  //

  /**
   * Get xml information about this Request Manager
   * 
   * @return <code>String</code> in xml formatted text
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RequestManager + " "
        + DatabasesXmlTags.ATT_caseSensitiveParsing + "=\""
        + isCaseSensitiveParsing + "\" " + DatabasesXmlTags.ATT_beginTimeout
        + "=\"" + beginTimeout / 1000 + "\" "
        + DatabasesXmlTags.ATT_commitTimeout + "=\"" + commitTimeout / 1000
        + "\" " + DatabasesXmlTags.ATT_rollbackTimeout + "=\""
        + rollbackTimeout / 1000 + "\">");
    if (scheduler != null)
      info.append(scheduler.getXml());

    if (metadataCache != null || parsingCache != null || resultCache != null)
    {
      info.append("<" + DatabasesXmlTags.ELT_RequestCache + ">");
      if (metadataCache != null)
        info.append(metadataCache.getXml());
      if (parsingCache != null)
        info.append(parsingCache.getXml());
      if (resultCache != null)
        info.append(resultCache.getXml());
      info.append("</" + DatabasesXmlTags.ELT_RequestCache + ">");
    }

    if (loadBalancer != null)
      info.append(loadBalancer.getXml());
    if (recoveryLog != null)
      info.append(this.recoveryLog.getXml());
    info.append("</" + DatabasesXmlTags.ELT_RequestManager + ">");
    return info.toString();
  }

  /**
   * Returns the backendStateListener value.
   * 
   * @return Returns the backendStateListener.
   */
  public BackendStateListener getBackendStateListener()
  {
    return backendStateListener;
  }

  /**
   * Returns the beginTimeout value.
   * 
   * @return Returns the beginTimeout.
   */
  public long getBeginTimeout()
  {
    return beginTimeout;
  }

  /**
   * Returns the cacheParsingranularity value.
   * 
   * @return Returns the cacheParsingranularity.
   */
  public int getCacheParsingranularity()
  {
    return cacheParsingranularity;
  }

  /**
   * Sets the cacheParsingranularity value.
   * 
   * @param cacheParsingranularity The cacheParsingranularity to set.
   */
  public void setCacheParsingranularity(int cacheParsingranularity)
  {
    this.cacheParsingranularity = cacheParsingranularity;
  }

  /**
   * Returns the commitTimeout value.
   * 
   * @return Returns the commitTimeout.
   */
  public long getCommitTimeout()
  {
    return commitTimeout;
  }

  /**
   * Returns the number of savepoints that are defined for a given transaction
   * 
   * @param tId the transaction id
   * @return the number of savepoints that are defined in the transaction whose
   *         id is tId
   */
  public int getNumberOfSavepointsInTransaction(long tId)
  {
    LinkedList<?> savepoints = tidSavepoints.get(new Long(tId));
    if (savepoints == null)
    {
      return 0;
    }
    else
      return savepoints.size();
  }

  /**
   * Returns the requiredParsingGranularity value.
   * 
   * @return Returns the requiredParsingGranularity.
   */
  public int getRequiredParsingGranularity()
  {
    return requiredParsingGranularity;
  }

  /**
   * Returns the rollbackTimeout value.
   * 
   * @return Returns the rollbackTimeout.
   */
  public long getRollbackTimeout()
  {
    return rollbackTimeout;
  }

  /**
   * Returns the schedulerParsingranularity value.
   * 
   * @return Returns the schedulerParsingranularity.
   */
  public int getSchedulerParsingranularity()
  {
    return schedulerParsingranularity;
  }

  /**
   * Sets the schedulerParsingranularity value.
   * 
   * @param schedulerParsingranularity The schedulerParsingranularity to set.
   */
  public void setSchedulerParsingranularity(int schedulerParsingranularity)
  {
    this.schedulerParsingranularity = schedulerParsingranularity;
  }

  /**
   * Returns the schemaIsStatic value.
   * 
   * @return Returns the schemaIsStatic.
   */
  public boolean isStaticSchema()
  {
    return schemaIsStatic;
  }

  /**
   * Sets the schemaIsStatic value.
   * 
   * @param schemaIsStatic The schemaIsStatic to set.
   */
  public void setStaticSchema(boolean schemaIsStatic)
  {
    this.schemaIsStatic = schemaIsStatic;
  }

  /**
   * Returns the isCaseSensitiveParsing value.
   * 
   * @return Returns the isCaseSensitiveParsing.
   */
  public boolean isCaseSensitiveParsing()
  {
    return isCaseSensitiveParsing;
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "requestmanager";
  }

  /**
   * Resume all transactions, writes and persistent connections.
   * 
   * @param interactiveResume indicates if this resumeActivity is interactively
   *          done, from the console.
   */
  public void resumeActivity(boolean interactiveResume)
  {
    scheduler.resumeWrites();
    scheduler.resumeNewTransactions();
    scheduler.resumeOpenClosePersistentConnection();
  }

  /**
   * Suspend all transactions, writes and persistent connections.
   * 
   * @throws SQLException if an error occured while waiting for writes or
   *           transactions to complete
   */
  public void suspendActivity() throws SQLException
  {
    // Suspend opening and closing of persistent connections
    scheduler.suspendOpenClosePersistentConnection();

    // Suspend new transactions
    scheduler.suspendNewTransactions();

    // Suspend new writes
    scheduler.suspendNewWrites();

    // Wait for suspended activity to complete
    scheduler.waitForSuspendedTransactionsToComplete();
    scheduler.waitForSuspendedWritesToComplete();
    scheduler.waitForPendingOpenClosePersistentConnection();
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.common.jmx.mbeans.RequestManagerMBean#parseSqlRequest(java.lang.String,
   *      java.lang.String)
   */
  public String parseSqlRequest(String sqlRequest, String lineSeparator)
  {
    RequestFactory requestFactory = ControllerConstants.CONTROLLER_FACTORY
        .getRequestFactory();
    AbstractRequest request = requestFactory.requestFromString(sqlRequest,
        false, false, 0, lineSeparator);
    try
    {
      request.parse(getDatabaseSchema(), ParsingGranularities.TABLE,
          isCaseSensitiveParsing);
    }
    catch (SQLException ignored)
    {
    }
    return request.getParsingResultsAsString();
  }
}