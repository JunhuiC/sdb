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
 * Contributor(s): Olivier Fambon, Jean-Bernard van Zuylen, Damian Arregui, Stephane Giron, 
 *   Peter Royal.
 */

package org.continuent.sequoia.controller.requestmanager.distributed;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.continuent.hedera.adapters.MulticastRequestAdapter;
import org.continuent.hedera.adapters.MulticastResponse;
import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoResultAvailableException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.protocol.BlockActivity;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DisableBackendsAndSetCheckpoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedClosePersistentConnection;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCommit;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedOpenPersistentConnection;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedReleaseSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollback;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollbackToSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.FailoverForPersistentConnection;
import org.continuent.sequoia.controller.virtualdatabase.protocol.FailoverForTransaction;
import org.continuent.sequoia.controller.virtualdatabase.protocol.GetRequestResultFromFailoverCache;
import org.continuent.sequoia.controller.virtualdatabase.protocol.NotifyCompletion;
import org.continuent.sequoia.controller.virtualdatabase.protocol.NotifyDisableBackend;
import org.continuent.sequoia.controller.virtualdatabase.protocol.NotifyEnableBackend;
import org.continuent.sequoia.controller.virtualdatabase.protocol.NotifyInconsistency;
import org.continuent.sequoia.controller.virtualdatabase.protocol.ResumeActivity;
import org.continuent.sequoia.controller.virtualdatabase.protocol.SuspendActivity;

/**
 * This class defines a Distributed Request Manager.
 * <p>
 * The DRM is composed of a Request Scheduler, an optional Query Cache, and a
 * Load Balancer and an optional Recovery Log. Unlike a non-distributed Request
 * Manager, this implementation is responsible for synchronizing the different
 * controllers components (schedulers, ...). Functions that are RAIDb level
 * dependent are implemented in sub-classes.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:Damian.Arregui@emicnetworks.com">Damian Arregui</a>
 * @author <a href="mailto:Stephane.Giron@continuent.com>Stephane Giron </a>
 * @version 1.0
 */
public abstract class DistributedRequestManager extends RequestManager
{
  protected DistributedVirtualDatabase dvdb;
  /**
   * List of queries that failed on all backends. Value contained in the map is
   * a boolean indicating whether the request has been scheduled or not before
   * the failure so that we know if the scheduler must be notified or not.
   */
  private HashMap                      failedOnAllBackends;
  /** Unique controller identifier */
  private long                         controllerId;
  /** List of transactions that have executed on multiple controllers */
  protected LinkedList                 distributedTransactions;

  /**
   * Constant to acknowledge the successful completion of a distributed query
   */
  public static final Integer          SUCCESSFUL_COMPLETION = new Integer(-1);

  /**
   * Builds a new <code>DistributedRequestManager</code> instance without
   * cache.
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
   * @throws NotCompliantMBeanException if this class is not a compliant JMX
   *           MBean
   */
  public DistributedRequestManager(DistributedVirtualDatabase vdb,
      AbstractScheduler scheduler, AbstractResultCache cache,
      AbstractLoadBalancer loadBalancer, RecoveryLog recoveryLog,
      long beginTimeout, long commitTimeout, long rollbackTimeout)
      throws SQLException, NotCompliantMBeanException
  {
    super(vdb, scheduler, cache, loadBalancer, recoveryLog, beginTimeout,
        commitTimeout, rollbackTimeout);
    dvdb = vdb;
    failedOnAllBackends = new HashMap();
    distributedTransactions = new LinkedList();
  }

  //
  // Controller identifier related functions
  //

  /**
   * Effective controllerIds are on the upper 16 bits of a long (64 bits).
   * Distributed transaction ids (longs) are layed out as [ControllerId(16bits) |
   * LocalTransactionId(64bits)]. <br/>This constant used in
   * DistributedVirtualDatabase.
   */
  public static final long CONTROLLER_ID_BIT_MASK   = 0xffff000000000000L;
  /**
   * TRANSACTION_ID_BIT_MASK is used to get the transaction id local to the
   * originating controller
   */
  public static final long TRANSACTION_ID_BIT_MASK  = ~CONTROLLER_ID_BIT_MASK;

  /**
   * @see #CONTROLLER_ID_BIT_MASK
   */
  public static final int  CONTROLLER_ID_SHIFT_BITS = 48;

  /**
   * @see #CONTROLLER_ID_BIT_MASK
   */
  public static final long CONTROLLER_ID_BITS       = 0x000000000000ffffL;

  /**
   * Returns the unique controller identifier.
   * 
   * @return Returns the controllerId.
   */
  public long getControllerId()
  {
    return controllerId;
  }

  /**
   * Sets the controller identifier value (this id must be unique). Parameter id
   * must hold on 16 bits (&lt; 0xffff), otherwise an exception is thrown.
   * Effective this.controllerId is <strong>not </strong> set to passed
   * parameter id, but to id &lt;&lt; ControllerIdShiftBits. The reason for all
   * this is that controllerIds are to be carried into ditributed transactions
   * ids, in the upper 16 bits.
   * 
   * @param id The controllerId to set.
   */
  public void setControllerId(long id)
  {
    if ((id & ~CONTROLLER_ID_BITS) != 0)
    {
      String msg = "Out of range controller id (" + id + ")";
      logger.error(msg);
      throw new RuntimeException(msg);
    }
    this.controllerId = (id << CONTROLLER_ID_SHIFT_BITS)
        & CONTROLLER_ID_BIT_MASK;
    if (logger.isDebugEnabled())
      logger.debug("Setting controller identifier to " + id
          + " (shifted value is " + controllerId + ")");

    scheduler.setControllerId(controllerId);
  }

  /**
   * Make the given persistent connection id unique cluster-wide
   * 
   * @param id original id
   * @return unique connection id
   */
  public long getNextConnectionId(long id)
  {
    // 2 first bytes are used for controller id
    // 6 right-most bytes are used for transaction id
    id = id & TRANSACTION_ID_BIT_MASK;
    id = id | controllerId;
    return id;
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#getNextRequestId()
   */
  public long getNextRequestId()
  {
    // We use the same bitmask as for transaction ids

    long id = super.getNextRequestId();
    // 2 first bytes are used for controller id
    // 6 right-most bytes are used for transaction id
    id = id & TRANSACTION_ID_BIT_MASK;
    id = id | controllerId;
    return id;
  }

  /**
   * Get the trace logger of this DistributedRequestManager
   * 
   * @return a <code>Trace</code> object
   */
  public Trace getLogger()
  {
    return logger;
  }

  /**
   * Returns the vdb value.
   * 
   * @return Returns the vdb.
   */
  public VirtualDatabase getVirtualDatabase()
  {
    return dvdb;
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#setScheduler(org.continuent.sequoia.controller.scheduler.AbstractScheduler)
   */
  public void setScheduler(AbstractScheduler scheduler)
  {
    super.setScheduler(scheduler);
    // Note: don't try to use this.dvdb here: setScheduler is called by the
    // c'tor, and dvdb is not set at this time.
    if (vdb.getTotalOrderQueue() == null)
      throw new RuntimeException(
          "New scheduler does not support total ordering and is not compatible with distributed virtual databases.");
  }

  //
  // Database Backends management
  //

  /**
   * Enable a backend that has been previously added to this virtual database
   * and that is in the disabled state. We check we the other controllers if
   * this backend must be enabled in read-only or read-write. The current policy
   * is that the first one to enable this backend will have read-write access to
   * it and others will be in read-only.
   * 
   * @param db The database backend to enable
   * @throws SQLException if an error occurs
   */
  public void enableBackend(DatabaseBackend db) throws SQLException
  {
    int size = dvdb.getAllMemberButUs().size();
    if (size > 0)
    {
      logger.debug(Translate
          .get("virtualdatabase.distributed.enable.backend.check"));

      try
      {
        // Notify other controllers that we enable this backend.
        // No answer is expected.
        dvdb.getMulticastRequestAdapter().multicastMessage(
            dvdb.getAllMemberButUs(),
            new NotifyEnableBackend(new BackendInfo(db)),
            MulticastRequestAdapter.WAIT_NONE,
            dvdb.getMessageTimeouts().getEnableBackendTimeout());
      }
      catch (Exception e)
      {
        String msg = "Error while enabling backend " + db.getName();
        logger.error(msg, e);
        throw new SQLException(msg + "(" + e + ")");
      }
    }

    super.enableBackend(db);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#disableBackend(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      boolean)
   */
  public void disableBackend(DatabaseBackend db, boolean forceDisable)
      throws SQLException
  {
    int size = dvdb.getAllMemberButUs().size();
    if (size > 0)
    {
      logger.debug(Translate.get("virtualdatabase.distributed.disable.backend",
          db.getName()));

      try
      {
        // Notify other controllers that we disable this backend.
        // No answer is expected.
        dvdb.getMulticastRequestAdapter().multicastMessage(
            dvdb.getAllMemberButUs(),
            new NotifyDisableBackend(new BackendInfo(db)),
            MulticastRequestAdapter.WAIT_NONE,
            dvdb.getMessageTimeouts().getDisableBackendTimeout());
      }
      catch (Exception e)
      {
        String msg = "Error while disabling backend " + db.getName();
        logger.error(msg, e);
        throw new SQLException(msg + "(" + e + ")");
      }
    }

    super.disableBackend(db, forceDisable);
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#disableBackendsWithCheckpoint(java.util.ArrayList,
   *      java.lang.String)
   */
  public void disableBackendsWithCheckpoint(ArrayList backendInfos,
      String checkpointName) throws SQLException
  {
    // Perform the distributed call through the group-comm, in order to
    // atomically disable the backend and store a cluster-wide checkpoint.
    try
    {
      // Suspend transactions
      suspendActivity();

      dvdb.sendMessageToControllers(dvdb.getAllMembers(),
          new DisableBackendsAndSetCheckpoint(backendInfos, checkpointName),
          dvdb.getMessageTimeouts().getDisableBackendTimeout());
    }
    catch (Exception e)
    {
      String msg = "Error while disabling backends " + backendInfos;
      logger.error(msg, e);
      throw new SQLException(msg + "(" + e + ")");
    }
    finally
    {
      resumeActivity(false);
    }
  }

  //
  // Failover management
  //

  private class FailureInformation
  {
    private boolean needSchedulerNotification;
    private long    logId = -1;
    private boolean success;
    private boolean disableBackendOnSuccess;
    private int     updateCount;

    /**
     * Creates a new <code>FailureInformation</code> object storing the
     * information about a failure that waits for a remote controller final
     * status.
     * 
     * @param needSchedulerNotification true if the scheduler must be notified
     * @param logId the recovery log id of the query
     */
    public FailureInformation(boolean needSchedulerNotification, long logId)
    {
      this.needSchedulerNotification = needSchedulerNotification;
      this.logId = logId;
    }

    /**
     * Creates a new <code>FailureInformation</code> object storing the final
     * status from a remote controller. This version of the constructor is used
     * when the remote controller sends the final status before the local
     * controller has completed the request. This can happen in the case of a
     * timeout.
     * 
     * @param success indicates the result of the operation on the remote
     *          controller
     * @param disableBackendOnSuccess indicates whether or not the backend
     *          should be disabled.
     * @param updateCount the update count for the request
     */
    public FailureInformation(boolean success, boolean disableBackendOnSuccess,
        int updateCount)
    {
      this.success = success;
      this.disableBackendOnSuccess = disableBackendOnSuccess;
      this.updateCount = updateCount;
    }

    /**
     * Returns the recovery log id value.
     * 
     * @return the recovery log id.
     */
    public final long getLogId()
    {
      return logId;
    }

    /**
     * Sets the local logId for the request
     * 
     * @param logId the log id to set
     */
    public void setLogId(long logId)
    {
      this.logId = logId;
    }

    /**
     * Returns true if scheduler notification is needed.
     * 
     * @return true if scheduler notification is needed.
     */
    public final boolean needSchedulerNotification()
    {
      return needSchedulerNotification;
    }

    /**
     * sets the local scheduler notification indicator
     * 
     * @param needSchedulerNotification true if scheduler notification is
     *          needed.
     */
    public void setNeedSchedulerNotification(boolean needSchedulerNotification)
    {
      this.needSchedulerNotification = needSchedulerNotification;
    }

    /**
     * Indicates whether or not the backend should be disabled.
     * 
     * @return true if backend must be disabled
     */
    public boolean isDisableBackendOnSuccess()
    {
      return disableBackendOnSuccess;
    }

    /**
     * Indicates whether or not the query was successful on the remote
     * controller.
     * 
     * @return true if the query was successful on one of the remote
     *         controllers.
     */
    public boolean isSuccess()
    {
      return success;
    }

    /**
     * Returns the update count (only meaningful if this was a request returning
     * an update count)
     * 
     * @return update count
     */
    public int getUpdateCount()
    {
      return updateCount;
    }

  }

  private void logRequestCompletionAndNotifyScheduler(AbstractRequest request,
      boolean success, FailureInformation failureInfo, int updateCount)
  {
    // Update recovery log with completion information
    if (recoveryLog != null)
    {
      boolean mustLog = !request.isReadOnly();
      if (request instanceof StoredProcedure)
      {
        DatabaseProcedureSemantic semantic = ((StoredProcedure) request)
            .getSemantic();
        mustLog = (semantic == null) || semantic.isWrite();
      }
      if (mustLog && failureInfo.getLogId() != 0)
        recoveryLog.logRequestCompletion(failureInfo.getLogId(), success,
            request.getExecTimeInMs(), updateCount);
    }

    if (failureInfo.needSchedulerNotification())
    {
      try
      {
        // Notify scheduler now, the notification was postponed when
        // addFailedOnAllBackends was called.
        if (request instanceof StoredProcedure)
          scheduler.storedProcedureCompleted((StoredProcedure) request);
        else if (!request.isAutoCommit()
            && (request instanceof UnknownWriteRequest))
        {
          String sql = request.getSqlOrTemplate();
          TransactionMetaData tm = new TransactionMetaData(request
              .getTransactionId(), 0, request.getLogin(), request
              .isPersistentConnection(), request.getPersistentConnectionId());
          if ("commit".equals(sql))
            scheduler.commitCompleted(tm, success);
          else if ("rollback".equals(sql) || "abort".equals(sql))
            scheduler.rollbackCompleted(tm, success);
          else if (sql.startsWith("rollback")) // rollback to savepoint
            scheduler.savepointCompleted(tm.getTransactionId());
          else if (sql.startsWith("release "))
            scheduler.savepointCompleted(tm.getTransactionId());
          else if (sql.startsWith("savepoint "))
            scheduler.savepointCompleted(tm.getTransactionId());
          else
            // Real UnknownWriteRequest
            scheduler.writeCompleted((AbstractWriteRequest) request);
        }
        else
          // Just an AbstractWriteRequest
          scheduler.writeCompleted((AbstractWriteRequest) request);
      }
      catch (SQLException e)
      {
        logger.warn("Failed to notify scheduler for request " + request, e);
      }
    }
  }

  /**
   * Add a request that failed on all backends.
   * 
   * @param request the request that failed
   * @param needSchedulerNotification true if the request has been scheduled but
   *          the scheduler has not been notified yet of the request completion
   * @see #completeFailedOnAllBackends(AbstractRequest, boolean, boolean)
   */
  public void addFailedOnAllBackends(AbstractRequest request,
      boolean needSchedulerNotification)
  {
    synchronized (failedOnAllBackends)
    {
      /*
       * Failure information may already exist if the request was timed out at
       * the originating controller. In which case, we have all the information
       * required to complete the request.
       */
      FailureInformation failureInfo = (FailureInformation) failedOnAllBackends
          .get(request);
      if (failureInfo == null)
        failedOnAllBackends.put(request, new FailureInformation(
            needSchedulerNotification, request.getLogId()));
      else
      {
        failureInfo.setLogId(request.getLogId());
        failureInfo.setNeedSchedulerNotification(needSchedulerNotification);
        completeFailedOnAllBackends(request, failureInfo.isSuccess(),
            failureInfo.isDisableBackendOnSuccess(), failureInfo.updateCount);
      }
    }
  }

  /**
   * Cleanup all queries registered as failed issued from the given controller.
   * This is used when a controller has failed and no status information will be
   * returned for failed queries. Queries are systematically tagged as failed.
   * <p>
   * FIXME: This is only correct with 2 controllers, in a 3+ controller
   * scenario, we need to check from other controllers if someone succeeded.
   * 
   * @param failedControllerId id of the controller that has failed
   */
  public void cleanupAllFailedQueriesFromController(long failedControllerId)
  {
    synchronized (failedOnAllBackends)
    {
      for (Iterator iter = failedOnAllBackends.keySet().iterator(); iter
          .hasNext();)
      {
        AbstractRequest request = (AbstractRequest) iter.next();
        if (((request.getId() & CONTROLLER_ID_BIT_MASK) == failedControllerId)
            || ((request.getTransactionId() & CONTROLLER_ID_BIT_MASK) == failedControllerId)
            || (request.isPersistentConnection() && (request
                .getPersistentConnectionId() & CONTROLLER_ID_BIT_MASK) == failedControllerId))
        { // Need to remove that entry
          FailureInformation failureInfo = (FailureInformation) failedOnAllBackends
              .get(request);
          // failedOnAllBackends can contain completion status information for
          // requests that failed before we started processing processing
          // requests. These entries do not have a logId and should be ignored.
          if (failureInfo.getLogId() > 0)
          {
            if (logger.isInfoEnabled())
              logger.info("No status information received for request "
                  + request + ", considering status as failed.");

            // If the current request is a rollback / abort, we need to call
            // logRequestCompletionAndNotifyScheduler with success set to true
            // for the transaction to be correctly cleaned up
            boolean isAbortOrRollback = (request instanceof UnknownWriteRequest)
                && ("rollback".equals(request.getSqlOrTemplate()) || "abort"
                    .equals(request.getSqlOrTemplate()));

            logRequestCompletionAndNotifyScheduler(request, isAbortOrRollback,
                failureInfo, -1);
          }
          iter.remove();
        }
      }
    }
  }

  /**
   * Notify completion of a request that either failed on all backends or was
   * not executed at all (NoMoreBackendException). If completion was successful
   * and query failed on all backends locally, all local backends are disabled
   * if disabledBackendOnSuccess is true.
   * 
   * @param request request that completed
   * @param success true if completion is successful
   * @param disableBackendOnSuccess disable all local backends if query was
   *          successful (but failed locally)
   * @param updateCount request update count to be logged in recovery log
   * @see #addFailedOnAllBackends(AbstractRequest, boolean)
   */
  public void completeFailedOnAllBackends(AbstractRequest request,
      boolean success, boolean disableBackendOnSuccess, int updateCount)
  {
    FailureInformation failureInfo;
    synchronized (failedOnAllBackends)
    {
      failureInfo = (FailureInformation) failedOnAllBackends.remove(request);
      if (failureInfo == null)
      {
        /*
         * If we can't find failureInformation, assume the remote controller
         * failed the request before it completed locally. This is probably due
         * to a timeout.
         */
        failureInfo = new FailureInformation(success, disableBackendOnSuccess,
            updateCount);
        failedOnAllBackends.put(request, failureInfo);

        logger.info("Unable to find request "
            + request.getSqlShortForm(dvdb.getSqlShortFormLength())
            + " in list of requests that failed on all backends.");
        return;
      }
    }
    logRequestCompletionAndNotifyScheduler(request, success, failureInfo,
        updateCount);

    if (disableBackendOnSuccess && success)
    {
      // Now really disable the backends
      String message = "Request "
          + request.getSqlShortForm(dvdb.getSqlShortFormLength())
          + " failed on all local backends but succeeded on other controllers. Disabling all local backends.";
      logger.error(message);
      endUserLogger.error(message);

      try
      {
        dvdb.disableAllBackends(true);
      }
      catch (VirtualDatabaseException e)
      {
        logger.error("An error occured while disabling all backends", e);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#closePersistentConnection(java.lang.String,
   *      long)
   */
  public void distributedClosePersistentConnection(String login,
      long persistentConnectionId)
  {
    List groupMembers = dvdb.getCurrentGroup().getMembers();

    if (logger.isDebugEnabled())
      logger.debug("Broadcasting closing persistent connection "
          + persistentConnectionId + " for user " + login
          + " to all controllers (" + dvdb.getChannel().getLocalMembership()
          + "->" + groupMembers.toString() + ")");

    // Send the query to everybody including us
    try
    {
      dvdb.getMulticastRequestAdapter().multicastMessage(
          groupMembers,
          new DistributedClosePersistentConnection(login,
              persistentConnectionId), MulticastRequestAdapter.WAIT_ALL, 0);
    }
    catch (Exception e)
    {
      String msg = "An error occured while executing distributed persistent connection "
          + persistentConnectionId + " closing";
      logger.warn(msg, e);
    }

    if (logger.isDebugEnabled())
      logger.debug("Persistent connection " + persistentConnectionId
          + " closed.");
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#openPersistentConnection(String,
   *      long)
   */
  public void distributedOpenPersistentConnection(String login,
      long persistentConnectionId) throws SQLException
  {
    List groupMembers = dvdb.getCurrentGroup().getMembers();

    if (logger.isDebugEnabled())
      logger.debug("Broadcasting opening persistent connection "
          + persistentConnectionId + " for user " + login
          + " to all controllers (" + dvdb.getChannel().getLocalMembership()
          + "->" + groupMembers.toString() + ")");

    boolean success = false;
    Exception exception = null;
    try
    {
      // Send the query to everybody including us
      MulticastResponse responses = dvdb.getMulticastRequestAdapter()
          .multicastMessage(
              groupMembers,
              new DistributedOpenPersistentConnection(login,
                  persistentConnectionId), MulticastRequestAdapter.WAIT_ALL, 0);

      // get a list that won't change while we go through it
      groupMembers = dvdb.getAllMembers();
      int size = groupMembers.size();
      ArrayList failedControllers = null;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof Boolean)
        {
          if (((Boolean) r).booleanValue())
            success = true;
          else
            logger.error("Unexpected result for controller  " + member);
        }
        else if (r instanceof Exception)
        {
          if (failedControllers == null)
            failedControllers = new ArrayList();
          failedControllers.add(member);
          if (exception == null)
            exception = (Exception) r;
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " failed to open persistent connection  "
                + persistentConnectionId + " (" + r + ")");
        }
      }

      /*
       * Notify all controllers where all backend failed (if any) that
       * completion was 'success'.
       */
      if (failedControllers != null)
      {
        UnknownWriteRequest notifRequest = new UnknownWriteRequest("open "
            + persistentConnectionId, false, 0, null);
        notifyRequestCompletion(notifRequest, success, false, failedControllers);
      }
    }
    catch (Exception e)
    {
      String msg = "An error occured while executing distributed persistent connection "
          + persistentConnectionId + " opening";
      logger.warn(msg, e);
    }

    if (success)
    {
      if (logger.isDebugEnabled())
        logger.debug("Persistent connection " + persistentConnectionId
            + " opened.");
      return; // This is a success if at least one controller has succeeded
    }

    // At this point, all controllers failed
    String msg = "Failed to open persistent connection "
        + persistentConnectionId + " on all controllers (" + exception + ")";
    logger.warn(msg);
    throw new SQLException(msg);
  }

  /**
   * Notify to all members the failover for the specified persistent connection.
   * 
   * @param persistentConnectionId persistent connection id
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#failoverForPersistentConnection(long)
   */
  public void distributedFailoverForPersistentConnection(
      long persistentConnectionId)
  {
    List groupMembers = dvdb.getCurrentGroup().getMembers();

    if (logger.isDebugEnabled())
      logger.debug("Broadcasting failover for persistent connection "
          + persistentConnectionId + " to all controllers ("
          + dvdb.getChannel().getLocalMembership() + "->"
          + groupMembers.toString() + ")");

    // Send the query to everybody including us
    try
    {
      dvdb.getMulticastRequestAdapter().multicastMessage(groupMembers,
          new FailoverForPersistentConnection(persistentConnectionId),
          MulticastRequestAdapter.WAIT_ALL, 0);
    }
    catch (Exception e)
    {
      String msg = "An error occured while notifying distributed persistent connection "
          + persistentConnectionId + " failover";
      logger.warn(msg, e);
    }
  }

  /**
   * Notify to all members the failover for the specified transaction.
   * 
   * @param currentTid transaction id
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#failoverForTransaction(long)
   */
  public void distributedFailoverForTransaction(long currentTid)
  {
    List groupMembers = dvdb.getCurrentGroup().getMembers();

    if (logger.isDebugEnabled())
      logger.debug("Broadcasting failover for transaction " + currentTid
          + " to all controllers (" + dvdb.getChannel().getLocalMembership()
          + "->" + groupMembers.toString() + ")");

    // Send the query to everybody including us
    try
    {
      dvdb.getMulticastRequestAdapter().multicastMessage(groupMembers,
          new FailoverForTransaction(currentTid),
          MulticastRequestAdapter.WAIT_ALL, 0);
    }
    catch (Exception e)
    {
      String msg = "An error occured while notifying distributed persistent connection "
          + currentTid + " failover";
      logger.warn(msg, e);
    }
  }

  //
  // Transaction management
  //

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#abort(long,
   *      boolean, boolean)
   */
  public void abort(long transactionId, boolean logAbort, boolean forceAbort)
      throws SQLException
  {
    Long lTid = new Long(transactionId);
    TransactionMetaData tm;
    try
    {
      tm = getTransactionMetaData(lTid);
      if (!forceAbort && tidSavepoints.get(lTid) != null)
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
      // We ignore the persistent connection id here (retrieved by connection
      // manager)
      tm = new TransactionMetaData(transactionId, 0, RecoveryLog.UNKNOWN_USER,
          false, 0);
      if (tidSavepoints.get(lTid) != null)
      {
        if (logger.isDebugEnabled())
          logger.debug("Transaction " + transactionId
              + " has savepoints, transaction will not be aborted");
        return;
      }
    }

    boolean isAWriteTransaction;
    synchronized (distributedTransactions)
    {
      isAWriteTransaction = distributedTransactions.contains(lTid);
    }
    if (isAWriteTransaction)
    {
      distributedAbort(tm.getLogin(), transactionId);
    }
    else
    {
      // read-only transaction, it is local but we still have to post the query
      // in the total order queue. Note that we post a Rollback object because
      // the load balancer will treat the abort as a rollback.
      LinkedList totalOrderQueue = dvdb.getTotalOrderQueue();
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.addLast(new DistributedRollback(tm.getLogin(),
            transactionId));
      }
      super.abort(transactionId, logAbort, forceAbort);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#begin(String,
   *      boolean, long) overrides RequestManager.begin(String) to apply bit
   *      masks to the tid returned by the scheduler
   */
  public long begin(String login, boolean isPersistentConnection,
      long persistentConnectionId) throws SQLException
  {
    long tid = scheduler.getNextTransactionId();
    // 2 first bytes are used for controller id
    // 6 right-most bytes are used for transaction id
    tid = tid & TRANSACTION_ID_BIT_MASK;
    tid = tid | controllerId;
    doBegin(login, tid, isPersistentConnection, persistentConnectionId);
    return tid;
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#commit(long,
   *      boolean, boolean)
   */
  public void commit(long transactionId, boolean logCommit,
      boolean emptyTransaction) throws SQLException
  {
    Long lTid = new Long(transactionId);
    TransactionMetaData tm = getTransactionMetaData(lTid);
    boolean isAWriteTransaction;
    synchronized (distributedTransactions)
    {
      isAWriteTransaction = distributedTransactions.contains(lTid);
    }
    if (isAWriteTransaction)
    {
      distributedCommit(tm.getLogin(), transactionId);
    }
    else
    {
      // read-only transaction, it is local
      DistributedCommit commit = new DistributedCommit(tm.getLogin(),
          transactionId);
      if (!emptyTransaction)
      {
        LinkedList totalOrderQueue = dvdb.getTotalOrderQueue();
        synchronized (totalOrderQueue)
        {
          totalOrderQueue.addLast(commit);
        }
      }
      try
      {
        super.commit(transactionId, logCommit, emptyTransaction);
      }
      catch (SQLException e)
      {
        if (logger.isWarnEnabled())
        {
          logger
              .warn("Ignoring failure of commit for read-only transaction, exception was: "
                  + e);
        }

        // Force transaction completion on scheduler
        scheduler.commit(tm, emptyTransaction, commit);
        scheduler.commitCompleted(tm, true);

        // Clean-up transactional context
        completeTransaction(lTid);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#completeTransaction(java.lang.Long)
   */
  public void completeTransaction(Long tid)
  {
    synchronized (distributedTransactions)
    {
      distributedTransactions.remove(tid);
    }
    super.completeTransaction(tid);
  }

  /**
   * Check if the transaction corresponding to the given query has been started
   * remotely and start the transaction locally in a lazy manner if needed. This
   * also checks if a local request must trigger the logging of a 'begin' in the
   * recovery log.
   * 
   * @param request query to execute
   * @throws SQLException if an error occurs
   */
  public void lazyTransactionStart(AbstractRequest request) throws SQLException
  {
    // Check if this is a remotely started transaction that we need to lazily
    // start locally. Note that we cannot decide from its id that a transaction
    // has been started remotely. In a failover case the client still uses the
    // transaction id given by the original controller. See SEQUOIA-930.
    if (!request.isAutoCommit())
    {
      long tid = request.getTransactionId();
      Long lTid = new Long(tid);
      TransactionMetaData tm = (TransactionMetaData) transactionMetaDatas
          .get(lTid);

      // Check if transaction is started
      if (tm != null)
      {
        /*
         * It may have been started by a failover before any writes were
         * executed.
         */
        if (tm.isReadOnly())
        {
          request.setIsLazyTransactionStart(true);
          tm.setReadOnly(false);
        }
        return; // transaction already started
      }
      // Begin this transaction
      try
      {
        tm = new TransactionMetaData(tid, beginTimeout, request.getLogin(),
            request.isPersistentConnection(), request
                .getPersistentConnectionId());
        tm.setReadOnly(false);

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("transaction.begin.lazy", String
              .valueOf(tid)));

        scheduler.begin(tm, true, request);

        try
        {
          // Send to load balancer
          loadBalancer.begin(tm);

          // We need to update the tid table first so that
          // logLazyTransactionBegin can retrieve the metadata
          transactionMetaDatas.put(lTid, tm);
          request.setIsLazyTransactionStart(true);

          synchronized (distributedTransactions)
          {
            if (!distributedTransactions.contains(lTid))
              distributedTransactions.add(lTid);
          }
        }
        catch (SQLException e)
        {
          if (recoveryLog != null)
            // In case logLazyTransactionBegin failed
            transactionMetaDatas.remove(lTid);
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
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#rollback(long,
   *      boolean)
   */
  public void rollback(long transactionId, boolean logRollback)
      throws SQLException
  {
    Long lTid = new Long(transactionId);
    TransactionMetaData tm = getTransactionMetaData(lTid);
    boolean isAWriteTransaction;
    synchronized (distributedTransactions)
    {
      isAWriteTransaction = distributedTransactions.contains(lTid);
    }
    if (isAWriteTransaction)
    {
      distributedRollback(tm.getLogin(), transactionId);
    }
    else
    {
      // read-only transaction, it is local
      DistributedRollback rollback = new DistributedRollback(tm.getLogin(),
          transactionId);
      LinkedList totalOrderQueue = dvdb.getTotalOrderQueue();
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.addLast(rollback);
      }
      try
      {
        super.rollback(transactionId, logRollback);
      }
      catch (SQLException e)
      {
        if (logger.isWarnEnabled())
        {
          logger
              .warn("Ignoring failure of rollback for read-only transaction, exception was: "
                  + e);
        }

        // Force transaction completion on scheduler
        try
        {
          scheduler.rollback(tm, rollback);
        }
        catch (SQLException ignore)
        {
        }
        scheduler.rollbackCompleted(tm, true);

        // Clean-up transactional context
        completeTransaction(lTid);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#rollback(long,
   *      String)
   */
  public void rollback(long transactionId, String savepointName)
      throws SQLException
  {
    Long lTid = new Long(transactionId);
    boolean isAWriteTransaction;
    synchronized (distributedTransactions)
    {
      isAWriteTransaction = distributedTransactions.contains(lTid);
    }
    if (isAWriteTransaction)
    {
      TransactionMetaData tm = getTransactionMetaData(lTid);
      distributedRollback(tm.getLogin(), transactionId, savepointName);
    }
    else
    { // read-only transaction, it is local
      LinkedList totalOrderQueue = dvdb.getTotalOrderQueue();
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.addLast(new DistributedRollbackToSavepoint(
            transactionId, savepointName));
      }
      super.rollback(transactionId, savepointName);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#setSavepoint(long)
   */
  public int setSavepoint(long transactionId) throws SQLException
  {
    Long lTid = new Long(transactionId);
    int savepointId = scheduler.incrementSavepointId();
    TransactionMetaData tm = getTransactionMetaData(lTid);
    synchronized (distributedTransactions)
    {
      if (!distributedTransactions.contains(lTid))
        distributedTransactions.add(lTid);
    }
    distributedSetSavepoint(tm.getLogin(), transactionId, String
        .valueOf(savepointId));
    return savepointId;
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#setSavepoint(long,
   *      String)
   */
  public void setSavepoint(long transactionId, String name) throws SQLException
  {
    Long lTid = new Long(transactionId);
    TransactionMetaData tm = getTransactionMetaData(lTid);
    synchronized (distributedTransactions)
    {
      if (!distributedTransactions.contains(lTid))
        distributedTransactions.add(lTid);
    }
    distributedSetSavepoint(tm.getLogin(), transactionId, name);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#releaseSavepoint(long,
   *      String)
   */
  public void releaseSavepoint(long transactionId, String name)
      throws SQLException
  {
    Long lTid = new Long(transactionId);
    boolean isAWriteTransaction;
    synchronized (distributedTransactions)
    {
      isAWriteTransaction = distributedTransactions.contains(lTid);
    }
    if (isAWriteTransaction)
    {
      TransactionMetaData tm = getTransactionMetaData(lTid);
      distributedReleaseSavepoint(tm.getLogin(), transactionId, name);
    }
    else
    {
      // read-only transaction, it is local
      LinkedList totalOrderQueue = dvdb.getTotalOrderQueue();
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.addLast(new DistributedReleaseSavepoint(transactionId,
            name));
      }
      super.releaseSavepoint(transactionId, name);
    }
  }

  /**
   * Add this transaction to the list of write transactions that needs to be
   * globally commited. This happens if the transaction has only be started
   * locally but not through a lazy start.
   */
  private void addToDistributedTransactionListIfNeeded(AbstractRequest request)
  {
    // Add to distributed transactions if needed
    if (!request.isAutoCommit())
    {
      Long lTid = new Long(request.getTransactionId());
      synchronized (distributedTransactions)
      {
        if (!distributedTransactions.contains(lTid))
          distributedTransactions.add(lTid);
      }
    }
  }

  /**
   * Retrieve the vLogin corresponding to the persistent connection id provided
   * and close the connection if found. This is used by the
   * ControllerFailureCleanupThread to cleanup reamining persistent connections
   * from a failed controller whose clients never recovered.
   * 
   * @param connectionId the persistent connection id
   */
  public void closePersistentConnection(Long connectionId)
  {
    String vLogin = scheduler.getPersistentConnectionLogin(connectionId);
    if (vLogin != null)
      super.closePersistentConnection(vLogin, connectionId.longValue());
  }

  /**
   * Performs a local read operation, as opposed to execReadRequest() which
   * attempts to use distributed reads when there is NoMoreBackendException.
   * 
   * @param request the read request to perform
   * @return a ControllerResultSet
   * @throws NoMoreBackendException when no more local backends are available to
   *           execute the request
   * @throws SQLException in case of error
   */
  public ControllerResultSet execLocalStatementExecuteQuery(
      SelectRequest request) throws NoMoreBackendException, SQLException
  {
    return super.statementExecuteQuery(request);
  }

  /**
   * Execute a read request on some remote controller - one in the group. Used
   * when the local controller has no backend available to execute the request.
   * 
   * @param request the request to execute
   * @return the query ResultSet
   * @throws SQLException in case of bad request
   */
  public abstract ControllerResultSet execRemoteStatementExecuteQuery(
      SelectRequest request) throws SQLException;

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#statementExecuteQuery(org.continuent.sequoia.controller.requests.SelectRequest)
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request)
      throws SQLException
  {
    if (!request.isMustBroadcast())
    {
      try
      {
        return execLocalStatementExecuteQuery(request);
      }
      catch (SQLException e)
      {
        if (!(e instanceof NoMoreBackendException))
          throw e;
        // else this failed locally, try it remotely
        // Request failed locally, try on other controllers.
        addToDistributedTransactionListIfNeeded(request);
        return execRemoteStatementExecuteQuery(request);
      }
    }
    addToDistributedTransactionListIfNeeded(request);
    return distributedStatementExecuteQuery(request);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#statementExecuteUpdate(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public ExecuteUpdateResult statementExecuteUpdate(AbstractWriteRequest request)
      throws SQLException
  {
    if (!request.isAutoCommit())
    { /*
       * Add this transaction to the list of write transactions that needs to be
       * globally commited. This happens if the transaction has only be started
       * locally but not through a lazy start.
       */
      Long lTid = new Long(request.getTransactionId());
      synchronized (distributedTransactions)
      {
        if (!distributedTransactions.contains(lTid))
          distributedTransactions.add(lTid);
      }
    }
    return distributedStatementExecuteUpdate(request);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#statementExecuteUpdateWithKeys(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request) throws SQLException
  {
    if (!request.isAutoCommit())
    { /*
       * Add this transaction to the list of write transactions that needs to be
       * globally commited. This happens if the transaction has only be started
       * locally but not through a lazy start.
       */
      Long lTid = new Long(request.getTransactionId());
      synchronized (distributedTransactions)
      {
        if (!distributedTransactions.contains(lTid))
          distributedTransactions.add(lTid);
      }
    }
    return distributedStatementExecuteUpdateWithKeys(request);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#statementExecute(AbstractRequest)
   */
  public ExecuteResult statementExecute(AbstractRequest request)
      throws SQLException
  {
    if (!request.isAutoCommit())
    { /*
       * Add this transaction to the list of write transactions that needs to be
       * globally commited. This happens if the transaction has only be started
       * locally but not through a lazy start.
       */
      Long lTid = new Long(request.getTransactionId());
      synchronized (distributedTransactions)
      {
        if (!distributedTransactions.contains(lTid))
          distributedTransactions.add(lTid);
      }
    }
    return distributedStatementExecute(request);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#scheduleExecWriteRequest(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public void scheduleExecWriteRequest(AbstractWriteRequest request)
      throws SQLException
  {
    lazyTransactionStart(request);
    super.scheduleExecWriteRequest(request);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#callableStatementExecuteQuery(StoredProcedure)
   */
  public ControllerResultSet callableStatementExecuteQuery(StoredProcedure proc)
      throws SQLException
  {
    // Parse the query first to update the semantic information
    getParsingFromCacheOrParse(proc);

    // If procedure is read-only, we don't broadcast
    DatabaseProcedureSemantic semantic = proc.getSemantic();
    if (proc.isReadOnly() || ((semantic != null) && (semantic.isReadOnly())))
    {
      try
      {
        proc.setIsReadOnly(true);
        return execLocallyCallableStatementExecuteQuery(proc);
      }
      catch (AllBackendsFailedException ignore)
      {
        // This failed locally, try it remotely
      }
      catch (SQLException e)
      {
        if (!(e instanceof NoMoreBackendException))
          throw e;
        // else this failed locally, try it remotely
      }
    }

    addToDistributedTransactionListIfNeeded(proc);
    return distributedCallableStatementExecuteQuery(proc);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#callableStatementExecuteUpdate(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public ExecuteUpdateResult callableStatementExecuteUpdate(StoredProcedure proc)
      throws SQLException
  {
    if (!proc.isAutoCommit())
    { /*
       * Add this transaction to the list of write transactions that needs to be
       * globally commited. This happens if the transaction has only be started
       * locally but not through a lazy start.
       */
      Long lTid = new Long(proc.getTransactionId());
      synchronized (distributedTransactions)
      {
        if (!distributedTransactions.contains(lTid))
          distributedTransactions.add(lTid);
      }
    }
    return distributedCallableStatementExecuteUpdate(proc);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#callableStatementExecute(StoredProcedure)
   */
  public ExecuteResult callableStatementExecute(StoredProcedure proc)
      throws SQLException
  {
    // Parse the query first to update the semantic information
    getParsingFromCacheOrParse(proc);

    // If procedure is read-only, we don't broadcast
    DatabaseProcedureSemantic semantic = proc.getSemantic();
    if (proc.isReadOnly() || ((semantic != null) && (semantic.isReadOnly())))
    {
      try
      {
        proc.setIsReadOnly(true);
        return execLocallyCallableStatementExecute(proc);
      }
      catch (AllBackendsFailedException ignore)
      {
        // This failed locally, try it remotely
      }
      catch (SQLException e)
      {
        if (!(e instanceof NoMoreBackendException))
          throw e;
        // else this failed locally, try it remotely
      }
    }

    if (!proc.isAutoCommit())
    { /*
       * Add this transaction to the list of write transactions that needs to be
       * globally commited. This happens if the transaction has only be started
       * locally but not through a lazy start.
       */
      Long lTid = new Long(proc.getTransactionId());
      synchronized (distributedTransactions)
      {
        if (!distributedTransactions.contains(lTid))
          distributedTransactions.add(lTid);
      }
    }
    return distributedCallableStatementExecute(proc);
  }

  /**
   * Fetch the result of a previously executed request from a remote controller
   * failover cache.
   * 
   * @param successfulControllers controllers to fetch the result from
   * @param id unique identifier of the query to look the result for
   * @return the request result
   * @throws NoResultAvailableException if no result could be retrieved from the
   *           failover cache
   */
  protected Serializable getRequestResultFromFailoverCache(
      List successfulControllers, long id) throws NoResultAvailableException
  {
    List groupMembers = new ArrayList(1);

    // Try all members in turn and return as soon as one succeeds
    for (Iterator iter = successfulControllers.iterator(); iter.hasNext();)
    {
      Member remoteController = (Member) iter.next();
      groupMembers.clear();
      groupMembers.add(remoteController);

      if (logger.isDebugEnabled())
        logger.debug("Getting result for request " + id + " from controllers "
            + remoteController);

      try
      { // Send the request to that controller
        MulticastResponse response = dvdb.getMulticastRequestAdapter()
            .multicastMessage(groupMembers,
                new GetRequestResultFromFailoverCache(id),
                MulticastRequestAdapter.WAIT_ALL, 0);
        Serializable result = response.getResult(remoteController);

        if ((result instanceof Exception)
            || (response.getFailedMembers() != null))
        { // Failure on the remote controller
          if (logger.isInfoEnabled())
            logger.info("Controller " + remoteController
                + " could not fetch result for request " + id,
                (Exception) result);
        }
        else
          return result;
      }
      catch (Exception e)
      {
        String msg = "An error occured while getching result for request " + id
            + " from controller " + remoteController;
        logger.warn(msg, e);
      }
    }
    throw new NoResultAvailableException(
        "All controllers failed when trying to fetch result for request " + id);
  }

  /**
   * Stores a result associated with a request in the request result failover
   * cache.
   * <p>
   * Only results for requests initiated on a remote controller are stored.
   * 
   * @param request the request executed
   * @param result the result of the request
   * @return true if the result was added to the cache, false if the request was
   *         local to this controller
   * @see org.continuent.sequoia.controller.virtualdatabase.RequestResultFailoverCache#store(AbstractRequest,
   *      Serializable)
   */
  public boolean storeRequestResult(AbstractRequest request, Serializable result)
  {
    // Cache only results for requests initiated by a remote controller.
    if ((request.getId() & CONTROLLER_ID_BIT_MASK) != dvdb.getControllerId())
    {
      dvdb.getRequestResultFailoverCache().store(request, result);
      return true;
    }
    return false;
  }

  //
  // RAIDb level specific methods
  //

  /**
   * Distributed implementation of an abort
   * 
   * @param login login that abort the transaction
   * @param transactionId id of the commiting transaction
   * @throws SQLException if an error occurs
   */
  public abstract void distributedAbort(String login, long transactionId)
      throws SQLException;

  /**
   * Distributed implementation of a commit
   * 
   * @param login login that commit the transaction
   * @param transactionId id of the commiting transaction
   * @throws SQLException if an error occurs
   */
  public abstract void distributedCommit(String login, long transactionId)
      throws SQLException;

  /**
   * Distributed implementation of a rollback
   * 
   * @param login login that rollback the transaction
   * @param transactionId id of the rollbacking transaction
   * @throws SQLException if an error occurs
   */
  public abstract void distributedRollback(String login, long transactionId)
      throws SQLException;

  /**
   * Distributed implementation of a rollback to a savepoint
   * 
   * @param login login that rollback the transaction
   * @param transactionId id of the transaction
   * @param savepointName name of the savepoint
   * @throws SQLException if an error occurs
   */
  public abstract void distributedRollback(String login, long transactionId,
      String savepointName) throws SQLException;

  /**
   * Distributed implementation of setting a savepoint to a transaction
   * 
   * @param login login that releases the savepoint
   * @param transactionId id of the transaction
   * @param name name of the savepoint to set
   * @throws SQLException if an error occurs
   */
  public abstract void distributedSetSavepoint(String login,
      long transactionId, String name) throws SQLException;

  /**
   * Distributed implementation of releasing a savepoint from a transaction
   * 
   * @param login login that set the savepoint
   * @param transactionId id of the transaction
   * @param name name of the savepoint to release
   * @throws SQLException if an error occurs
   */
  public abstract void distributedReleaseSavepoint(String login,
      long transactionId, String name) throws SQLException;

  /**
   * Distributed implementation of a select request execution that returns a
   * ResultSet.
   * 
   * @param request request to execute
   * @return ResultSet containing the auto-generated keys.
   * @throws SQLException if an error occurs
   */
  public abstract ControllerResultSet distributedStatementExecuteQuery(
      SelectRequest request) throws SQLException;

  /**
   * Distributed implementation of a write request execution.
   * 
   * @param request request to execute
   * @return number of modified rows
   * @throws SQLException if an error occurs
   */
  public abstract ExecuteUpdateResult distributedStatementExecuteUpdate(
      AbstractWriteRequest request) throws SQLException;

  /**
   * Distributed implementation of a write request execution that returns
   * auto-generated keys.
   * 
   * @param request request to execute
   * @return update count and ResultSet containing the auto-generated keys.
   * @throws SQLException if an error occurs
   */
  public abstract GeneratedKeysResult distributedStatementExecuteUpdateWithKeys(
      AbstractWriteRequest request) throws SQLException;

  /**
   * Distributed implementation of a Statement.execute() execution.
   * 
   * @param request request to execute
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   */
  public abstract ExecuteResult distributedStatementExecute(
      AbstractRequest request) throws SQLException;

  /**
   * Distributed implementation of a stored procedure
   * CallableStatement.executeQuery() execution.
   * 
   * @param proc stored procedure to execute
   * @return ResultSet corresponding to this stored procedure execution
   * @throws SQLException if an error occurs
   */
  public abstract ControllerResultSet distributedCallableStatementExecuteQuery(
      StoredProcedure proc) throws SQLException;

  /**
   * Distributed implementation of a stored procedure
   * CallableStatement.executeUpdate() execution.
   * 
   * @param proc stored procedure to execute
   * @return number of modified rows
   * @throws SQLException if an error occurs
   */
  public abstract ExecuteUpdateResult distributedCallableStatementExecuteUpdate(
      StoredProcedure proc) throws SQLException;

  /**
   * Distributed implementation of a stored procedure
   * CallableStatement.execute() execution.
   * 
   * @param proc stored procedure to execute
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   */
  public abstract ExecuteResult distributedCallableStatementExecute(
      StoredProcedure proc) throws SQLException;

  /**
   * Once the request has been dispatched, it can be executed using the code
   * from <code>RequestManager</code>
   * 
   * @param proc stored procedure to execute
   * @return ResultSet corresponding to this stored procedure execution
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @throws SQLException if an error occurs
   */
  public ControllerResultSet execLocallyCallableStatementExecuteQuery(
      StoredProcedure proc) throws AllBackendsFailedException, SQLException
  {
    return super.callableStatementExecuteQuery(proc);
  }

  /**
   * Once the request has been dispatched, it can be executed using the code
   * from <code>RequestManager</code>
   * 
   * @param proc stored procedure to execute
   * @return ExecuteResult corresponding to this stored procedure execution
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @throws SQLException if an error occurs
   */
  public ExecuteResult execLocallyCallableStatementExecute(StoredProcedure proc)
      throws AllBackendsFailedException, SQLException
  {
    return super.callableStatementExecute(proc);
  }

  /**
   * Test if a transaction has been started on this controller, but initialized
   * by a remote controller.
   * 
   * @param currentTid Current transaction Id
   * @return True if this transaction of Id currentId has already been started
   *         on the current controller
   */
  public boolean isDistributedTransaction(long currentTid)
  {
    synchronized (distributedTransactions)
    {
      return distributedTransactions.contains(new Long(currentTid));
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#resumeActivity(boolean)
   */
  public void resumeActivity(boolean interactiveResume)
  {
    // Perform the distributed call through the group-comm, in order to
    // resume all the activity suspended above.
    try
    {
      logger.info("Resuming activity for " + dvdb.getDatabaseName());
      dvdb.getMulticastRequestAdapter().multicastMessage(dvdb.getAllMembers(),
          new ResumeActivity(interactiveResume), MulticastRequestAdapter.WAIT_ALL,
          dvdb.getMessageTimeouts().getDisableBackendTimeout());
      dvdb.setSuspendedFromLocalController(false);
      logger.info("All activity is now resumed for " + dvdb.getDatabaseName());
    }
    catch (Exception e)
    {
      String msg = "Error while resuming activity";
      logger.error(msg, e);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#suspendActivity()
   */
  public void suspendActivity() throws SQLException
  {
    // Perform the distributed call through the group-comm, in order to
    // atomically suspend all activity on the system.
    try
    {
      logger.info("Suspending activity for " + dvdb.getDatabaseName());
      // Suspend transactions
      dvdb.getMulticastRequestAdapter().multicastMessage(dvdb.getAllMembers(),
          new SuspendActivity(), MulticastRequestAdapter.WAIT_ALL,
          dvdb.getMessageTimeouts().getDisableBackendTimeout());
      dvdb.setSuspendedFromLocalController(true);
      logger.info("All activity is suspended for " + dvdb.getDatabaseName());
    }
    catch (Exception e)
    {
      String msg = "Error while suspending activity";
      logger.error(msg, e);
      throw (SQLException) new SQLException(msg + "(" + e + ")").initCause(e);
    }
  }

  /**
   * Suspend all transactions, writes and persistent connections and <strong>do
   * not wait for completion of in-flight transactions and/or persistent
   * connections</strong>.
   * <p>
   * This method blocks activity in the cluster and should not be called in
   * normal cluster situation. It should be reserved for extraordinary situation
   * such as network partition detection &amp; reconciliation.
   * </p>
   * 
   * @throws SQLException if an error occured
   */
  public void blockActivity() throws SQLException
  {
    try
    {
      dvdb.getMulticastRequestAdapter().multicastMessage(dvdb.getAllMembers(),
          new BlockActivity(), MulticastRequestAdapter.WAIT_ALL,
          dvdb.getMessageTimeouts().getDisableBackendTimeout());
      logger.info("All activity is blocked for " + dvdb.getDatabaseName());
    }
    catch (Exception e)
    {
      String msg = "Error while blocking activity";
      logger.error(msg, e);
      throw (SQLException) new SQLException(msg + "(" + e + ")").initCause(e);
    }
  }

  /**
   * Notify controllers that they are now inconsistent with the cluster and that
   * they sould disable themselves.
   * 
   * @param request request that generated the consistency
   * @param inconsistentControllers controllers that need to be notified
   * @throws SQLException if an error occurs
   */
  protected void notifyControllerInconsistency(AbstractRequest request,
      ArrayList inconsistentControllers) throws SQLException
  {
    try
    {
      dvdb.getMulticastRequestAdapter().multicastMessage(
          inconsistentControllers, new NotifyInconsistency(request),
          MulticastRequestAdapter.WAIT_ALL, 0);
    }
    catch (Exception e)
    {
      String msg = "An error occured while notifying controllers ("
          + inconsistentControllers
          + ") of inconsistency due to distributed request " + request.getId();
      logger.warn(msg, e);
      throw new SQLException(msg + " (" + e + ")");
    }
  }

  /**
   * Notify a set of backends of the query completion.
   * 
   * @param request the request that has completed
   * @param success true if the request has successfully completed
   * @param disableBackendOnSuccess disable all local backends if query was
   *          successful (but failed locally). Usually set to true in case of
   *          AllBackendsFailedException and false for NoMoreBackendException.
   * @param backendsToNotify list of backends to notify (returns right away if
   *          the list is null)
   * @throws SQLException if an error occurs
   */
  protected void notifyRequestCompletion(AbstractRequest request,
      boolean success, boolean disableBackendOnSuccess,
      ArrayList backendsToNotify) throws SQLException
  {
    if (backendsToNotify == null)
      return;
    try
    {
      dvdb.getMulticastRequestAdapter().multicastMessage(backendsToNotify,
          new NotifyCompletion(request, success, disableBackendOnSuccess),
          MulticastRequestAdapter.WAIT_ALL,
          dvdb.getMessageTimeouts().getNotifyCompletionTimeout());
    }
    catch (Exception e)
    {
      String msg = "An error occured while notifying all controllers of failure of distributed request "
          + request.getId();
      logger.warn(msg, e);
      throw new SQLException(msg + " (" + e + ")");
    }
  }

  /**
   * Notify a set of backends of the query completion.
   * 
   * @param request the request that has completed
   * @param success true if the request has successfully completed
   * @param disableBackendOnSuccess disable all local backends if query was
   *          successful (but failed locally). Usually set to true in case of
   *          AllBackendsFailedException and false for NoMoreBackendException.
   * @param backendsToNotify list of backends to notify (returns right away if
   *          the list is null)
   * @param requestUpdateCount the request update count to be logged if it
   *          succeeded somewhere
   * @throws SQLException
   */
  protected void notifyRequestCompletion(AbstractRequest request,
      boolean success, boolean disableBackendOnSuccess,
      ArrayList backendsToNotify, int requestUpdateCount) throws SQLException
  {
    if (backendsToNotify == null)
      return;
    try
    {
      dvdb.getMulticastRequestAdapter().multicastMessage(
          backendsToNotify,
          new NotifyCompletion(request, success, disableBackendOnSuccess,
              requestUpdateCount), MulticastRequestAdapter.WAIT_ALL,
          dvdb.getMessageTimeouts().getNotifyCompletionTimeout());
    }
    catch (Exception e)
    {
      String msg = "An error occured while notifying all controllers of failure of distributed request "
          + request.getId();
      logger.warn(msg, e);
      throw new SQLException(msg + " (" + e + ")");
    }
  }

  /**
   * Cleanup all queries from a given transaction that were registered as failed
   * and issued from the other controller. This is used when a controller has
   * failed and no status information will be returned for failed queries.
   * Queries are systematically tagged as failed. This method is called only for
   * failover during a rollback / abort to properly close the transaction on the
   * remaining controller.
   * 
   * @param tId the transaction id that we are looking for
   */
  public void cleanupRollbackFromOtherController(long tId)
  {
    long cid = this.getControllerId();
    synchronized (failedOnAllBackends)
    {
      for (Iterator iter = failedOnAllBackends.keySet().iterator(); iter
          .hasNext();)
      {
        AbstractRequest request = (AbstractRequest) iter.next();
        if (((request.getId() & CONTROLLER_ID_BIT_MASK) != cid)
            || ((request.getTransactionId() & CONTROLLER_ID_BIT_MASK) != cid)
            && request.getTransactionId() == tId)
        { // Need to remove that entry
          if (logger.isInfoEnabled())
            logger.info("Failover while rollbacking the transaction " + tId
                + " detected. No status information received for request "
                + request + ", considering status as failed.");
          FailureInformation failureInfo = (FailureInformation) failedOnAllBackends
              .get(request);

          // If the current request is a rollback / abort, we need to call
          // logRequestCompletionAndNotifyScheduler with success set to true
          // for the transaction to be correctly cleaned up
          boolean isAbortOrRollback = (request instanceof UnknownWriteRequest)
              && ("rollback".equals(request.getSqlOrTemplate()) || "abort"
                  .equals(request.getSqlOrTemplate()));

          logRequestCompletionAndNotifyScheduler(request, isAbortOrRollback,
              failureInfo, -1);
          iter.remove();
        }
      }
    }
  }
}