/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): Jean-Bernard van Zuylen, Peter Royal, Damian Arregui.
 */

package org.continuent.sequoia.controller.scheduler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.continuent.sequoia.common.exceptions.RollbackException;
import org.continuent.sequoia.common.exceptions.VDBisShuttingDownException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCommit;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedOpenPersistentConnection;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedReleaseSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollback;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollbackToSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedSetSavepoint;

/**
 * The Request Scheduler should schedule the request according to a given
 * policy.
 * <p>
 * The requests comes from the Request Controller and are sent later to the next
 * ccontroller omponents (cache and load balancer).
 *
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public abstract class AbstractScheduler implements XmlComponent
{

  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor
  // 3. Getter/Setter
  // 4. Request handling
  // 5. Transaction management
  // 6. Checkpoint management
  // 7. Debug/Monitoring
  //

  //
  // 1. Member variables
  //

  // Request handling
  private int              suspendWrites                            = 0;
  private int              pendingWrites                            = 0;
  private final Object     writesSync                               = new Object();
  private final Object     endOfCurrentWrites                       = new Object();
  /**
   * Read requests only account for SelectRequest objects (stored procedures
   * even with a read-only semantic will be in the write requests list).
   */
  private Map              activeReadRequests                       = new HashMap();

  /**
   * Write requests also include stored procedures.
   */
  private Map              activeWriteRequests                      = new HashMap();
  private Set              suspendedRequests                        = new HashSet();

  // Transaction management
  private long             controllerId                             = 0;
  private long             transactionId                            = 0;
  private int              savepointId                              = 0;
  private int              suspendTransactions                      = 0;
  private int              pendingTransactions                      = 0;
  private final Object     transactionsSync                         = new Object();
  private final Object     endOfCurrentTransactions                 = new Object();
  private List             activeTransactions                       = new ArrayList();
  private long             waitForSuspendedTransactionsTimeout;

  // Persistent connection management
  private int              suspendNewPersistentConnections          = 0;
  private int              suspendOpenClosePersistentConnections    = 0;
  private int              pendingOpenClosePersistentConnections    = 0;
  private final Object     persistentConnectionsSync                = new Object();
  private final Object     suspendOpenClosePersistentConnectionSync = new Object();
  private final Object     endOfCurrentPersistentConnections        = new Object();
  private long             waitForPersistentConnectionsTimeout;

  /**
   * List of persistent connections that have been created <br>
   * persistentConnectionId (Long) -> vLogin (String)
   */
  protected Hashtable      activePersistentConnections              = new Hashtable();

  // Monitoring values
  private int              numberRead                               = 0;
  private int              numberWrite                              = 0;

  // Other
  protected int            raidbLevel;
  protected int            parsingGranularity;

  /** Reference to the associated distributed virtual database */
  private VirtualDatabase  vdb                                      = null;

  private static final int INITIAL_WAIT_TIME                        = 15000;
  protected static Trace   logger                                   = Trace
                                                                        .getLogger("org.continuent.sequoia.controller.scheduler");

  //
  // 2. Constructor
  //

  /**
   * Default scheduler to assign scheduler RAIDb level, needed granularity and
   * SQL macro handling (on the fly instanciation of NOW(), RAND(), ...).
   *
   * @param raidbLevel RAIDb level of this scheduler
   * @param parsingGranularity Parsing granularity needed by the scheduler
   * @param vdb virtual database using this scheduler (needed to access its
   *          total order queue)
   */
  public AbstractScheduler(int raidbLevel, int parsingGranularity,
      VirtualDatabase vdb, long waitForSuspendedTransactionsTimeout,
      long waitForPersistentConnectionsTimeout)
  {
    this.raidbLevel = raidbLevel;
    this.parsingGranularity = parsingGranularity;
    this.vdb = vdb;
    this.waitForSuspendedTransactionsTimeout = waitForSuspendedTransactionsTimeout;
    this.waitForPersistentConnectionsTimeout = waitForPersistentConnectionsTimeout;
  }

  /**
   * Default scheduler to assign scheduler RAIDb level, needed granularity and
   * SQL macro handling (on the fly instanciation of NOW(), RAND(), ...).
   *
   * @param raidbLevel RAIDb level of this scheduler
   * @param parsingGranularity Parsing granularity needed by the scheduler
   * @deprecated This constructor is used only by unsupported scheduler
   *             sub-classes.
   */
  public AbstractScheduler(int raidbLevel, int parsingGranularity)
  {
    this.raidbLevel = raidbLevel;
    this.parsingGranularity = parsingGranularity;
    this.waitForSuspendedTransactionsTimeout = 5 * 60 * 1000;
    this.waitForPersistentConnectionsTimeout = 5 * 60 * 1000;
  }

  //
  // 3. Getter/Setter methods
  //

  /**
   * Get the needed query parsing granularity.
   *
   * @return needed query parsing granularity
   */
  public final int getParsingGranularity()
  {
    return parsingGranularity;
  }

  /**
   * Assigns the local controllerId. It is used for generating transactionIds
   * for autocommit requests.
   *
   * @param controllerId for this controller
   */
  public void setControllerId(long controllerId)
  {
    this.controllerId = controllerId;
  }

  /**
   * Returns the RAIDbLevel.
   *
   * @return int
   */
  public final int getRAIDbLevel()
  {
    return raidbLevel;
  }

  /**
   * Sets the <code>DatabaseSchema</code> of the current virtual database.
   * This is only needed by some schedulers that will have to define their own
   * scheduler schema
   *
   * @param dbs a <code>DatabaseSchema</code> value
   * @see org.continuent.sequoia.controller.scheduler.schema.SchedulerDatabaseSchema
   */
  public void setDatabaseSchema(DatabaseSchema dbs)
  {
    if (logger.isInfoEnabled())
      logger.info(Translate.get("scheduler.doesnt.support.schemas"));
  }

  /**
   * Merge the given <code>DatabaseSchema</code> with the current one.
   *
   * @param dbs a <code>DatabaseSchema</code> value
   * @see org.continuent.sequoia.controller.scheduler.schema.SchedulerDatabaseSchema
   */
  public void mergeDatabaseSchema(DatabaseSchema dbs)
  {
    logger.info(Translate.get("scheduler.doesnt.support.schemas"));
  }

  //
  // 4. Request handling
  //

  /**
   * Returns the list of active read requests <request id, SelectRequest>.
   *
   * @return Returns the active read requests.
   */
  public final Map getActiveReadRequests()
  {
    return activeReadRequests;
  }

  /**
   * Returns the list of active write requests <request id, AbstractRequest>.
   * Write requests can be either StoredProcedure or AbstractWriteRequest
   * objects.
   *
   * @return Returns the active write requests.
   */
  public final Map getActiveWriteRequests()
  {
    return activeWriteRequests;
  }

  /**
   * Returns the number of pending writes.
   *
   * @return int
   */
  public final int getPendingWrites()
  {
    return pendingWrites;
  }

  /**
   * Returns true if the given request id is in the active request list.
   *
   * @param requestId the request unique id
   * @return true if the request is active, false otherwise
   */
  public boolean isActiveRequest(long requestId)
  {
    Long lId = new Long(requestId);
    synchronized (activeReadRequests)
    {
      if (activeReadRequests.containsKey(lId))
        return true;
    }
    synchronized (activeWriteRequests)
    {
      return activeWriteRequests.containsKey(lId);
    }
  }

  /**
   * Wait for the completion of the given request id. The method returns as soon
   * as the request completion has been notified to the scheduler.
   *
   * @param requestId the unique request identifier
   */
  public void waitForRequestCompletion(long requestId)
  {
    Long lId = new Long(requestId);
    synchronized (activeReadRequests)
    {
      while (activeReadRequests.containsKey(lId))
      {
        try
        {
          activeReadRequests.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }
    }
    synchronized (activeWriteRequests)
    {
      while (activeWriteRequests.containsKey(lId))
      {
        try
        {
          activeWriteRequests.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }
    }
  }

  /**
   * Schedule a read request
   *
   * @param request select request to schedule
   * @exception SQLException if a timeout occurs or a query with the same id has
   *              already been scheduled.
   */
  public void scheduleReadRequest(SelectRequest request) throws SQLException
  {
    Long id = new Long(request.getId());
    synchronized (activeReadRequests)
    {
      if (activeReadRequests.containsKey(id))
        throw new SQLException("A read request with id " + id
            + " has already been scheduled");
      activeReadRequests.put(id, request);
    }

    // Assign a unique transaction id to requests in autocommit mode as well
    if (request.isAutoCommit() && request.isMustBroadcast())
    {
      long fakeTid = getNextTransactionId();
      fakeTid = fakeTid & DistributedRequestManager.TRANSACTION_ID_BIT_MASK;
      fakeTid = fakeTid | controllerId;
      request.setTransactionId(fakeTid);
    }

    try
    {
      scheduleNonSuspendedReadRequest(request);
    }
    catch (SQLException e)
    {
      // Remove query for the active queue if we failed to schedule
      synchronized (activeReadRequests)
      {
        activeReadRequests.remove(id);
      }
      throw e;
    }
  }

  /**
   * Schedule a read request (implementation specific)
   *
   * @param request Select request to schedule (SQL macros are already handled
   *          if needed)
   * @exception SQLException if a timeout occurs
   */
  protected abstract void scheduleNonSuspendedReadRequest(SelectRequest request)
      throws SQLException;

  /**
   * Notify the completion of a read statement.
   *
   * @param request the completed request
   * @throws SQLException if the query was not in the list of active read
   *           requests (not scheduled)
   */
  public final void readCompleted(SelectRequest request) throws SQLException
  {
    Long id = new Long(request.getId());
    synchronized (activeReadRequests)
    {
      if (activeReadRequests.remove(id) == null)
        throw new SQLException("Query " + id
            + " is not in the list of currently scheduled queries");
      activeReadRequests.notifyAll();
    }
    numberRead++;
    this.readCompletedNotify(request);
  }

  /**
   * Notify the completion of a read statement.
   *
   * @param request the completed request
   */
  protected abstract void readCompletedNotify(SelectRequest request);

  /**
   * Schedule a write request. This method blocks if the writes are suspended.
   * Then the number of pending writes is updated and the implementation
   * specific scheduleNonSuspendedWriteRequest function is called. SQL macros
   * are replaced in the request if the scheduler has needSQLMacroHandling set
   * to true.
   *
   * @param request Write request to schedule
   * @exception SQLException if a timeout occurs or a query with the same id has
   *              already been scheduled.
   * @exception RollbackException if an error occurs
   * @see #scheduleNonSuspendedWriteRequest(AbstractWriteRequest)
   */
  public final void scheduleWriteRequest(AbstractWriteRequest request)
      throws SQLException, RollbackException
  {
    suspendWriteIfNeededAndAddQueryToActiveRequests(request);
    scheduleNonSuspendedWriteRequest(request);

    // Assign a unique transaction id to requests in autocommit mode as well
    if (request.isAutoCommit())
    {
      long fakeTid = getNextTransactionId();
      fakeTid = fakeTid & DistributedRequestManager.TRANSACTION_ID_BIT_MASK;
      fakeTid = fakeTid | controllerId;
      request.setTransactionId(fakeTid);
    }
  }

  /**
   * Schedule a write request (implementation specific). This method blocks
   * until the request can be executed.
   *
   * @param request Write request to schedule (SQL macros are already handled if
   *          needed)
   * @exception SQLException if a timeout occurs
   * @exception RollbackException if the transaction must be rollbacked
   */
  protected abstract void scheduleNonSuspendedWriteRequest(
      AbstractWriteRequest request) throws SQLException, RollbackException;

  /**
   * Notify the completion of a write statement.
   * <p>
   * This method updates the number of pending writes and calls the
   * implementation specific notifyWriteCompleted function.
   * <p>
   * Finally, the suspendWrites() function is notified if needed.
   *
   * @param request the completed request
   * @throws SQLException if the query is not in the list of scheduled queries
   * @see #notifyWriteCompleted(AbstractWriteRequest)
   * @see #checkPendingWrites()
   */
  public final void writeCompleted(AbstractWriteRequest request)
      throws SQLException
  {
    Long id = new Long(request.getId());

    synchronized (writesSync)
    {
      synchronized (activeWriteRequests)
      {
        if (activeWriteRequests.remove(id) == null)
          throw new SQLException("Query " + id
              + " is not in the list of currently scheduled queries");

        activeWriteRequests.notifyAll();
      }
      pendingWrites--;

      if (pendingWrites < 0)
      {
        logger
            .error("Negative pending writes detected on write request completion ("
                + request + ")");
        pendingWrites = 0;
      }

      if (logger.isDebugEnabled())
        logger.debug("Write completed, remaining pending writes: "
            + pendingWrites);

      notifyWriteCompleted(request);

      checkPendingWrites();
    }
    numberWrite++;
  }

  /**
   * Notify the completion of a write statement. This method does not need to be
   * synchronized, it is enforced by the caller.
   *
   * @param request the completed request
   * @see #writeCompleted(AbstractWriteRequest)
   */
  protected abstract void notifyWriteCompleted(AbstractWriteRequest request);

  /**
   * Schedule a write request. This method blocks if the writes are suspended.
   * Then the number of pending writes is updated and the implementation
   * specific scheduleNonSuspendedWriteRequest function is called. SQL macros
   * are replaced in the request if the scheduler has needSQLMacroHandling set
   * to true.
   *
   * @param proc Stored procedure to schedule
   * @exception SQLException if a timeout occurs
   * @exception RollbackException if an error occurs
   * @see #scheduleNonSuspendedStoredProcedure(StoredProcedure)
   */
  public final void scheduleStoredProcedure(StoredProcedure proc)
      throws SQLException, RollbackException
  {
    suspendWriteIfNeededAndAddQueryToActiveRequests(proc);
    scheduleNonSuspendedStoredProcedure(proc);

    // Assign a unique transaction id to requests in autocommit mode as well
    if (proc.isAutoCommit())
    {
      long fakeTid = getNextTransactionId();
      fakeTid = fakeTid & DistributedRequestManager.TRANSACTION_ID_BIT_MASK;
      fakeTid = fakeTid | controllerId;
      proc.setTransactionId(fakeTid);
    }
  }

  /**
   * Schedule a write request (implementation specific). This method blocks
   * until the request can be executed.
   *
   * @param proc Stored procedure to schedule
   * @exception SQLException if a timeout occurs
   * @exception RollbackException if the transaction must be rollbacked
   */
  protected abstract void scheduleNonSuspendedStoredProcedure(
      StoredProcedure proc) throws SQLException, RollbackException;

  /**
   * Notify the completion of a stored procedure.
   * <p>
   * This method updates the number of pending writes and calls the
   * implementation specific notifyStoredProcedureCompleted function.
   * <p>
   * Finally, the suspendWrites() function is notified if needed.
   *
   * @param proc the completed stored procedure
   * @throws SQLException if the stored procedure was not scheduled before (not
   *           in the active request list)
   * @see #notifyStoredProcedureCompleted(StoredProcedure)
   * @see #checkPendingWrites()
   */
  public final void storedProcedureCompleted(StoredProcedure proc)
      throws SQLException
  {
    Long id = new Long(proc.getId());

    synchronized (writesSync)
    {
      synchronized (activeWriteRequests)
      {
        if (activeWriteRequests.remove(id) == null)
          throw new SQLException("Query " + id
              + " is not in the list of currently scheduled queries");

        activeWriteRequests.notifyAll();
      }

      pendingWrites--;

      if (pendingWrites < 0)
      {
        logger
            .error("Negative pending writes detected on stored procedure completion ("
                + proc + ")");
        pendingWrites = 0;
      }

      if (logger.isDebugEnabled())
        logger.debug("Stored procedure completed, remaining pending writes: "
            + pendingWrites);

      notifyStoredProcedureCompleted(proc);

      checkPendingWrites();
    }
    numberWrite++;
  }

  /**
   * Notify the completion of a stored procedure. This method does not need to
   * be synchronized, it is enforced by the caller.
   *
   * @param proc the completed stored procedure
   * @see #storedProcedureCompleted(StoredProcedure)
   */
  protected abstract void notifyStoredProcedureCompleted(StoredProcedure proc);

  /**
   * Suspend write requests if suspendedWrites is active. Adds the request to
   * the list of active requests after successful scheduling.
   *
   * @param request the request to suspend (a write request or a stored
   *          procedure)
   * @throws SQLException if the request timeout has expired or a query with the
   *           same id has already been scheduled.
   */
  private void suspendWriteIfNeededAndAddQueryToActiveRequests(
      AbstractRequest request) throws SQLException
  {
    Long id = new Long(request.getId());

    synchronized (writesSync)
    {
      if (suspendWrites > 0)
      {
        // Let requests in active transactions to execute since they might
        // unblock queries of other transactions.
        boolean mustBeSuspended = !request.isPersistentConnection()
            && (request.isAutoCommit() || !activeTransactions
                .contains(new TransactionMetaData(request.getTransactionId(),
                    0, request.getLogin(), request.isPersistentConnection(),
                    request.getPersistentConnectionId())));

        if (mustBeSuspended)
        {
          addSuspendedRequest(request);
          try
          {
            // Wait on writesSync
            int timeout = request.getTimeout();
            if (timeout > 0)
            {
              long start = System.currentTimeMillis();
              long lTimeout = timeout * 1000L;
              writesSync.wait(lTimeout);
              long end = System.currentTimeMillis();
              int remaining = (int) (lTimeout - (end - start));
              if (remaining > 0)
                request.setTimeout(remaining);
              else
              {
                String msg = Translate.get("scheduler.request.timeout",
                    new String[]{String.valueOf(request.getId()),
                        String.valueOf(request.getTimeout()),
                        String.valueOf(pendingWrites)});
                logger.warn(msg);
                throw new SQLException(msg);
              }
            }
            else
              this.writesSync.wait();
          }
          catch (InterruptedException e)
          {
            String msg = Translate.get("scheduler.request.timeout.failed", e);
            logger.warn(msg);
            throw new SQLException(msg);
          }
        }
      }

      synchronized (activeWriteRequests)
      {
        if (activeWriteRequests.containsKey(id))
          throw new SQLException("A write request with id " + id
              + " has already been scheduled");

        activeWriteRequests.put(id, request);
      }
      pendingWrites++;

      if (logger.isDebugEnabled())
        logger.debug("Schedule " + request.getUniqueKey()
            + " - Current pending writes: " + pendingWrites);
    }
  }

  /**
   * Signals the start of a persistent connection opening operation.
   *
   * @param dmsg distributed message which triggered this operation
   */
  public void scheduleOpenPersistentConnection(
      DistributedOpenPersistentConnection dmsg)
  {
    checkForSuspendedOpenClosePersistentConnectionsAndIncreasePendingCount();

    // Underlying Hashtable is synchronized and we systematically overwrite
    // any previous value, it is as fast as checking first.
    // Check if persistent connections creation is suspended
    synchronized (persistentConnectionsSync)
    {
      if (suspendNewPersistentConnections > 0)
      {
        addSuspendedRequest(dmsg);
        try
        {
          persistentConnectionsSync.wait();
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
      }
      activePersistentConnections.put(
          new Long(dmsg.getPersistentConnectionId()), dmsg.getLogin());
    }
  }

  /**
   * Schedule a close persistent connection.
   */
  public void scheduleClosePersistentConnection()
  {
    checkForSuspendedOpenClosePersistentConnectionsAndIncreasePendingCount();
  }

  private void checkForSuspendedOpenClosePersistentConnectionsAndIncreasePendingCount()
  {
    synchronized (suspendOpenClosePersistentConnectionSync)
    {
      while (suspendOpenClosePersistentConnections > 0)
      {
        try
        {
          suspendOpenClosePersistentConnectionSync.wait();
        }
        catch (InterruptedException e)
        {
        }
      }
      pendingOpenClosePersistentConnections++;
    }
  }

  private void decrementOpenClosePersistentConnectionCount()
  {
    synchronized (suspendOpenClosePersistentConnectionSync)
    {
      pendingOpenClosePersistentConnections--;
      if (pendingOpenClosePersistentConnections < 0)
      {
        logger
            .error("Negative count of pending open/close persistent connections");
        pendingOpenClosePersistentConnections = 0;
      }
      if (suspendOpenClosePersistentConnections == 0)
        suspendOpenClosePersistentConnectionSync.notifyAll();
    }
  }

  /**
   * Notify open persistent connection completion. If it failed the connection
   * is removed from the persistentConnections table.
   *
   * @param persistentConnectionId id of the opened persistent connection
   * @param success true if connection opening was successful in which case the
   *          connection is added to the persistent connection list
   */
  public void openPersistentConnectionCompleted(long persistentConnectionId,
      boolean success)
  {
    decrementOpenClosePersistentConnectionCount();
    if (!success)
      synchronized (endOfCurrentPersistentConnections)
      {
        activePersistentConnections.remove(new Long(persistentConnectionId));
        endOfCurrentPersistentConnections.notifyAll();
      }
  }

  /**
   * Signals the completion of a persistent connection closing operation.
   *
   * @param persistentConnectionId id of the closed persistent connection
   */
  public void closePersistentConnectionCompleted(long persistentConnectionId)
  {
    decrementOpenClosePersistentConnectionCount();
    synchronized (endOfCurrentPersistentConnections)
    {
      activePersistentConnections.remove(new Long(persistentConnectionId));
      endOfCurrentPersistentConnections.notifyAll();
    }
  }

  /**
   * Returns the login associated with a given persistent connection.
   *
   * @param persistentConnectionId the id of the persistent connection
   * @return the associated login
   */
  public String getPersistentConnectionLogin(Long persistentConnectionId)
  {
    return (String) activePersistentConnections.get(persistentConnectionId);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#hasPersistentConnection(long)
   */
  public boolean hasPersistentConnection(long persistentConnectionId)
  {
    return activePersistentConnections
        .contains(new Long(persistentConnectionId));
  }

  /**
   * Returns a hashtable of all the open persistent connections (and their
   * associated login).
   *
   * @return persistent connection hashtable
   */
  public Hashtable getOpenPersistentConnections()
  {
    return activePersistentConnections;
  }

  //
  // 5. Transaction management
  //

  /**
   * Returns the list of active transactions (list contains transaction ids).
   *
   * @return Returns the active transaction ids.
   */
  public final List getActiveTransactions()
  {
    return activeTransactions;
  }

  /**
   * Retrieve the next transaction identifier
   *
   * @return next transaction identifier
   */
  public long getNextTransactionId()
  {
    synchronized (transactionsSync)
    {
      return transactionId++;
    }
  }

  /**
   * Increments the savepoint id for un-named savepoints
   *
   * @return the next savepoint Id
   */
  public synchronized int incrementSavepointId()
  {
    savepointId++;
    return savepointId;
  }

  /**
   * Initialize the transaction id with the given value (usually retrieved from
   * the recovery log).
   *
   * @param transactionId new current transaction identifier
   */
  public final void initializeTransactionId(long transactionId)
  {
    synchronized (transactionsSync)
    {
      // Use the max operator as a safeguard: IDs may have been delivered but
      // not logged yet.
      this.transactionId = Math.max(this.transactionId + 1, transactionId);
    }
  }

  /**
   * Begin a new transaction with the transaction identifier provided in the
   * transaction meta data parameter. Note that this id must retrieve beforehand
   * by calling getNextTransactionId(). This method is called from the driver
   * when setAutoCommit(false) is called.
   *
   * @param tm The transaction marker metadata
   * @param isLazyStart true if this begin is triggered by a lazy transaction
   *          start of a transaction initiated by a remote controller. In that
   *          case, suspended transactions will be ignored (but not suspended
   *          writes)
   * @param request request which triggered this operation
   * @throws SQLException if an error occurs
   */
  public final void begin(TransactionMetaData tm, boolean isLazyStart,
      AbstractRequest request) throws SQLException
  {
    // Check if transactions are suspended
    boolean retry;
    do
    {
      retry = false;
      synchronized (transactionsSync)
      {
        if ((suspendTransactions > 0) && !isLazyStart
            && !tm.isPersistentConnection())
        {
          addSuspendedRequest(request);
          try
          {
            // Wait on transactionSync
            long timeout = tm.getTimeout();
            if (timeout > 0)
            {
              long start = System.currentTimeMillis();
              transactionsSync.wait(timeout);
              long end = System.currentTimeMillis();
              long remaining = timeout - (end - start);
              if (remaining > 0)
                tm.setTimeout(remaining);
              else
              {
                String msg = Translate.get(
                    "scheduler.begin.timeout.transactionSync",
                    pendingTransactions);
                logger.warn(msg);
                throw new SQLException(msg);
              }
            }
            else
              transactionsSync.wait();
          }
          catch (InterruptedException e)
          {
            String msg = Translate.get(
                "scheduler.begin.timeout.transactionSync", pendingTransactions)
                + " (" + e + ")";
            logger.error(msg);
            throw new SQLException(msg);
          }
        }
        if (vdb != null && vdb.isRejectingNewTransaction())
          throw new VDBisShuttingDownException(
              "VDB is shutting down... can't start a new transaction");

        pendingTransactions++;

        if (logger.isDebugEnabled())
          logger.debug("Begin scheduled - current pending transactions: "
              + pendingTransactions);
      }

      // Check if writes are suspended
      synchronized (writesSync)
      {
        /*
         * If suspendedTransaction changed after we left the block above, we
         * need to go back and wait there.
         */
        synchronized (transactionsSync)
        {
          if ((suspendTransactions > 0) && !isLazyStart
              && !tm.isPersistentConnection())
          {
            retry = true;
            pendingTransactions--;
            checkPendingTransactions();
            continue;
          }
        }
        if ((suspendWrites > 0) && !isLazyStart && !tm.isPersistentConnection())
        {
          addSuspendedRequest(request);
          try
          {
            // Wait on writesSync
            long timeout = tm.getTimeout();
            if (timeout > 0)
            {
              long start = System.currentTimeMillis();
              writesSync.wait(timeout);
              long end = System.currentTimeMillis();
              long remaining = timeout - (end - start);
              if (remaining > 0)
                tm.setTimeout(remaining);
              else
              {
                String msg = Translate.get(
                    "scheduler.begin.timeout.writesSync", pendingWrites);
                logger.warn(msg);
                synchronized (transactionsSync)
                {
                  pendingTransactions--;
                }
                checkPendingTransactions();
                throw new SQLException(msg);
              }
            }
            else
              writesSync.wait();
          }
          catch (InterruptedException e)
          {
            String msg = Translate.get("scheduler.begin.timeout.writesSync",
                pendingWrites)
                + " (" + e + ")";
            logger.error(msg);
            synchronized (transactionsSync)
            {
              pendingTransactions--;
            }
            checkPendingTransactions();
            throw new SQLException(msg);
          }
        }
        pendingWrites++;

        if (logger.isDebugEnabled())
          logger.debug("Begin scheduled - current pending writes: "
              + pendingWrites);

        // Check if the transaction has not already been started and add it to
        // the
        // active transaction list
        if (activeTransactions.contains(tm))
        {
          logger.error("Trying to start twice transaction "
              + tm.getTransactionId());
        }
        else
          activeTransactions.add(tm);
      }
    }
    while (retry);
  }

  /**
   * Notify the completion of a begin command.
   *
   * @param transactionId of the completed begin
   */
  public final void beginCompleted(long transactionId)
  {
    // Take care of suspended write
    synchronized (writesSync)
    {
      pendingWrites--;
      if (pendingWrites < 0)
      {
        logger
            .error("Negative pending writes detected on begin completion for transaction "
                + transactionId);
        pendingWrites = 0;
      }

      if (logger.isDebugEnabled())
        logger.debug("Begin completed, remaining pending writes: "
            + pendingWrites);

      checkPendingWrites();
    }
  }

  /**
   * Commit a transaction.
   * <p>
   * Calls the implementation specific commitTransaction()
   *
   * @param tm The transaction marker metadata
   * @param emptyTransaction true if we are committing a transaction that did
   *          not execute any query
   * @param dmsg distributed message which triggered this operation
   * @throws SQLException if an error occurs
   * @see #commitTransaction(long)
   */
  public final void commit(TransactionMetaData tm, boolean emptyTransaction,
      DistributedCommit dmsg) throws SQLException
  {
    // Check if writes are suspended
    synchronized (writesSync)
    {
      if (!activeTransactions.contains(tm))
        throw new SQLException("Transaction " + tm.getTransactionId()
            + " is not active, rejecting the commit.");

      // if ((suspendedWrites > 0) && !tm.isPersistentConnection())
      if (false) // never suspend a commit
      {
        addSuspendedRequest(dmsg);
        try
        {
          // Wait on writesSync
          long timeout = tm.getTimeout();
          if (timeout > 0)
          {
            long start = System.currentTimeMillis();
            writesSync.wait(timeout);
            long end = System.currentTimeMillis();
            long remaining = timeout - (end - start);
            if (remaining > 0)
              tm.setTimeout(remaining);
            else
            {
              String msg = Translate.get("scheduler.commit.timeout.writesSync",
                  pendingWrites);
              logger.warn(msg);
              throw new SQLException(msg);
            }
          }
          else
            writesSync.wait();
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get("scheduler.commit.timeout.writesSync",
              pendingWrites)
              + " (" + e + ")";
          logger.error(msg);
          throw new SQLException(msg);
        }
      }
      pendingWrites++;

      if (logger.isDebugEnabled())
        logger.debug("Commit scheduled - current pending writes: "
            + pendingWrites);
    }
    if (!emptyTransaction)
      commitTransaction(tm.getTransactionId());
  }

  /**
   * Commit a transaction given its id.
   *
   * @param transactionId the transaction id
   */
  protected abstract void commitTransaction(long transactionId);

  /**
   * Notify the completion of a commit command.
   *
   * @param tm The transaction marker metadata
   * @param isSuccess true if commit was successful, false otherwise
   */
  public final void commitCompleted(TransactionMetaData tm, boolean isSuccess)
  {
    boolean transactionIsActive = false;
    synchronized (writesSync)
    {
      if (isSuccess)
      {
        transactionIsActive = activeTransactions.remove(tm);
      }
    }
    if (transactionIsActive)
    {
      // Take care of suspended transactions
      synchronized (transactionsSync)
      {
        pendingTransactions--;
        if (pendingTransactions < 0)
        {
          logger
              .error("Negative pending transactions detected on commit completion for transaction "
                  + tm.getTransactionId());
          pendingTransactions = 0;
        }

        if (logger.isDebugEnabled())
          logger.debug("Commit completed, remaining pending transactions: "
              + pendingTransactions);

        checkPendingTransactions();
      }
    }
    else if ((isSuccess) && (logger.isDebugEnabled()))
      logger.debug("Transaction " + tm.getTransactionId()
          + " has already completed.");

    // Take care of suspended write
    synchronized (writesSync)
    {
      pendingWrites--;
      if (pendingWrites < 0)
      {
        logger
            .error("Negative pending writes detected on commit completion for transaction"
                + tm.getTransactionId());
        pendingWrites = 0;
      }

      if (logger.isDebugEnabled())
        logger.debug("Commit completed, remaining pending writes: "
            + pendingWrites);

      checkPendingWrites();
    }
  }

  /**
   * Rollback a transaction.
   * <p>
   * Calls the implementation specific rollbackTransaction()
   *
   * @param tm The transaction marker metadata
   * @param dmsg distributed message which triggered this operation
   * @exception SQLException if an error occurs
   * @see #rollbackTransaction(long)
   */
  public final void rollback(TransactionMetaData tm, DistributedRollback dmsg)
      throws SQLException
  {
    // Check if writes are suspended
    synchronized (writesSync)
    {
      if (!activeTransactions.contains(tm))
        throw new SQLException("Transaction " + tm.getTransactionId()
            + " is not active, rejecting the rollback.");

      // if ((suspendedWrites > 0) && !tm.isPersistentConnection())
      if (false) // never suspend a rollback
      {
        addSuspendedRequest(dmsg);
        try
        {
          // Wait on writesSync
          long timeout = tm.getTimeout();
          if (timeout > 0)
          {
            long start = System.currentTimeMillis();
            writesSync.wait(timeout);
            long end = System.currentTimeMillis();
            long remaining = timeout - (end - start);
            if (remaining > 0)
              tm.setTimeout(remaining);
            else
            {
              String msg = Translate.get(
                  "scheduler.rollback.timeout.writesSync", pendingWrites);
              logger.warn(msg);
              throw new SQLException(msg);
            }
          }
          else
            writesSync.wait();
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get("scheduler.rollback.timeout.writesSync",
              pendingWrites)
              + " (" + e + ")";
          logger.error(msg);
          throw new SQLException(msg);
        }
      }
      pendingWrites++;

      if (logger.isDebugEnabled())
        logger.debug("Rollback scheduled - current pending writes: "
            + pendingWrites);
    }
    rollbackTransaction(tm.getTransactionId());
  }

  /**
   * Rollback a transaction to a savepoint.
   * <p>
   * Calls the implementation specific rollbackTransaction()
   *
   * @param tm transaction marker metadata
   * @param savepointName name of the savepoint
   * @param dmsg distributed message which triggered this operation
   * @throws SQLException if an error occurs
   */
  public final void rollback(TransactionMetaData tm, String savepointName,
      DistributedRollbackToSavepoint dmsg) throws SQLException
  {
    // Check if writes are suspended
    synchronized (writesSync)
    {
      // if ((suspendedWrites > 0) && !tm.isPersistentConnection())
      if (false) // never suspend a rollback
      {
        addSuspendedRequest(dmsg);
        try
        {
          // Wait on writesSync
          long timeout = tm.getTimeout();
          if (timeout > 0)
          {
            long start = System.currentTimeMillis();
            writesSync.wait(timeout);
            long end = System.currentTimeMillis();
            long remaining = timeout - (end - start);
            if (remaining > 0)
              tm.setTimeout(remaining);
            else
            {
              String msg = Translate.get(
                  "scheduler.rollbacksavepoint.timeout.writeSync",
                  pendingWrites);
              logger.warn(msg);
              throw new SQLException(msg);
            }
          }
          else
            writesSync.wait();
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get(
              "scheduler.rollbacksavepoint.timeout.writeSync", pendingWrites)
              + " (" + e + ")";
          logger.error(msg);
          throw new SQLException(msg);
        }
      }
      pendingWrites++;

      if (logger.isDebugEnabled())
        logger.debug("Rollback " + savepointName
            + " scheduled - current pending writes: " + pendingWrites);
    }

    this.rollbackTransaction(tm.getTransactionId(), savepointName);
  }

  /**
   * Rollback a transaction given its id.
   *
   * @param transactionId the transaction id
   */
  protected abstract void rollbackTransaction(long transactionId);

  /**
   * Rollback a transaction given its id to a savepoint given its name.
   *
   * @param transactionId the transaction id
   * @param savepointName the name of the savepoint
   */
  protected abstract void rollbackTransaction(long transactionId,
      String savepointName);

  /**
   * Notify the completion of a rollback command.
   *
   * @param tm The transaction marker metadata
   * @param isSuccess true if commit was successful, false otherwise
   */
  public final void rollbackCompleted(TransactionMetaData tm, boolean isSuccess)
  {
    boolean transactionIsActive = false;
    synchronized (writesSync)
    {
      if (isSuccess)
      {
        transactionIsActive = activeTransactions.remove(tm);
      }
    }
    if (transactionIsActive)
    {
      // Take care of suspended transactions
      synchronized (transactionsSync)
      {
        pendingTransactions--;
        if (pendingTransactions < 0)
        {
          logger
              .error("Negative pending transactions detected on rollback completion for transaction "
                  + tm.getTransactionId());
          pendingTransactions = 0;
        }

        if (logger.isDebugEnabled())
          logger.debug("Rollback completed, remaining pending transactions: "
              + pendingTransactions);

        checkPendingTransactions();
      }
    }
    else if ((isSuccess) && (logger.isDebugEnabled()))
      logger.debug("Transaction " + tm.getTransactionId()
          + " has already completed.");

    // Take care of suspended write
    synchronized (writesSync)
    {
      pendingWrites--;

      if (pendingWrites < 0)
      {
        logger
            .error("Negative pending writes detected on rollback completion for transaction "
                + tm.getTransactionId());
        pendingWrites = 0;
      }

      if (logger.isDebugEnabled())
        logger.debug("Rollback completed, remaining pending writes: "
            + pendingWrites);

      checkPendingWrites();
    }
  }

  /**
   * Set an unnamed savepoint.
   * <p>
   * Calls the implementation specific setSavepointTransaction()
   *
   * @param tm transaction marker metadata
   * @return savepoint Id
   * @throws SQLException if an error occurs
   */
  public final int setSavepoint(TransactionMetaData tm) throws SQLException
  {
    // Check if writes are suspended
    synchronized (writesSync)
    {
      if (suspendWrites > 0)
      {
        try
        {
          // Wait on writesSync
          long timeout = tm.getTimeout();
          if (timeout > 0)
          {
            long start = System.currentTimeMillis();
            writesSync.wait(timeout);
            long end = System.currentTimeMillis();
            long remaining = timeout - (end - start);
            if (remaining > 0)
              tm.setTimeout(remaining);
            else
            {
              String msg = Translate.get(
                  "scheduler.setsavepoint.timeout.writeSync", pendingWrites);
              logger.warn(msg);
              throw new SQLException(msg);
            }
          }
          else
            writesSync.wait();
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get(
              "scheduler.setsavepoint.timeout.writeSync", pendingWrites)
              + " (" + e + ")";
          logger.error(msg);
          throw new SQLException(msg);
        }
      }
      pendingWrites++;

      if (logger.isDebugEnabled())
        logger.debug("Set savepoint scheduled - current pending writes: "
            + pendingWrites);
    }

    int savepointId = this.incrementSavepointId();
    this.setSavepointTransaction(tm.getTransactionId(), String
        .valueOf(savepointId));
    return savepointId;
  }

  /**
   * Set a named savepoint.
   * <p>
   * Calls the implementation specific setSavepointTransaction()
   *
   * @param tm transaction marker metadata
   * @param name name of the savepoint
   * @param dmsg distributed message which triggered this operation
   * @throws SQLException if an error occurs
   */
  public final void setSavepoint(TransactionMetaData tm, String name,
      DistributedSetSavepoint dmsg) throws SQLException
  {
    // Check if writes are suspended
    synchronized (writesSync)
    {
      if (suspendWrites > 0)
      {
        addSuspendedRequest(dmsg);
        try
        {
          // Wait on writesSync
          long timeout = tm.getTimeout();
          if (timeout > 0)
          {
            long start = System.currentTimeMillis();
            writesSync.wait(timeout);
            long end = System.currentTimeMillis();
            long remaining = timeout - (end - start);
            if (remaining > 0)
              tm.setTimeout(remaining);
            else
            {
              String msg = Translate.get(
                  "scheduler.setsavepoint.timeout.writeSync", pendingWrites);
              logger.warn(msg);
              throw new SQLException(msg);
            }
          }
          else
            writesSync.wait();
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get(
              "scheduler.setsavepoint.timeout.writeSync", pendingWrites)
              + " (" + e + ")";
          logger.error(msg);
          throw new SQLException(msg);
        }
      }
      pendingWrites++;

      if (logger.isDebugEnabled())
        logger.debug("Set savepoint " + name
            + " scheduled - current pending writes: " + pendingWrites);
    }

    this.setSavepointTransaction(tm.getTransactionId(), name);
  }

  /**
   * Set a savepoint given its name to a transaction given its id.
   *
   * @param transactionId the transaction id
   * @param name the name of the savepoint
   */
  protected abstract void setSavepointTransaction(long transactionId,
      String name);

  /**
   * Release a savepoint.
   * <p>
   * Calls the implementation specific releaseSavepointTransaction()
   *
   * @param tm transaction marker metadata
   * @param name name of the savepoint
   * @param dmsg distributed message which triggered this operation
   * @throws SQLException if an error occurs
   */
  public final void releaseSavepoint(TransactionMetaData tm, String name,
      DistributedReleaseSavepoint dmsg) throws SQLException
  {
    // Check if writes are suspended
    synchronized (writesSync)
    {
      if (suspendWrites > 0)
      {
        addSuspendedRequest(dmsg);
        try
        {
          // Wait on writesSync
          long timeout = tm.getTimeout();
          if (timeout > 0)
          {
            long start = System.currentTimeMillis();
            writesSync.wait(timeout);
            long end = System.currentTimeMillis();
            long remaining = timeout - (end - start);
            if (remaining > 0)
              tm.setTimeout(remaining);
            else
            {
              String msg = Translate
                  .get("scheduler.releasesavepoint.timeout.writeSync",
                      pendingWrites);
              logger.warn(msg);
              throw new SQLException(msg);
            }
          }
          else
            writesSync.wait();
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get(
              "scheduler.releasesavepoint.timeout.writeSync", pendingWrites)
              + " (" + e + ")";
          logger.error(msg);
          throw new SQLException(msg);
        }
      }
      pendingWrites++;

      if (logger.isDebugEnabled())
        logger.debug("Release savepoint " + name
            + " scheduled - current pending writes: " + pendingWrites);
    }

    this.releaseSavepointTransaction(tm.getTransactionId(), name);
  }

  /**
   * Release a savepoint given its name from a transaction given its id.
   *
   * @param transactionId the transaction id
   * @param name the name of the savepoint
   */
  protected abstract void releaseSavepointTransaction(long transactionId,
      String name);

  /**
   * Notify the conpletion of a savepoint action.
   *
   * @param transactionId the transaction identifier
   */
  public final void savepointCompleted(long transactionId)
  {
    synchronized (writesSync)
    {
      pendingWrites--;

      if (pendingWrites < 0)
      {
        logger
            .error("Negative pending writes detected on savepoint completion for transaction"
                + transactionId);
        pendingWrites = 0;
      }

      if (logger.isDebugEnabled())
        logger.debug("Savepoint completed, remaining pending writes: "
            + pendingWrites);

      checkPendingWrites();
    }
  }

  //
  // 6. Checkpoint management
  //

  /**
   * Resume new transactions that were suspended by
   * suspendNewTransactionsForCheckpoint().
   *
   * @see #suspendNewTransactions()
   */
  public final void resumeNewTransactions()
  {
    if (logger.isDebugEnabled())
      logger.debug("Resuming new transactions");

    synchronized (transactionsSync)
    {
      suspendTransactions--;
      if (suspendTransactions < 0)
      {
        suspendTransactions = 0;
        logger
            .error("Unexpected negative suspendedTransactions in AbstractScheduler.resumeNewTransactions()");
      }
      if (suspendTransactions == 0)
      {
        // Wake up all pending begin statements
        transactionsSync.notifyAll();
      }
    }
  }

  /**
   * Suspend all calls to begin() until until resumeWrites() is called. This
   * method does not block and returns immediately. To synchronize on suspended
   * writes completion, you must call waitForSuspendedWritesToComplete().
   * <p>
   * New transactions remain suspended until resumeNewTransactions() is called.
   *
   * @see #resumeNewTransactions()
   * @see #waitForSuspendedTransactionsToComplete()
   */
  public final void suspendNewTransactions()
  {
    if (logger.isDebugEnabled())
      logger.debug("Suspending new transactions");

    synchronized (transactionsSync)
    {
      suspendTransactions++;
    }
  }

  /**
   * Suspend all calls to begin() until until resumeWrites() and
   * resumeNewTransactions are called. This method does not block and returns
   * immediately. To synchronize on suspended writes completion, you must call
   * waitForSuspendedWritesToComplete(). Suspending writes and transactions is
   * done atomically in order to close a window in begin().
   * <p>
   * New transactions remain suspended until resumeNewTransactions() and
   * resumeWrites are called.
   *
   * @see #resumeNewTransactions()
   * @see #waitForSuspendedTransactionsToComplete()
   */
  public void suspendNewTransactionsAndWrites()
  {
    if (logger.isDebugEnabled())
      logger.debug("Suspending new transactions and writes");

    synchronized (writesSync)
    {
      synchronized (transactionsSync)
      {
        suspendTransactions++;
        suspendWrites++;
      }
    }
  }

  /**
   * Wait for suspended transactions to complete. Returns as soon as number of
   * pending transactions has reached 0.
   *
   * @throws SQLException if an error occured during wait
   */
  public void waitForSuspendedTransactionsToComplete() throws SQLException
  {
    synchronized (transactionsSync)
    {
      if (pendingTransactions == 0)
      {
        if (logger.isDebugEnabled())
          logger.debug("All transactions suspended");
        return;
      }
    }

    // Wait for pending transactions to end
    boolean checkForTimeout = waitForSuspendedTransactionsTimeout > 0;
    long waitTime = INITIAL_WAIT_TIME;
    long totalWaitTime = 0;
    long start;
    long realWait;
    while (true)
    {
      synchronized (endOfCurrentTransactions)
      {
        // Here we have a potential synchronization problem since the last
        // transaction completion could have happened before we entered this
        // synchronized block. Therefore we recheck if there is effectively
        // still pending transactions. If this is not the case, we don't have
        // to sleep and we can immediately return.
        if (pendingTransactions == 0)
        {
          if (logger.isDebugEnabled())
            logger.debug("All new transactions suspended");
          return;
        }

        if (logger.isDebugEnabled())
          logger.debug("Waiting for " + pendingTransactions
              + " transactions to complete.");

        try
        {
          start = System.currentTimeMillis();
          endOfCurrentTransactions.wait(waitTime);
          realWait = System.currentTimeMillis() - start;
          totalWaitTime += realWait;
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get("scheduler.suspend.transaction.failed", e);
          logger.warn(msg);
          throw new SQLException(msg);
        }
      }
      synchronized (transactionsSync)
      {
        if (pendingTransactions == 0)
        {
          checkForTimeout = false;
          break;
        }
        if (logger.isWarnEnabled() && (activeTransactions.size() > 0))
        {
          StringBuffer transactions = new StringBuffer("[");
          for (Iterator iter = activeTransactions.iterator(); iter.hasNext();)
            transactions.append((transactions.length() > 1 ? ", " : "")
                + ((TransactionMetaData) iter.next()).getTransactionId());
          transactions.append("]");
          logger.warn("Waited for " + Math.round((totalWaitTime / 1000.0))
              + " secs but " + activeTransactions.size()
              + " transactions still open: " + transactions);
          if (checkForTimeout)
            logger
                .warn("Will wait for "
                    + Math
                        .max(
                            0,
                            Math
                                .round((waitForSuspendedTransactionsTimeout - totalWaitTime) / 1000.0))
                    + " secs more and attempt to abort them");
        }
        if (checkForTimeout
            && totalWaitTime >= waitForSuspendedTransactionsTimeout)
          break;
        waitTime *= 2;
        if (checkForTimeout)
          waitTime = Math.min(waitTime, waitForSuspendedTransactionsTimeout
              - totalWaitTime);
      }
    }
    if (checkForTimeout && totalWaitTime >= waitForSuspendedTransactionsTimeout)
    {
      if (logger.isWarnEnabled())
        logger.warn("Timeout reached ("
            + Math.round(waitForSuspendedTransactionsTimeout / 1000.0)
            + " secs), aborting remaining active transactions");
      abortRemainingActiveTransactions();
    }
    if (logger.isDebugEnabled())
      logger.debug("All new transactions suspended");
  }

  /**
   * Resume the execution of the <em>new write queries</em> that were
   * suspended by <code>suspendNewWrites()</code>.
   *
   * @see #suspendNewWrites()
   */
  public void resumeWrites()
  {
    if (logger.isDebugEnabled())
      logger.debug("Resuming writes");

    synchronized (writesSync)
    {
      suspendWrites--;
      if (suspendWrites < 0)
      {
        suspendWrites = 0;
        logger
            .error("Unexpected negative suspendedWrites in AbstractScheduler.resumeWrites()");
      }
      if (suspendWrites == 0)
      {
        // Wake up all waiting writes
        writesSync.notifyAll();
      }
    }
  }

  /**
   * Checks if the write queries are suspended and there is no remaining pending
   * writes. In that case, notify <code>endOcCurrentWrites</code>
   */
  private void checkPendingWrites()
  {
    synchronized (writesSync)
    {
      // If this is the last write to complete and writes are
      // suspended we have to notify suspendedWrites()
      if ((suspendWrites > 0) && (pendingWrites == 0))
      {
        synchronized (endOfCurrentWrites)
        {
          endOfCurrentWrites.notifyAll();
        }
      }
    }
  }

  /**
   * Checks if the transactions are suspended and that there is no remaining
   * pending transactions. In that case, notify
   * <code>endOfCurrentTransactions</code>
   *
   * @see #suspendNewTransactions()
   */
  private void checkPendingTransactions()
  {
    synchronized (transactionsSync)
    {
      // If it is the last pending transaction to complete and we
      // are waiting for pending transactions to complete, then wake
      // up suspendNewTransactionsForCheckpoint()
      if ((suspendTransactions > 0) && (pendingTransactions == 0))
      {
        synchronized (endOfCurrentTransactions)
        {
          endOfCurrentTransactions.notifyAll();
        }
      }
    }
  }

  /**
   * Resume suspended writes, transactions and persistent connections (in this
   * order).
   */
  public void resumeWritesTransactionsAndPersistentConnections()
  {
    clearSuspendedRequests();
    resumeWrites();
    resumeNewTransactions();
    resumeNewPersistentConnections();
  }

  /**
   * Suspend all <em>new write queries</em> until resumeWrites() is called.
   * This method does not block and returns immediately. To synchronize on
   * suspended writes completion, you must call
   * waitForSuspendedWritesToComplete().
   *
   * @see #resumeWrites()
   * @see #waitForSuspendedWritesToComplete()
   */
  public void suspendNewWrites()
  {
    if (logger.isDebugEnabled())
      logger.debug("Suspending new writes");

    synchronized (writesSync)
    {
      suspendWrites++;
    }
  }

  /**
   * @return Returns the suspendedWrites.
   */
  public boolean isSuspendedWrites()
  {
    return suspendWrites > 0;
  }

  /**
   * Wait for suspended writes to complete. Returns as soon as number of pending
   * writes has reached 0.
   *
   * @throws SQLException if an error occured during wait
   */
  public void waitForSuspendedWritesToComplete() throws SQLException
  {
    synchronized (writesSync)
    {
      if (pendingWrites == 0)
      {
        if (logger.isDebugEnabled())
          logger.debug("All writes suspended");
        return;
      }
    }

    long waitTime = INITIAL_WAIT_TIME;
    while (true)
    {
      synchronized (endOfCurrentWrites)
      {
        // Here we have a potential synchronization problem since the last
        // write completion could have happened before we entered this
        // synchronized block. Therefore we recheck if there is effectively
        // still pending writes. If this is not the case, we don't have
        // to sleep and we can immediately return.
        if (pendingWrites == 0)
        {
          if (logger.isDebugEnabled())
            logger.debug("All writes suspended");
          return;
        }

        if (logger.isDebugEnabled())
          logger.debug("Wait for " + pendingWrites + " writes to complete.");

        // Wait for pending writes to end
        try
        {
          endOfCurrentWrites.wait(waitTime);
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get("scheduler.suspend.writes.failed", e);
          logger.error(msg);
          throw new SQLException(msg);
        }
      }
      synchronized (writesSync)
      {
        if (pendingWrites == 0)
          break;
        else
        {
          logger.warn("Waiting for " + pendingWrites + " pending writes");
          waitTime *= 2;
        }
      }
    }

    if (logger.isDebugEnabled())
      logger.debug("All writes suspended");
  }

  /**
   * Resumes openinh and closing of persistent connections.
   */
  public void resumeOpenClosePersistentConnection()
  {
    synchronized (suspendOpenClosePersistentConnectionSync)
    {
      suspendOpenClosePersistentConnections--;
      if (suspendOpenClosePersistentConnections == 0)
        suspendOpenClosePersistentConnectionSync.notifyAll();
    }
  }

  /**
   * Resume new persistent connections creations that were suspended by
   * suspendNewPersistentConnections().
   *
   * @see #suspendNewPersistentConnections()
   */
  public final void resumeNewPersistentConnections()
  {
    if (logger.isDebugEnabled())
      logger.debug("Resuming new persistent connections");

    synchronized (persistentConnectionsSync)
    {
      suspendNewPersistentConnections--;
      if (suspendNewPersistentConnections < 0)
      {
        suspendNewPersistentConnections = 0;
        logger
            .error("Unexpected negative suspendedPersistentConnections in AbstractScheduler.resumeNewPersistentConnections()");
      }
      if (suspendNewPersistentConnections == 0)
      {
        // Wake up all pending persistent connections creation
        persistentConnectionsSync.notifyAll();
      }
    }
  }

  /**
   * Suspends open and closing of persistent connections.
   *
   * @see org.continuent.sequoia.controller.requestmanager.RequestManager#closePersistentConnection(String,
   *      long)
   */
  public void suspendOpenClosePersistentConnection()
  {
    synchronized (suspendOpenClosePersistentConnectionSync)
    {
      suspendOpenClosePersistentConnections++;
    }
  }

  /**
   * Suspend all new persistent connections creation. This method does not block
   * and returns immediately. New connections remain suspended until
   * resumeNewPersistentConnections() is called.
   *
   * @see #resumeNewPersistentConnections()
   * @see #waitForSuspendedPersistentConnectionsToComplete()
   */
  public void suspendNewPersistentConnections()
  {
    if (logger.isDebugEnabled())
      logger.debug("Suspending new persistent connections");

    synchronized (persistentConnectionsSync)
    {
      suspendNewPersistentConnections++;
    }
  }

  /**
   * Wait for opened persistent connections to complete. Returns as soon as
   * number of pending persistent connections has reached 0.
   *
   * @throws SQLException if an error occured during wait
   */
  public void waitForPersistentConnectionsToComplete() throws SQLException
  {
    synchronized (persistentConnectionsSync)
    {
      if (activePersistentConnections.isEmpty())
      {
        if (logger.isDebugEnabled())
          logger.debug("All persistent connections closed");
        return;
      }
    }

    // Wait for persistent connections to end
    boolean checkForTimeout = waitForPersistentConnectionsTimeout > 0;
    long totalWaitTime = 0;
    synchronized (endOfCurrentPersistentConnections)
    {
      if (activePersistentConnections.isEmpty())
      {
        if (logger.isDebugEnabled())
          logger.debug("All persistent connections closed");
        return;
      }

      if (logger.isDebugEnabled())
        logger.debug("Waiting for " + activePersistentConnections.size()
            + " persistent connections to be closed.");

      long waitTime = INITIAL_WAIT_TIME;
      long start;
      long realWait;
      while (!activePersistentConnections.isEmpty())
        try
        {
          start = System.currentTimeMillis();
          endOfCurrentPersistentConnections.wait(waitTime);
          realWait = System.currentTimeMillis() - start;
          totalWaitTime += realWait;
          if (logger.isWarnEnabled()
              && (activePersistentConnections.size() > 0))
          {
            logger.warn("Waited for " + Math.round((totalWaitTime / 1000.0))
                + " secs but " + activePersistentConnections.size()
                + " persistent connections still open: "
                + activePersistentConnections.keySet());
            if (checkForTimeout)
              logger
                  .warn("Will wait for "
                      + Math
                          .max(
                              0,
                              Math
                                  .round((waitForPersistentConnectionsTimeout - totalWaitTime) / 1000.0))
                      + " secs more and attempt to close them");
          }
          if (checkForTimeout
              && totalWaitTime >= waitForPersistentConnectionsTimeout)
            break;
          waitTime *= 2;
          if (checkForTimeout)
            waitTime = Math.min(waitTime, waitForPersistentConnectionsTimeout
                - totalWaitTime);
        }
        catch (InterruptedException e)
        {
          String msg = Translate.get("scheduler.suspend.transaction.failed", e);
          logger.warn(msg);
          throw new SQLException(msg);
        }
    }
    if (checkForTimeout && totalWaitTime >= waitForPersistentConnectionsTimeout)
    {
      if (logger.isWarnEnabled())
        logger.warn("Timeout reached ("
            + Math.round(waitForPersistentConnectionsTimeout / 1000.0)
            + " secs), closing remaining active persistent connections");
      closeRemainingPersistentConnections();
    }
    if (logger.isDebugEnabled())
      logger.debug("All persistent connections closed");
  }

  /**
   * Blocks until all pending open/close persistent connections operations are
   * completed.
   */
  public void waitForPendingOpenClosePersistentConnection()
  {
    synchronized (suspendOpenClosePersistentConnectionSync)
    {
      while (pendingOpenClosePersistentConnections > 0)
      {
        try
        {
          suspendOpenClosePersistentConnectionSync.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }
    }
  }

  /**
   * Adds an object to the suspended requests list.
   *
   * @param obj suspended request.
   */
  private void addSuspendedRequest(Object obj)
  {
    synchronized (suspendedRequests)
    {
      suspendedRequests.add(obj);
    }
    if (vdb.isDistributed())
    { // Distributed virtual database only
      List totalOrderQueue = vdb.getTotalOrderQueue();
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.notifyAll();
      }
    }
  }

  /**
   * Checks if an object is in the suspended requests list.
   *
   * @param obj request to be checked
   * @return true if the request is suspended, false otherwise
   */
  public boolean isSuspendedRequest(Object obj)
  {
    synchronized (suspendedRequests)
    {
      return suspendedRequests.contains(obj);
    }
  }

  /**
   * Removes all objects from the suspended requests list.
   */
  private void clearSuspendedRequests()
  {
    synchronized (suspendedRequests)
    {
      suspendedRequests.clear();
    }
    if (vdb.isDistributed())
    { // Distributed virtual database only
      List totalOrderQueue = vdb.getTotalOrderQueue();
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.notifyAll();
      }
    }
  }

  //
  // 7. Debug/Monitoring
  //

  protected abstract String getXmlImpl();

  /**
   * Get information about the Request Scheduler in xml format
   *
   * @return <code>String</code> containing information in xml
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RequestScheduler + ">");
    info.append(this.getXmlImpl());
    info.append("</" + DatabasesXmlTags.ELT_RequestScheduler + ">");
    return info.toString();
  }

  /**
   * Returns live information on the scheduler
   *
   * @return array of data
   */
  public String[] getSchedulerData()
  {
    String[] data = new String[7];
    data[0] = String.valueOf(numberRead);
    data[1] = String.valueOf(numberWrite);
    data[2] = String.valueOf(pendingTransactions);
    data[3] = String.valueOf(pendingWrites);
    data[4] = String.valueOf(numberRead + numberWrite);
    data[5] = String.valueOf(suspendTransactions);
    data[6] = String.valueOf(suspendWrites);
    return data;
  }

  /**
   * @return Returns the numberRead.
   */
  public int getNumberRead()
  {
    return numberRead;
  }

  /**
   * @return Returns the numberWrite.
   */
  public int getNumberWrite()
  {
    return numberWrite;
  }

  /**
   * @return Returns the pendingTransactions.
   */
  public int getPendingTransactions()
  {
    return pendingTransactions;
  }

  private void abortRemainingActiveTransactions() throws SQLException
  {
    List transactionsToAbort = new ArrayList();
    List transactionsAbortFailureList = new ArrayList();

    synchronized (writesSync)
    {
      transactionsToAbort.addAll(activeTransactions);
    }
    for (Iterator iter = transactionsToAbort.iterator(); iter.hasNext();)
    {
      long transactionId = ((TransactionMetaData) iter.next())
          .getTransactionId();
      if (logger.isWarnEnabled())
        logger.warn("Aborting transaction " + transactionId);

      // Now trying to abort all transactions. If one abort fails, we will try
      // to abort other transactions anyway, to clean up everything.
      try
      {
        vdb.abort(transactionId, true, true);
      }
      catch (SQLException e)
      {
        logger.warn("Failed to abort transaction " + transactionId + " : " + e);
        transactionsAbortFailureList.add(new Long(transactionId));
      }
    }
    if (!transactionsAbortFailureList.isEmpty())
    {
      StringBuffer transactions = new StringBuffer("[");
      for (Iterator iter = transactionsAbortFailureList.iterator(); iter
          .hasNext();)
        transactions.append((transactions.length() > 1 ? ", " : "")
            + ((TransactionMetaData) iter.next()).getTransactionId());
      transactions.append("]");
      throw new SQLException("Some transactions failed to abort : "
          + transactions);
    }
  }

  private void closeRemainingPersistentConnections()
  {
    Map persistentConnectionsToClose = new HashMap();
    synchronized (endOfCurrentPersistentConnections)
    {
      persistentConnectionsToClose.putAll(activePersistentConnections);
    }
    for (Iterator iter = persistentConnectionsToClose.keySet().iterator(); iter
        .hasNext();)
    {
      Long persistentConnectionId = (Long) iter.next();
      if (logger.isWarnEnabled())
        logger.warn("Closing persistent connection " + persistentConnectionId);
      vdb.closePersistentConnection((String) persistentConnectionsToClose
          .get(persistentConnectionId), persistentConnectionId.longValue());
    }
  }

}