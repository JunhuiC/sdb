/**
 * Sequoia: Database clustering technology.
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
 * Contributor(s): Stephane Giron.
 */

package org.continuent.sequoia.controller.loadbalancer;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.locks.DeadlockDetectionThread;
import org.continuent.sequoia.common.locks.TransactionLogicalLock;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.BeginTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.KillThreadTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackTask;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.CreateRequest;
import org.continuent.sequoia.controller.requests.InsertRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * This class defines task queues that stores the requests to be executed on a
 * database backend.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @author <a href="mailto:Stephane.Giron@continuent.com">Stephane Giron </a>
 * @version 1.0
 */
public class BackendTaskQueues
{
  /** Queue in which queries arrive in total order */
  private LinkedList<AbstractTask>              totalOrderQueue;
  /**
   * Queue for stored procedures without semantic information (locking the whole
   * database)
   */
  private LinkedList<BackendTaskQueueEntry>              storedProcedureQueue;
  /**
   * Queue for conflicting requests (only first request of the queue can be
   * executed)
   */
  private LinkedList<BackendTaskQueueEntry>              conflictingRequestsQueue;
  /**
   * Queue for non-conflicting requests that can be executed in parallel, in any
   * order.
   */
  private LinkedList<BackendTaskQueueEntry>              nonConflictingRequestsQueue;
  /** Backend these queues are attached to */
  private DatabaseBackend         backend;
  private WaitForCompletionPolicy waitForCompletionPolicy;
  private RequestManager          requestManager;
  private boolean                 allowTasksToBePosted;
  private final Object            ALLOW_TASKS_SYNC        = new Object();

  private DeadlockDetectionThread deadlockDetectionThread;

  // Number of stored procedures that have been posted in the queue and that
  // have not completed yet
  private int                     storedProcedureInQueue  = 0;

  private int                     writesWithMultipleLocks = 0;
  private Trace                   logger;

  /**
   * Creates a new <code>BackendTaskQueues</code> object
   * 
   * @param backend DatabaseBackend associated to these queues
   * @param waitForCompletionPolicy the load balancer wait for completion policy
   * @param requestManager the request manager associated with these queues
   */
  public BackendTaskQueues(DatabaseBackend backend,
      WaitForCompletionPolicy waitForCompletionPolicy,
      RequestManager requestManager)
  {
    this.backend = backend;
    this.logger = backend.getLogger();
    this.waitForCompletionPolicy = waitForCompletionPolicy;
    this.requestManager = requestManager;
    totalOrderQueue = new LinkedList<AbstractTask>();
    storedProcedureQueue = new LinkedList<BackendTaskQueueEntry>();
    conflictingRequestsQueue = new LinkedList<BackendTaskQueueEntry>();
    nonConflictingRequestsQueue = new LinkedList<BackendTaskQueueEntry>();
    allowTasksToBePosted = false;
  }

  /**
   * Abort all queries belonging to the provided transaction.
   * 
   * @param tid the transaction identifier
   * @return true if a rollback is already in progress
   */
  public boolean abortAllQueriesForTransaction(long tid)
  {
    synchronized (this)
    {
      boolean rollbackInProgress = abortAllQueriesEvenRunningInTransaction(tid,
          storedProcedureQueue);
      if (abortAllQueriesEvenRunningInTransaction(tid, conflictingRequestsQueue))
        rollbackInProgress = true;
      if (abortAllQueriesEvenRunningInTransaction(tid,
          nonConflictingRequestsQueue))
        rollbackInProgress = true;
      return rollbackInProgress;
    }
  }

  /**
   * Abort all queries belonging to the given transaction even if they are
   * currently processed by a BackendWorkerThread.
   * 
   * @param tid transaction identifier
   * @param queue queue to scan for queries to abort
   * @return true if a rollback is already in progress
   */
  private boolean abortAllQueriesEvenRunningInTransaction(long tid,
      LinkedList<BackendTaskQueueEntry> queue)
  {
    boolean rollbackInProgress = false;
    synchronized (queue)
    {
      for (Iterator<BackendTaskQueueEntry> iter = queue.iterator(); iter.hasNext();)
      {
        BackendTaskQueueEntry entry = (BackendTaskQueueEntry) iter.next();
        boolean isProcessing = false;
        AbstractTask task = entry.getTask();
        if (task.getTransactionId() == tid)
        {
          if (task instanceof RollbackTask)
            rollbackInProgress = true;
          else
          {
            if (logger.isDebugEnabled())
              logger.debug("Aborting request " + task.getRequest()
                  + " on backend " + backend.getName());

            BackendWorkerThread processingThread = entry.getProcessingThread();
            if (processingThread != null)
            { // A thread is working on it, cancel the task
              isProcessing = true;
              Statement s = processingThread.getCurrentStatement();
              if (s != null)
              {
                try
                {
                  s.cancel();
                }
                catch (SQLException e)
                {
                  logger.warn("Unable to cancel execution of request", e);
                }
                catch (NullPointerException e)
                {
                  if (logger.isWarnEnabled())
                    logger
                        .warn(
                            "Ignoring NullPointerException caused by Connector/J 5.0.4 bug #24721",
                            e);
                }
              }
            }
            if (!task.hasCompleted())
            {
              try
              {
                if (processingThread == null)
                { // abort has been called on a non-processed query, use a
                  // random worker thread for notification
                  processingThread = backend
                      .getBackendWorkerThreadForNotification();
                  if (processingThread == null)
                  { // No worker thread left, should never happen.
                    // Backend already disabled?
                    logger
                        .warn("No worker thread found for request abort notification, creating fake worker thread");
                    processingThread = new BackendWorkerThread(backend,
                        requestManager.getLoadBalancer());
                  }
                }
                task.notifyFailure(processingThread, -1L, new SQLException(
                    "Transaction aborted due to deadlock"));
              }
              catch (SQLException ignore)
              {
              }
            }
            if (!isProcessing)
            {
              /*
               * If the task was being processed by a thread, the completion
               * will be notified by the thread itself
               */
              completedEntryExecution(entry, iter);
            }
          }
        }
      }
    }
    return rollbackInProgress;
  }

  /**
   * Abort all requests remaining in the queues. This is usually called when the
   * backend is disabled and no backend worker thread should be processing any
   * request any more (this will generate a warning otherwise).
   */
  public void abortRemainingRequests()
  {
    setAllowTasksToBePosted(false);
    abortRemainingRequests(storedProcedureQueue);
    abortRemainingRequests(conflictingRequestsQueue);
    abortRemainingRequests(nonConflictingRequestsQueue);
  }

  /**
   * Abort the remaining request in the given queue
   * 
   * @param queue the queue to purge
   */
  private void abortRemainingRequests(LinkedList<BackendTaskQueueEntry> queue)
  {
    synchronized (this)
    {
      synchronized (queue)
      {
        for (Iterator<BackendTaskQueueEntry> iter = queue.iterator(); iter.hasNext();)
        {
          BackendTaskQueueEntry entry = (BackendTaskQueueEntry) iter.next();
          AbstractTask task = entry.getTask();

          // Do not cancel KillThreadTasks
          if (task instanceof KillThreadTask)
            continue;

          if (entry.getProcessingThread() != null)
          { // A thread is working on it, warn and cancel the task
            logger.warn("A worker thread was still processing task " + task
                + ", aborting the request execution.");
            Statement s = entry.getProcessingThread().getCurrentStatement();
            if (s != null)
            {
              try
              {
                s.cancel();
              }
              catch (SQLException e)
              {
                logger.warn("Unable to cancel execution of request", e);
              }
            }
          }
          if (!task.hasCompleted())
          {
            if (logger.isDebugEnabled())
              logger.debug("Cancelling task " + task);
            task.notifyCompletion(entry.getProcessingThread());
          }
          completedEntryExecution(entry, iter);
        }
      }
    }
  }

  /**
   * Add a task at the end of the backend total order queue
   * 
   * @param task the task to add
   */
  public final void addTaskToBackendTotalOrderQueue(AbstractTask task)
  {
    synchronized (this)
    {
      synchronized (totalOrderQueue)
      {
        if (logger.isDebugEnabled())
          logger.debug("Adding " + task + " to total order queue");

        totalOrderQueue.addLast(task);
      }

      /*
       * Wake up all worker threads in case we post multiple tasks before a
       * thread had time to take this task into account (this would result in a
       * lost notified event).
       */
      this.notifyAll();
    }
  }

  /**
   * Add a task at the end of the backend total order queue. Block as long as
   * the total order queue size if over the indicated queue size.
   * 
   * @param task the task to add
   * @param queueSize the maximum queue size
   */
  public final void addTaskToBackendTotalOrderQueue(AbstractTask task,
      int queueSize)
  {
    synchronized (this)
    {
      boolean mustNotify = false;
      do
      {
        synchronized (totalOrderQueue)
        {
          if (totalOrderQueue.size() < queueSize)
          {
            if (logger.isDebugEnabled())
              logger.debug("Adding " + task + " to total order queue");

            totalOrderQueue.addLast(task);
            mustNotify = true;
          }
        }

        if (mustNotify)
        {
          /*
           * Wake up all worker threads in case we post multiple tasks before a
           * thread had time to take this task into account (this would result
           * in a lost notified event).
           */
          this.notifyAll();
          return; // exit method here
        }
        else
        {
          try
          { // Wait for queue to free an entry
            this.wait();
          }
          catch (InterruptedException e)
          {
          }
        }
      }
      while (!mustNotify);
    }
  }

  /**
   * Add a task in the ConflictingRequestsQueue.
   * 
   * @param task task to add
   */
  private void addTaskInConflictingRequestsQueue(AbstractTask task)
  {
    addTaskToQueue(conflictingRequestsQueue, task, false);
  }

  /**
   * Add a task in the NonConflictingRequestsQueue.
   * 
   * @param task task to add
   * @param isACommitOrRollback true if the task is a commit or a rollback
   */
  private void addTaskInNonConflictingRequestsQueue(AbstractTask task,
      boolean isACommitOrRollback)
  {
    addTaskToQueue(nonConflictingRequestsQueue, task, isACommitOrRollback);
  }

  /**
   * Add a task in the StoredProcedureQueue.
   * 
   * @param task task to add
   */
  private void addTaskInStoredProcedureQueue(AbstractTask task)
  {
    addTaskToQueue(storedProcedureQueue, task, false);
  }

  /**
   * Add the task in the given queue and notify the queue. Note that the task is
   * also added to the backend pending write request queue. This method is
   * supposed to be called in a synchronized block on this. If this is not the
   * case, this method will fail and you probably need to check the interaction
   * with fetchNextQueryFromBackendTotalOrderQueue,
   * addTaskToBackendTotalOrderQueue and completedEntryExecution.
   * 
   * @param queue queue in which the task must be added
   * @param task the task to add
   * @param isACommitOrRollback true if the task is a commit or a rollback
   */
  private void addTaskToQueue(LinkedList<BackendTaskQueueEntry> queue, AbstractTask task,
      boolean isACommitOrRollback)
  {
    if (!allowTasksToBePosted())
    {
      if (logger.isDebugEnabled())
        logger.debug("Cancelling task " + task);
      task.notifyCompletion(null);
      return;
    }

    // We assume that all requests here are writes
    backend.addPendingTask(task);
    if (logger.isDebugEnabled())
      logger.debug("Adding task " + task + " to pending request queue");

    // Add to the queue
    synchronized (queue)
    {
      queue
          .addLast(new BackendTaskQueueEntry(task, queue, isACommitOrRollback));
    }

    /*
     * Wake up all worker threads in case we post multiple tasks before a thread
     * had time to take this task into account (this would result in a lost
     * notified event).
     */
    this.notifyAll();
  }

  /**
   * Check for priority inversion in the conflicting queue or possibly stored
   * procedure queue flushing if a transaction executing a stored procedure has
   * just completed. remove this entry and possibly re-arrange the queues
   */
  public final void checkForPriorityInversion()
  {
    DatabaseSchema schema = backend.getDatabaseSchema();

    // Let's check the conflicting queue for priority inversion
    synchronized (conflictingRequestsQueue)
    {
      for (Iterator<BackendTaskQueueEntry> iter = conflictingRequestsQueue.iterator(); iter.hasNext();)
      {
        BackendTaskQueueEntry entry = (BackendTaskQueueEntry) iter.next();

        // If the entry is currently processed, don't try to move it else it
        // would be duplicated in the non-conflicting queue!!!
        if (entry.processingThread != null)
          continue;

        AbstractTask task = entry.getTask();
        AbstractRequest request = task.getRequest();
        SortedSet<?> lockedTables = request.getWriteLockedDatabaseTables();
        if (lockedTables != null)
        {
          boolean queryIsConflicting = false;
          for (Iterator<?> iterator = lockedTables.iterator(); iterator.hasNext()
              && !queryIsConflicting;)
          {
            String tableName = (String) iterator.next();
            DatabaseTable table = schema.getTable(tableName, false);
            if (table == null)
            { // No table found, stay in the conflicting queue
              logger
                  .warn("Unable to find table "
                      + tableName
                      + " in database schema, when checking priority inversion for query "
                      + request.toStringShortForm(requestManager
                          .getVirtualDatabase().getSqlShortFormLength()));
            }
            else
            {
              /*
               * If the table we are conflicting with now belongs to us then we
               * can go in the non-conflicting queue. Note that it is not
               * possible for the lock to be free since we have acquired it
               * earlier and we are waiting for our turn.
               */
              TransactionLogicalLock lock = table.getLock();
              if (!lock.isLocked())
                logger.warn("Unexpected free lock on table " + table);
              else
              { // Check that the lock belong to our transaction
                queryIsConflicting = lock.getLocker() != task
                    .getTransactionId();
              }
            }
          }
          if (!queryIsConflicting)
          { // Locks are now free, move to the non-conflicting queue
            // Do not try to take the lock again else it will not be released
            if (logger.isDebugEnabled())
              logger.debug("Priority inversion for request "
                  + task.getRequest());
            moveToNonConflictingQueue(iter, entry);
          }
        }
        else
        { // Query does not lock anything, it should not have been posted in the
          // conflicting queue
          logger.warn("Non-locking task " + task
              + " was posted in conflicting queue");
          if (logger.isDebugEnabled())
            logger.debug("Priority inversion for request " + task.getRequest());
          moveToNonConflictingQueue(iter, entry);
        }
      }
    }

    // Look at the stored procedure queue
    synchronized (storedProcedureQueue)
    {
      for (Iterator<BackendTaskQueueEntry> iter = storedProcedureQueue.iterator(); iter.hasNext();)
      {
        BackendTaskQueueEntry entry = (BackendTaskQueueEntry) iter.next();

        TransactionLogicalLock globalLock = schema.getLock();
        AbstractTask task = entry.getTask();
        AbstractRequest request = task.getRequest();
        if (globalLock.isLocked())
        { // Stored procedure is executing
          if (task.getTransactionId() == globalLock.getLocker())
          {
            // Just wait for current transactions to complete if all locks are
            // not free.
            if (!schema.allTablesAreUnlockedOrLockedByTransaction(request))
              return;

            /*
             * We belong to the transaction that executes the stored procedure
             * (or to the auto commit request that holds the lock), let's go in
             * the non-conflicting request queue.
             */
            moveToNonConflictingQueue(iter, entry);
            // if we are in auto commit, it means that we are the stored
            // procedure which has acquired the lock during the atomic post
            if (task.isAutoCommit())
              return;
            continue;
          }
          else
          { // Check if the stored procedure currently executing is not
            // somewhere in the stored procedure queue.

            boolean currentStoredProcedureInQueue = false;
            for (Iterator<BackendTaskQueueEntry> iter2 = storedProcedureQueue.iterator(); iter2
                .hasNext();)
            {
              BackendTaskQueueEntry entry2 = (BackendTaskQueueEntry) iter2
                  .next();
              AbstractTask task2 = entry2.getTask();
              if ((task2 != null)
                  && (task2.getTransactionId() == globalLock.getLocker()))
                currentStoredProcedureInQueue = true;
            }

            // If the stored procedure is not in the queue then it is currently
            // executing and we have to wait for its completion
            if (!currentStoredProcedureInQueue)
              return;
          }
        }

        // Schema is not locked, no stored procedure currently executes
        TransactionMetaData tm = getTransactionMetaData(request);

        if ((request instanceof SelectRequest)
            || (request instanceof AbstractWriteRequest))
        {
          SortedSet<?> writeLockedTables = request.getWriteLockedDatabaseTables();

          if (writeLockedTables == null || writeLockedTables.isEmpty())
          { // This request does not lock anything
            moveToNonConflictingQueue(iter, entry);
            continue;
          }

          moveMultipleWriteLocksQuery(schema, iter, entry, task, request, tm);
        }
        else
        {
          if (request instanceof StoredProcedure)
          {
            StoredProcedure sp = (StoredProcedure) request;
            DatabaseProcedureSemantic semantic = sp.getSemantic();
            if (semantic != null)
            {
              // Try to optimize the stored procedure execution based on its
              // semantic information
              if (semantic.isCommutative() || semantic.isReadOnly()
                  || (request.getWriteLockedDatabaseTables() == null))
                moveToNonConflictingQueue(iter, entry);
              else
                moveMultipleWriteLocksQuery(schema, iter, entry, task, request,
                    tm);
              continue;
            }
          }

          // Stored procedure or unknown query, take the global lock and proceed
          // if all other locks are free.

          globalLock.acquire(request);
          if (tm != null)
          {
            List<?> acquiredLocks = tm.getAcquiredLocks(backend);
            if ((acquiredLocks == null) || !acquiredLocks.contains(globalLock))
              tm.addAcquiredLock(backend, globalLock);
          }
          else
          {
            task.addLock(backend, globalLock);
          }

          // Just wait for current transactions to complete if all locks are not
          // free.
          if (!schema.allTablesAreUnlockedOrLockedByTransaction(request))
            return;

          // Clear to go, all locks are free. Acquire the global lock and move
          // to the non-conflicting queue.
          moveToNonConflictingQueue(iter, entry);
          continue;
        }
      }
    }
  }

  private void moveMultipleWriteLocksQuery(DatabaseSchema schema,
      Iterator<BackendTaskQueueEntry> iter, BackendTaskQueueEntry entry, AbstractTask task,
      AbstractRequest request, TransactionMetaData tm)
  {
    /*
     * Assume that we will get all locks and that we will execute in the
     * non-conflicting queue. If there is any issue, the queue will be set to
     * conflicting queue.
     */
    boolean allLocksAcquired = true;
    for (Iterator<?> lockIter = request.getWriteLockedDatabaseTables().iterator(); lockIter
        .hasNext();)
    {
      String tableName = (String) lockIter.next();
      DatabaseTable table = schema.getTable(tableName, false);
      if (table == null)
      { // No table found, let's go for the conflicting queue
        logger.warn("Unable to find table "
            + tableName
            + " in database schema, scheduling query "
            + request.toStringShortForm(requestManager.getVirtualDatabase()
                .getSqlShortFormLength()) + " in conflicting queue.");
        allLocksAcquired = false;
      }
      else
      { /*
         * If we get the lock we go in the non conflicting queue else we go in
         * the conflicting queue
         */
        TransactionLogicalLock tableLock = table.getLock();
        if (!tableLock.acquire(request))
          allLocksAcquired = false;
        /*
         * Make sure that the lock is added only once to the list especially if
         * multiple backends execute this piece of code when checking for
         * priority inversion in their own queue (if the lock was already
         * acquired, tableLock.acquire() returns directly true)
         */
        if (tm != null)
        {
          List<?> acquiredLocks = tm.getAcquiredLocks(backend);
          if ((acquiredLocks == null) || !acquiredLocks.contains(tableLock))
            tm.addAcquiredLock(backend, tableLock);
        }
        else
        {
          task.addLock(backend, tableLock);
        }
      }
    }
    // if we acquired all locks, we can go to the non conflicting queue
    if (allLocksAcquired)
      moveToNonConflictingQueue(iter, entry);
    else
      moveToConflictingQueue(iter, entry);
  }

  private void moveToConflictingQueue(Iterator<BackendTaskQueueEntry> iter, BackendTaskQueueEntry entry)
  {
    iter.remove();
    if (logger.isDebugEnabled())
      logger.debug("Moving " + entry.getTask() + " to conflicting queue");
    synchronized (conflictingRequestsQueue)
    {
      entry.setQueue(conflictingRequestsQueue);
      conflictingRequestsQueue.addLast(entry);
    }
  }

  private void moveToNonConflictingQueue(Iterator<BackendTaskQueueEntry> iter,
      BackendTaskQueueEntry entry)
  {
    iter.remove();
    if (logger.isDebugEnabled())
      logger.debug("Moving " + entry.getTask() + " to non conflicting queue");
    synchronized (nonConflictingRequestsQueue)
    {
      entry.setQueue(nonConflictingRequestsQueue);
      nonConflictingRequestsQueue.addLast(entry);
    }
  }

  private static final int UNASSIGNED_QUEUE       = -1;
  private static final int CONFLICTING_QUEUE      = 0;
  private static final int NON_CONFLICTING_QUEUE  = 1;
  private static final int STORED_PROCEDURE_QUEUE = 2;
  private final Object     atomicPostSyncObject   = new Object();

  /**
   * Lock list variable is set by getQueueAndWriteLockTables and retrieved by
   * atomicTaskPostInQueueAndReleaseLock. This is safe since this happens in the
   * synchronized (ATOMIC_POST_SYNC_OBJECT) block.
   */
  private ArrayList<TransactionLogicalLock>        lockList               = null;

  /**
   * Fetch the next task from the backend total order queue and post it to one
   * of the queues (conflicting or not).
   * <p>
   * Note that this method must be called within a synchronized block on this.
   * 
   * @return true if an entry was processed, false if there is the total order
   *         queue is empty.
   */
  private boolean fetchNextQueryFromBackendTotalOrderQueue()
  {
    DatabaseSchema schema = backend.getDatabaseSchema();
    TransactionMetaData tm = null;
    int queueToUse = UNASSIGNED_QUEUE;

    AbstractTask task;
    AbstractRequest request;

    // Fetch first task from queue
    synchronized (totalOrderQueue)
    {
      if (totalOrderQueue.isEmpty())
        return false;
      task = (AbstractTask) totalOrderQueue.removeFirst();

      if (waitForCompletionPolicy.getPolicy() != WaitForCompletionPolicy.ALL)
      { /*
         * If asynchronous execution is allowed, we have to ensure that queries
         * of the same transaction are executed in order that is only one at a
         * time. We also have to ensure that late queries execute before new
         * queries accessing the same resources.
         */

        /*
         * SYNCHRONIZATION: this check has to be performed in a synchronized
         * block to avoid race conditions with terminating taks that perform
         * priority inversions. Such operations move tasks across the backend
         * queues and may invalidate the check.
         */
        synchronized (atomicPostSyncObject)
        {
          while (mustWaitForLateTask(task))
          {
            if (logger.isDebugEnabled())
            {
              logger.debug("Task " + task.toString()
                  + " must wait for late task\n" + dump());
            }

            totalOrderQueue.addFirst(task); // Put back request in queue
            // Behave as an empty queue, we will be notified when the blocking
            // query has completed
            return false;
          }
        }
      }

      // Now process the task
      request = task.getRequest();
      if (request == null || task instanceof BeginTask)
      {
        addTaskInNonConflictingRequestsQueue(task, !task.isAutoCommit());
        return true;
      }
      else
      { // Parse the request if needed, should only happen at recover time
        try
        {
          if (!request.isParsed())
            request.parse(backend.getDatabaseSchema(),
                ParsingGranularities.TABLE, false);
        }
        catch (SQLException e)
        {
          logger.warn("Parsing of request " + request
              + " failed in recovery process", e);
        }
      }
      if (backend.isReplaying())
      {
        /*
         * Read-only stored procedures can get logged when the schema is
         * unavailable, because no backends are enabled. They do not need to be
         * replayed and they may slow down recovery significantly.
         */
        if (request instanceof StoredProcedure)
        {
          StoredProcedure sp = (StoredProcedure) request;
          DatabaseProcedureSemantic semantic = sp.getSemantic();
          if (semantic != null && semantic.isReadOnly())
          {
            task.notifySuccess(null);
            synchronized (this)
            {// The RecoverThread may be waiting to add a request
              notifyAll();
            }
            return true;
          }
        }
      }
      if (!request.isAutoCommit())
      { // Retrieve the transaction marker metadata
        try
        {
          tm = requestManager.getTransactionMetaData(new Long(request
              .getTransactionId()));
        }
        catch (SQLException e)
        {
          // We didn't start or lazy start the transaction
          if (logger.isDebugEnabled())
            logger.debug("No transaction medatada found for transaction "
                + request.getTransactionId());
        }
      }

      if (schema == null)
      {
        try
        {
          task.notifyFailure((BackendWorkerThread) Thread.currentThread(), 0,
              new SQLException(
                  "No schema available to perform request locking on backend "
                      + backend.getName()));
        }
        catch (SQLException ignore)
        {
          // Wait interrupted in notifyFailure
        }
        return true;
      }

      synchronized (atomicPostSyncObject)
      {
        lockList = null;

        boolean requestIsAStoredProcedure = request instanceof StoredProcedure;
        if (requestIsAStoredProcedure)
          storedProcedureInQueue++;

        SortedSet<?> writeLockedTables = request.getWriteLockedDatabaseTables();
        if ((writeLockedTables != null) && (writeLockedTables.size() > 1))
          writesWithMultipleLocks++;

        // Check if a stored procedure is locking the database
        TransactionLogicalLock globalLock = schema.getLock();
        if (globalLock.isLocked())
        {
          if (request.isAutoCommit())
          {
            queueToUse = STORED_PROCEDURE_QUEUE;
          }
          else
          {
            /*
             * If we are the transaction executing the stored procedure, then we
             * can proceed in the conflicting queue.
             */
            if (globalLock.getLocker() == request.getTransactionId())
              queueToUse = NON_CONFLICTING_QUEUE;
            else
            {
              /*
               * If we are one of the transactions that already has acquired
               * locks then we should try to complete our transaction else we
               * stack in the stored procedure queue.
               */
              if ((tm == null) || (tm.getAcquiredLocks(backend) == null))
              {
                // No locks taken so far, or transaction not [lazy] started =>
                // go in the stored procedure queue
                queueToUse = STORED_PROCEDURE_QUEUE;
              }
            }
          }
        }

        if (queueToUse == UNASSIGNED_QUEUE)
        { // No stored procedure or transaction that started before the stored
          // procedure was posted
          if (request instanceof AbstractWriteRequest
              && !((AbstractWriteRequest) request).requiresGlobalLock())
          {
            try
            {
              queueToUse = getQueueAndWriteLockTables(request, schema, tm);
            }
            catch (SQLException e)
            {
              try
              {
                task.notifyFailure(
                    (BackendWorkerThread) Thread.currentThread(), 0, e);
              }
              catch (SQLException ignore)
              {
                // Wait interrupted in notifyFailure
              }
              return true;
            }
          }
          else if (request instanceof SelectRequest)
          {
            /*
             * Note that SelectRequest scheduling is a little bit tricky to
             * understand. Basically, we should just allow one select request at
             * a time. If they are in different transactions, this is fine they
             * will be properly isolated by the underlying database and queries
             * from the same transaction are guaranteed to be executed in order
             * (therefore they will go to the non-conflicting queue since their
             * write lock set is null). If SELECT is in autocommit, we ensure
             * that only one autocommit request is executed at a time, so
             * finally we are safe in all cases. SELECT...FOR UPDATE are treated
             * as writes since their write lock tables is set accordingly.
             */
            try
            {
              queueToUse = getQueueAndWriteLockTables(request, schema, tm);
            }
            catch (SQLException e)
            {
              try
              {
                task.notifyFailure(
                    (BackendWorkerThread) Thread.currentThread(), 0, e);
              }
              catch (SQLException ignore)
              {
                // Wait interrupted in notifyFailure
              }
              return true;
            }
          }
          else
          {
            if (requestIsAStoredProcedure)
            {
              StoredProcedure sp = (StoredProcedure) request;
              DatabaseProcedureSemantic semantic = sp.getSemantic();
              if (semantic != null)
              {
                // Try to optimize the stored procedure execution based on its
                // semantic information
                if (semantic.isReadOnly()
                    || (request.getWriteLockedDatabaseTables() == null))
                  queueToUse = NON_CONFLICTING_QUEUE;
                else
                {
                  try
                  {
                    queueToUse = getQueueAndWriteLockTables(request, schema, tm);
                  }
                  catch (SQLException e)
                  {
                    try
                    {
                      task.notifyFailure((BackendWorkerThread) Thread
                          .currentThread(), 0, e);
                    }
                    catch (SQLException ignore)
                    {
                      // Wait interrupted in notifyFailure
                    }
                    return true;
                  }
                  if (semantic.isCommutative())
                    queueToUse = NON_CONFLICTING_QUEUE;
                }
              }
            }

            if (queueToUse == UNASSIGNED_QUEUE)
            {
              /*
               * Stored procedure or unknown query, let's assume it blocks the
               * whole database. Check if we can lock everything else we wait
               * for all locks to be free.
               */
              if (!globalLock.isLocked())
              { // Lock the whole database so that we can execute when all
                // locks are released
                globalLock.acquire(request);
                if (tm != null)
                  tm.addAcquiredLock(backend, globalLock);
                else
                {
                  if (lockList == null)
                    lockList = new ArrayList<TransactionLogicalLock>();
                  lockList.add(globalLock);
                }
                if (schema.allTablesAreUnlockedOrLockedByTransaction(request))
                  // Clear to go, all locks are free
                  queueToUse = NON_CONFLICTING_QUEUE;
                else
                  // We will have to wait for everyone to release its locks
                  queueToUse = STORED_PROCEDURE_QUEUE;
              }
              else
              { /*
                 * A stored procedure is holding the lock but we are in a
                 * transaction that already acquired locks so we are authorized
                 * to complete.
                 */
                if (schema.allTablesAreUnlockedOrLockedByTransaction(request))
                {
                  queueToUse = NON_CONFLICTING_QUEUE;
                  List<TransactionLogicalLock> locks = schema.lockAllTables(request);
                  if (tm != null)
                    tm.addAcquiredLocks(backend, locks);
                  else
                  {
                    if (lockList == null)
                      lockList = new ArrayList<TransactionLogicalLock>();
                    lockList.addAll(locks);
                  }
                }
                else
                { /*
                   * We will have to wait for the completion of the transaction
                   * of the stored procedure currently holding the global lock.
                   */
                  queueToUse = STORED_PROCEDURE_QUEUE;
                }
              }
            }
          }
        }

        if (queueToUse == NON_CONFLICTING_QUEUE)
        {
          if (logger.isDebugEnabled())
            logger.debug("Scheduling request " + request
                + " in non conflicting queue");
          addTaskInNonConflictingRequestsQueue(task, false);
        }
        else if (queueToUse == CONFLICTING_QUEUE)
        {
          if (logger.isDebugEnabled())
            logger.debug("Scheduling request " + request
                + " in conflicting queue");
          addTaskInConflictingRequestsQueue(task);
        }
        else if (queueToUse == STORED_PROCEDURE_QUEUE)
        {
          if (logger.isDebugEnabled())
            logger.debug("Scheduling request " + request
                + " in stored procedure queue");
          addTaskInStoredProcedureQueue(task);
        }

        task.setLocks(backend, lockList);
      } // synchronized (atomicPostSyncObject)
    } // synchronized (totalOrderQueue)

    return true;
  }

  /**
   * Schedule a query that takes write on multiple tables and tell in which
   * queue the task should be posted. This updates the conflictingTable above if
   * the query must be posted in the CONFLICTING_QUEUE and this always update
   * the list of locks taken by this request (either directly in tm lock list if
   * tm is not null, or by updating lockList defined above)
   * 
   * @param request the request to schedule
   * @param schema the current database schema containing lock information
   * @param tm the transaction marker metadata (null if request is autocommit)
   * @return the queue to use (NON_CONFLICTING_QUEUE or CONFLICTING_QUEUE)
   * @throws SQLException if a table is not found in the schema and
   *           enforceTableExistenceIntoSchema is set to true for the VDB
   */
  private int getQueueAndWriteLockTables(AbstractRequest request,
      DatabaseSchema schema, TransactionMetaData tm) throws SQLException
  {
    SortedSet<?> writeLockedTables = request.getWriteLockedDatabaseTables();

    if (writeLockedTables == null || writeLockedTables.isEmpty())
    { // This request does not lock anything
      return NON_CONFLICTING_QUEUE;
    }
    else if (request.isCreate() && writeLockedTables.size() == 1)
    { // This request does not lock anything
      // create table : we do not need to execute
      // in conflicting queue, but we have to lock the table for recovery
      // operations (that are done in a parallel way)
      return NON_CONFLICTING_QUEUE;
    }

    /*
     * Assume that we will get all locks and that we will execute in the
     * non-conflicting queue. If there is any issue, the queue will be set to
     * conflicting queue.
     */
    int queueToUse = NON_CONFLICTING_QUEUE;
    for (Iterator<?> iter = writeLockedTables.iterator(); iter.hasNext();)
    {
      String tableName = (String) iter.next();
      DatabaseTable table = schema.getTable(tableName, false);
      if (table == null)
      { // table not found in the database schema.
        if (request.isCreate()
            && tableName.equals(((CreateRequest) request).getTableName()))
        {
          // We are trying to create a table, so it can obiously not be found in
          // the database schema. Let's go for the conflicting queue.
          logger.warn("Creating table "
              + tableName
              + ", scheduling query "
              + request.toStringShortForm(requestManager.getVirtualDatabase()
                  .getSqlShortFormLength()) + " in conflicting queue.");
          queueToUse = CONFLICTING_QUEUE;
          continue;
        }

        // Check if it is a session-dependant temporary table that could not be
        // found in the database schema
        if (!request.isAutoCommit())
        {
          PooledConnection pc = backend
              .getConnectionManager(request.getLogin())
              .retrieveConnectionForTransaction(request.getTransactionId());
          if (pc != null && pc.existsTemporaryTable(tableName))
            continue;
        }
        else if (request.isPersistentConnection())
        {
          try
          {
            PooledConnection pc = backend.getConnectionManager(
                request.getLogin()).retrieveConnectionInAutoCommit(request);
            if (pc != null && pc.existsTemporaryTable(tableName))
              continue;
          }
          catch (UnreachableBackendException e)
          {
          }
        }

        // At this point, we did not find the table either in the database
        // schema, nor inside the connection's context.
        if (!request.tableExistenceCheckIsDisabled()
            && requestManager.getVirtualDatabase()
                .enforceTableExistenceIntoSchema())
        {
          String errMsg = "Unable to find table "
              + tableName
              + " in database schema, rejecting query "
              + request.toStringShortForm(requestManager.getVirtualDatabase()
                  .getSqlShortFormLength()) + ".";
          logger.warn(errMsg);
          throw new SQLException(errMsg);
        }
        else
        {
          // No table found, let's go for the conflicting queue
          logger.warn("Unable to find table "
              + tableName
              + " in database schema, scheduling query "
              + request.toStringShortForm(requestManager.getVirtualDatabase()
                  .getSqlShortFormLength()) + " in conflicting queue.");
          queueToUse = CONFLICTING_QUEUE;
        }
      }
      else
      { /*
         * If we get the lock we go in the non conflicting queue else we go in
         * the conflicting queue
         */
        if (!table.getLock().acquire(request))
        {
          queueToUse = CONFLICTING_QUEUE;
          if (logger.isDebugEnabled())
            logger.debug("Request " + request + " waits for lock on table "
                + table + "\nHold by : " + table.getLock().getLocker()
                + "\nWaiting list : " + table.getLock().getWaitingList());
        }
        if (tm != null)
          tm.addAcquiredLock(backend, table.getLock());
        else
        {
          if (lockList == null)
            lockList = new ArrayList<TransactionLogicalLock>();
          lockList.add(table.getLock());
        }
      }
    }
    return queueToUse;
  }

  /**
   * Release the locks acquired by a request executed in autocommit mode.
   * 
   * @param locks the list of locks acquired by the request
   * @param transactionId the "fake" transaction id assign to the autocommit
   *          request releasing the locks
   */
  private void releaseLocksForAutoCommitRequest(List<?> locks, long transactionId)
  {
    if (locks == null)
      return; // No locks acquired
    for (Iterator<?> iter = locks.iterator(); iter.hasNext();)
    {
      TransactionLogicalLock lock = (TransactionLogicalLock) iter.next();
      if (lock == null)
        logger.warn("Unexpected null lock for transaction " + transactionId
            + " when releasing " + locks.toArray());
      else
        lock.release(transactionId);
    }
  }

  /**
   * Release the locks held by the given transaction at commit/rollback time.
   * 
   * @param transactionId the transaction releasing the locks
   */
  private void releaseLocksForTransaction(long transactionId)
  {
    try
    {
      TransactionMetaData tm = requestManager.getTransactionMetaData(new Long(
          transactionId));
      releaseLocksForAutoCommitRequest(tm.removeBackendLocks(backend),
          transactionId);
    }
    catch (SQLException e)
    {
      /*
       * this is expected to fail when replaying the recovery log, since the
       * request manager won't have any transaction metadatas for transactions
       * we are replaying => we don't log warnings in this case.
       */
      if (!backend.isReplaying())
        if (logger.isWarnEnabled())
          logger.warn("No transaction medatada found for transaction "
              + transactionId + " releasing locks manually");
      if (backend.getDatabaseSchema() != null)
        backend.getDatabaseSchema().releaseLocksOnAllTables(transactionId);
      else
      {
        /*
         * At this point, schema can be null, for example if the backend is down
         */
        if (logger.isWarnEnabled())
          logger
              .warn("Cannot release locks, as no schema is available on this backend. "
                  + "This backend is problably not available anymore.");
      }
    }
  }

  private TransactionMetaData getTransactionMetaData(AbstractRequest request)
  {
    TransactionMetaData tm = null;
    if ((request != null) && !request.isAutoCommit())
    { // Retrieve the transaction marker metadata
      try
      {
        tm = requestManager.getTransactionMetaData(new Long(request
            .getTransactionId()));
      }
      catch (SQLException e)
      {
        // We didn't start or lazy start the transaction
        if (logger.isDebugEnabled())
          logger.debug("No transaction medatada found for transaction "
              + request.getTransactionId()
              + " while getting transaction metadata");
      }
    }
    return tm;
  }

  /**
   * Notify the completion of the given entry. The corresponding task completion
   * is notified to the backend.
   * 
   * @param entry the executed entry
   */
  public final void completedEntryExecution(BackendTaskQueueEntry entry)
  {
    completedEntryExecution(entry, null);
  }

  /**
   * Perform the cleanup to release locks and priority inversion checkings after
   * a stored procedure execution
   * 
   * @param task the task that completed
   */
  public void completeStoredProcedureExecution(AbstractTask task)
  {
    AbstractRequest request = task.getRequest();
    long transactionId = request.getTransactionId();
    synchronized (atomicPostSyncObject)
    {
      if (request.isAutoCommit())
      {
        releaseLocksForAutoCommitRequest(task.getLocks(backend), transactionId);
        checkForPriorityInversion();
        SortedSet<?> writeLockedTables = request.getWriteLockedDatabaseTables();
        if ((writeLockedTables != null) && (writeLockedTables.size() > 1))
          writesWithMultipleLocks--;
      }
      storedProcedureInQueue--;
    }
  }

  /**
   * Perform the cleanup to release locks and priority inversion checkings after
   * a write query execution
   * 
   * @param task the task that completed
   */
  public void completeWriteRequestExecution(AbstractTask task)
  {
    AbstractRequest request = task.getRequest();
    SortedSet<?> writeLockedTables = request.getWriteLockedDatabaseTables();
    if ((writeLockedTables != null) && (writeLockedTables.size() > 1))
      synchronized (atomicPostSyncObject)
      {
        writesWithMultipleLocks--;
      }

    long transactionId = request.getTransactionId();
    if (request.isAutoCommit())
    {
      synchronized (atomicPostSyncObject)
      {
        releaseLocksForAutoCommitRequest(task.getLocks(backend), transactionId);
        // Make sure we release the requests locking multiple tables or the
        // stored procedures that are blocked if any
        if (writesWithMultipleLocks > 0
            || waitForCompletionPolicy.isEnforceTableLocking()
            || waitForCompletionPolicy
                .isEnforceTableLockOnAutoIncrementInsert())
          checkForPriorityInversion();
        else if (storedProcedureInQueue > 0)
          checkForPriorityInversion();
      }
    }
  }

  /**
   * Releasing locks and checking for priority inversion. Usually used at commit
   * or rollback completion time.
   * 
   * @param tm the transaction metadata
   */
  public void releaseLocksAndCheckForPriorityInversion(TransactionMetaData tm)
  {
    synchronized (atomicPostSyncObject)
    {
      releaseLocksForTransaction(tm.getTransactionId());
      checkForPriorityInversion();
    }
  }

  /**
   * Removes the specified entry from its queue and notifies threads waiting on
   * this backend task queue. The removal is performed using the iterator, if
   * specified (non-null), or directly on the queue otherwize.
   * 
   * @param entry the entry to remove from its queue
   * @param iter the iterator on which to call remove(), or null if not
   *          applicable.
   */
  private void completedEntryExecution(BackendTaskQueueEntry entry,
      Iterator<BackendTaskQueueEntry> iter)
  {
    if (entry == null)
      return;

    // Notify the backend that this query execution is complete
    AbstractTask task = entry.getTask();
    if (!backend.removePendingTask(task))
      logger.warn("Unable to remove task " + task
          + " from pending request queue");

    synchronized (this)
    {
      // Remove the entry from its queue
      LinkedList<?> queue = entry.getQueue();
      synchronized (queue)
      {
        if (iter != null)
          iter.remove();
        else
        {
          if (!queue.remove(entry))
            logger.error("Failed to remove task " + task + " from " + queue);
        }
      }

      // Notify the queues to unblock queries waiting in getNextEntryToExecute
      // for the completion of the current request.
      this.notifyAll();
    }
  }

  /**
   * Return the first entry in the conflicting requests queue (does not remove
   * it from the list).
   * 
   * @return the first entry in the conflicting queue
   */
  public final BackendTaskQueueEntry getFirstConflictingRequestQueueOrStoredProcedureQueueEntry()
  {
    synchronized (conflictingRequestsQueue)
    {
      if (conflictingRequestsQueue.isEmpty())
      {
        synchronized (storedProcedureQueue)
        {
          if (storedProcedureQueue.isEmpty())
            return null;
          return (BackendTaskQueueEntry) storedProcedureQueue.getFirst();
        }
      }
      return (BackendTaskQueueEntry) conflictingRequestsQueue.getFirst();
    }
  }

  /**
   * Returns the stored procedure queue. This is needed for deadlock detection
   * but clearly does break the abstarction layer as it exposes a private field
   * in an un-controlled way.
   * 
   * @return the stored procedure queue.
   */
  public List<BackendTaskQueueEntry> getStoredProcedureQueue()
  {
    return storedProcedureQueue;
  }

  /**
   * Get the next available task entry to process from the queues. If the
   * backend is killed, this method will return a KillThreadTask else it will
   * wait for a task to be ready to be executed. Note that the task is left in
   * the queue and flagged as processed by the thread given as a parameter. The
   * task will only be removed from the queue when the thread notifies the
   * completion of the task.
   * 
   * @param thread the thread that will execute the task
   * @return the task to execute
   */
  public final BackendTaskQueueEntry getNextEntryToExecute(
      BackendWorkerThread thread)
  {
    BackendTaskQueueEntry entry = null;

    /*
     * The strategy is to look first for the non-conflicting queue so that
     * non-conflicting transactions could progress as fast as possible. Then we
     * check the conflicting queue if we did not find a task to execute.<p> If
     * we failed to find something to execute in the active queues, we process
     * everything available in the total order queue to push the tasks in the
     * active queues.
     */

    while (true)
    {
      Object firstNonConflictingTask = null;
      Object lastNonConflictingTask = null;
      // Check the non-conflicting queue first
      synchronized (nonConflictingRequestsQueue)
      {
        if (!nonConflictingRequestsQueue.isEmpty())
        {
          firstNonConflictingTask = nonConflictingRequestsQueue.getFirst();
          lastNonConflictingTask = nonConflictingRequestsQueue.getLast();
          for (Iterator<BackendTaskQueueEntry> iter = nonConflictingRequestsQueue.iterator(); iter
              .hasNext();)
          {
            entry = (BackendTaskQueueEntry) iter.next();
            if (entry.getProcessingThread() == null)
            { // This task is not currently processed, let's execute it
              entry.setProcessingThread(thread);
              return entry;
            }
          }
        }
      }

      // Nothing to be executed now in the non-conflicting queue, check the
      // conflicting queue
      Object firstConflictingTask = null;
      Object lastConflictingTask = null;
      synchronized (conflictingRequestsQueue)
      {
        if (!conflictingRequestsQueue.isEmpty())
        {
          firstConflictingTask = conflictingRequestsQueue.getFirst();
          lastConflictingTask = conflictingRequestsQueue.getLast();
          // Only check the first task since we must execute them only one at a
          // time.
          entry = (BackendTaskQueueEntry) conflictingRequestsQueue.getFirst();
          if (entry.getProcessingThread() == null)
          { // The task is not currently processed.
            AbstractRequest request = entry.getTask().getRequest();
            SortedSet<?> lockedTables = request.getWriteLockedDatabaseTables();
            if ((lockedTables != null) && (lockedTables.size() > 0))
            {
              /**
               * Check if there are requests in the non-conflicting queue that
               * belongs to a transaction that is holding a lock on which we
               * conflict.
               * <p>
               * Note that if we need to lock multiple tables and that we are in
               * the conflicting queue, we are going to wait until all locks are
               * free or a deadlock detection occurs.
               */
              boolean conflictingQueryDetected = false;
              synchronized (nonConflictingRequestsQueue)
              {
                DatabaseSchema schema = backend.getDatabaseSchema();

                if (waitForCompletionPolicy
                    .isEnforceTableLockOnAutoIncrementInsert()
                    && request.isInsert())
                {
                  DatabaseTable table = schema
                      .getTable(((InsertRequest) request).getTableName());
                  if (table != null && table.containsAutoIncrementedKey())
                    conflictingQueryDetected = true;
                }
                else if (!nonConflictingRequestsQueue.isEmpty()
                    || waitForCompletionPolicy.isEnforceTableLocking())
                { // Check for a potential conflict
                  int locksNotOwnedByMe = 0;
                  long transactionId = entry.getTask().getTransactionId();

                  for (Iterator<?> iterator = lockedTables.iterator(); iterator
                      .hasNext()
                      && !conflictingQueryDetected;)
                  {
                    String tableName = (String) iterator.next();
                    DatabaseTable table = schema.getTable(tableName, false);
                    if (table == null)
                    { // No table found, let's go for the conflicting queue
                      logger
                          .warn("Unable to find table "
                              + tableName
                              + " in database schema, when getting next entry to execute : "
                              + request
                                  .toStringShortForm(requestManager
                                      .getVirtualDatabase()
                                      .getSqlShortFormLength()));

                      // Assume conflict since non-conflicting queue is not
                      // empty
                      conflictingQueryDetected = true;
                    }
                    else
                    {
                      TransactionLogicalLock lock = table.getLock();
                      if (lock.isLocked())
                      {
                        if (lock.getLocker() != transactionId)
                          locksNotOwnedByMe++;

                        /*
                         * Check if we find a query in the conflicting queue
                         * that owns the lock or waits for the lock we need
                         */
                        for (Iterator<BackendTaskQueueEntry> iter = nonConflictingRequestsQueue
                            .iterator(); iter.hasNext();)
                        {
                          BackendTaskQueueEntry nonConflictingEntry = (BackendTaskQueueEntry) iter
                              .next();
                          long nonConflictingRequestTransactionId = nonConflictingEntry
                              .getTask().getTransactionId();
                          if ((lock.getLocker() == nonConflictingRequestTransactionId)
                              || lock
                                  .isWaiting(nonConflictingRequestTransactionId))
                          {
                            conflictingQueryDetected = true;
                            break;
                          }
                        }
                      }
                    }
                  }

                  /*
                   * If table level locking is enforced, we don't allow a
                   * request to execute before it has all its locks
                   */
                  if (waitForCompletionPolicy.isEnforceTableLocking())
                    conflictingQueryDetected = locksNotOwnedByMe > 0;

                  /*
                   * If we don't own a single lock (in case of multiple locks)
                   * needed by this query then we wait for the locks to be
                   * released or the deadlock detection to abort a transaction
                   * that is holding at least one of the locks that we need.
                   */
                  conflictingQueryDetected = conflictingQueryDetected
                      || ((locksNotOwnedByMe > 1) && (locksNotOwnedByMe == lockedTables
                          .size()));
                }
              }

              // If everyone is done in the non-conflicting queue, then
              // let's go with this conflicting request
              if (!conflictingQueryDetected)
              {
                entry.setProcessingThread(thread);
                return entry;
              }
            }
            else
            {
              if (logger.isWarnEnabled())
                logger.warn("Detected non-locking task " + entry.getTask()
                    + " in conflicting queue");

              /*
               * No clue on where the conflict happens, it might well be that we
               * don't access any table but in that case we shouldn't have ended
               * up in the conflicting queue. To be safer, let's wait for the
               * non-conflicting queue to be empty.
               */
              synchronized (nonConflictingRequestsQueue)
              {
                if (nonConflictingRequestsQueue.isEmpty())
                {
                  entry.setProcessingThread(thread);
                  return entry;
                }
              }
            }
          }
        }
      }

      synchronized (this)
      {
        // No entry in the queues or all entries are currently processed,
        // process the total order queue.
        if (fetchNextQueryFromBackendTotalOrderQueue())
          continue;

        // Nothing in the total order queue either !
        // Double-check that something was not posted in the queue after we
        // scanned it
        synchronized (nonConflictingRequestsQueue)
        {
          if (!nonConflictingRequestsQueue.isEmpty())
          {
            if (firstNonConflictingTask != nonConflictingRequestsQueue
                .getFirst())
              continue;
            if (lastNonConflictingTask != nonConflictingRequestsQueue.getLast())
              continue;
          }
          else if (firstNonConflictingTask != null)
            continue; // The queue was emptied all at once
        }
        synchronized (conflictingRequestsQueue)
        {
          if (!conflictingRequestsQueue.isEmpty())
          {
            if (firstConflictingTask != conflictingRequestsQueue.getFirst())
              continue;
            if (lastConflictingTask != conflictingRequestsQueue.getLast())
              continue;
          }
          else if (firstConflictingTask != null)
            continue; // The queue was emptied all at once
        }

        // Wait until a new task is posted
        try
        {
          this.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }

    }
  }

  /**
   * Get the next available commit or rollback task to process from the queues.
   * If the backend is killed, this method will return a KillThreadTask else it
   * will wait for a task to be ready to be executed. Note that the task is left
   * in the queue and flagged as processed by the thread given as a parameter.
   * The task will only be removed from the queue when the thread notifies the
   * completion of the task.
   * 
   * @param thread the thread that will execute the task
   * @return the commmit or rollback task to execute
   */
  public BackendTaskQueueEntry getNextCommitRollbackToExecute(
      BackendWorkerThread thread)
  {
    boolean found = false;
    BackendTaskQueueEntry entry = null;
    while (!found)
    {
      Object firstNonConflictingTask = null;
      Object lastNonConflictingTask = null;
      // Check the non-conflicting queue first
      synchronized (nonConflictingRequestsQueue)
      {
        if (!nonConflictingRequestsQueue.isEmpty())
        {
          firstNonConflictingTask = nonConflictingRequestsQueue.getFirst();
          lastNonConflictingTask = nonConflictingRequestsQueue.getLast();
          for (Iterator<BackendTaskQueueEntry> iter = nonConflictingRequestsQueue.iterator(); iter
              .hasNext();)
          {
            entry = (BackendTaskQueueEntry) iter.next();
            if ((entry.isACommitOrRollback() || (entry.getTask() instanceof KillThreadTask))
                && (entry.getProcessingThread() == null))
            { // This task is not currently processed, let's execute it
              entry.setProcessingThread(thread);
              return entry;
            }
          }
        }
      }

      synchronized (this)
      {
        // No entry in the queues or all entries are currently processed,
        // process the total order queue.
        if (fetchNextQueryFromBackendTotalOrderQueue())
          continue;

        // Double-check that something was not posted in the queue after we
        // scanned it
        synchronized (nonConflictingRequestsQueue)
        {
          if (!nonConflictingRequestsQueue.isEmpty())
          {
            if (firstNonConflictingTask != nonConflictingRequestsQueue
                .getFirst())
              continue;
            if (lastNonConflictingTask != nonConflictingRequestsQueue.getLast())
              continue;
          }
        }

        try
        {
          this.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }

    }
    // We should never reach this point
    return null;
  }

  /**
   * Returns true if there is an executing request in any of the queues for the
   * given transaction.
   * 
   * @param tid transaction id we need to look for
   * @return true if a task with such transaction id has been found
   */
  public synchronized boolean hasAnExecutingTaskForTransaction(long tid)
  {
    return backend.hasTaskForTransaction(tid);
  }

  /**
   * Returns true if there is a pending (or executing) request in any of the
   * queues for the given transaction.
   * 
   * @param tid transaction id we need to look for
   * @return true if a task with such transaction id has been found
   */
  public synchronized boolean hasAPendingTaskForTransaction(long tid)
  {
    return hasAbstractTaskForTransactionInQueue(totalOrderQueue, tid)
        || hasAnExecutingTaskForTransaction(tid);
  }

  /**
   * Returns true if all task queues are empty.
   * 
   * @return true if no task is pending or executing on this backend
   */
  public boolean hasNoTaskInQueues()
  {
    return totalOrderQueue.isEmpty() && nonConflictingRequestsQueue.isEmpty()
        && conflictingRequestsQueue.isEmpty() && storedProcedureQueue.isEmpty();
  }

  /**
   * Checks if the current entry needs to wait for a later entry before being
   * able to execute.
   * 
   * @param currentTask the current <code>AbstractTask</code> candidate for
   *          scheduling
   * @return <code>true</code> if the current task needs to wait for a late
   *         task before being able to execute, <code>false</code> else
   */
  private boolean mustWaitForLateTask(AbstractTask currentTask)
  {
    if (currentTask.isPersistentConnection())
    {
      long currentCid = currentTask.getPersistentConnectionId();
      // Check if there are other requests for this transaction in
      // the queue
      synchronized (this)
      {
        if (hasTaskForPersistentConnectionInQueue(nonConflictingRequestsQueue,
            currentCid)
            || hasTaskForPersistentConnectionInQueue(conflictingRequestsQueue,
                currentCid)
            || hasTaskForPersistentConnectionInQueue(storedProcedureQueue,
                currentCid))
          // Skip this commit/rollback until the conflicting request completes
          return true;
      }
    }

    if (!currentTask.isAutoCommit())
    {
      long currentTid = currentTask.getTransactionId();
      // Check if there are other requests for this transaction in
      // the queue
      if (hasAnExecutingTaskForTransaction(currentTid))
        // Skip this commit/rollback until the conflicting request completes
        return true;
    }

    synchronized (this)
    {
      return hasDDLTaskInQueue(nonConflictingRequestsQueue)
          || hasDDLTaskInQueue(conflictingRequestsQueue)
          || hasDDLTaskInQueue(storedProcedureQueue);
    }
  }

  /**
   * Check if there is a DDL task in the queue. Warning, this method must be
   * called with proper synchronization on the queue as concurrent modifications
   * won't be handled here.
   * 
   * @param queue the queue to parse
   * @return true if there is a DDL task in the queue
   */
  private boolean hasDDLTaskInQueue(List<BackendTaskQueueEntry> queue)
  {
    for (Iterator<BackendTaskQueueEntry> iter = queue.iterator(); iter.hasNext();)
    {
      BackendTaskQueueEntry otherEntry = (BackendTaskQueueEntry) iter.next();
      AbstractTask otherTask = otherEntry.getTask();
      AbstractRequest request = otherTask.getRequest();
      /**
       * For the moment just check if this is a create, drop or alter statement,
       * we could also check AbstractRequest#altersDatabaseSchema() but we don't
       * want to block if this is not a DDL (a stored procedure might alter the
       * schema because of its default semantic but still might need other
       * queries to be executed before it can really execute).
       */
      if ((request != null)
          && (request.isCreate() || request.isAlter() || request.isDrop()))
      {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if there is a task belonging to the given persistent connection in
   * the queue. Warning, this method must be called with proper synchronization
   * on the queue as concurrent modifications won't be handled here.
   * 
   * @param queue the queue to parse
   * @param cid persistent connection id
   * @return true if there is a task belonging to the persistent connection in
   *         the queue
   */
  private boolean hasTaskForPersistentConnectionInQueue(List<BackendTaskQueueEntry> queue, long cid)
  {
    for (Iterator<BackendTaskQueueEntry> iter = queue.iterator(); iter.hasNext();)
    {
      BackendTaskQueueEntry otherEntry = (BackendTaskQueueEntry) iter.next();

      AbstractTask otherTask = otherEntry.getTask();

      // Check if the query is in the same transaction
      if (otherTask.isPersistentConnection()
          && (otherTask.getPersistentConnectionId() == cid))
      {
        return true;
      }
    }
    return false;
  }

  private boolean hasAbstractTaskForTransactionInQueue(List<AbstractTask> totalOrderQueue,
      long tid)
  {
    synchronized (totalOrderQueue)
    {
      for (Iterator<AbstractTask> iter = totalOrderQueue.iterator(); iter.hasNext();)
      {
        AbstractTask task = (AbstractTask) iter.next();

        // Check if the query is in the same transaction
        if (!task.isAutoCommit() && (task.getTransactionId() == tid))
        {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Return true if tasks are allowed to be posted to the queue. If false, all
   * tasks posted to the queue are systematically notified for completion
   * without being executed (abort behavior)
   * 
   * @return Returns the allowTasksToBePosted.
   */
  public boolean allowTasksToBePosted()
  {
    synchronized (ALLOW_TASKS_SYNC)
    {
      return allowTasksToBePosted;
    }
  }

  /**
   * Set to true if tasks are allowed to be posted to the queue else, all tasks
   * posted to the queue are systematically notified for completion without
   * being executed (abort behavior)
   * 
   * @param allowTasksToBePosted The allowTasksToBePosted to set.
   */
  public void setAllowTasksToBePosted(boolean allowTasksToBePosted)
  {
    synchronized (ALLOW_TASKS_SYNC)
    {
      this.allowTasksToBePosted = allowTasksToBePosted;
    }
  }

  /**
   * Start a new Deadlock Detection Thread (throws a RuntimeException if called
   * twice without stopping the thread before the second call).
   * 
   * @param vdb the virtual database the backend is attached to
   */
  public void startDeadlockDetectionThread(VirtualDatabase vdb)
  {
    if (deadlockDetectionThread != null)
      throw new RuntimeException(
          "Trying to start multiple times a deadlock detection thread on the same backend "
              + backend.getName());

    deadlockDetectionThread = new DeadlockDetectionThread(backend, vdb,
        atomicPostSyncObject, waitForCompletionPolicy.getDeadlockTimeoutInMs());
    deadlockDetectionThread.start();
  }

  /**
   * Terminate the Deadlock Detection Thread. Throws a RuntimeException is the
   * thread was already stopped (or not started).
   */
  public void terminateDeadlockDetectionThread()
  {
    if (deadlockDetectionThread == null)
    {
      logger.debug("No deadlock detection thread to stop on backend "
          + backend.getName() + " (it was only read enabled)");
      return;
    }
    deadlockDetectionThread.kill();
    deadlockDetectionThread = null;
  }

  /**
   * Returns a <code>String</code> corresponding to the dump of the internal
   * state of this BackendTaskQueues.<br />
   * This method is synchronized to provided a consistent snapshots of the
   * queues.
   * 
   * @return a <code>String</code> representing the internal state of this
   *         BackendTaskQueues
   */
  protected synchronized String dump()
  {
    StringBuffer buff = new StringBuffer();
    buff.append("Total order Queue (" + totalOrderQueue.size() + ")\n");
    for (Iterator<AbstractTask> iter = totalOrderQueue.iterator(); iter.hasNext();)
    {
      AbstractTask task = (AbstractTask) iter.next();
      buff.append("\t" + task + "\n");
    }
    buff.append("Non Conflicting Requests Queue ("
        + nonConflictingRequestsQueue.size() + ")\n");
    for (Iterator<BackendTaskQueueEntry> iter = nonConflictingRequestsQueue.iterator(); iter.hasNext();)
    {
      BackendTaskQueueEntry entry = (BackendTaskQueueEntry) iter.next();
      buff.append("\t" + entry + "\n");
    }
    buff.append("Conflicting Requests Queue ("
        + conflictingRequestsQueue.size() + ")\n");
    for (Iterator<BackendTaskQueueEntry> iter = conflictingRequestsQueue.iterator(); iter.hasNext();)
    {
      BackendTaskQueueEntry entry = (BackendTaskQueueEntry) iter.next();
      buff.append("\t" + entry + "\n");
    }
    buff.append("Stored Procedures Queue (" + storedProcedureQueue.size()
        + ")\n");
    for (Iterator<BackendTaskQueueEntry> iter = storedProcedureQueue.iterator(); iter.hasNext();)
    {
      BackendTaskQueueEntry entry = (BackendTaskQueueEntry) iter.next();
      buff.append("\t" + entry + "\n");
    }
    return buff.toString();
  }

  /**
   * {@inheritDoc}
   * 
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return dump();
  }

}
