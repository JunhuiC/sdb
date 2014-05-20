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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet, Stephane Giron.
 */

package org.continuent.sequoia.controller.recoverylog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.management.ObjectName;

import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.management.BackendState;
import org.continuent.sequoia.common.jmx.notifications.SequoiaNotificationList;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueues;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.BeginTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.ClosePersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CommitTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.KillThreadTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.OpenPersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackTask;
import org.continuent.sequoia.controller.recoverylog.events.LogEntry;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;

/**
 * This class defines a RecoverThread that is in charge of replaying the
 * recovery log on a given backend to re-synchronize it with the other nodes of
 * the cluster.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Stephane.Giron@continuent.com>Stephane Giron </a>
 * @version 1.0
 */
public class RecoverThread extends Thread
{
  static Trace              logger                     = Trace
                                                           .getLogger(RecoverThread.class
                                                               .getName());
  /** end user logger */
  static Trace              endUserLogger              = Trace
                                                           .getLogger("org.continuent.sequoia.enduser");
  private RecoveryLog       recoveryLog;
  private DatabaseBackend   backend;
  private RequestManager    requestManager;

  // an exception thrown while recovering
  private SQLException      exception;

  /**
   * a List&lt;Long&gt; of persistent connection IDs that are re-opened during
   * recovery
   */
  private List<Long>              persistentConnections;

  /**
   * HashMap of transaction IDs which are replayed during recovery (key is
   * transaction id, value is login)
   */
  private HashMap<Long, AbstractRequest>           tids;

  /**
   * The scheduler used to suspend writes during the recovery process
   */
  private AbstractScheduler scheduler;

  private String            checkpointName;

  /** Size of the pendingRecoveryTasks queue used during recovery */
  private int               recoveryBatchSize;

  /** Enable backend with read/write or read only */
  private boolean           isWrite;
  private Boolean           recoveryProcessInterrupted = Boolean.FALSE;
  private long              previousExecutingLogId     = -1;
  private boolean           canBeInterrupted;

  /**
   * Creates a new <code>RecoverThread</code> object
   * 
   * @param scheduler the currently used scheduler
   * @param recoveryLog Recovery log that creates this thread
   * @param backend database backend for logging
   * @param requestManager the request manager to use for recovery
   * @param checkpointName the checkpoint from which is started the recovery
   */
  public RecoverThread(AbstractScheduler scheduler, RecoveryLog recoveryLog,
      DatabaseBackend backend, RequestManager requestManager,
      String checkpointName, boolean isWrite)
  {
    super("RecoverThread for backend " + backend.getName());
    this.scheduler = scheduler;
    this.recoveryLog = recoveryLog;
    this.backend = backend;
    this.requestManager = requestManager;
    this.checkpointName = checkpointName;
    this.isWrite = isWrite;
    this.recoveryBatchSize = recoveryLog.getRecoveryBatchSize();
    tids = new HashMap<Long, AbstractRequest>();
    persistentConnections = new ArrayList<Long>();
    recoveryProcessInterrupted = Boolean.FALSE;
  }

  /**
   * Returns the exception value.
   * 
   * @return Returns the exception.
   */
  public SQLException getException()
  {
    return exception;
  }

  /**
   * @see java.lang.Runnable#run()
   */
  public void run()
  {
    backend.setState(BackendState.REPLAYING);
    try
    {
      if (!backend.isInitialized())
        backend.initializeConnections();
    }
    catch (SQLException e)
    {
      recoveryFailed(e);
      return;
    }
    // notify the recovery log that a new
    // recovery is about to begin
    recoveryLog.beginRecovery();

    // Get the checkpoint from the recovery log
    long logIdx;
    try
    {
      logIdx = recoveryLog.getCheckpointLogId(checkpointName);
    }
    catch (SQLException e)
    {
      recoveryLog.endRecovery();
      String msg = Translate.get("recovery.cannot.get.checkpoint", e);
      logger.error(msg, e);
      recoveryFailed(new SQLException(msg));
      return;
    }

    try
    {
      startRecovery();

      logger.info(Translate.get("recovery.start.process"));

      // Play write queries from the recovery log until the last entry or the
      // first executing entry
      LinkedList<RecoveryTask> pendingRecoveryTasks = new LinkedList<RecoveryTask>();
      try
      {
        logIdx = recover(logIdx, pendingRecoveryTasks);
      }
      catch (EndOfRecoveryLogException e)
      {
        logIdx = e.getLogIdx();
      }

      requestManager.suspendActivity();

      /*
       * We need to make sure that the logger thread queue has been flushed to
       * the database, so we need a synchronization event that will make sure
       * that this happens. The getCheckpointLogId method posts a new object in
       * the logger thread queue and waits for it to be processed before
       * returning the result. We don't care about the result that is even
       * supposed to throw an exception but at least we are sure that the whole
       * queue has been flushed to disk.
       */
      try
      {
        recoveryLog
            .getCheckpointLogId("Just a big hack to synchronize the logger thread queue. Expected to fail ...");
      }
      catch (SQLException ignore)
      {
      }

      // Play the remaining writes that were pending and which have been logged
      boolean replayedAllLog = false;
      do
      { // Loop until the whole recovery log has been replayed
        // Or stop if the activity is resumed by force
        synchronized (recoveryProcessInterrupted)
        {
          canBeInterrupted = true;
        }
        try
        {
          logIdx = recover(logIdx, pendingRecoveryTasks);
          // The status update for the last request (probably a commit/rollback)
          // is not be there yet. Wait for it to be flushed to the log and
          // retry.
          try
          {
            recoveryLog
                .getCheckpointLogId("Just a big hack to synchronize the logger thread queue. Expected to fail ...");
          }
          catch (SQLException ignore)
          {
          }
        }
        catch (EndOfRecoveryLogException e)
        {
          replayedAllLog = true;
        }

        synchronized (recoveryProcessInterrupted)
        {
          if (recoveryProcessInterrupted.booleanValue())
          {
            recoveryLog.isDirty = true;
            throw new VirtualDatabaseException(
                "Recovery process was interrupted on administrative purpose. "
                    + "You should consider to initialize this backend from a new dump.");
          }
        }
      }
      while (!replayedAllLog);
      synchronized (recoveryProcessInterrupted)
      {
        if (recoveryProcessInterrupted.booleanValue())
        {
          recoveryLog.isDirty = true;
          throw new VirtualDatabaseException(
              "Recovery process was interrupted on administrative purpose. "
                  + "You should consider to initialize this backend from a new dump.");
        }
        canBeInterrupted = false;
      }
      waitForAllTasksCompletion(pendingRecoveryTasks);
    }
    catch (SQLException e)
    {
      recoveryFailed(e);
      // Resume writes, transactions and persistent connections
      requestManager.resumeActivity(false);
      return;
    }
    catch (VirtualDatabaseException e)
    {
      recoveryFailed(new SQLException(e.getMessage()));
      return;
    }
    catch (Throwable e)
    {
      SQLException sqlex = new SQLException(
          "Recovery process failed with an unexpected error");
      sqlex.initCause(e);
      recoveryFailed(sqlex);
      requestManager.resumeActivity(false);
      return;
    }
    finally
    {
      endRecovery();
    }

    // Now enable it
    try
    {
      requestManager.getLoadBalancer().enableBackend(backend, isWrite);
    }
    catch (SQLException e)
    {
      recoveryFailed(e);
      return;
    }
    finally
    {
      // Resume writes, transactions and persistent connections
      requestManager.resumeActivity(false);
    }
    logger.info(Translate.get("backend.state.enabled", backend.getName()));
  }

  /**
   * Unset the last known checkpoint and set the backend to disabled state. This
   * should be called when the recovery has failed.
   * 
   * @param e cause of the recovery failure
   */
  private void recoveryFailed(SQLException e)
  {
    this.exception = e;

    backend.setLastKnownCheckpoint(null);
    backend.setState(BackendState.DISABLED);
    try
    {
      backend.finalizeConnections();
    }
    catch (SQLException ignore)
    {
    }
    backend.notifyJmxError(
        SequoiaNotificationList.VIRTUALDATABASE_BACKEND_REPLAYING_FAILED, e);
  }

  /**
   * Replay the recovery log from the given logIdx index. Note that
   * startRecovery() must have been called to fork and start the
   * BackendWorkerThread before calling recover. endRecovery() must be called
   * after recover() to terminate the thread.
   * 
   * @param logIdx logIdx used to start the recovery
   * @param pendingRecoveryTasks
   * @return last logIdx that was replayed.
   * @throws SQLException if fails
   * @see #startRecovery()
   * @see #endRecovery()
   */
  private long recover(long logIdx, LinkedList<RecoveryTask> pendingRecoveryTasks)
      throws SQLException, EndOfRecoveryLogException
  {
    RecoveryTask recoveryTask = null;
    AbstractTask abstractTask = null;

    Long tid = null;
    long previousRemaining = 0;
    // Replay the whole log
    do
    {
      try
      {
        recoveryTask = recoveryLog.recoverNextRequest(logIdx, scheduler);
      }
      catch (SQLException e)
      {
        // Signal end of recovery and kill worker thread
        recoveryLog.endRecovery();
        addWorkerTask(new KillThreadTask(1, 1));
        String msg = Translate.get("recovery.cannot.recover.from.index", e);
        logger.error(msg, e);
        throw new SQLException(msg);
      }
      if (recoveryTask == null)
        throw new EndOfRecoveryLogException(logIdx);

      abstractTask = recoveryTask.getTask();
      if (abstractTask == null)
        throw new SQLException(
            "Unexpected null abstract task in recovery task " + recoveryTask);

      if (LogEntry.EXECUTING.equals(recoveryTask.getStatus()))
      {
        if (recoveryTask.getId() != previousExecutingLogId)
        {
          logger.warn("Recovery log entry marked as still executing : "
              + abstractTask);
          previousExecutingLogId = recoveryTask.getId();
        }
        // Ok, wait for current tasks to complete and notify the recovery that
        // we stopped on this entry
        break;
      }

      if (!LogEntry.SUCCESS.equals(recoveryTask.getStatus()))
      { // Ignore failed queries
        logIdx++;
        continue;
      }
      if ((logIdx % 1000) == 0)
      {
        long remaining = recoveryLog.getCurrentLogId() - logIdx;
        endUserLogger.info("Recovering log entry " + logIdx
            + " remaining entries " + remaining);
        if (previousRemaining > 0 && remaining > previousRemaining)
        {
          endUserLogger.warn("Recovery falling behind pending requests ="
              + pendingRecoveryTasks.size());
        }
        previousRemaining = remaining;
      }
      if (abstractTask.isPersistentConnection())
      {
        long cid = abstractTask.getPersistentConnectionId();
        if (abstractTask instanceof OpenPersistentConnectionTask)
          persistentConnections.add(new Long(cid));
        else if (abstractTask instanceof ClosePersistentConnectionTask)
          persistentConnections.remove(new Long(cid));
        else if (!persistentConnections.contains(new Long(cid)))
        {
          /**
           * If the task persistent connection id does not have a corresponding
           * connection opening (it is not in the persistent connections list),
           * then this task has already been played when the backend was
           * disabled. So we can skip it.
           * <p>
           * Note that if the task is a BeginTask, skipping the begin will skip
           * all requests in the transaction which is the expected behavior on a
           * persistent connection (transaction has been played before the
           * connection was closed, i.e. the backend was disabled).
           */
          logIdx++;
          continue;
        }
      }

      // Used to retrieve login and persistent connection id
      AbstractRequest request = null;
      if (!abstractTask.isAutoCommit())
      {
        tid = new Long(recoveryTask.getTid());
        if (abstractTask instanceof BeginTask)
        {
          if (tids.containsKey(tid))
          {
            // Skip multiple begins of the same transaction if exists (this is
            // possible !!!)
            logIdx++;
            continue;
          }
          tids.put(tid, abstractTask.getRequest());
        }
        else
        {
          request = (AbstractRequest) tids.get(tid);
          if (request == null)
          {
            /*
             * if the task transaction id does not have a corresponding begin
             * (it is not in the tids list), then this task has already been
             * played when the backend was disabled. So we can skip it.
             */
            logIdx++;
            continue;
          }
          if (abstractTask instanceof RollbackTask)
          {
            // Override login in case it was logged with UNKNOWN_USER
            ((RollbackTask) abstractTask).getTransactionMetaData().setLogin(
                request.getLogin());
          }
          // Restore persistent connection id information
          abstractTask
              .setPersistentConnection(request.isPersistentConnection());
          abstractTask.setPersistentConnectionId(request
              .getPersistentConnectionId());
        }
      } // else autocommit ok

      if ((abstractTask instanceof CommitTask)
          || (abstractTask instanceof RollbackTask))
      {
        tids.remove(tid);
      }

      logIdx = recoveryTask.getId();
      // Add the task for execution by the BackendWorkerThread
      addWorkerTask(abstractTask);

      // Add it to the list of currently executing tasks
      pendingRecoveryTasks.addLast(recoveryTask);

      do
      {
        // Now let's check which tasks have completed and remove them from the
        // pending queue.
        for (Iterator<RecoveryTask> iter = pendingRecoveryTasks.iterator(); iter.hasNext();)
        {
          recoveryTask = (RecoveryTask) iter.next();
          abstractTask = recoveryTask.getTask();
          if (abstractTask.hasFullyCompleted())
          { // Task has completed, remove it from the list
            iter.remove();

            if (LogEntry.SUCCESS.equals(recoveryTask.getStatus()))
            { // Only deal with successful tasks

              if (abstractTask.getFailed() > 0)
              { // We fail to recover that task. Signal end of recovery and kill
                // worker thread
                String msg;
                if (abstractTask.isAutoCommit())
                  msg = Translate.get("recovery.failed.with.error",
                      new Object[]{
                          abstractTask,
                          ((Exception) abstractTask.getExceptions().get(0))
                              .getMessage()});
                else
                  msg = Translate.get("recovery.failed.with.error.transaction",
                      new Object[]{
                          Long.toString(abstractTask.getTransactionId()),
                          abstractTask,
                          ((Exception) abstractTask.getExceptions().get(0))
                              .getMessage()});
                recoveryLog.endRecovery();
                addWorkerTask(new KillThreadTask(1, 1));
                pendingRecoveryTasks.clear();
                logger.error(msg);
                throw new SQLException(msg);
              }
            }
          }
        }

        /*
         * Transactions and persistentConnections limit by the number of pending
         * requests at the backend total order queue. Only one request per
         * transaction or persistent connection will be moved from the total
         * order queue to the task queues. When all the requests are auto
         * commit, we need to limit the number of requests here. Otherwise, the
         * conflicting queue can grow indefinitely large.
         */
        if (tids.isEmpty() && persistentConnections.isEmpty()
            && pendingRecoveryTasks.size() >= recoveryBatchSize)
          try
          {
            recoveryTask = (RecoveryTask) pendingRecoveryTasks.getFirst();
            abstractTask = recoveryTask.getTask();
            synchronized (abstractTask)
            {
              if (!abstractTask.hasFullyCompleted())
                abstractTask.wait();
            }
          }
          catch (InterruptedException e)
          {
            break;
          }
        else
          break;
      }
      while (true);
    }
    while (logIdx != -1); // while we have not reached the last querys

    return logIdx;
  }

  /**
   * Wait for all tasks in the given list to complete. Note that endRecovery()
   * is called upon failure.
   * 
   * @param pendingRecoveryTasks list of <code>RecoveryTask</code> currently
   *          executing tasks
   * @throws SQLException if a failure occurs
   */
  private void waitForAllTasksCompletion(LinkedList<RecoveryTask> pendingRecoveryTasks)
      throws SQLException
  {
    RecoveryTask recoveryTask;
    AbstractTask abstractTask;

    while (!pendingRecoveryTasks.isEmpty())
    {
      recoveryTask = (RecoveryTask) pendingRecoveryTasks.removeFirst();
      abstractTask = recoveryTask.getTask();
      synchronized (abstractTask)
      {
        // Wait for task completion if needed
        while (!abstractTask.hasFullyCompleted())
          try
          {
            abstractTask.wait();
          }
          catch (InterruptedException ignore)
          {
          }

        if (LogEntry.SUCCESS.equals(recoveryTask.getStatus()))
        { // Only deal with successful tasks
          if (abstractTask.getFailed() > 0)
          { // We fail to recover that task. Signal end of recovery and kill
            // worker thread
            recoveryLog.endRecovery();
            addWorkerTask(new KillThreadTask(1, 1));
            pendingRecoveryTasks.clear();
            String msg;
            if (abstractTask.isAutoCommit())
              msg = Translate.get("recovery.failed.with.error", new Object[]{
                  abstractTask,
                  ((Exception) abstractTask.getExceptions().get(0))
                      .getMessage()});
            else
              msg = Translate.get("recovery.failed.with.error.transaction",
                  new Object[]{
                      Long.toString(abstractTask.getTransactionId()),
                      abstractTask,
                      ((Exception) abstractTask.getExceptions().get(0))
                          .getMessage()});
            logger.error(msg);
            throw new SQLException(msg);
          }
        }
      }
    }
  }

  /**
   * Add a task to a DatabaseBackend using the proper synchronization.
   * 
   * @param task the task to add to the thread queue
   */
  private void addWorkerTask(AbstractTask task)
  {
    backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task,
        recoveryBatchSize);
  }

  /**
   * Properly end the recovery and kill the worker thread used for recovery if
   * it exists.
   * 
   * @see #startRecovery()
   */
  private void endRecovery()
  {
    // We are done with the recovery
    logger.info(Translate.get("recovery.process.complete"));
    backend.terminateWorkerThreads();

    recoveryLog.endRecovery();
  }

  /**
   * Start the recovery process by forking a BackendWorkerThread. <br />
   * You must call endRecovery() to terminate the thread.
   * <p>
   * when starting the recovery, we create a new BackendTaskQueues for the
   * backend but only its non-conflicting queue will be used.<br />
   * We also use only one BackendWorkerThread to ensure that the request will be
   * replayed serially in the same order they were logged.
   * </p>
   * <p>
   * A new BackendTaskQueues will be set on the backend when it is enabled in
   * the endRecovery() method.
   * </p>
   * 
   * @see #endRecovery()
   * @see #addWorkerTask(AbstractTask)
   */
  private void startRecovery()
  {
    try
    {
      ObjectName taskQueuesObjectName = JmxConstants
          .getBackendTaskQueuesObjectName(backend.getVirtualDatabaseName(),
              backend.getName());
      if (MBeanServerManager.getInstance().isRegistered(taskQueuesObjectName))
      {
        MBeanServerManager.unregister(JmxConstants
            .getBackendTaskQueuesObjectName(backend.getVirtualDatabaseName(),
                backend.getName()));
      }
    }
    catch (Exception e)
    {
      if (logger.isWarnEnabled())
      {
        logger.warn("Exception while unregistering backend task queues mbean",
            e);
      }
    }
    // find the correct enforceTableLocking option for this backend
    WaitForCompletionPolicy waitForCompletionPolicy = requestManager
        .getLoadBalancer().waitForCompletionPolicy;
    backend.setTaskQueues(new BackendTaskQueues(backend,
        new WaitForCompletionPolicy(WaitForCompletionPolicy.FIRST,
            waitForCompletionPolicy.isEnforceTableLocking(),
            waitForCompletionPolicy.isEnforceTableLockOnAutoIncrementInsert(),
            0), requestManager));
    backend.startWorkerThreads(requestManager.getLoadBalancer());
  }

  /*
   * Used to signal that we have reached the end of the recovery log during the
   * recovery process. There are other conditions that interrupt the recovery
   * process, such as finding a request which is still executing. In such case
   * we will not throw this exception.
   */
  private class EndOfRecoveryLogException extends Exception
  {
    private static final long serialVersionUID = 2826202288239306426L;
    private long              logIdx;

    /**
     * Creates a new <code>EndOfRecoveryLogException</code> object
     * 
     * @param logIdx recovery log index we stopped at
     */
    public EndOfRecoveryLogException(long logIdx)
    {
      this.logIdx = logIdx;
    }

    /**
     * Return the last recovery log index reached
     * 
     * @return last recovery log index
     */
    public long getLogIdx()
    {
      return logIdx;
    }
  }

  /**
   * Indicates to the recover thread that the activity should be resumed, which
   * will lead the thread to stop the recovering process.
   * 
   * @return True if the RecoverThread is going to be interrupted
   */
  public boolean interruptRecoveryProcess()
  {
    synchronized (recoveryProcessInterrupted)
    {
      if (canBeInterrupted)
      {
        this.recoveryProcessInterrupted = Boolean.TRUE;
        return true;
      }

      return false;
    }

  }

  /**
   * Check if the recover thread is in a phase when it can be interrupted
   * 
   * @return True if the RecoverThread can be interrupted
   */
  public boolean canBeInterrupted()
  {
    synchronized (recoveryProcessInterrupted)
    {
      return canBeInterrupted;
    }
  }

}