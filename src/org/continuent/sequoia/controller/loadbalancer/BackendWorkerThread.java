/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Contributor(s):
 */

package org.continuent.sequoia.controller.loadbalancer;

import java.sql.SQLException;
import java.sql.Statement;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.loadbalancer.tasks.KillThreadTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackTask;
import org.continuent.sequoia.controller.virtualdatabase.activity.ActivityService;

/**
 * Process sequentially a set of tasks and send them to a backend.
 *
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class BackendWorkerThread extends Thread
{
  //
  // How the code is organized ?
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Task management
  // 4. Getter/Setters
  //

  private AbstractLoadBalancer loadBalancer;
  private DatabaseBackend      backend;
  private boolean              isKilled                  = false;

  private Trace                logger                    = null;
  private BackendTaskQueues    queues;
  private Statement            currentStatement;

  /** True if this thread only plays commit/rollback tasks */
  private boolean              playingCommitRollbackOnly = false;

  /*
   * Constructor
   */

  /**
   * Creates a new <code>BackendWorkerThread</code>.
   *
   * @param backend the backend this thread is associated to.
   * @param loadBalancer the load balancer instanciating this thread
   */
  public BackendWorkerThread(DatabaseBackend backend,
      AbstractLoadBalancer loadBalancer)
  {
    this(loadBalancer.vdb.getVirtualDatabaseName()
        + " - BackendWorkerThread for backend '" + backend.getName()
        + "' with RAIDb level:" + loadBalancer.getRAIDbLevel(), backend,
        loadBalancer);
  }

  /**
   * Creates a new <code>BackendWorkerThread</code>.
   *
   * @param name the name to give to the thread
   * @param backend the backend this thread is associated to.
   * @param loadBalancer the load balancer instanciating this thread
   */
  public BackendWorkerThread(String name, DatabaseBackend backend,
      AbstractLoadBalancer loadBalancer)
  {
    super(name);
    // Sanity checks
    if (backend == null)
    {
      String msg = Translate.get("backendworkerthread.null.backend");
      logger = Trace
          .getLogger("org.continuent.sequoia.controller.backend.DatabaseBackend");
      logger.error(msg);
      throw new NullPointerException(msg);
    }

    logger = Trace
        .getLogger("org.continuent.sequoia.controller.backend.DatabaseBackend."
            + backend.getName());

    if (loadBalancer == null)
    {
      String msg = Translate.get("backendworkerthread.null.loadbalancer");
      logger.error(msg);
      throw new NullPointerException(msg);
    }

    this.backend = backend;
    this.loadBalancer = loadBalancer;
    this.queues = backend.getTaskQueues();
  }

  /**
   * Kills this thread after the next task processing and forces the load
   * balancer to disable the backend. It also marks all remaining tasks in the
   * task list as failed.
   */
  public void killAndForceDisable()
  {
    kill(true);
  }

  /**
   * Kills this thread after the next task processing. It also disables the
   * backend.
   */
  public void killWithoutDisablingBackend()
  {
    kill(false);
  }

  /**
   * Kills this thread after the next task processing. It also marks all
   * remaining tasks in the task list as failed.
   *
   * @param forceDisable true if the task must call the load balancer to disable
   *          the backend
   */
  private void kill(boolean forceDisable)
  {
    synchronized (this)
    {
      if (logger.isDebugEnabled())
        logger.debug(this.getName() + " is shutting down");

      isKilled = true;
      notify(); // Wake up thread
    }
    if (forceDisable)
    {
      try
      {
        // This ensure that all worker threads get removed from the load
        // balancer list and that the backend state is set to disable.
        loadBalancer.disableBackend(backend, true);
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Process the tasklist and call <code>wait()</code> (on itself) when the
   * tasklist becomes empty.
   */
  public void run()
  {
    BackendTaskQueueEntry currentlyProcessingEntry = null;

    while (!isKilled)
    {
      try
      {
        // Take the next available task
        if (isPlayingCommitRollbackOnly())
          currentlyProcessingEntry = queues
              .getNextCommitRollbackToExecute(this);
        else
          currentlyProcessingEntry = queues.getNextEntryToExecute(this);

        if (currentlyProcessingEntry == null)
        {
          logger.warn("Null task in BackendWorkerThread");
          continue;
        }

        // Execute the tasks
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("backendworkerthread.execute.task",
              currentlyProcessingEntry.getTask().toString()));
        currentlyProcessingEntry.getTask().execute(this);
      }
      catch (SQLException e)
      {
        // Task should have notified of failure
        logger.warn(Translate.get("backendworkerthread.task.failed", e));
      }
      catch (Throwable re)
      {
        logger.fatal(Translate.get(
            "backendworkerthread.task.unexpected.throwable",
            currentlyProcessingEntry == null
                ? "null task"
                : currentlyProcessingEntry.toString()), re);

        // We can't know for sure if the task has notified the failure or not.
        // To prevent a deadlock, we force the failure notification here.
        try
        {
          currentlyProcessingEntry.getTask().notifyFailure(this, 1,
              new SQLException(re.getMessage()));
        }
        catch (SQLException e1)
        {
          // just notify
        }
      }
      finally
      {
        // do not treat "rollback" or "kill thread" tasks
        // as activity reaching the backend
        if (currentlyProcessingEntry != null
            && !(currentlyProcessingEntry.getTask() instanceof RollbackTask)
            && !(currentlyProcessingEntry.getTask() instanceof KillThreadTask))
        {
          ActivityService.getInstance().notifyActivityFor(backend.getVirtualDatabaseName());
        }
        setCurrentStatement(null);
        try
        {
          if (currentlyProcessingEntry != null)
          {
            queues.completedEntryExecution(currentlyProcessingEntry);
            if (logger.isDebugEnabled())
              logger.debug(Translate.get(
                  "backendworkerthread.execute.task.completed",
                  currentlyProcessingEntry.getTask().toString()));
          }
        }
        catch (RuntimeException e)
        {
          logger.warn(
              Translate.get("backendworkerthread.remove.task.error", e), e);
        }
        // Trying to speed-up GC on potentially large request objects
        // referenced by this task (think BLOBs).
        currentlyProcessingEntry = null;
      }
    } // end while (!isKilled)
  }

  /*
   * Getter/Setter
   */

  /**
   * Returns the backend.
   *
   * @return a <code>DatabaseBackend</code> instance
   */
  public DatabaseBackend getBackend()
  {
    return backend;
  }

  /**
   * Returns the currentStatement value.
   *
   * @return Returns the currentStatement.
   */
  public final Statement getCurrentStatement()
  {
    return currentStatement;
  }

  /**
   * Sets the currentStatement value.
   *
   * @param currentStatement The currentStatement to set.
   */
  public final void setCurrentStatement(Statement currentStatement)
  {
    this.currentStatement = currentStatement;
  }

  /**
   * Returns the loadBalancer value.
   *
   * @return Returns the loadBalancer.
   */
  public final AbstractLoadBalancer getLoadBalancer()
  {
    return loadBalancer;
  }

  /**
   * Returns the logger for tracing.
   *
   * @return a <code>Trace</code> instance
   */
  public Trace getLogger()
  {
    return logger;
  }

  /**
   * Returns the playCommitRollbackOnly value.
   *
   * @return Returns the playCommitRollbackOnly.
   */
  public boolean isPlayingCommitRollbackOnly()
  {
    return playingCommitRollbackOnly;
  }

  /**
   * Sets the playCommitRollbackOnly value.
   *
   * @param playCommitRollbackOnly The playCommitRollbackOnly to set.
   */
  public void setPlayCommitRollbackOnly(boolean playCommitRollbackOnly)
  {
    this.playingCommitRollbackOnly = playCommitRollbackOnly;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj)
  {
    if (obj instanceof BackendWorkerThread)
    {
      return backend.equals(((BackendWorkerThread) obj).getBackend());
    }
    return false;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return backend.hashCode();
  }

}