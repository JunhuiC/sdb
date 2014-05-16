/**
 * C-JDBC: Clustered JDBC.
 * Copyright (C) 2005 Emic Networks
 * Science And Control (INRIA).
 * Contact: c-jdbc@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or any later
 * version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.locks;

import java.sql.SQLException;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueueEntry;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueues;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * This class defines a DeadlockDetectionThread that runs periodically to detect
 * possible deadlocks in the wait for graph of a given database schema.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DeadlockDetectionThread extends Thread
{
  private DatabaseBackend   backend;
  private VirtualDatabase   vdb;
  private Object            dbsLock;
  private long              timeout;
  private boolean           killed = false;
  private BackendTaskQueues queues;
  private WaitForGraph      waitForGraph;
  protected static Trace    logger = Trace
                                       .getLogger("org.continuent.sequoia.controller.loadbalancer");

  /**
   * Creates a new <code>DeadlockDetectionThread</code> object
   * 
   * @param backend the backend we are taking care of
   * @param vdb virtual database the backend is attached to
   * @param dbsLock the object on which we can synchronize to prevent database
   *          schema modifications while we are running the deadlock detection
   *          algorithm
   * @param timeout the time to wait before starting to look for deadlocks
   */
  public DeadlockDetectionThread(DatabaseBackend backend, VirtualDatabase vdb,
      Object dbsLock, long timeout)
  {
    super("Deadlock detection thread");
    this.backend = backend;
    this.vdb = vdb;
    this.dbsLock = dbsLock;
    this.timeout = timeout;
    this.queues = backend.getTaskQueues();
    waitForGraph = new WaitForGraph(backend, queues.getStoredProcedureQueue());
  }

  /**
   * Terminate this thread
   */
  public synchronized void kill()
  {
    killed = true;
    notify();
  }

  /**
   * @see java.lang.Thread#run()
   */
  public void run()
  {
    BackendTaskQueueEntry lastEntry = queues
        .getFirstConflictingRequestQueueOrStoredProcedureQueueEntry();
    BackendTaskQueueEntry newEntry;
    while (!killed)
    {
      newEntry = queues
          .getFirstConflictingRequestQueueOrStoredProcedureQueueEntry();
      synchronized (this)
      {
        if ((newEntry == null) || (newEntry != lastEntry))
        { // Queue empty or queue has changed
          lastEntry = newEntry;
          try
          {
            wait(timeout);
          }
          catch (InterruptedException ignore)
          {
          }
        }
        else
        { // Queue seems stucked, it is not empty and we have
          // newEntry==lastEntry. Let's look for deadlocks
          boolean deadlockDetected;
          long tid = 0;
          synchronized (dbsLock)
          {
            deadlockDetected = waitForGraph.detectDeadlocks();
            if (deadlockDetected)
            { // Need to abort the transaction
              tid = waitForGraph.getVictimTransactionId();
            }
            // Prevent infinite loop and let's sleep for a while since there
            // is no one to kill now
            lastEntry = null;
          } // synchronized (dbsLock)
          if (deadlockDetected)
          {
            if (logger.isInfoEnabled())
              logger.info("Deadlock detected on backend " + backend.getName()
                  + ", aborting transaction " + tid);

            try
            {
              // Abort the transaction
              vdb.abort(tid, true, true);
            }
            catch (SQLException e)
            {
              logger.warn("Failed to abort transaction " + tid, e);
            }
          }
        } // else
      } // synchronized
    } // while
  }

}
