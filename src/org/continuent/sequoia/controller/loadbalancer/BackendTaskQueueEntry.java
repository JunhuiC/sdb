/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.loadbalancer;

import java.util.LinkedList;

import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;

/**
 * This class defines a BackendTaskQueueEntry that is an element of a queue in
 * BackendTaskQueues.
 * 
 * @see org.continuent.sequoia.controller.loadbalancer.BackendTaskQueues
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class BackendTaskQueueEntry
{
  AbstractTask        task;
  private LinkedList<?>  queue;
  BackendWorkerThread processingThread;
  private boolean     isACommitOrRollback;
  private long timestamp;

  /**
   * Creates a new <code>BackendTaskQueueEntry</code> object
   * 
   * @param task the task to put in the entry
   * @param queue the queue in which the task has been posted
   * @param isACommitOrRollback true if the task is a commit or a rollback
   */
  public BackendTaskQueueEntry(AbstractTask task, LinkedList<?> queue,
      boolean isACommitOrRollback)
  {
    this.task = task;
    this.queue = queue;
    this.processingThread = null;
    this.isACommitOrRollback = isACommitOrRollback;
    this.timestamp = System.currentTimeMillis();
  }

  /**
   * Returns the processingThread value (null if no BackendWorkerThread is
   * currently processing the task).
   * 
   * @return Returns the processingThread.
   */
  public final BackendWorkerThread getProcessingThread()
  {
    return processingThread;
  }

  /**
   * Returns the isACommitOrRollback value.
   * 
   * @return Returns the isACommitOrRollback.
   */
  public final boolean isACommitOrRollback()
  {
    return isACommitOrRollback;
  }

  /**
   * Sets the processingThread value.
   * 
   * @param processingThread The processingThread to set.
   */
  public final void setProcessingThread(BackendWorkerThread processingThread)
  {
    this.processingThread = processingThread;
  }

  /**
   * Returns the queue in which the task has been posted.
   * 
   * @return Returns the queue holding the query.
   */
  public final LinkedList<?> getQueue()
  {
    return queue;
  }

  /**
   * Returns the task value.
   * 
   * @return Returns the task.
   */
  public final AbstractTask getTask()
  {
    return task;
  }

  /**
   * Sets the queue value.
   * 
   * @param queue The queue to set.
   */
  public void setQueue(LinkedList<?> queue)
  {
    this.queue = queue;
  }
  
  /**
   * <strong>for debug purpose only</strong>
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    long timeInSeconds = (System.currentTimeMillis() - timestamp) / 1000;
    return getTask().toString() + " ("  + timeInSeconds + ")";
  }
}