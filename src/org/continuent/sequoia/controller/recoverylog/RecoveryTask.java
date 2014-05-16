/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Contributor(s): Julie Marguerite.
 */

package org.continuent.sequoia.controller.recoverylog;

import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;

/**
 * Recovery task containing an <code>AbstractTask</code> and the id of the
 * task in the recovery log.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @version 1.0
 */
public class RecoveryTask
{
  private long         id;
  private long         tid;
  private AbstractTask task;
  private String       status;

  /**
   * Constructs a new <code>RecoveryTask</code> instance.
   * 
   * @param tid transaction id
   * @param id task id in the recovery log
   * @param task task to be executed
   * @param status request execution status as defined in LogEntry constants
   */
  public RecoveryTask(long tid, long id, AbstractTask task, String status)
  {
    this.id = id;
    this.tid = tid;
    this.task = task;
    this.status = status;
  }

  /**
   * Returns the id.
   * 
   * @return int
   */
  public long getId()
  {
    return id;
  }

  /**
   * Returns the status value.
   * 
   * @return Returns the status.
   */
  public final String getStatus()
  {
    return status;
  }

  /**
   * Returns the task.
   * 
   * @return AbstractTask
   */
  public AbstractTask getTask()
  {
    return task;
  }

  /**
   * Returns the tid value.
   * 
   * @return Returns the tid.
   */
  public long getTid()
  {
    return tid;
  }

}
