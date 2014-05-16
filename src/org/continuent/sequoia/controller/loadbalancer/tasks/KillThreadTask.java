/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Contributor(s): Julie Marguerite.
 */

package org.continuent.sequoia.controller.loadbalancer.tasks;

import java.sql.SQLException;

import org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This task is used to kill backend worker threads.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite</a>
 * @version 1.0
 */
public class KillThreadTask extends AbstractTask
{

  /**
   * Creates a new <code>KillThreadTask</code> instance that must be executed
   * by <code>nbToComplete</code> backend threads
   * 
   * @param nbToComplete number of threads that must succeed before returning
   * @param totalNb total number of threads
   */
  public KillThreadTask(int nbToComplete, int totalNb)
  {
    super(nbToComplete, totalNb, false, 0);
  }

  /**
   * This function call the backendThread kill function and notifies the task
   * completion success.
   * 
   * @param backendThread the backend thread that will execute the task
   * @throws SQLException if an error occurs
   */
  public void executeTask(BackendWorkerThread backendThread)
      throws SQLException
  {
    backendThread.killWithoutDisablingBackend();
    notifySuccess(backendThread);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#getRequest()
   */
  public AbstractRequest getRequest()
  {
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#getTransactionId()
   */
  public long getTransactionId()
  {
    return 0;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#isAutoCommit()
   */
  public boolean isAutoCommit()
  {
    return true;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "KillThreadTask";
  }
}