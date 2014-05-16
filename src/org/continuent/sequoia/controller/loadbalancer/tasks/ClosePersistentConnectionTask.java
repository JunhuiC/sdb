/**
 * Sequoia: Database clustering technology.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.loadbalancer.tasks;

import java.sql.SQLException;

import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This class defines a ClosePersistentConnectionTask that closes a persistent
 * connection.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ClosePersistentConnectionTask extends AbstractTask
{
  private String login;
  private long   persistentConnectionId;

  /**
   * Creates a new <code>ClosePersistentConnectionTask</code> object
   * 
   * @param nbToComplete number of threads that must succeed before returning
   * @param totalNb total number of threads
   * @param login login requesting the connection closing
   * @param persistentConnectionId id of the persistent connection to close
   */
  public ClosePersistentConnectionTask(int nbToComplete, int totalNb,
      String login, long persistentConnectionId)
  {
    super(nbToComplete, totalNb, true, persistentConnectionId);
    this.login = login;
    this.persistentConnectionId = persistentConnectionId;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#executeTask(org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread)
   */
  public void executeTask(BackendWorkerThread backendThread)
      throws SQLException
  {
    DatabaseBackend backend = backendThread.getBackend();
    AbstractConnectionManager cm = backend.getConnectionManager(login);
    if (cm == null)
    {
      // Nothing to do, no such connection
      notifyCompletion(backendThread);
      return;
    }

    try
    {
      if (backend.canAcceptTasks(persistentConnectionId))
      {
        // Release the connection
        cm.releasePersistentConnectionInAutoCommit(persistentConnectionId);
        backend.removePersistentConnection(persistentConnectionId);
        notifySuccess(backendThread);
      }
      else
        notifyCompletion(backendThread);
    }
    catch (Exception e)
    {
      notifyFailure(backendThread, -1, e);
    }
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
    return persistentConnectionId;
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
    return "Close persistent connection " + persistentConnectionId;
  }

}
