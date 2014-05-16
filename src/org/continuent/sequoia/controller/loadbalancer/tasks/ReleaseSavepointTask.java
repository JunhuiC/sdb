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
 * Initial developer(s): Jean-Bernard van Zuylen.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.loadbalancer.tasks;

import java.sql.SQLException;
import java.sql.Savepoint;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * Task to remove a savepoint from a transaction.
 * 
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public class ReleaseSavepointTask extends AbstractTask
{
  /** Transaction metadata (login, transaction id, timeout) */
  private TransactionMetaData tm;
  /** Name of the savepoint. */
  private String              savepointName;

  static Trace                endUserLogger = Trace
                                                .getLogger("org.continuent.sequoia.enduser");

  /**
   * Creates a new <code>ReleaseSavepointTask</code> object
   * 
   * @param nbToComplete number of threads that must succeed before returning
   * @param totalNb total number of threads
   * @param tm transaction metadata
   * @param savepointName the savepoint to remove
   * @throws NullPointerException if tm is null
   */
  public ReleaseSavepointTask(int nbToComplete, int totalNb,
      TransactionMetaData tm, String savepointName) throws NullPointerException
  {
    super(nbToComplete, totalNb, tm.isPersistentConnection(), tm
        .getPersistentConnectionId());
    if (tm == null)
      throw new NullPointerException("Unexpected null metadata in BeginTask");
    this.tm = tm;
    this.savepointName = savepointName;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#executeTask(org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread)
   */
  public void executeTask(BackendWorkerThread backendThread)
      throws SQLException
  {
    DatabaseBackend backend = backendThread.getBackend();
    Long lTid = new Long(tm.getTransactionId());

    AbstractConnectionManager cm = backend.getConnectionManager(tm.getLogin());
    if (cm == null)
    {
      SQLException se = new SQLException(
          "No Connection Manager for Virtual Login:" + tm.getLogin());
      try
      {
        notifyFailure(backendThread, -1, se);
      }
      catch (SQLException ignore)
      {

      }
      throw se;
    }
    PooledConnection c = cm.retrieveConnectionForTransaction(tm
        .getTransactionId());

    // Sanity check
    if (c == null)
    { // Bad connection
      backend.stopTransaction(lTid);
      SQLException se = new SQLException(
          "Unable to retrieve connection for transaction "
              + tm.getTransactionId());

      try
      { // All backends failed, just ignore
        if (!notifyFailure(backendThread, tm.getTimeout(), se))
          return;
      }
      catch (SQLException ignore)
      {
      }
      // Disable this backend (it is no more in sync) by killing the backend
      // thread
      backendThread.getLoadBalancer().disableBackend(backend, true);
      String msg = "Failed to release savepoint for transaction "
          + tm.getTransactionId() + " on backend " + backend.getName()
          + " but " + getSuccess() + " succeeded (" + se + ")";
      backendThread.getLogger().error(msg);
      endUserLogger.error(Translate.get("loadbalancer.backend.disabling",
          backend.getName()));
      throw new SQLException(msg);
    }

    // Execute Query
    Savepoint savepoint = null;
    try
    {
      savepoint = backend.getSavepoint(lTid, savepointName);
      c.getConnection().releaseSavepoint(savepoint);
    }
    catch (Exception e)
    {
      try
      {
        if (!notifyFailure(backendThread, tm.getTimeout(), new SQLException(e
            .getMessage())))
          return;
      }
      catch (SQLException ignore)
      {
      }
      // Disable this backend (it is no more in sync) by killing the backend
      // thread
      backendThread.getLoadBalancer().disableBackend(backend, true);
      String msg = "Failed to release savepoint for transaction "
          + tm.getTransactionId() + " on backend " + backend.getName()
          + " but " + getSuccess() + " succeeded (" + e + ")";
      backendThread.getLogger().error(msg);
      endUserLogger.error(Translate.get("loadbalancer.backend.disabling",
          backend.getName()));
      throw new SQLException(msg);
    }
    finally
    {
      backend.removeSavepoint(lTid, savepoint);
    }

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
   * @return savepoint name
   */
  public String getSavepointName()
  {
    return savepointName;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#getTransactionId()
   */
  public long getTransactionId()
  {
    return tm.getTransactionId();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#isAutoCommit()
   */
  public boolean isAutoCommit()
  {
    return false;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof ReleaseSavepointTask))
      return false;

    ReleaseSavepointTask releaseSavepoint = (ReleaseSavepointTask) other;
    return (this.getTransactionId() == releaseSavepoint.getTransactionId())
        && (this.savepointName.equals(releaseSavepoint.getSavepointName()));
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return (int) this.getTransactionId();
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "ReleaseSavepointTask for transaction " + tm.getTransactionId()
        + " (" + savepointName + ")";
  }
}
