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
 * Contributor(s): Julie Marguerite, Stephane Giron.
 */

package org.continuent.sequoia.controller.loadbalancer.tasks;

import java.sql.Connection;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;

/**
 * Task to rollback a transaction.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Stephane.Giron@continuent.com">Stephane Giron </a>
 * @version 1.0
 */
public class RollbackTask extends AbstractTask
{
  // Transaction metadata (login, transaction id, timeout)
  private TransactionMetaData tm;

  static Trace                endUserLogger = Trace
                                                .getLogger("org.continuent.sequoia.enduser");

  /**
   * Commits a transaction given a login and a transaction id.
   * 
   * @param nbToComplete number of threads that must succeed before returning
   * @param totalNb total number of threads
   * @param tm transaction metadata
   * @throws NullPointerException if tm is null
   */
  public RollbackTask(int nbToComplete, int totalNb, TransactionMetaData tm)
      throws NullPointerException
  {
    super(nbToComplete, totalNb, tm.isPersistentConnection(), tm
        .getPersistentConnectionId());
    this.tm = tm;
  }

  /**
   * Rollbacks a transaction with the given backend thread.
   * 
   * @param backendThread the backend thread that will execute the task
   * @throws SQLException if an error occurs
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

    PooledConnection pc = null;

    if (backend.isStartedTransaction(lTid))
    {
      pc = cm.retrieveConnectionForTransaction(tm.getTransactionId());
      // Sanity check
      if (pc == null)
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
        String msg = "Failed to rollback transaction " + tm.getTransactionId()
            + " on backend " + backend.getName() + " but " + getSuccess()
            + " succeeded (" + se + ")";
        backendThread.getLogger().error(msg);
        endUserLogger.error(Translate.get("loadbalancer.backend.disabling",
            backend.getName()));
        throw new SQLException(msg);
      }
    }
    else
    {
      // When a rollback is done on a non started transaction, then there is
      // nothing to rollback on the backend. However, we have to release locks
      // in order to unblock from a deadlock situation, for example.
      backend.getTaskQueues().releaseLocksAndCheckForPriorityInversion(tm);
      notifySuccess(backendThread);
      return;
    }

    // The intent with this sync block is to cope with the specific
    // situation where two RollbackTasks for the same transaction are executing
    // in parallel.
    synchronized (pc)
    {
      if (backend.isStartedTransaction(lTid))
      {
        try
        {
          Connection c = pc.getConnection();
          c.rollback();
          if (tm.altersDatabaseSchema())
          { // Flag the schema as dirty in case the transaction contained a DDL
            UnknownWriteRequest fakeRequest = new UnknownWriteRequest(
                "rollback " + getTransactionId(), false, 0, null);
            fakeRequest.setLogin(tm.getLogin());
            fakeRequest.setIsAutoCommit(false);
            fakeRequest.setTransactionId(getTransactionId());
            fakeRequest.setPersistentConnection(isPersistentConnection());
            fakeRequest.setPersistentConnectionId(getPersistentConnectionId());
            backendThread.getBackend().setSchemaIsDirty(true, fakeRequest);
          }
          c.setAutoCommit(true);
        }
        catch (Exception e)
        {
          try
          {
            if (!notifyFailure(backendThread, tm.getTimeout(),
                new SQLException(e.getMessage())))
              return;
          }
          catch (SQLException ignore)
          {
          }
          // Disable this backend (it is no more in sync) by killing the backend
          // thread
          backendThread.getLoadBalancer().disableBackend(backend, true);
          String msg = "Failed to rollback transaction "
              + tm.getTransactionId() + " on backend " + backend.getName()
              + " but " + getSuccess() + " succeeded (" + e + ")";
          backendThread.getLogger().error(msg);
          endUserLogger.error(Translate.get("loadbalancer.backend.disabling",
              backend.getName()));
          throw new SQLException(msg);
        }
        finally
        {
          cm.releaseConnectionForTransaction(tm.getTransactionId());
          backend.stopTransaction(lTid);
          backend.getTaskQueues().releaseLocksAndCheckForPriorityInversion(tm);
        }
      }
      else
      {
        if (backendThread.getLogger().isWarnEnabled())
          backendThread
              .getLogger()
              .warn(
                  "Transaction "
                      + lTid
                      + " not found when trying to rollback (probably because of a concurrent rollback)");
      }
      notifySuccess(backendThread);
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
    if ((other == null) || !(other instanceof RollbackTask))
      return false;

    RollbackTask rollback = (RollbackTask) other;
    return this.getTransactionId() == rollback.getTransactionId();
  }

  /**
   * Returns the transaction metadata value.
   * 
   * @return Returns the tm.
   */
  public final TransactionMetaData getTransactionMetaData()
  {
    return tm;
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
    return "RollbackTask (" + tm.getTransactionId() + ")";
  }
}