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
 * Contributor(s): Julie Marguerite.
 */

package org.continuent.sequoia.controller.loadbalancer.tasks;

import java.sql.Connection;
import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.NoTransactionStartWhenDisablingException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.UnknownReadRequest;

/**
 * Task to begin a transaction. Note that this task does not properly set the
 * transaction isolation but this is not a real issue since it is meant to be
 * used by the recovery log that does not execute reads and provide its own
 * serial order.
 * <p>
 * <strong>This task is meant to be only used to replay the recovery log</strong>
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @version 1.0
 */
public class BeginTask extends AbstractTask
{
  // Transaction metadata (login, transaction id, timeout)
  private TransactionMetaData tm;
  private AbstractRequest     request;
  static Trace                endUserLogger = Trace
                                                .getLogger("org.continuent.sequoia.enduser");

  /**
   * Begins a new transaction given a login and a transaction id.
   * 
   * @param nbToComplete number of threads that must succeed before returning
   * @param totalNb total number of threads
   * @param tm transaction metadata
   * @throws NullPointerException if tm is null
   */
  public BeginTask(int nbToComplete, int totalNb, TransactionMetaData tm)
      throws NullPointerException
  {
    super(nbToComplete, totalNb, tm.isPersistentConnection(), tm
        .getPersistentConnectionId());
    this.tm = tm;
    this.request = new UnknownReadRequest("begin", false, 0, "");
    request.setLogin(tm.getLogin());
    request.setIsAutoCommit(false);
    request.setTransactionId(tm.getTransactionId());
    request.setPersistentConnection(tm.isPersistentConnection());
    request.setPersistentConnectionId(tm.getPersistentConnectionId());
    request
        .setTransactionIsolation(org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL);
  }

  /**
   * Begins a new transaction with the given backend thread.
   * 
   * @param backendThread the backend thread that will execute the task
   * @exception SQLException if an error occurs
   */
  public void executeTask(BackendWorkerThread backendThread)
      throws SQLException
  {
    DatabaseBackend backend = backendThread.getBackend();
    if (!backend.isReplaying())
      throw new SQLException(
          "BeginTask should only be used for recovery purposes. Detected incorrect backend state:"
              + backend.getState());

    try
    {
      AbstractConnectionManager cm = backend
          .getConnectionManager(tm.getLogin());
      if (cm == null)
      {
        SQLException se = new SQLException(
            "No Connection Manager for Virtual Login:" + tm.getLogin());
        try
        {
          notifyFailure(backendThread, 1, se);
        }
        catch (SQLException ignore)
        {

        }
        throw se;
      }

      Connection c;
      Long lTid = new Long(tm.getTransactionId());
      Trace logger = backendThread.getLogger();

      try
      {
        c = AbstractLoadBalancer.getConnectionAndBeginTransaction(backend, cm,
            request);
        backend.startTransaction(lTid);
      }
      catch (UnreachableBackendException ube)
      {
        SQLException se = new SQLException("Backend " + backend.getName()
            + " is no more reachable.");
        try
        {
          notifyFailure(backendThread, -1, se);
        }
        catch (SQLException ignore)
        {
        }
        // Disable this backend (it is no more in sync) by killing the backend
        // thread
        backendThread.getLoadBalancer().disableBackend(backend, true);
        String msg = Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName());
        logger.error(msg);
        endUserLogger.error(msg);
        throw se;
      }
      catch (NoTransactionStartWhenDisablingException e)
      {
        // Backend is disabling, we do not execute queries except the one in the
        // transaction we already started. Just notify the completion for the
        // others.
        notifyCompletion(backendThread);
        return;
      }
      catch (SQLException e1)
      {
        SQLException se = new SQLException(
            "Unable to get connection for transaction " + lTid);
        try
        { // All backends failed, just ignore
          if (!notifyFailure(backendThread, tm.getTimeout(), se))
            return;
        }
        catch (SQLException ignore)
        {
        }
        // Disable this backend (it is no more in sync) by killing the
        // backend thread
        backendThread.getLoadBalancer().disableBackend(backend, true);
        String msg = "Begin of transaction " + tm.getTransactionId()
            + " failed on backend " + backend.getName() + " but "
            + getSuccess() + " succeeded (" + se + ")";
        logger.error(msg);
        endUserLogger.error(Translate.get(
            "loadbalancer.backend.disabling", backend.getName()));
        throw new SQLException(msg);
      }

      // Sanity check
      if (c == null)
      { // Bad connection
        SQLException se = new SQLException(
            "No more connection to start a new transaction.");
        try
        { // All backends failed, just ignore
          if (!notifyFailure(backendThread, tm.getTimeout(), se))
            return;
        }
        catch (SQLException ignore)
        {
        }
      }
      else
      {
        notifySuccess(backendThread);
      }
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
      String msg = "Failed to begin transaction " + tm.getTransactionId()
          + " on backend " + backend.getName() + " (" + e + ")";
      backendThread.getLogger().error(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#getRequest()
   */
  public AbstractRequest getRequest()
  {
    return request;
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
    if ((other == null) || !(other instanceof BeginTask))
      return false;

    BeginTask begin = (BeginTask) other;
    return this.getTransactionId() == begin.getTransactionId();
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
    return "BeginTask (" + tm.getLogin() + "," + tm.getTransactionId() + ")";
  }

}