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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.loadbalancer.tasks;

import java.sql.Connection;
import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.NoTransactionStartWhenDisablingException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;

/**
 * Executes a <code>StoredProcedure</code> call using
 * CallableStatement.executeQuery() and returns a ResultSet.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class CallableStatementExecuteQueryTask extends AbstractTask
{
  private StoredProcedure     proc;
  private ControllerResultSet result        = null;
  private MetadataCache       metadataCache;

  static Trace                endUserLogger = Trace
                                                .getLogger("org.continuent.sequoia.enduser");

  /**
   * Creates a new <code>CallableStatementExecuteQueryTask</code>.
   * 
   * @param nbToComplete number of threads that must succeed before returning
   * @param totalNb total number of threads
   * @param proc the <code>StoredProcedure</code> to call
   * @param metadataCache the metadataCache if any or null
   */
  public CallableStatementExecuteQueryTask(int nbToComplete, int totalNb,
      StoredProcedure proc, MetadataCache metadataCache)
  {
    super(nbToComplete, totalNb, proc.isPersistentConnection(), proc
        .getPersistentConnectionId());
    this.proc = proc;
    this.metadataCache = metadataCache;
  }

  /**
   * Call a stored procedure that returns a ResultSet on the given backend
   * thread.
   * 
   * @param backendThread the backend thread that will execute the task
   * @throws SQLException if an error occurs
   */
  public void executeTask(BackendWorkerThread backendThread)
      throws SQLException
  {
    DatabaseBackend backend = backendThread.getBackend();

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(proc
          .getLogin());
      if (cm == null)
      {
        SQLException se = new SQLException(
            "No Connection Manager for Virtual Login:" + proc.getLogin());
        try
        {
          notifyFailure(backendThread, -1, se);
        }
        catch (SQLException ignore)
        {

        }
        throw se;
      }

      Trace logger = backendThread.getLogger();
      if (proc.isAutoCommit())
        executeInAutoCommit(backendThread, backend, cm, logger);
      else
        executeInTransaction(backendThread, backend, cm, logger);

      if (result != null)
        notifySuccess(backendThread);
    }
    finally
    {
      backend.getTaskQueues().completeStoredProcedureExecution(this);
    }
  }

  private void executeInAutoCommit(BackendWorkerThread backendThread,
      DatabaseBackend backend, AbstractConnectionManager cm, Trace logger)
      throws SQLException
  {
    if (!backend.canAcceptTasks(proc))
    {
      // Backend is disabling, we do not execute queries except the one in
      // the
      // transaction we already started. Just notify the completion for the
      // others.
      notifyCompletion(backendThread);
      return;
    }

    // Use a connection just for this request
    PooledConnection c = null;
    try
    {
      c = cm.retrieveConnectionInAutoCommit(proc);
    }
    catch (UnreachableBackendException e1)
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
      String msg = Translate.get("loadbalancer.backend.disabling.unreachable",
          backend.getName());
      logger.error(msg);
      endUserLogger.error(msg);
      throw se;
    }

    // Sanity check
    if (c == null)
    {
      SQLException se = new SQLException("No more connections");
      try
      { // All backends failed, just ignore
        if (!notifyFailure(backendThread, proc.getTimeout() * 1000L, se))
          return;
      }
      catch (SQLException ignore)
      {
      }
      // Disable this backend (it is no more in sync) by killing the backend
      // thread
      backendThread.getLoadBalancer().disableBackend(backend, true);
      String msg = "Stored procedure '"
          + proc.getSqlShortForm(backend.getSqlShortFormLength())
          + "' failed on backend " + backend.getName() + " but " + getSuccess()
          + " succeeded (" + se + ")";
      logger.error(msg);
      endUserLogger.error(Translate.get(
          "loadbalancer.backend.disabling", backend.getName()));
      throw new SQLException(msg);
    }

    // Execute Query
    try
    {
      result = AbstractLoadBalancer
          .executeCallableStatementExecuteQueryOnBackend(proc, backend,
              backendThread, c.getConnection(), metadataCache);

      DatabaseProcedureSemantic semantic = proc.getSemantic();
      if ((semantic == null) || semantic.hasDDLWrite())
        backend.setSchemaIsDirty(true, proc);
    }
    catch (Exception e)
    {
      try
      { // All backends failed, just ignore
        if (!notifyFailure(backendThread, proc.getTimeout() * 1000L, e))
        {
          result = null;
          return;
        }
      }
      catch (SQLException ignore)
      {
      }
      // Disable this backend (it is no more in sync) by killing the backend
      // thread
      backendThread.getLoadBalancer().disableBackend(backend, true);
      String msg = "Stored procedure '"
          + proc.getSqlShortForm(backend.getSqlShortFormLength())
          + "' failed on backend " + backend.getName() + " but " + getSuccess()
          + " succeeded (" + e + ")";
      logger.error(msg);
      endUserLogger.error(Translate.get(
          "loadbalancer.backend.disabling", backend.getName()));
      throw new SQLException(msg);
    }
    finally
    {
      cm.releaseConnectionInAutoCommit(proc, c);
    }
  }

  private void executeInTransaction(BackendWorkerThread backendThread,
      DatabaseBackend backend, AbstractConnectionManager cm, Trace logger)
      throws SQLException
  {
    // Re-use the connection used by this transaction
    Connection c;
    long tid = proc.getTransactionId();

    try
    {
      c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(proc, cm);
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
      // Backend is disabling, we do not execute queries except the one in
      // the
      // transaction we already started. Just notify the completion for the
      // others.
      notifyCompletion(backendThread);
      return;
    }
    catch (SQLException e1)
    {
      SQLException se = new SQLException(
          "Unable to get connection for transaction " + tid);
      try
      { // All backends failed, just ignore
        if (!notifyFailure(backendThread, proc.getTimeout() * 1000L, se))
          return;
      }
      catch (SQLException ignore)
      {
      }
      // Disable this backend (it is no more in sync) by killing the
      // backend thread
      backendThread.getLoadBalancer().disableBackend(backend, true);
      String msg = "Request '"
          + proc.getSqlShortForm(backend.getSqlShortFormLength())
          + "' failed on backend " + backend.getName() + " but " + getSuccess()
          + " succeeded (" + se + ")";
      logger.error(msg);
      endUserLogger.error(Translate.get(
          "loadbalancer.backend.disabling", backend.getName()));
      throw new SQLException(msg);
    }

    // Sanity check
    if (c == null)
    { // Bad connection
      SQLException se = new SQLException(
          "Unable to retrieve connection for transaction " + tid);
      try
      { // All backends failed, just ignore
        if (!notifyFailure(backendThread, proc.getTimeout() * 1000L, se))
          return;
      }
      catch (SQLException ignore)
      {
      }
      // Disable this backend (it is no more in sync) by killing the
      // backend thread
      backendThread.getLoadBalancer().disableBackend(backend, true);
      String msg = "Request '"
          + proc.getSqlShortForm(backend.getSqlShortFormLength())
          + "' failed on backend " + backend.getName() + " but " + getSuccess()
          + " succeeded (" + se + ")";
      logger.error(msg);
      endUserLogger.error(Translate.get(
          "loadbalancer.backend.disabling", backend.getName()));
      throw new SQLException(msg);
    }

    // Execute Query
    try
    {
      result = AbstractLoadBalancer
          .executeCallableStatementExecuteQueryOnBackend(proc, backend,
              backendThread, c, metadataCache);

      DatabaseProcedureSemantic semantic = proc.getSemantic();
      if ((semantic == null) || semantic.hasDDLWrite())
        backend.setSchemaIsDirty(true, proc);
    }
    catch (Exception e)
    {
      try
      { // All backends failed, just ignore
        if (!notifyFailure(backendThread, proc.getTimeout() * 1000L, e))
        {
          result = null;
          return;
        }
      }
      catch (SQLException ignore)
      {
      }
      // Disable this backend (it is no more in sync) by killing the backend
      // thread
      backendThread.getLoadBalancer().disableBackend(backend, true);
      String msg = "Stored procedure '"
          + proc.getSqlShortForm(backend.getSqlShortFormLength())
          + "' failed on backend " + backend.getName() + " but " + getSuccess()
          + " succeeded (" + e + ")";
      logger.error(msg);
      endUserLogger.error(Translate.get(
          "loadbalancer.backend.disabling", backend.getName()));
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#getRequest()
   */
  public AbstractRequest getRequest()
  {
    return proc;
  }

  /**
   * Returns the result.
   * 
   * @return a <code>ResultSet</code>
   */
  public ControllerResultSet getResult()
  {
    return result;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#getTransactionId()
   */
  public long getTransactionId()
  {
    return proc.getTransactionId();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask#isAutoCommit()
   */
  public boolean isAutoCommit()
  {
    return proc.isAutoCommit();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object other)
  {
    if ((other == null)
        || !(other instanceof CallableStatementExecuteQueryTask))
      return false;

    CallableStatementExecuteQueryTask cseqt = (CallableStatementExecuteQueryTask) other;
    if (proc == null)
      return cseqt.getRequest() == null;
    return proc.equals(cseqt.getRequest());
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return (int) proc.getId();
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    if (proc.isAutoCommit())
      return "Autocommit CallableStatementExecuteQueryTask "
          + proc.getTransactionId() + " (" + proc.getUniqueKey() + ")";
    else
      return "CallableStatementExecuteQueryTask for transaction "
          + proc.getTransactionId() + " (" + proc.getUniqueKey() + ")";
  }

}