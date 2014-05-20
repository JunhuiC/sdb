/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2007 Continuent, Inc.
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
 * Initial developer(s): Emmanuel Cecchet
 * Contributor(s): Stephane Giron
 */

package org.continuent.sequoia.controller.loadbalancer.raidb1;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;

import org.continuent.sequoia.common.exceptions.BadConnectionException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoTransactionStartWhenDisablingException;
import org.continuent.sequoia.common.exceptions.SQLExceptionFactory;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.AutocommitConnectionResource;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueues;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueuesControl;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CallableStatementExecuteQueryTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CallableStatementExecuteTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CallableStatementExecuteUpdateTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.ClosePersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CommitTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.OpenPersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.ReleaseSavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackToSavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.SavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.StatementExecuteQueryTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.StatementExecuteTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.StatementExecuteUpdateTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.StatementExecuteUpdateWithKeysTask;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedClosePersistentConnection;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCommit;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedOpenPersistentConnection;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedReleaseSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollback;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollbackToSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedSetSavepoint;

/**
 * RAIDb-1 load balancer.
 * <p>
 * This class is an abstract call because the read requests coming from the
 * request controller are NOT treated here but in the subclasses. Transaction
 * management and write requests are broadcasted to all backends.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Stephane.Giron@continuent.com">Stephane Giron </a>
 * @version 1.0
 */
public abstract class RAIDb1 extends AbstractLoadBalancer
{
  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Transaction handling
  // 5. Backend management
  //

  protected static Trace logger        = Trace
                                           .getLogger("org.continuent.sequoia.controller.loadbalancer.RAIDb1");

  protected static Trace endUserLogger = Trace
                                           .getLogger("org.continuent.sequoia.enduser");

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-1 Round Robin request load balancer. A new backend
   * worker thread is created for each backend.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy How many backends must complete before
   *          returning the result?
   * @exception Exception if an error occurs
   */
  public RAIDb1(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy) throws Exception
  {
    super(vdb, RAIDbLevels.RAIDb1, ParsingGranularities.TABLE);
    this.waitForCompletionPolicy = waitForCompletionPolicy;
  }

  /*
   * Request Handling
   */

  /**
   * Perform a read request. If request.isMustBroadcast() is set, then the query
   * is broadcasted to all nodes else a single node is chosen according to the
   * load balancing policy.
   * 
   * @param request the <code>SelectRequest</code> to execute
   * @param metadataCache MetadataCache (null if none)
   * @return the corresponding <code>ControllerResultSet</code>
   * @exception SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request,
      MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException
  {
    if (request.isMustBroadcast())
      return execBroadcastReadRequest(request, metadataCache);
    else
      return execSingleBackendReadRequest(request, metadataCache);
  }

  /**
   * Implementation specific execution of a request on a single backend chosen
   * according to the load balancing strategy.
   * 
   * @param request the request to execute
   * @param metadataCache the metadata cache if any or null
   * @return the ResultSet
   * @throws SQLException if an error occurs
   */
  public abstract ControllerResultSet execSingleBackendReadRequest(
      SelectRequest request, MetadataCache metadataCache) throws SQLException;

  /**
   * Broadcast a read request execution on all backends. This is similar to a
   * write execution and is useful for queries such as SELECT ... FOR UPDATE.
   * 
   * @param request the <code>SelectRequest</code> to execute
   * @param metadataCache MetadataCache (null if none)
   * @return the corresponding <code>ControllerResultSet</code>
   * @exception SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws NoMoreBackendException if no backend was available to execute the
   *           stored procedure
   */
  private ControllerResultSet execBroadcastReadRequest(SelectRequest request,
      MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException, NoMoreBackendException
  {
    // Handle macros
    handleMacros(request);

    // Total ordering for distributed virtual databases.
    boolean removeFromTotalOrderQueue = waitForTotalOrder(request, true);

    // Log lazy begin if needed
    if (request.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          request.getTransactionId());

    // Log request
    if (recoveryLog != null)
      request.setLogId(recoveryLog.logRequestExecuting(request));

    int nbOfThreads = acquireLockAndCheckNbOfThreads(request, String
        .valueOf(request.getId()));

    // Create the task and just wait for the first node to return
    StatementExecuteQueryTask task = new StatementExecuteQueryTask(1,
        nbOfThreads, request, metadataCache);

    // Selects are always posted in the non conflicting queue
    atomicTaskPostInQueueAndReleaseLock(request, task, nbOfThreads,
        removeFromTotalOrderQueue);

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(request.getTimeout() * 1000L, String
            .valueOf(request.getId()), task);

      checkTaskCompletion(task);
    }

    // Update log with success
    if (recoveryLog != null)
      recoveryLog.logRequestCompletion(request.getLogId(), true, request
          .getExecTimeInMs());

    return task.getResult();
  }

  /**
   * Performs a write request. This request is broadcasted to all nodes.
   * 
   * @param request an <code>AbstractWriteRequest</code>
   * @return number of rows affected by the request
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @exception NoMoreBackendException if no backends are left to execute the
   *              request
   * @exception SQLException if an error occurs
   */
  public ExecuteUpdateResult statementExecuteUpdate(AbstractWriteRequest request)
      throws AllBackendsFailedException, NoMoreBackendException, SQLException
  {
    return ((StatementExecuteUpdateTask) execWriteRequest(request, false, null))
        .getResult();
  }

  /**
   * Perform a write request and return the auto generated keys.
   * 
   * @param request the request to execute
   * @param metadataCache the metadataCache if any or null
   * @return update count and auto generated keys.
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @exception NoMoreBackendException if no backends are left to execute the
   *              request
   * @exception SQLException if an error occurs
   */
  public GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request, MetadataCache metadataCache)
      throws AllBackendsFailedException, NoMoreBackendException, SQLException
  {
    return ((StatementExecuteUpdateWithKeysTask) execWriteRequest(request,
        true, metadataCache)).getResult();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#statementExecute(AbstractRequest,
   *      MetadataCache)
   */
  public ExecuteResult statementExecute(AbstractRequest request,
      MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException
  {
    StatementExecuteTask task = (StatementExecuteTask) callStoredProcedure(
        request, STATEMENT_EXECUTE_TASK, metadataCache);
    return task.getResult();
  }

  /**
   * Common code for execWriteRequest(AbstractWriteRequest) and
   * execWriteRequestWithKeys(AbstractWriteRequest).
   * <p>
   * Note that macros are processed here.
   * <p>
   * The result is given back in AbstractTask.getResult().
   * 
   * @param request the request to execute
   * @param useKeys true if this must give an auto generated keys ResultSet
   * @param metadataCache the metadataCache if any or null
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  private AbstractTask execWriteRequest(AbstractWriteRequest request,
      boolean useKeys, MetadataCache metadataCache)
      throws AllBackendsFailedException, NoMoreBackendException, SQLException
  {
    // Handle macros
    handleMacros(request);

    // Total ordering mainly for distributed virtual databases.
    boolean removeFromTotalOrderQueue = waitForTotalOrder(request, true);

    // Log lazy begin if needed
    if (request.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          request.getTransactionId());

    // Log request
    if (recoveryLog != null)
      recoveryLog.logRequestExecuting(request);

    int nbOfThreads = acquireLockAndCheckNbOfThreads(request, String
        .valueOf(request.getId()));

    // Create the task
    AbstractTask task;
    if (useKeys)
      task = new StatementExecuteUpdateWithKeysTask(getNbToWait(nbOfThreads),
          nbOfThreads, request, metadataCache);
    else
      task = new StatementExecuteUpdateTask(getNbToWait(nbOfThreads),
          nbOfThreads, request);

    atomicTaskPostInQueueAndReleaseLock(request, task, nbOfThreads,
        removeFromTotalOrderQueue);

    try
    {
      synchronized (task)
      {
        if (!task.hasCompleted())
          waitForTaskCompletion(request.getTimeout() * 1000L, String
              .valueOf(request.getId()), task);

        checkTaskCompletion(task);
        return task;
      }
    }
    finally
    {
      if (!request.isAutoCommit())
      { // Check that transaction was not aborted in parallel
        try
        {
          this.vdb.getRequestManager().getTransactionMetaData(
              new Long(request.getTransactionId()));
        }
        catch (SQLException e)
        { // Transaction was aborted it cannot be found anymore in the active
          // transaction list. Force an abort
          enforceTransactionAbort(request);
        }
      }
    }
  }

  private void enforceTransactionAbort(AbstractRequest request)
      throws SQLException
  {
    logger.info("Concurrent abort detected, re-enforcing abort of transaction "
        + request.getTransactionId());

    TransactionMetaData transactionMetaData = new TransactionMetaData(request
        .getTransactionId(), 0, request.getLogin(), request
        .isPersistentConnection(), request.getPersistentConnectionId());
    try
    {
      abort(transactionMetaData);
    }
    finally
    {
      // We should log the completion of the previous abort, otherwise it
      // will left an inconsistent recovery log, making the recovery
      // process hang, if done from a dump taken prior to this rollback.
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(transactionMetaData.getLogId(), true,
            0);
    }
  }

  protected static final int STATEMENT_EXECUTE_QUERY          = 0;
  protected static final int CALLABLE_STATEMENT_EXECUTE_QUERY = 1;
  protected static final int CALLABLE_STATEMENT_EXECUTE       = 2;

  /**
   * Execute a read request on the selected backend.
   * 
   * @param request the request to execute
   * @param backend the backend that will execute the request
   * @param metadataCache the metadataCache if any or null
   * @return the ResultSet
   * @throws SQLException if an error occurs
   */
  protected ControllerResultSet executeRequestOnBackend(SelectRequest request,
      DatabaseBackend backend, MetadataCache metadataCache)
      throws SQLException, UnreachableBackendException
  {
    // Handle macros
    handleMacros(request);

    // In case of asynchronous execution, we must make sure that the read does
    // not execute ahead of previous writes. (see SEQUOIA-955)
    if ((waitForCompletionPolicy.getPolicy() != WaitForCompletionPolicy.ALL)
        && !backend.getTaskQueues().hasNoTaskInQueues())
    { // There are late queries on that backend. Post the query in the backend
      // queue for proper serializable execution.

      StatementExecuteQueryTask task = new StatementExecuteQueryTask(1, 1,
          request, metadataCache);
      backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);

      synchronized (task)
      {
        if (!task.hasCompleted())
          waitForTaskCompletion(request.getTimeout() * 1000L, String
              .valueOf(request.getId()), task);

        try
        {
          checkTaskCompletion(task);
        }
        catch (AllBackendsFailedException e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }

      return task.getResult();
    }

    // The backend is ready to execute here, let's go.

    // Ok, we have a backend, let's execute the request
    AbstractConnectionManager cm = backend.getConnectionManager(request
        .getLogin());

    // Sanity check
    if (cm == null)
    {
      String msg = Translate.get("loadbalancer.connectionmanager.not.found",
          new String[]{request.getLogin(), backend.getName()});
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Execute the query
    if (request.isAutoCommit())
    {
      ControllerResultSet rs = null;
      boolean badConnection;
      do
      {
        badConnection = false;
        // Use a connection just for this request
        PooledConnection c = null;
        try
        {
          c = cm.retrieveConnectionInAutoCommit(request);
        }
        catch (UnreachableBackendException e1)
        {
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName());
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          throw new UnreachableBackendException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }

        // Sanity check
        if (c == null)
          throw new UnreachableBackendException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        try
        {
          rs = executeStatementExecuteQueryOnBackend(request, backend, null, c
              .getConnection(), metadataCache);

          // Put the connection into the result set, which decides when to free
          // it.
          AutocommitConnectionResource resource = new AutocommitConnectionResource(
              request, c, cm);
          rs.saveConnectionResource(resource);
        }
        catch (SQLException e)
        {
          cm.releaseConnectionInAutoCommit(request, c);
          throw SQLExceptionFactory.getSQLException(e, Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        catch (BadConnectionException e)
        { // Get rid of the bad connection
          cm.deleteConnection(c);
          if (request.isPersistentConnection())
          {
            cm.deletePersistentConnection(request.getPersistentConnectionId());
          }
          badConnection = true;
        }
        catch (UnreachableBackendException e)
        {
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName());
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          throw new UnreachableBackendException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }
        catch (Throwable e)
        {

          logger.error("Unexpected exception:", e);
          cm.releaseConnectionInAutoCommit(request, c);
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
      while (badConnection);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.on", new String[]{
            String.valueOf(request.getId()), backend.getName()}));
      return rs;
    }
    else
    { // Inside a transaction
      Connection c;
      long tid = request.getTransactionId();

      try
      {
        c = backend
            .getConnectionForTransactionAndLazyBeginIfNeeded(request, cm);
      }
      catch (UnreachableBackendException e1)
      {
        String msg = Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw new UnreachableBackendException(Translate.get(
            "loadbalancer.backend.unreacheable", backend.getName()));
      }
      catch (NoTransactionStartWhenDisablingException e)
      {
        String msg = Translate.get("loadbalancer.backend.is.disabling",
            new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName()});
        logger.error(msg);
        throw new UnreachableBackendException(msg);
      }

      // Sanity check
      if (c == null)
        throw new SQLException(Translate.get(
            "loadbalancer.unable.retrieve.connection", new String[]{
                String.valueOf(tid), backend.getName()}));

      // Execute Query
      ControllerResultSet rs = null;
      try
      {
        rs = executeStatementExecuteQueryOnBackend(request, backend, null, c,
            metadataCache);
      }
      catch (SQLException e)
      {
        throw SQLExceptionFactory.getSQLException(e, Translate.get(
            "loadbalancer.request.failed.on.backend", new String[]{
                request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
      catch (BadConnectionException e)
      {
        logger.error("Got an exception while executing request "
            + request.getId() + " in transaction " + request.getTransactionId()
            + "(" + request.getSqlShortForm(75) + ") : " + e);

        // Connection failed, so did the transaction
        // Disable the backend.
        cm.deleteConnection(tid);
        String msg = Translate.get(
            "loadbalancer.backend.disabling.connection.failure", backend
                .getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw new UnreachableBackendException(msg);
      }
      catch (UnreachableBackendException e)
      {
        String msg = Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw e;
      }
      catch (Throwable e)
      {
        logger.error("Unexpected exception:", e);
        throw new SQLException(Translate.get(
            "loadbalancer.request.failed.on.backend", new String[]{
                request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(request.getId()),
                backend.getName()}));
      return rs;
    }
  }

  /**
   * Execute a stored procedure on the selected backend.
   * 
   * @param proc the stored procedure to execute
   * @param isExecuteQuery true if we must call CallableStatement.executeQuery,
   *          false if we must call CallableStatement.execute()
   * @param backend the backend that will execute the request
   * @param metadataCache the metadataCache if any or null
   * @return a <code>ControllerResultSet</code> if isExecuteQuery is true, an
   *         <code>ExecuteResult</code> object otherwise
   * @throws SQLException if an error occurs
   */
  protected Object executeStoredProcedureOnBackend(StoredProcedure proc,
      boolean isExecuteQuery, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException,
      UnreachableBackendException
  {
    // Ok, we have a backend, let's execute the request
    AbstractConnectionManager cm = backend
        .getConnectionManager(proc.getLogin());

    // Sanity check
    if (cm == null)
    {
      String msg = Translate.get("loadbalancer.connectionmanager.not.found",
          new String[]{proc.getLogin(), backend.getName()});
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Execute the query
    if (proc.isAutoCommit())
    {
      Object result = null;
      boolean badConnection;
      PooledConnection c = null;
      do
      {
        badConnection = false;
        PooledConnection previousConnection = c;
        // Use a connection just for this request
        try
        {
          c = cm.retrieveConnectionInAutoCommit(proc);
        }
        catch (UnreachableBackendException e1)
        {
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName());
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          throw new UnreachableBackendException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }

        // Sanity check
        if (c == null || c == previousConnection)
          throw new UnreachableBackendException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        try
        {
          if (isExecuteQuery)
            result = AbstractLoadBalancer
                .executeCallableStatementExecuteQueryOnBackend(proc, backend,
                    null, c.getConnection(), metadataCache);
          else
            result = AbstractLoadBalancer
                .executeCallableStatementExecuteOnBackend(proc, backend, null,
                    c, metadataCache);
        }
        catch (BadConnectionException e)
        { // Get rid of the bad connection
          cm.deleteConnection(c);
          if (proc.isPersistentConnection())
            cm.deletePersistentConnection(proc.getPersistentConnectionId());
          badConnection = true;
        }
        catch (Throwable e)
        {
          logger.error("Unexpected exception:", e);
          throw new SQLException(Translate.get(
              "loadbalancer.storedprocedure.failed.on.backend", new String[]{
                  proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        finally
        {
          cm.releaseConnectionInAutoCommit(proc, c);
        }
      }
      while (badConnection);

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.storedprocedure.on",
            new String[]{String.valueOf(proc.getId()), backend.getName()}));

      return result;
    }
    else
    { // Inside a transaction
      Connection c;
      long tid = proc.getTransactionId();

      try
      {
        c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(proc, cm);
      }
      catch (UnreachableBackendException e)
      {
        // intercept the UBE to disable the unreachable backend
        // and propagate the exception
        endUserLogger.error(Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName()));
        disableBackend(backend, true);
        throw e;
      }
      catch (NoTransactionStartWhenDisablingException e)
      {
        String msg = Translate.get("loadbalancer.backend.is.disabling",
            new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName()});
        logger.error(msg);
        throw new UnreachableBackendException(msg);
      }

      // Sanity check
      if (c == null)
        throw new SQLException(Translate.get(
            "loadbalancer.unable.retrieve.connection", new String[]{
                String.valueOf(tid), backend.getName()}));

      // Execute Query
      try
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("loadbalancer.execute.transaction.on",
              new String[]{String.valueOf(tid), String.valueOf(proc.getId()),
                  backend.getName()}));
        if (isExecuteQuery)
          return AbstractLoadBalancer
              .executeCallableStatementExecuteQueryOnBackend(proc, backend,
                  null, c, metadataCache);
        else
          return AbstractLoadBalancer.executeCallableStatementExecuteOnBackend(
              proc, backend, null, cm.retrieveConnectionForTransaction(tid),
              metadataCache);
      }
      catch (BadConnectionException e)
      {
        logger.error("Got an exception while executing request " + proc.getId()
            + " in transaction " + proc.getTransactionId() + "("
            + proc.getSqlShortForm(75) + ") : " + e);

        // Connection failed, so did the transaction
        // Disable the backend.
        cm.deleteConnection(tid);
        String msg = Translate.get(
            "loadbalancer.backend.disabling.connection.failure", backend
                .getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw new UnreachableBackendException(msg);
      }
      catch (Throwable e)
      {
        logger.error("Unexpected exception:", e);
        throw new SQLException(Translate.get(
            "loadbalancer.storedprocedure.failed.on.backend", new String[]{
                proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet callableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException
  {
    CallableStatementExecuteQueryTask task = (CallableStatementExecuteQueryTask) callStoredProcedure(
        proc, EXECUTE_QUERY_TASK, metadataCache);
    return task.getResult();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecuteUpdate(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public ExecuteUpdateResult callableStatementExecuteUpdate(StoredProcedure proc)
      throws SQLException, AllBackendsFailedException
  {
    CallableStatementExecuteUpdateTask task = (CallableStatementExecuteUpdateTask) callStoredProcedure(
        proc, EXECUTE_UPDATE_TASK, null);
    return task.getResult();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecute(StoredProcedure,
   *      MetadataCache)
   */
  public ExecuteResult callableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException
  {
    CallableStatementExecuteTask task = (CallableStatementExecuteTask) callStoredProcedure(
        proc, CALLABLE_EXECUTE_TASK, metadataCache);
    return task.getResult();
  }

  private static final int EXECUTE_QUERY_TASK     = 0;
  private static final int EXECUTE_UPDATE_TASK    = 1;
  private static final int CALLABLE_EXECUTE_TASK  = 2;
  private static final int STATEMENT_EXECUTE_TASK = 3;

  /**
   * Post the stored procedure call in the threads task list.
   * <p>
   * Note that macros are also processed here.
   * 
   * @param request the stored procedure to call or the request to execute if
   *          taskType is STATEMENT_EXECUTE_TASK
   * @param taskType one of EXECUTE_QUERY_TASK, EXECUTE_UPDATE_TASK,
   *          CALLABLE_EXECUTE_TASK or STATEMENT_EXECUTE_TASK
   * @param metadataCache the metadataCache if any or null
   * @return the task that has been executed (caller can get the result by
   *         calling getResult())
   * @throws SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @throws NoMoreBackendException if no backend was available to execute the
   *           stored procedure
   */
  private AbstractTask callStoredProcedure(AbstractRequest request,
      int taskType, MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException, NoMoreBackendException
  {
    // Handle macros
    handleMacros(request);

    // Total ordering mainly for distributed virtual databases.
    boolean removeFromTotalOrderQueue = waitForTotalOrder(request, true);

    // Log lazy begin if needed
    if (request.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          request.getTransactionId());

    // Log request
    if (recoveryLog != null)
    {
      boolean mustLog = !request.isReadOnly();
      if (taskType != STATEMENT_EXECUTE_TASK)
      { // faster than (request instanceof StoredProcedure)
        DatabaseProcedureSemantic semantic = ((StoredProcedure) request)
            .getSemantic();
        mustLog = (semantic == null) || semantic.isWrite();
      }
      if (mustLog)
        recoveryLog.logRequestExecuting(request);
    }

    int nbOfThreads = acquireLockAndCheckNbOfThreads(request, String
        .valueOf(request.getId()));

    // Create the task
    AbstractTask task;
    switch (taskType)
    {
      case EXECUTE_QUERY_TASK :
        task = new CallableStatementExecuteQueryTask(getNbToWait(nbOfThreads),
            nbOfThreads, (StoredProcedure) request, metadataCache);
        break;
      case EXECUTE_UPDATE_TASK :
        task = new CallableStatementExecuteUpdateTask(getNbToWait(nbOfThreads),
            nbOfThreads, (StoredProcedure) request);
        break;
      case CALLABLE_EXECUTE_TASK :
        task = new CallableStatementExecuteTask(getNbToWait(nbOfThreads),
            nbOfThreads, (StoredProcedure) request, metadataCache);
        break;
      case STATEMENT_EXECUTE_TASK :
        task = new StatementExecuteTask(getNbToWait(nbOfThreads), nbOfThreads,
            (AbstractWriteRequest) request, metadataCache);
        break;
      default :
        throw new RuntimeException("Unhandled task type " + taskType
            + " in callStoredProcedure");
    }

    atomicTaskPostInQueueAndReleaseLock(request, task, nbOfThreads,
        removeFromTotalOrderQueue);

    try
    {
      synchronized (task)
      {
        if (!task.hasCompleted())
          waitForTaskCompletion(request.getTimeout() * 1000L, String
              .valueOf(request.getId()), task);

        checkTaskCompletion(task);
        return task;
      }
    }
    finally
    {
      if (!request.isAutoCommit())
      { // Check that transaction was not aborted in parallel
        try
        {
          this.vdb.getRequestManager().getTransactionMetaData(
              new Long(request.getTransactionId()));
        }
        catch (SQLException e)
        { // Transaction was aborted it cannot be found anymore in the active
          // transaction list. Force an abort
          enforceTransactionAbort(request);
        }
      }
    }
  }

  /**
   * Check the completion status of the task and throws appropriate Exceptions
   * if the status of the task was not successful, otherwise do nothing.
   * 
   * @param task the completed AbstractTask
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @exception NoMoreBackendException if no backends are left to execute the
   *              request
   * @exception SQLException if an error occurs
   */
  private void checkTaskCompletion(AbstractTask task)
      throws NoMoreBackendException, AllBackendsFailedException, SQLException
  {
    AbstractRequest request = task.getRequest();

    if (task.getSuccess() > 0)
      return;

    // Check that someone failed, it might be the case that we only have
    // disabling backends left and they have not played this query (thus
    // none of them have succeeded or failed).
    if (task.getFailed() == 0)
    {
      throw new NoMoreBackendException(Translate
          .get("loadbalancer.backendlist.empty"));
    }

    if (task.getSuccess() == 0)
    {
      // All backends that executed the query failed
      List<?> exceptions = task.getExceptions();
      if (exceptions == null)
        throw new AllBackendsFailedException(Translate.get(
            "loadbalancer.request.failed.all", new Object[]{request.getType(),
                String.valueOf(request.getId())}));
      else
      {
        String errorMsg = Translate.get("loadbalancer.request.failed.stack",
            new Object[]{request.getType(), String.valueOf(request.getId())})
            + "\n";
        SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
            errorMsg);
        logger.info(ex.getMessage());
        throw ex;
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getPreparedStatementGetMetaData(org.continuent.sequoia.controller.requests.AbstractRequest)
   */
  public ControllerResultSet getPreparedStatementGetMetaData(
      AbstractRequest request) throws SQLException
  {
    // Choose a backend
    DatabaseBackend backend = chooseFirstBackendForReadRequest(request);

    // Ok, we have a backend, let's execute the request
    AbstractConnectionManager cm = backend.getConnectionManager(request
        .getLogin());

    // Sanity check
    if (cm == null)
    {
      String msg = Translate.get("loadbalancer.connectionmanager.not.found",
          new String[]{request.getLogin(), backend.getName()});
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Execute the query
    if (request.isAutoCommit())
    {
      ControllerResultSet rs = null;
      boolean badConnection;
      do
      {
        badConnection = false;
        // Use a connection just for this request
        PooledConnection c = null;
        try
        {
          c = cm.retrieveConnectionInAutoCommit(request);
        }
        catch (UnreachableBackendException e1)
        {
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName());
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          // Retry on a different backend
          return getPreparedStatementGetMetaData(request);
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        try
        {
          rs = preparedStatementGetMetaDataOnBackend(
              request.getSqlOrTemplate(), backend, c.getConnection());
          cm.releaseConnectionInAutoCommit(request, c);
        }
        catch (SQLException e)
        {
          cm.releaseConnectionInAutoCommit(request, c);
          throw SQLExceptionFactory.getSQLException(e, Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        catch (BadConnectionException e)
        { // Get rid of the bad connection
          cm.deleteConnection(c);
          badConnection = true;
        }
        catch (Throwable e)
        {
          cm.releaseConnectionInAutoCommit(request, c);

          logger.error("Unexpected exception:", e);
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
      while (badConnection);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.on", new String[]{
            String.valueOf(request.getId()), backend.getName()}));
      return rs;
    }
    else
    { // Inside a transaction
      Connection c;
      long tid = request.getTransactionId();

      try
      {
        c = backend
            .getConnectionForTransactionAndLazyBeginIfNeeded(request, cm);
      }
      catch (UnreachableBackendException e1)
      {
        String msg = Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw new SQLException(Translate.get(
            "loadbalancer.backend.unreacheable", backend.getName()));
      }
      catch (NoTransactionStartWhenDisablingException e)
      {
        String msg = Translate.get("loadbalancer.backend.is.disabling",
            new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName()});
        logger.error(msg);
        throw new SQLException(msg);
      }

      // Sanity check
      if (c == null)
        throw new SQLException(Translate.get(
            "loadbalancer.unable.retrieve.connection", new String[]{
                String.valueOf(tid), backend.getName()}));

      // Execute Query
      ControllerResultSet rs = null;
      try
      {
        rs = preparedStatementGetMetaDataOnBackend(request.getSqlOrTemplate(),
            backend, c);
      }
      catch (SQLException e)
      {
        throw e;
      }
      catch (BadConnectionException e)
      {
        logger.error("Got an exception while executing request "
            + request.getId() + " in transaction " + request.getTransactionId()
            + "(" + request.getSqlShortForm(75) + ") : " + e);

        // Connection failed, so did the transaction
        // Disable the backend.
        cm.deleteConnection(tid);
        String msg = Translate.get(
            "loadbalancer.backend.disabling.connection.failure", backend
                .getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw new SQLException(msg);
      }
      catch (Throwable e)
      {

        logger.error("Unexpected exception:", e);
        throw new SQLException(Translate.get(
            "loadbalancer.request.failed.on.backend", new String[]{
                request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(request.getId()),
                backend.getName()}));
      return rs;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getPreparedStatementGetParameterMetaData(org.continuent.sequoia.controller.requests.AbstractRequest)
   *      TODO: factorize this code with at least
   *      {@link #getPreparedStatementGetMetaData(AbstractRequest)}
   *      (reconnection code makes it non-straightforward)
   */
  public ParameterMetaData getPreparedStatementGetParameterMetaData(
      AbstractRequest request) throws SQLException
  {
    // Choose a backend
    DatabaseBackend backend = chooseFirstBackendForReadRequest(request);

    // Ok, we have a backend, let's execute the request
    AbstractConnectionManager cm = backend.getConnectionManager(request
        .getLogin());

    // Sanity check
    if (cm == null)
    {
      String msg = Translate.get("loadbalancer.connectionmanager.not.found",
          new String[]{request.getLogin(), backend.getName()});
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Execute the query
    if (request.isAutoCommit())
    {
      ParameterMetaData pmd = null;
      boolean badConnection;
      do
      {
        badConnection = false;
        // Use a connection just for this request
        PooledConnection c = null;
        try
        {
          c = cm.retrieveConnectionInAutoCommit(request);
        }
        catch (UnreachableBackendException e1)
        {
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName());
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          // Retry on a different backend
          return getPreparedStatementGetParameterMetaData(request);
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        try
        {
          pmd = preparedStatementGetParameterMetaDataOnBackend(request
              .getSqlOrTemplate(), backend, c.getConnection());
          cm.releaseConnectionInAutoCommit(request, c);
        }
        catch (SQLException e)
        {
          cm.releaseConnectionInAutoCommit(request, c);
          throw SQLExceptionFactory.getSQLException(e, Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        catch (BadConnectionException e)
        { // Get rid of the bad connection
          cm.deleteConnection(c);
          badConnection = true;
        }
        catch (Throwable e)
        {
          cm.releaseConnectionInAutoCommit(request, c);

          logger.error("Unexpected exception:", e);
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
      while (badConnection);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.on", new String[]{
            String.valueOf(request.getId()), backend.getName()}));
      return pmd;
    }
    else
    { // Inside a transaction
      Connection c;
      long tid = request.getTransactionId();

      try
      {
        c = backend
            .getConnectionForTransactionAndLazyBeginIfNeeded(request, cm);
      }
      catch (UnreachableBackendException e1)
      {
        String msg = Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw new SQLException(Translate.get(
            "loadbalancer.backend.unreacheable", backend.getName()));
      }
      catch (NoTransactionStartWhenDisablingException e)
      {
        String msg = Translate.get("loadbalancer.backend.is.disabling",
            new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName()});
        logger.error(msg);
        throw new SQLException(msg);
      }

      // Sanity check
      if (c == null)
        throw new SQLException(Translate.get(
            "loadbalancer.unable.retrieve.connection", new String[]{
                String.valueOf(tid), backend.getName()}));

      // Execute Query
      ParameterMetaData pmd = null;
      try
      {
        pmd = preparedStatementGetParameterMetaDataOnBackend(request
            .getSqlOrTemplate(), backend, c);
      }
      catch (SQLException e)
      {
        throw e;
      }
      catch (BadConnectionException e)
      {
        logger.error("Got an exception while executing request "
            + request.getId() + " in transaction " + request.getTransactionId()
            + "(" + request.getSqlShortForm(75) + ") : " + e);

        // Connection failed, so did the transaction
        // Disable the backend.
        cm.deleteConnection(tid);
        String msg = Translate.get(
            "loadbalancer.backend.disabling.connection.failure", backend
                .getName());
        logger.error(msg);
        endUserLogger.error(msg);
        disableBackend(backend, true);
        throw new SQLException(msg);
      }
      catch (Throwable e)
      {

        logger.error("Unexpected exception:", e);
        throw new SQLException(Translate.get(
            "loadbalancer.request.failed.on.backend", new String[]{
                request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(request.getId()),
                backend.getName()}));
      return pmd;
    }
  }

  /*
   * Transaction management
   */

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#abort(org.continuent.sequoia.controller.requestmanager.TransactionMetaData)
   */
  public void abort(TransactionMetaData tm) throws SQLException
  {
    long tid = tm.getTransactionId();
    boolean executeRollback = false;
    DistributedRollback toqObject = null;
    /*
     * Let previous queries be flushed into the load balancer queues so that we
     * can abort them and that no queries for that transaction wait in the total
     * order queue while we are aborting. Note that the wait and remove from the
     * total order queue will be done in the call to rollback at the end of this
     * method.
     */
    if (vdb.getTotalOrderQueue() != null)
    {
      toqObject = new DistributedRollback(tm.getLogin(), tid);
      waitForTotalOrder(toqObject, false);
    }

    try
    {
      // Acquire the lock
      String requestDescription = "abort " + tid;
      int nbOfThreads = acquireLockAndCheckNbOfThreads(toqObject,
          requestDescription);

      boolean rollbackInProgress = false;
      synchronized (enabledBackends)
      {
        // Abort all queries on all backends that have started this transaction
        for (int i = 0; i < nbOfThreads; i++)
        {
          DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
          rollbackInProgress = rollbackInProgress
              || backend.getTaskQueues().abortAllQueriesForTransaction(tid);
        }
      }

      // Release the lock
      backendListLock.releaseRead();

      if (rollbackInProgress)
      { // already aborting
        if (vdb.getTotalOrderQueue() != null)
          removeObjectFromAndNotifyTotalOrderQueue(toqObject);
        return;
      }

      executeRollback = true;
      rollback(tm);
    }
    catch (NoMoreBackendException ignore)
    {
      if (!executeRollback && (recoveryLog != null))
        recoveryLog.logAbort(tm); // Executing status
    }
  }

  /**
   * Begins a new transaction.
   * 
   * @param tm the transaction marker metadata
   * @exception SQLException if an error occurs
   */
  public final void begin(TransactionMetaData tm) throws SQLException
  {
  }

  /**
   * Commits a transaction.
   * 
   * @param tm the transaction marker metadata
   * @exception SQLException if an error occurs
   */
  public void commit(TransactionMetaData tm) throws SQLException
  {
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);

    // Ordering for distributed virtual database
    boolean canTakeReadLock = false;
    DistributedCommit totalOrderCommit = null;
    if (vdb.getTotalOrderQueue() != null)
    {
      // Total ordering mainly for distributed virtual databases.
      // If waitForTotalOrder returns true then the query has been scheduled in
      // total order and there is no need to take a write lock later to resolve
      // potential conflicts.
      totalOrderCommit = new DistributedCommit(tm.getLogin(), tid);
      canTakeReadLock = waitForTotalOrder(totalOrderCommit, false);
      if (!canTakeReadLock)
        // This is a local commit no total order info
        totalOrderCommit = null;
    }

    // Update the recovery log
    if (recoveryLog != null)
      recoveryLog.logCommit(tm);

    // Acquire the lock
    String requestDescription = "commit " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(totalOrderCommit,
        requestDescription);

    // Build the list of backends that need to commit this transaction
    ArrayList<DatabaseBackend> commitList = new ArrayList<DatabaseBackend>(nbOfThreads);
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      /*
       * Check that the backend is just not simply late (SEQUOIA-955). We need
       * to check for the late task first since the transaction will only start
       * on the backend after the task has been executed. If the test is done
       * with backend.isStartedTransaction() first, the result might be false
       * but by the time we check the queues the task has completed.
       */
      if (((waitForCompletionPolicy.getPolicy() != WaitForCompletionPolicy.ALL) && backend
          .getTaskQueues().hasAPendingTaskForTransaction(tid))
          || backend.isStartedTransaction(lTid))
      {
        commitList.add(backend);
      }
      else
      { // SEQUOIA-955: Print suspicious enabled backends that don't commit a
        // write transaction
        if (logger.isDebugEnabled() && !tm.isReadOnly())
        {
          logger.debug("Transaction " + tid + " is not started on backend "
              + backend + "\n" + backend.getTaskQueues());
        }
      }
    }

    int nbOfThreadsToCommit = commitList.size();
    CommitTask task = null;
    if (nbOfThreadsToCommit != 0)
      task = new CommitTask(getNbToWait(nbOfThreadsToCommit),
          nbOfThreadsToCommit, tm);

    // Post the task in the non-conflicting queues.
    synchronized (enabledBackends)
    {
      for (int i = 0; i < nbOfThreadsToCommit; i++)
      {
        DatabaseBackend backend = (DatabaseBackend) commitList.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderCommit != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderCommit);

    // Check if someone had something to commit
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.commit.all.failed", tid));
        else
        {
          String errorMsg = Translate.get("loadbalancer.commit.failed.stack",
              tid)
              + "\n";
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              errorMsg);
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  /**
   * Rollbacks a transaction.
   * 
   * @param tm the transaction marker metadata
   * @exception SQLException if an error occurs
   */
  public void rollback(TransactionMetaData tm) throws SQLException
  {
    long tid = tm.getTransactionId();

    // Ordering for distributed virtual database
    DistributedRollback totalOrderRollback = null;
    boolean canTakeReadLock = false;
    if (vdb.getTotalOrderQueue() != null)
    {
      totalOrderRollback = new DistributedRollback(tm.getLogin(), tid);
      // Total ordering mainly for distributed virtual databases.
      // If waitForTotalOrder returns true then the query has been scheduled in
      // total order and there is no need to take a write lock later to resolve
      // potential conflicts.
      canTakeReadLock = waitForTotalOrder(totalOrderRollback, false);
      if (!canTakeReadLock)
        // This is a local rollback no total order info
        totalOrderRollback = null;
    }

    // Update the recovery log
    if (recoveryLog != null)
      recoveryLog.logRollback(tm);

    // Acquire the lock
    String requestDescription = "rollback " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(totalOrderRollback,
        requestDescription);

    // Build the list of backends that need to rollback this transaction
    ArrayList<DatabaseBackend> rollbackList = new ArrayList<DatabaseBackend>();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      rollbackList.add(backend);
    }

    int nbOfThreadsToRollback = rollbackList.size();
    RollbackTask task = null;
    task = new RollbackTask(getNbToWait(nbOfThreadsToRollback),
        nbOfThreadsToRollback, tm);

    // Post the task in the non-conflicting queues.
    synchronized (enabledBackends)
    {
      for (int i = 0; i < nbOfThreadsToRollback; i++)
      {
        DatabaseBackend backend = (DatabaseBackend) rollbackList.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderRollback != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderRollback);

    // Check if someone had something to rollback
    if (nbOfThreadsToRollback == 0)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() > 0)
        return;

      // All tasks failed
      List<?> exceptions = task.getExceptions();
      if (exceptions == null)
        throw new SQLException(Translate.get(
            "loadbalancer.rollback.all.failed", tid));
      else
      {
        String errorMsg = Translate.get("loadbalancer.rollback.failed.stack",
            tid)
            + "\n";
        SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
            errorMsg);
        logger.error(ex.getMessage());
        throw ex;
      }
    }
  }

  /**
   * Rollback a transaction to a savepoint
   * 
   * @param tm The transaction marker metadata
   * @param savepointName The name of the savepoint
   * @throws SQLException if an error occurs
   */
  public void rollbackToSavepoint(TransactionMetaData tm, String savepointName)
      throws SQLException
  {
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);

    // Ordering for distributed virtual database
    DistributedRollbackToSavepoint totalOrderRollback = null;
    boolean canTakeReadLock = false;
    if (vdb.getTotalOrderQueue() != null)
    {
      totalOrderRollback = new DistributedRollbackToSavepoint(tid,
          savepointName);
      // Total ordering mainly for distributed virtual databases.
      // If waitForTotalOrder returns true then the query has been scheduled in
      // total order and there is no need to take a write lock later to resolve
      // potential conflicts.
      canTakeReadLock = waitForTotalOrder(totalOrderRollback, false);
      if (!canTakeReadLock)
        // This is a local commit no total order info
        totalOrderRollback = null;
    }

    // Update the recovery log
    if (recoveryLog != null)
      recoveryLog.logRollbackToSavepoint(tm, savepointName);

    // Acquire the lock
    String requestDescription = "rollback " + savepointName + " " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

    // Build the list of backends that need to rollback this transaction
    ArrayList<DatabaseBackend> rollbackList = new ArrayList<DatabaseBackend>();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      /*
       * Check that the backend is just not simply late (SEQUOIA-955). We need
       * to check for the late task first since the transaction will only start
       * on the backend after the task has been executed. If the test is done
       * with backend.isStartedTransaction() first, the result might be false
       * but by the time we check the queues the task has completed.
       */
      if (((waitForCompletionPolicy.getPolicy() != WaitForCompletionPolicy.ALL) && backend
          .getTaskQueues().hasAPendingTaskForTransaction(tid))
          || backend.isStartedTransaction(lTid))
        rollbackList.add(backend);
    }

    int nbOfThreadsToRollback = rollbackList.size();
    RollbackToSavepointTask task = null;
    if (nbOfThreadsToRollback != 0)
      task = new RollbackToSavepointTask(getNbToWait(nbOfThreadsToRollback),
          nbOfThreadsToRollback, tm, savepointName);

    // Post the task in the non-conflicting queues.
    synchronized (enabledBackends)
    {
      for (int i = 0; i < nbOfThreadsToRollback; i++)
      {
        DatabaseBackend backend = (DatabaseBackend) rollbackList.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderRollback != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderRollback);

    // Check if someone had something to rollback
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.rollbacksavepoint.all.failed", new String[]{
                  savepointName, String.valueOf(tid)}));
        else
        {
          String errorMsg = Translate.get(
              "loadbalancer.rollbacksavepoint.failed.stack", new String[]{
                  savepointName, String.valueOf(tid)})
              + "\n";
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              errorMsg);
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  /**
   * Release a savepoint from a transaction
   * 
   * @param tm The transaction marker metadata
   * @param savepointName The name of the savepoint ro release
   * @throws SQLException if an error occurs
   */
  public void releaseSavepoint(TransactionMetaData tm, String savepointName)
      throws SQLException
  {
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);

    // Ordering for distributed virtual database
    DistributedReleaseSavepoint totalOrderRelease = null;
    boolean canTakeReadLock = false;
    if (vdb.getTotalOrderQueue() != null)
    {
      totalOrderRelease = new DistributedReleaseSavepoint(tid, savepointName);
      // Total ordering mainly for distributed virtual databases.
      // If waitForTotalOrder returns true then the query has been scheduled in
      // total order and there is no need to take a write lock later to resolve
      // potential conflicts.
      canTakeReadLock = waitForTotalOrder(totalOrderRelease, false);
      if (!canTakeReadLock)
        // This is a local commit no total order info
        totalOrderRelease = null;
    }

    // Update the recovery log
    if (recoveryLog != null)
      recoveryLog.logReleaseSavepoint(tm, savepointName);

    // Acquire the lock
    String requestDescription = "release savepoint " + savepointName + " "
        + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

    // Build the list of backends that need to rollback this transaction
    ArrayList<DatabaseBackend> savepointList = new ArrayList<DatabaseBackend>();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      /*
       * Check that the backend is just not simply late (SEQUOIA-955). We need
       * to check for the late task first since the transaction will only start
       * on the backend after the task has been executed. If the test is done
       * with backend.isStartedTransaction() first, the result might be false
       * but by the time we check the queues the task has completed.
       */
      if (((waitForCompletionPolicy.getPolicy() != WaitForCompletionPolicy.ALL) && backend
          .getTaskQueues().hasAPendingTaskForTransaction(tid))
          || backend.isStartedTransaction(lTid))
        savepointList.add(backend);
    }

    int nbOfSavepoints = savepointList.size();
    ReleaseSavepointTask task = null;
    if (nbOfSavepoints != 0)
      task = new ReleaseSavepointTask(getNbToWait(nbOfThreads), nbOfThreads,
          tm, savepointName);

    // Post the task in the non-conflicting queues.
    synchronized (enabledBackends)
    {
      for (int i = 0; i < nbOfSavepoints; i++)
      {
        DatabaseBackend backend = (DatabaseBackend) savepointList.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderRelease != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderRelease);

    // Check if someone had something to release
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.releasesavepoint.all.failed", new String[]{
                  savepointName, String.valueOf(tid)}));
        else
        {
          String errorMsg = Translate.get(
              "loadbalancer.releasesavepoint.failed.stack", new String[]{
                  savepointName, String.valueOf(tid)})
              + "\n";
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              errorMsg);
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  /**
   * Set a savepoint to a transaction.
   * 
   * @param tm The transaction marker metadata
   * @param savepointName The name of the new savepoint
   * @throws SQLException if an error occurs
   */
  public void setSavepoint(TransactionMetaData tm, String savepointName)
      throws SQLException
  {
    long tid = tm.getTransactionId();

    // Ordering for distributed virtual database
    DistributedSetSavepoint totalOrderSavepoint = null;
    boolean canTakeReadLock = false;
    if (vdb.getTotalOrderQueue() != null)
    {
      totalOrderSavepoint = new DistributedSetSavepoint(tm.getLogin(), tid,
          savepointName);
      // Total ordering mainly for distributed virtual databases.
      // If waitForTotalOrder returns true then the query has been scheduled in
      // total order and there is no need to take a write lock later to resolve
      // potential conflicts.
      canTakeReadLock = waitForTotalOrder(totalOrderSavepoint, false);
      if (!canTakeReadLock)
        // This is a local commit no total order info
        totalOrderSavepoint = null;
    }

    // Update the recovery log
    if (recoveryLog != null)
      recoveryLog.logSetSavepoint(tm, savepointName);

    // Acquire the lock
    String requestDescription = "set savepoint " + savepointName + " " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(totalOrderSavepoint,
        requestDescription);

    SavepointTask task = null;

    // Post the task in the non-conflicting queues of all backends.
    synchronized (enabledBackends)
    {
      if (nbOfThreads != 0)
      {
        task = new SavepointTask(getNbToWait(nbOfThreads), nbOfThreads, tm,
            savepointName);
        for (int i = 0; i < nbOfThreads; i++)
        {
          DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
          backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
        }
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderSavepoint != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderSavepoint);

    // Check if someone had something to release
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.setsavepoint.all.failed", new String[]{
                  savepointName, String.valueOf(tid)}));
        else
        {
          String errorMsg = Translate.get(
              "loadbalancer.setsavepoint.failed.stack", new String[]{
                  savepointName, String.valueOf(tid)})
              + "\n";
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              errorMsg);
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  //
  // Utility functions
  //

  /**
   * Check in which queue the task should be posted and atomically posts the
   * task in the queue of all backends. The list of locks acquired by the
   * request is set on the task if the request is in autocommit mode (if it is
   * in a transaction it is automatically added to the transaction lock list).
   * The list of lock can be null if no lock has been acquired.
   * 
   * @param task the task to post
   * @param nbOfThreads number of threads in the backend list (must already be
   *          locked)
   * @param removeFromTotalOrderQueue true if the query must be removed from the
   *          total order queue
   */
  private void atomicTaskPostInQueueAndReleaseLock(AbstractRequest request,
      AbstractTask task, int nbOfThreads, boolean removeFromTotalOrderQueue)
  {
    synchronized (enabledBackends)
    {
      for (int i = 0; i < nbOfThreads; i++)
      {
        BackendTaskQueues queues = ((DatabaseBackend) enabledBackends.get(i))
            .getTaskQueues();
        queues.addTaskToBackendTotalOrderQueue(task);
      }
    }

    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (removeFromTotalOrderQueue)
    {
      removeObjectFromAndNotifyTotalOrderQueue(request);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#closePersistentConnection(java.lang.String,
   *      long)
   */
  public void closePersistentConnection(String login,
      long persistentConnectionId) throws SQLException
  {
    /*
     * We assume a synchronous execution and connection closing can only come
     * after all requests have been executed in that connection. We post to all
     * backends and let the task deal with whether that backend had a persistent
     * connection or not.
     */

    String requestDescription = "closing persistent connection "
        + persistentConnectionId;
    int nbOfThreads = 0;

    DistributedClosePersistentConnection totalOrderQueueObject = null;
    boolean removefromTotalOrder = false;
    if (vdb.getTotalOrderQueue() != null)
    {
      totalOrderQueueObject = new DistributedClosePersistentConnection(login,
          persistentConnectionId);
      removefromTotalOrder = waitForTotalOrder(totalOrderQueueObject, false);
    }

    ClosePersistentConnectionTask task = null;
    try
    {
      nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

      task = new ClosePersistentConnectionTask(getNbToWait(nbOfThreads),
          nbOfThreads, login, persistentConnectionId);

      // Post the task in the non-conflicting queues.
      synchronized (enabledBackends)
      {
        for (int i = 0; i < nbOfThreads; i++)
        {
          DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
          backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
        }
      }

      // Release the lock
      backendListLock.releaseRead();

      if (removefromTotalOrder)
        removeObjectFromAndNotifyTotalOrderQueue(totalOrderQueueObject);
      totalOrderQueueObject = null;

      synchronized (task)
      {
        if (!task.hasCompleted())
          try
          {
            waitForTaskCompletion(0, requestDescription, task);
          }
          catch (SQLException ignore)
          {
          }
      }
    }
    finally
    {
      if (totalOrderQueueObject != null)
      { // NoMoreBackendException occured
        removeObjectFromAndNotifyTotalOrderQueue(totalOrderQueueObject);
      }

      if (logger.isDebugEnabled())
        logger.debug(requestDescription + " completed on " + nbOfThreads
            + " backends.");
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#openPersistentConnection(String,
   *      long)
   */
  public void openPersistentConnection(String login, long persistentConnectionId)
      throws SQLException
  {
    String requestDescription = "opening persistent connection "
        + persistentConnectionId;
    int nbOfThreads = 0;

    DistributedOpenPersistentConnection totalOrderQueueObject = null;
    if (vdb.getTotalOrderQueue() != null)
    {
      totalOrderQueueObject = new DistributedOpenPersistentConnection(login,
          persistentConnectionId);
      waitForTotalOrder(totalOrderQueueObject, true);
    }

    OpenPersistentConnectionTask task = null;
    try
    {
      nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

      task = new OpenPersistentConnectionTask(getNbToWait(nbOfThreads),
          nbOfThreads, login, persistentConnectionId);

      // Post the task in the non-conflicting queues.
      synchronized (enabledBackends)
      {
        for (int i = 0; i < nbOfThreads; i++)
        {
          DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
          backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
        }
      }

      // Release the lock
      backendListLock.releaseRead();

      removeObjectFromAndNotifyTotalOrderQueue(totalOrderQueueObject);
      totalOrderQueueObject = null;

      synchronized (task)
      {
        if (!task.hasCompleted())
          try
          {
            waitForTaskCompletion(0, requestDescription, task);
          }
          catch (SQLException ignore)
          {
          }
      }
    }
    finally
    {
      if (totalOrderQueueObject != null)
      { // NoMoreBackendException occured
        removeObjectFromAndNotifyTotalOrderQueue(totalOrderQueueObject);
      }

      if (logger.isDebugEnabled())
        logger.debug(requestDescription + " completed on " + nbOfThreads
            + " backends.");
    }
  }

  /**
   * Enables a Backend that was previously disabled.
   * <p>
   * Ask the corresponding connection manager to initialize the connections if
   * needed.
   * <p>
   * No sanity checks are performed by this function.
   * 
   * @param db the database backend to enable
   * @param writeEnabled True if the backend must be enabled for writes
   * @throws SQLException if an error occurs
   */
  public synchronized void enableBackend(DatabaseBackend db,
      boolean writeEnabled) throws SQLException
  {
    if (!db.isInitialized())
      db.initializeConnections();

    if (writeEnabled && db.isWriteCanBeEnabled())
    {
      BackendTaskQueues taskqueues = new BackendTaskQueues(db,
          waitForCompletionPolicy, this.vdb.getRequestManager());
      // Create the new backend task queues
      try
      {
        ObjectName taskQueuesObjectName = JmxConstants
            .getBackendTaskQueuesObjectName(db.getVirtualDatabaseName(), db
                .getName());
        if (MBeanServerManager.getInstance().isRegistered(taskQueuesObjectName))
        {
          MBeanServerManager.unregister(taskQueuesObjectName);
        }
        MBeanServerManager.registerMBean(new BackendTaskQueuesControl(
            taskqueues), taskQueuesObjectName);
      }
      catch (Exception e)
      {
        if (logger.isWarnEnabled())
        {
          logger.warn("failed to register task queue mbeans for " + db, e);
        }
      }
      db.setTaskQueues(taskqueues);
      db.startWorkerThreads(this);
      db.startDeadlockDetectionThread(this.vdb);
      db.enableWrite();
    }

    db.enableRead();
    try
    {
      backendListLock.acquireWrite();
    }
    catch (InterruptedException e)
    {
      logger.error("Error while acquiring write lock in enableBackend", e);
    }

    synchronized (enabledBackends)
    {
      enabledBackends.add(db);
    }

    backendListLock.releaseWrite();
  }

  /**
   * Disables a backend that was previously enabled.
   * <p>
   * Ask the corresponding connection manager to finalize the connections if
   * needed.
   * <p>
   * No sanity checks are performed by this function.
   * 
   * @param db the database backend to disable
   * @param forceDisable true if disabling must be forced on the backend
   * @throws SQLException if an error occurs
   */
  public void disableBackend(DatabaseBackend db, boolean forceDisable)
      throws SQLException
  {
    if (!db.disable())
    {
      // Another thread has already started the disable process
      return;
    }
    synchronized (this)
    {
      try
      {
        backendListLock.acquireWrite();
      }
      catch (InterruptedException e)
      {
        logger.error("Error while acquiring write lock in enableBackend", e);
      }

      try
      {
        synchronized (enabledBackends)
        {
          enabledBackends.remove(db);
          if (enabledBackends.isEmpty())
          {
            // Cleanup schema for any remaining locks
            this.vdb.getRequestManager().setDatabaseSchema(null, false);
          }
        }

        if (!forceDisable)
          terminateThreadsAndConnections(db);
      }
      finally
      {
        backendListLock.releaseWrite();
      }

      if (forceDisable)
      {
        db.shutdownConnectionManagers();
        terminateThreadsAndConnections(db, false);
      }

      // sanity check on backend's active transaction
      if (!db.getActiveTransactions().isEmpty())
      {
        if (logger.isWarnEnabled())
        {
          logger.warn("Active transactions after backend " + db.getName()
              + " is disabled: " + db.getActiveTransactions());
        }
      }
    }
  }

  private void terminateThreadsAndConnections(DatabaseBackend db)
      throws SQLException
  {
    terminateThreadsAndConnections(db, true);
  }

  private void terminateThreadsAndConnections(DatabaseBackend db, boolean wait)
      throws SQLException
  {
    db.terminateWorkerThreads(wait);
    db.terminateDeadlockDetectionThread();

    if (db.isInitialized())
      db.finalizeConnections();
  }

  //
  // Debug/Monitoring
  //

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getXmlImpl
   */
  public String getXmlImpl()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RAIDb_1 + ">");
    if (waitForCompletionPolicy != null)
      info.append(waitForCompletionPolicy.getXml());
    if (macroHandler != null)
      info.append(macroHandler.getXml());
    info.append(getRaidb1Xml());
    info.append("</" + DatabasesXmlTags.ELT_RAIDb_1 + ">");
    return info.toString();
  }

  /**
   * Surrounding raidb1 tags can be treated by <method>getXmlImpl </method>
   * above, but more detailed content have to be returned by the method
   * <method>getRaidb1Xml </method> below.
   * 
   * @return content of Raidb1 xml
   */
  public abstract String getRaidb1Xml();
}
