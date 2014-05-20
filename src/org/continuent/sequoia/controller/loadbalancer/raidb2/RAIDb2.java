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
 * Initial developer(s): Emmanuel Cecchet. 
 * Contributor(s): Jean-Bernard van Zuylen.
 */

package org.continuent.sequoia.controller.loadbalancer.raidb2;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.continuent.sequoia.common.exceptions.BadConnectionException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoTransactionStartWhenDisablingException;
import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.exceptions.SQLExceptionFactory;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueues;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableException;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTablePolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableRule;
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
 * RAIDb-2 load balancer.
 * <p>
 * This class is an abstract call because the read requests coming from the
 * Request Manager are NOT treated here but in the subclasses. Transaction
 * management and write requests are broadcasted to all backends owning the
 * written table.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public abstract class RAIDb2 extends AbstractLoadBalancer
{
  //
  // How the code is organized ?
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Transaction handling
  // 5. Backend management
  //

  protected CreateTablePolicy createTablePolicy;
  protected static Trace      logger = Trace
                                         .getLogger("org.continuent.sequoia.controller.loadbalancer.raidb2");

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-2 request load balancer. A new backend worker thread is
   * created for each backend.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy how many backends must complete before
   *          returning the result ?
   * @param createTablePolicy the policy defining how 'create table' statements
   *          should be handled
   * @exception Exception if an error occurs
   */
  public RAIDb2(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy,
      CreateTablePolicy createTablePolicy) throws Exception
  {
    super(vdb, RAIDbLevels.RAIDb2, ParsingGranularities.TABLE);

    this.waitForCompletionPolicy = waitForCompletionPolicy;
    this.createTablePolicy = createTablePolicy;
  }

  /*
   * Request Handling
   */

  /**
   * Implementation specific load balanced read execution.
   * 
   * @param request an <code>SelectRequest</code>
   * @param metadataCache the metadataCache if any or null
   * @return the corresponding <code>java.sql.ResultSet</code>
   * @exception SQLException if an error occurs
   */
  public abstract ControllerResultSet statementExecuteQuery(
      SelectRequest request, MetadataCache metadataCache) throws SQLException;

  /**
   * Performs a write request. This request is broadcasted to all nodes that
   * owns the table to be written.
   * 
   * @param request an <code>AbstractWriteRequest</code>
   * @return number of rows affected by the request
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @exception SQLException if an error occurs
   */
  public ExecuteUpdateResult statementExecuteUpdate(AbstractWriteRequest request)
      throws AllBackendsFailedException, SQLException
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
   * @exception SQLException if an error occurs
   */
  public GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request, MetadataCache metadataCache)
      throws AllBackendsFailedException, SQLException
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
    throw new NotImplementedException(
        "Statement.execute() is currently not supported with RAIDb-2");
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
      throws AllBackendsFailedException, SQLException
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
    recoveryLog.logRequestExecuting(request);

    int nbOfThreads = acquireLockAndCheckNbOfThreads(request, String
        .valueOf(request.getId()));

    List<DatabaseBackend> writeList = new ArrayList<DatabaseBackend>();

    if (request.isCreate())
    {
      try
      {
        writeList = getBackendsForCreateTableRequest(request.getTableName());
      }
      catch (CreateTableException e)
      {
        releaseLockAndUnlockNextQuery(request);
        throw new SQLException(Translate.get(
            "loadbalancer.create.table.rule.failed", e.getMessage()));
      }
    }
    else
    {
      writeList = getBackendsWithTable(request.getTableName(), nbOfThreads);
    }

    nbOfThreads = writeList.size();
    if (nbOfThreads == 0)
    {
      String msg = Translate.get("loadbalancer.execute.no.backend.found",
          request.getSqlShortForm(vdb.getSqlShortFormLength()));
      logger.warn(msg);

      releaseLockAndUnlockNextQuery(request);
      throw new SQLException(msg);
    }
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate.get("loadbalancer.execute.on.several", String
          .valueOf(request.getId()), String.valueOf(nbOfThreads)));
    }

    // Create the task
    AbstractTask task;
    if (useKeys)
      task = new StatementExecuteUpdateWithKeysTask(getNbToWait(nbOfThreads),
          nbOfThreads, request, metadataCache);
    else
      task = new StatementExecuteUpdateTask(getNbToWait(nbOfThreads),
          nbOfThreads, request);

    atomicTaskPostInQueueAndReleaseLock(request, task, writeList,
        removeFromTotalOrderQueue);

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(request.getTimeout() * 1000L, String
            .valueOf(request.getId()), task);

      checkTaskCompletion(task);
      return task;
    }
  }

  /**
   * Gets a <code>&lt;DatabaseBacken&gt;List</code> of all backends containing
   * the table identified by <code>tableName</code>.
   * 
   * @param tableName name of the table
   * @param nbOfThreads number of threads
   * @return a <code>&lt;DatabaseBacken&gt;List</code> of all backends
   *         containing the table identified by <code>tableName</code>
   */
  // FIXME why don't we just iterate on the enabledBackends list (assuming we
  // acquire/release the read lock)?
  // TODO should be moved to VirtualDatabase
  private List<DatabaseBackend> getBackendsWithTable(String tableName, int nbOfThreads)
  {
    List<DatabaseBackend> backendsWithTable = new ArrayList<DatabaseBackend>();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend b = (DatabaseBackend) enabledBackends.get(i);
      if (b.hasTable(tableName))
        backendsWithTable.add(b);
    }
    return backendsWithTable;
  }

  /**
   * Gets a <code>&lt;DatabaseBacken&gt;List</code> of all backends with a
   * transaction identified by <code>tid</code> already started.
   * 
   * @param tid Transaction identifier
   * @param nbOfThreads number of threads
   * @return a <code>&lt;DatabaseBacken&gt;List</code> of all backends with a
   *         transaction identified by <code>tid</code> already started
   */
  // FIXME why don't we just iterate on the enabledBackends list (assuming we
  // acquire/release the read lock)?
  // TODO should be moved to VirtualDatabase
  private List<DatabaseBackend> getBackendsWithStartedTransaction(Long tid, int nbOfThreads)
  {
    // Build the list of backends that need to commit this transaction
    List<DatabaseBackend> backendsWithStartedTransaction = new ArrayList<DatabaseBackend>(nbOfThreads);
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      if (backend.isStartedTransaction(tid))
        backendsWithStartedTransaction.add(backend);
    }
    return backendsWithStartedTransaction;
  }

  /**
   * Gets a <code>&lt;DatabaseBacken&gt;List</code> of all backends containing
   * the stored procedure identified by <code>procedureName</code>.
   * 
   * @param procedureName name of the stored procedure
   * @param nbOfParameters number of parameters of the stored procedure
   * @param nbOfThreads number of threads
   * @return a <code>&lt;DatabaseBacken&gt;List</code> of all backends
   *         containing the table identified by <code>tableName</code>
   */
  // FIXME why don't we just iterate on the enabledBackends list (assuming we
  // acquire/release the read lock)?
  // TODO should be moved to VirtualDatabase
  private List<DatabaseBackend> getBackendsWithStoredProcedure(String procedureName,
      int nbOfParameters, int nbOfThreads)
  {
    List<DatabaseBackend> backendsWithStoredProcedure = new ArrayList<DatabaseBackend>(nbOfThreads);
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend b = (DatabaseBackend) enabledBackends.get(i);
      if (b.hasStoredProcedure(procedureName, nbOfParameters))
        backendsWithStoredProcedure.add(b);
    }
    return backendsWithStoredProcedure;
  }

  /**
   * Gets a <code>&lt;DatabaseBacken&gt;List</code> of all backends according
   * to the create table policy.
   * 
   * @param tableName the name of the table to create
   * @return a <code>&lt;DatabaseBacken&gt;List</code> of all backends
   *         according to the create table policy.
   * @throws CreateTableException if an error occurs while getting the backends
   *           according to the create table policy
   */
  // TODO should be moved to VirtualDatabase
  private List<DatabaseBackend> getBackendsForCreateTableRequest(String tableName)
      throws CreateTableException
  {
    // Choose the backend according to the defined policy
    CreateTableRule rule = createTablePolicy.getTableRule(tableName);
    if (rule == null)
    {
      rule = createTablePolicy.getDefaultRule();
    }
    return rule.getBackends(vdb.getBackends());
  }

  /**
   * Executes a select request <em>within a transation</em> on a given
   * backend. The transaction is lazily begun if it was not already started.
   * 
   * @param request the Select request to execute
   * @param backend the backend on which the request must be executed
   * @param metadataCache the metadata cache
   * @return a <code>ControllerResultSet</code>
   * @throws SQLException if an error occurs while executing the request
   */
  private ControllerResultSet executeSelectRequestInTransaction(
      SelectRequest request, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException
  {
    AbstractConnectionManager cm = backend.getConnectionManager(request
        .getLogin());

    if (cm == null)
    {
      String msg = Translate.get("loadbalancer.connectionmanager.not.found",
          request.getLogin(), backend.getName());
      logger.error(msg);
      throw new SQLException(msg);
    }

    Connection c;
    long tid = request.getTransactionId();

    try
    {
      c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(request, cm);
    }
    catch (UnreachableBackendException e1)
    {
      logger.error(Translate.get("loadbalancer.backend.disabling.unreachable",
          backend.getName()));
      disableBackend(backend, true);
      throw new SQLException(Translate.get("loadbalancer.backend.unreacheable",
          backend.getName()));
    }
    catch (NoTransactionStartWhenDisablingException e)
    {
      String msg = Translate.get("loadbalancer.backend.is.disabling", request
          .getSqlShortForm(vdb.getSqlShortFormLength()), backend.getName());
      logger.error(msg);
      throw new SQLException(msg);
    }

    if (c == null)
      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.connection", String.valueOf(tid),
          backend.getName()));

    try
    {
      ControllerResultSet rs = executeStatementExecuteQueryOnBackend(request,
          backend, null, c, metadataCache);
      if (logger.isDebugEnabled())
      {
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(request.getId()),
                backend.getName()}));
      }
      return rs;
    }
    catch (SQLException e)
    {
      throw e;
    }
    catch (BadConnectionException e)
    { // Connection failed, so did the transaction
      // Disable the backend.
      cm.deleteConnection(tid);
      String msg = Translate.get(
          "loadbalancer.backend.disabling.connection.failure", backend
              .getName());
      logger.error(msg);
      disableBackend(backend, true);
      throw new SQLException(msg);
    }
    catch (Throwable e)
    {
      throw new SQLException(Translate.get(
          "loadbalancer.request.failed.on.backend", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()}));
    }
  }

  /**
   * Executes a select request <em>in autocommit mode</em> on a given backend.
   * 
   * @param request the Select request to execute
   * @param backend the backend on which the request must be executed
   * @param metadataCache the metadata cache
   * @return a <code>ControllerResultSet</code>
   * @throws SQLException if an error occurs while executing the request
   * @throws UnreachableBackendException if the backend is not reachable
   */
  private ControllerResultSet executeSelectRequestInAutoCommit(
      SelectRequest request, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException,
      UnreachableBackendException
  {
    AbstractConnectionManager cm = backend.getConnectionManager(request
        .getLogin());

    // Sanity check
    if (cm == null)
    {
      String msg = Translate.get("loadbalancer.connectionmanager.not.found",
          request.getLogin(), backend.getName());
      logger.error(msg);
      throw new SQLException(msg);
    }

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
        logger.error(Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName()));
        disableBackend(backend, true);
        throw new UnreachableBackendException(Translate.get(
            "loadbalancer.backend.unreacheable", backend.getName()));
      }

      // Sanity check
      if (c == null)
        throw new UnreachableBackendException("No more connections on backend "
            + backend.getName());

      // Execute Query
      try
      {
        rs = executeStatementExecuteQueryOnBackend(request, backend, null, c
            .getConnection(), metadataCache);
        cm.releaseConnectionInAutoCommit(request, c);
      }
      catch (SQLException e)
      {
        cm.releaseConnectionInAutoCommit(request, c);
        throw new SQLException(Translate.get(
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
        throw new SQLException(Translate.get(
            "loadbalancer.request.failed.on.backend", new String[]{
                request.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
    }
    while (badConnection);
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("loadbalancer.execute.on", String
          .valueOf(request.getId()), backend.getName()));
    return rs;
  }

  /**
   * Execute a read request on the selected backend.
   * 
   * @param request the request to execute
   * @param backend the backend that will execute the request
   * @param metadataCache a metadataCache if any or null
   * @return the ResultSet
   * @throws SQLException if an error occurs
   */
  protected ControllerResultSet executeReadRequestOnBackend(
      SelectRequest request, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException,
      UnreachableBackendException
  {
    // Handle macros
    handleMacros(request);

    // Execute the query
    if (request.isAutoCommit())
    {
      return executeSelectRequestInAutoCommit(request, backend, metadataCache);
    }
    else
    {
      return executeSelectRequestInTransaction(request, backend, metadataCache);
    }
  }

  protected static final int CALLABLE_STATEMENT_EXECUTE_QUERY = 1;
  protected static final int CALLABLE_STATEMENT_EXECUTE       = 2;

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
    // Execute the query
    if (proc.isAutoCommit())
    {
      return executeStoredProcedureInAutoCommit(proc, isExecuteQuery, backend,
          metadataCache);
    }
    else
    {
      return executeStoredProcedureInTransaction(proc, isExecuteQuery, backend,
          metadataCache);
    }
  }

  /**
   * Executes a stored procedure <em>within a transation</em> on the selected
   * backend. The transaction is lazily started if it has not already begun.
   * 
   * @param proc the <em>autocommit</em> stored procedure to execute
   * @param isExecuteQuery true if we must call CallableStatement.executeQuery,
   *          false if we must call CallableStatement.execute()
   * @param backend the backend that will execute the request
   * @param metadataCache the metadataCache if any or null
   * @return a <code>ControllerResultSet</code> if isExecuteQuery is true, an
   *         <code>ExecuteResult</code> object otherwise
   * @throws SQLException if an error occurs
   */
  private Object executeStoredProcedureInTransaction(StoredProcedure proc,
      boolean isExecuteQuery, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException
  {
    // Inside a transaction
    // Ok, we have a backend, let's execute the request
    AbstractConnectionManager cm = backend
        .getConnectionManager(proc.getLogin());

    // Sanity check
    if (cm == null)
    {
      String msg = Translate.get("loadbalancer.connectionmanager.not.found",
          proc.getLogin(), backend.getName());
      logger.error(msg);
      throw new SQLException(msg);
    }

    Connection c;
    long tid = proc.getTransactionId();

    try
    {
      c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(proc, cm);
    }
    catch (UnreachableBackendException e1)
    {
      logger.error(Translate.get("loadbalancer.backend.disabling.unreachable",
          backend.getName()));
      disableBackend(backend, true);
      throw new SQLException(Translate.get("loadbalancer.backend.unreacheable",
          backend.getName()));
    }
    catch (NoTransactionStartWhenDisablingException e)
    {
      String msg = Translate.get("loadbalancer.backend.is.disabling", proc
          .getSqlShortForm(vdb.getSqlShortFormLength()), backend.getName());
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Sanity check
    if (c == null)
      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.connection", String.valueOf(tid),
          backend.getName()));

    // Execute Query
    try
    {
      if (isExecuteQuery)
        return AbstractLoadBalancer
            .executeCallableStatementExecuteQueryOnBackend(proc, backend, null,
                c, metadataCache);
      else
        return AbstractLoadBalancer.executeCallableStatementExecuteOnBackend(
            proc, backend, null, cm.retrieveConnectionForTransaction(tid),
            metadataCache);
    }
    catch (Exception e)
    {
      throw new SQLException(Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()}));
    }
    finally
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(proc.getId()),
                backend.getName()}));
    }
  }

  /**
   * Executes a stored procedure <em>in autocommit mode</em> on the selected
   * backend.
   * 
   * @param proc the <em>autocommit</em> stored procedure to execute
   * @param isExecuteQuery true if we must call CallableStatement.executeQuery,
   *          false if we must call CallableStatement.execute()
   * @param backend the backend that will execute the request
   * @param metadataCache the metadataCache if any or null
   * @return a <code>ControllerResultSet</code> if isExecuteQuery is true, an
   *         <code>ExecuteResult</code> object otherwise
   * @throws SQLException if an error occurs
   */
  private Object executeStoredProcedureInAutoCommit(StoredProcedure proc,
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
          proc.getLogin(), backend.getName());
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Use a connection just for this request
    PooledConnection c = null;
    try
    {
      c = cm.retrieveConnectionInAutoCommit(proc);
    }
    catch (UnreachableBackendException e1)
    {
      logger.error(Translate.get("loadbalancer.backend.disabling.unreachable",
          backend.getName()));
      disableBackend(backend, true);
      throw new UnreachableBackendException(Translate.get(
          "loadbalancer.backend.unreacheable", backend.getName()));
    }

    // Sanity check
    if (c == null)
      throw new SQLException(Translate.get(
          "loadbalancer.backend.no.connection", backend.getName()));

    // Execute Query
    try
    {
      if (isExecuteQuery)
        return AbstractLoadBalancer
            .executeCallableStatementExecuteQueryOnBackend(proc, backend, null,
                c.getConnection(), metadataCache);
      else
        return AbstractLoadBalancer.executeCallableStatementExecuteOnBackend(
            proc, backend, null, c, metadataCache);
    }
    catch (Exception e)
    {
      throw new SQLException(Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()}));
    }
    finally
    {
      cm.releaseConnectionInAutoCommit(proc, c);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.storedprocedure.on", String
            .valueOf(proc.getId()), backend.getName()));
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
        proc, EXECUTE_TASK, metadataCache);
    return task.getResult();
  }

  private static final int EXECUTE_QUERY_TASK  = 0;
  private static final int EXECUTE_UPDATE_TASK = 1;
  private static final int EXECUTE_TASK        = 2;

  /**
   * Post the stored procedure call in the threads task list.
   * <p>
   * Note that macros are also processed here.
   * 
   * @param proc the stored procedure to call
   * @param taskType one of EXECUTE_QUERY_TASK, EXECUTE_UPDATE_TASK or
   *          EXECUTE_TASK
   * @param metadataCache the metadataCache if any or null
   * @return the task that has been executed (caller can get the result by
   *         calling getResult())
   * @throws SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           stored procedure
   * @throws NoMoreBackendException if no backend was available to execute the
   *           stored procedure
   */
  private AbstractTask callStoredProcedure(StoredProcedure proc, int taskType,
      MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException, NoMoreBackendException
  {
    // Total ordering mainly for distributed virtual databases.
    boolean removeFromTotalOrderQueue = waitForTotalOrder(proc, true);

    // Handle macros
    handleMacros(proc);

    int nbOfThreads = acquireLockAndCheckNbOfThreads(proc, String.valueOf(proc
        .getId()));

    // Create the task
    AbstractTask task;
    switch (taskType)
    {
      case EXECUTE_QUERY_TASK :
        task = new CallableStatementExecuteQueryTask(getNbToWait(nbOfThreads),
            nbOfThreads, proc, metadataCache);
        break;
      case EXECUTE_UPDATE_TASK :
        task = new CallableStatementExecuteUpdateTask(getNbToWait(nbOfThreads),
            nbOfThreads, proc);
        break;
      case EXECUTE_TASK :
        task = new CallableStatementExecuteTask(getNbToWait(nbOfThreads),
            nbOfThreads, proc, metadataCache);
        break;
      default :
        throw new RuntimeException("Unhandled task type " + taskType
            + " in callStoredProcedure");
    }

    List<DatabaseBackend> backendList = getBackendsWithStoredProcedure(proc.getProcedureKey(),
        proc.getNbOfParameters(), nbOfThreads);

    if (backendList.size() == 0)
    {
      releaseLockAndUnlockNextQuery(proc);
      throw new SQLException(Translate.get(
          "loadbalancer.backend.no.required.storedprocedure", proc
              .getProcedureKey()));
    }

    task.setTotalNb(backendList.size());
    atomicTaskPostInQueueAndReleaseLock(proc, task, backendList,
        removeFromTotalOrderQueue);

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(proc.getTimeout() * 1000L, String.valueOf(proc
            .getId()), task);

      checkTaskCompletion(task);
      return task;
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
        logger.error(ex.getMessage());
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
          logger.error(Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName()));
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
        logger.error(Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName()));
        disableBackend(backend, true);
        throw new NoMoreBackendException(Translate.get(
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
      { // Connection failed, so did the transaction
        // Disable the backend.
        cm.deleteConnection(tid);
        String msg = Translate.get(
            "loadbalancer.backend.disabling.connection.failure", backend
                .getName());
        logger.error(msg);
        disableBackend(backend, true);
        throw new SQLException(msg);
      }
      catch (Throwable e)
      {
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
          logger.error(Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName()));
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
        logger.error(Translate.get(
            "loadbalancer.backend.disabling.unreachable", backend.getName()));
        disableBackend(backend, true);
        throw new NoMoreBackendException(Translate.get(
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
      { // Connection failed, so did the transaction
        // Disable the backend.
        cm.deleteConnection(tid);
        String msg = Translate.get(
            "loadbalancer.backend.disabling.connection.failure", backend
                .getName());
        logger.error(msg);
        disableBackend(backend, true);
        throw new SQLException(msg);
      }
      catch (Throwable e)
      {
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

    // Acquire the lock
    String requestDescription = "abort " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

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

    rollback(tm);
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

    // Acquire the lock
    int nbOfThreads = acquireLockAndCheckNbOfThreads(totalOrderCommit,
        "commit " + tid);

    List<DatabaseBackend> commitList = getBackendsWithStartedTransaction(new Long(tid),
        nbOfThreads);

    int nbOfThreadsToCommit = commitList.size();
    CommitTask task = null;
    if (nbOfThreadsToCommit != 0)
    {
      task = new CommitTask(getNbToWait(nbOfThreadsToCommit),
          nbOfThreadsToCommit, tm);
    }
    // FIXME waht if nbOfThreadsToCommit == 0?

    // Post the task in the non-conflicting queues.
    synchronized (enabledBackends)
    {
      for (int i = 0; i < nbOfThreadsToCommit; i++)
      {
        DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderCommit != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderCommit);

    waitForCommit(task, tm.getTimeout());
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

    // Acquire the lock
    int nbOfThreads = acquireLockAndCheckNbOfThreads(totalOrderRollback,
        "rollback " + tid);

    // Build the list of backends that need to rollback this transaction
    List<DatabaseBackend> rollbackList = getBackendsWithStartedTransaction(new Long(tid),
        nbOfThreads);

    int nbOfThreadsToRollback = rollbackList.size();
    RollbackTask task = null;
    if (nbOfThreadsToRollback != 0)
      task = new RollbackTask(getNbToWait(nbOfThreadsToRollback),
          nbOfThreadsToRollback, tm);

    // Post the task in the non-conflicting queues.
    synchronized (enabledBackends)
    {
      for (int i = 0; i < nbOfThreadsToRollback; i++)
      {
        DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderRollback != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderRollback);

    waitForRollback(task, tm.getTimeout());
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

    // Acquire the lock
    int nbOfThreads = acquireLockAndCheckNbOfThreads(totalOrderRollback,
        "rollback " + savepointName + " " + tid);

    // Build the list of backends that need to rollback this transaction
    List<DatabaseBackend> rollbackList = getBackendsWithStartedTransaction(new Long(tid),
        nbOfThreads);

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
        DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderRollback != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderRollback);

    waitForRollbackToSavepoint(task, savepointName, tm.getTimeout());
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

    // Acquire the lock
    int nbOfThreads = acquireLockAndCheckNbOfThreads(totalOrderRelease,
        "release savepoint " + savepointName + " " + tid);

    // Build the list of backends that need to rollback this transaction
    List<DatabaseBackend> savepointList = getBackendsWithStartedTransaction(new Long(tid),
        nbOfThreads);

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
        DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    if (totalOrderRelease != null)
      removeObjectFromAndNotifyTotalOrderQueue(totalOrderRelease);

    waitForReleaseSavepoint(task, savepointName, tm.getTimeout());
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
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

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

    waitForSavepoint(task, savepointName, tm.getTimeout());
  }

  /**
   * Wait for a <code>CommitTask</code> to complete
   * 
   * @param task the <code>CommitTask</code>
   * @param timeout the timeout (in milliseconds) for task completion
   * @throws SQLException if an error occurs while completing the commit task
   */
  // TODO method can be reused for RAIDb0, RAIDb1 and RAIDb2
  // FIXME the timeout is a field of the CommitTask but the getter is not
  // defined
  private void waitForCommit(CommitTask task, long timeout) throws SQLException
  {
    // Check if someone had something to commit
    if (task == null)
      return;

    long tid = task.getTransactionId();

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(timeout, "commit " + tid, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.commit.all.failed", tid));
        else
        {
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              Translate.get("loadbalancer.commit.failed.stack", tid) + "\n");
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  /**
   * Wait for a <code>RollbackTask</code> to complete
   * 
   * @param task the <code>RollbackTask</code>
   * @param timeout the timeout (in milliseconds) for task completion
   * @throws SQLException if an error occurs while completing the rollback task
   */
  // TODO method can be reused for RAIDb0, RAIDb1 and RAIDb2
  // FIXME the timeout is a field of the RollbacTask but the getter is not
  // defined
  private void waitForRollback(RollbackTask task, long timeout)
      throws SQLException
  {
    // Check if someone had something to rollback
    if (task == null)
      return;

    long tid = task.getTransactionId();

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(timeout, "rollback " + tid, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.rollback.all.failed", tid));
        else
        {
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              Translate.get("loadbalancer.rollback.failed.stack", tid) + "\n");
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  /**
   * Wait for a <code>RollbackToSavepointTask</code> to complete
   * 
   * @param task the <code>RollbackToSavepointTask</code>
   * @param savepoint the savepoint name
   * @param timeout the timeout (in milliseconds) for task completion
   * @throws SQLException if an error occurs while completing the rollback to
   *           savepoint task
   */
  // TODO method can be reused for RAIDb0, RAIDb1 and RAIDb2
  // FIXME the timeout is a field of the RollbackToSavepointTask but the getter
  // is not defined
  // FIXME the savepoint is a field of the RollbackToSavepointTask but the
  // getter is not defined
  private void waitForRollbackToSavepoint(RollbackToSavepointTask task,
      String savepoint, long timeout) throws SQLException
  {
    // Check if someone had something to rollback
    if (task == null)
      return;

    long tid = task.getTransactionId();

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(timeout, "rollback " + savepoint + " " + tid,
            task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.rollbacksavepoint.all.failed", savepoint, String
                  .valueOf(tid)));
        else
        {
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              Translate.get("loadbalancer.rollbacksavepoint.failed.stack",
                  savepoint, String.valueOf(tid))
                  + "\n");
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  /**
   * Wait for a <code>ReleaseSavepointTask</code> to complete
   * 
   * @param task the <code>ReleaseSavepointTask</code>
   * @param savepoint the savepoint name
   * @param timeout the timeout (in milliseconds) for task completion
   * @throws SQLException if an error occurs while completing the release
   *           savepoint task
   */
  // TODO method can be reused for RAIDb0, RAIDb1 and RAIDb2
  // FIXME the timeout is a field of the ReleaseSavepointTask but the getter is
  // not defined
  // FIXME the savepoint is a field of the ReleaseSavepointTask but the getter
  // is not defined
  private void waitForReleaseSavepoint(ReleaseSavepointTask task,
      String savepoint, long timeout) throws SQLException
  {
    // Check if someone had something to release
    if (task == null)
      return;

    long tid = task.getTransactionId();

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(timeout, "release savepoint " + savepoint + " "
            + tid, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.releasesavepoint.all.failed", savepoint, String
                  .valueOf(tid)));
        else
        {
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              Translate.get("loadbalancer.releasesavepoint.failed.stack",
                  savepoint, String.valueOf(tid))
                  + "\n");
          logger.error(ex.getMessage());
          throw ex;
        }
      }
    }
  }

  /**
   * Wait for a <code>SavepointTask</code> to complete
   * 
   * @param task the <code>SavepointTask</code>
   * @param savepoint the savepoint name
   * @param timeout the timeout (in milliseconds) for task completion
   * @throws SQLException if an error occurs while completing the savepoint task
   */
  // TODO method can be reused for RAIDb0, RAIDb1 and RAIDb2
  // FIXME the timeout is a field of the SavepointTask but the getter is not
  // defined
  // FIXME the savepoint is a field of the SavepointTask but the getter is not
  // defined
  private void waitForSavepoint(SavepointTask task, String savepoint,
      long timeout) throws SQLException
  {
    // Check if someone had something to release
    if (task == null)
      return;

    long tid = task.getTransactionId();

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(timeout,
            "set savepoint " + savepoint + " " + tid, task);

      if (task.getSuccess() == 0)
      { // All tasks failed
        List<?> exceptions = task.getExceptions();
        if (exceptions == null)
          throw new SQLException(Translate.get(
              "loadbalancer.setsavepoint.all.failed", savepoint, String
                  .valueOf(tid)));
        else
        {
          SQLException ex = SQLExceptionFactory.getSQLException(exceptions,
              Translate.get("loadbalancer.setsavepoint.failed.stack",
                  savepoint, String.valueOf(tid))
                  + "\n");
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
   * task in the queue of all backends.
   * 
   * @param task the task to post
   * @param writeList backend list to post the task to
   * @param removeFromTotalOrderQueue true if the query must be removed from the
   *          total order queue
   */
  private void atomicTaskPostInQueueAndReleaseLock(AbstractRequest request,
      AbstractTask task, List<DatabaseBackend> writeList, boolean removeFromTotalOrderQueue)
  {
    synchronized (enabledBackends)
    {
      int nbOfThreads = writeList.size();
      for (int i = 0; i < nbOfThreads; i++)
      {
        BackendTaskQueues queues = ((DatabaseBackend) writeList.get(i))
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
      removefromTotalOrder = waitForTotalOrder(totalOrderQueueObject, true);
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
      // Create the new backend task queues
      db.setTaskQueues(new BackendTaskQueues(db, waitForCompletionPolicy,
          this.vdb.getRequestManager()));
      db.startWorkerThreads(this);
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
        terminateThreadsAndConnections(db);
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
    db.terminateWorkerThreads();

    if (db.isInitialized())
      db.finalizeConnections();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getXmlImpl
   */
  public String getXmlImpl()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RAIDb_2 + ">");
    if (createTablePolicy != null)
      info.append(createTablePolicy.getXml());
    if (waitForCompletionPolicy != null)
      info.append(waitForCompletionPolicy.getXml());
    if (macroHandler != null)
      info.append(macroHandler.getXml());
    this.getRaidb2Xml();
    info.append("</" + DatabasesXmlTags.ELT_RAIDb_2 + ">");
    return info.toString();
  }

  /**
   * return xml formatted information about this raidb2 load balancer
   * 
   * @return xml formatted string
   */
  public abstract String getRaidb2Xml();

}