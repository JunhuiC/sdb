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
 * Contributor(s): Jaco Swart, Jean-Bernard van Zuylen
 */

package org.continuent.sequoia.controller.loadbalancer.raidb0;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
import org.continuent.sequoia.controller.loadbalancer.tasks.ClosePersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.CommitTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.OpenPersistentConnectionTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.ReleaseSavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.RollbackToSavepointTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.SavepointTask;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-0: database partitioning.
 * <p>
 * The requests are sent to the backend nodes hosting the tables needed to
 * execute the request. If no backend has the needed tables to perform a
 * request, it will fail.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jaco.swart@iblocks.co.uk">Jaco Swart </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public class RAIDb0 extends AbstractLoadBalancer
{
  //
  // How the code is organized?
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Transaction handling
  // 5. Backend management
  // 6. Debug/Monitoring
  //

  private CreateTablePolicy createTablePolicy;
  protected static Trace    logger = Trace
                                       .getLogger("org.continuent.sequoia.controller.loadbalancer.RAIDb0");

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-0 request load balancer.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param createTablePolicy the policy defining how 'create table' statements
   *          should be handled
   * @throws Exception if an error occurs
   */
  public RAIDb0(VirtualDatabase vdb, CreateTablePolicy createTablePolicy)
      throws Exception
  {
    super(vdb, RAIDbLevels.RAIDb0, ParsingGranularities.TABLE);
    this.createTablePolicy = createTablePolicy;
    this.waitForCompletionPolicy = new WaitForCompletionPolicy(
        WaitForCompletionPolicy.ALL, false, false, 0);
  }

  /*
   * Request Handling
   */

  /**
   * Performs a read request on the backend that has the needed tables to
   * executes the request.
   * 
   * @param request an <code>SelectRequest</code>
   * @param metadataCache the metadataCache if any or null
   * @return the corresponding <code>java.sql.ResultSet</code>
   * @exception SQLException if an error occurs
   * @see AbstractLoadBalancer#statementExecuteQuery(SelectRequest,
   *      MetadataCache)
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request,
      MetadataCache metadataCache) throws SQLException
  {
    try
    {
      vdb.acquireReadLockBackendLists(); // Acquire read lock
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
      throw new SQLException(msg);
    }

    try
    {
      ControllerResultSet rs = null;
      Collection fromTables = request.getFrom();

      if (fromTables == null)
        throw new SQLException(Translate.get("loadbalancer.from.not.found",
            request.getSqlShortForm(vdb.getSqlShortFormLength())));

      // Find the backend that has the needed tables
      ArrayList backends = vdb.getBackends();
      int size = backends.size();
      int enabledBackends = 0;

      DatabaseBackend backend = null;
      // The backend that will execute the query
      for (int i = 0; i < size; i++)
      {
        backend = (DatabaseBackend) backends.get(i);
        if (backend.isReadEnabled())
          enabledBackends++;
        if (backend.isReadEnabled() && backend.hasTables(fromTables))
          break;
        else
          backend = null;
      }

      if (backend == null)
      {
        if (enabledBackends == 0)
          throw new NoMoreBackendException(Translate.get(
              "loadbalancer.execute.no.backend.enabled", request.getId()));
        else
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.required.tables", fromTables.toString()));
      }

      if (logger.isDebugEnabled())
      {
        logger.debug("Backend " + backend.getName()
            + " has all tables which are:");
        for (Iterator iter = fromTables.iterator(); iter.hasNext();)
          logger.debug(iter.next());
      }

      // Execute the request on the chosen backend
      try
      {
        rs = executeRequestOnBackend(request, backend, metadataCache);
      }
      catch (SQLException se)
      {
        String msg = Translate.get("loadbalancer.request.failed", new String[]{
            String.valueOf(request.getId()), se.getMessage()});
        if (logger.isInfoEnabled())
          logger.info(msg);
        throw new SQLException(msg);
      }

      return rs;
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.fatal(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      vdb.releaseReadLockBackendLists(); // Release the lock
    }
  }

  /**
   * Performs a write request on the backend that has the needed tables to
   * executes the request.
   * 
   * @param request an <code>AbstractWriteRequest</code>
   * @return number of rows affected by the request
   * @exception SQLException if an error occurs
   */
  public ExecuteUpdateResult statementExecuteUpdate(AbstractWriteRequest request)
      throws SQLException
  {
    // Handle macros
    handleMacros(request);

    try
    {
      vdb.acquireReadLockBackendLists(); // Acquire read lock
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
      throw new SQLException(msg);
    }

    boolean success = false;
    try
    {
      // Log lazy begin if needed
      if (request.isLazyTransactionStart())
        this.vdb.getRequestManager().logLazyTransactionBegin(
            request.getTransactionId());

      // Log request
      if (recoveryLog != null)
        recoveryLog.logRequestExecuting(request);

      String table = request.getTableName();
      AbstractConnectionManager cm = null;

      if (table == null)
        throw new SQLException(Translate.get(
            "loadbalancer.request.target.table.not.found", request
                .getSqlShortForm(vdb.getSqlShortFormLength())));

      // Find the backend that has the needed table
      ArrayList backends = vdb.getBackends();
      int size = backends.size();

      DatabaseBackend backend = null;
      // The backend that will execute the query
      if (request.isCreate())
      { // Choose the backend according to the defined policy
        CreateTableRule rule = createTablePolicy.getTableRule(request
            .getTableName());
        if (rule == null)
          rule = createTablePolicy.getDefaultRule();

        // Ask the rule to pickup a backend
        ArrayList choosen;
        try
        {
          choosen = rule.getBackends(backends);
        }
        catch (CreateTableException e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.create.table.rule.failed", e.getMessage()));
        }

        // Get the connection manager from the chosen backend
        if (choosen != null)
          backend = (DatabaseBackend) choosen.get(0);
        if (backend != null)
          cm = backend.getConnectionManager(request.getLogin());
      }
      else
      { // Find the backend that has the table
        for (int i = 0; i < size; i++)
        {
          backend = (DatabaseBackend) backends.get(i);
          if ((backend.isWriteEnabled() || backend.isDisabling())
              && backend.hasTable(table))
          {
            cm = backend.getConnectionManager(request.getLogin());
            break;
          }
        }
      }

      // Sanity check
      if (cm == null)
        throw new SQLException(Translate.get(
            "loadbalancer.backend.no.required.table", table));

      // Ok, let's execute the query

      if (request.isAutoCommit())
      {
        // We do not execute request outside the already open transactions if we
        // are disabling the backend.
        if (!backend.canAcceptTasks(request))
          throw new SQLException(Translate.get(
              "loadbalancer.backend.is.disabling", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName()}));

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
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        ExecuteUpdateResult result;
        try
        {
          result = executeStatementExecuteUpdateOnBackend(request, backend,
              null, c);
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get("loadbalancer.request.failed",
              new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  e.getMessage()}));
        }
        finally
        {
          cm.releaseConnectionInAutoCommit(request, c);
        }
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("loadbalancer.execute.on", new String[]{
              String.valueOf(request.getId()), backend.getName()}));
        return result;
      }
      else
      { // Inside a transaction
        Connection c;
        long tid = request.getTransactionId();

        try
        {
          c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(request,
              cm);
        }
        catch (UnreachableBackendException e1)
        {
          logger.error(Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName()));
          disableBackend(backend, true);
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }
        catch (NoTransactionStartWhenDisablingException e)
        {
          String msg = Translate.get("loadbalancer.backend.is.disabling",
              new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName()});
          logger.error(msg);
          throw new SQLException(msg);
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection", new String[]{
                  String.valueOf(tid), backend.getName()}));

        // Execute the query
        ExecuteUpdateResult result;
        try
        {
          result = executeStatementExecuteUpdateOnBackend(request, backend,
              null, cm.retrieveConnectionForTransaction(tid));
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get("loadbalancer.request.failed",
              new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  e.getMessage()}));
        }
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("loadbalancer.execute.on", new String[]{
              String.valueOf(request.getId()), backend.getName()}));
        success = true;
        return result;
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.fatal(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      vdb.releaseReadLockBackendLists(); // Release the lock
      if (!success)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());
    }
  }

  /**
   * @see AbstractLoadBalancer#statementExecuteUpdateWithKeys(AbstractWriteRequest,
   *      MetadataCache)
   */
  public GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request, MetadataCache metadataCache)
      throws SQLException
  {
    // Handle macros
    handleMacros(request);

    try
    {
      vdb.acquireReadLockBackendLists(); // Acquire
      // read
      // lock
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
      throw new SQLException(msg);
    }

    boolean success = false;
    try
    {
      // Log lazy begin if needed
      if (request.isLazyTransactionStart())
        this.vdb.getRequestManager().logLazyTransactionBegin(
            request.getTransactionId());

      // Log request
      if (recoveryLog != null)
        recoveryLog.logRequestExecuting(request);

      String table = request.getTableName();
      AbstractConnectionManager cm = null;

      if (table == null)
        throw new SQLException(Translate.get(
            "loadbalancer.request.target.table.not.found", request
                .getSqlShortForm(vdb.getSqlShortFormLength())));

      // Find the backend that has the needed table
      ArrayList backends = vdb.getBackends();
      int size = backends.size();

      DatabaseBackend backend = null;
      // The backend that will execute the query
      if (request.isCreate())
      { // Choose the backend according to the defined policy
        CreateTableRule rule = createTablePolicy.getTableRule(request
            .getTableName());
        if (rule == null)
          rule = createTablePolicy.getDefaultRule();

        // Ask the rule to pickup a backend
        ArrayList choosen;
        try
        {
          choosen = rule.getBackends(backends);
        }
        catch (CreateTableException e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.create.table.rule.failed", e.getMessage()));
        }

        // Get the connection manager from the chosen backend
        if (choosen != null)
          backend = (DatabaseBackend) choosen.get(0);
        if (backend != null)
          cm = backend.getConnectionManager(request.getLogin());
      }
      else
      { // Find the backend that has the table
        for (int i = 0; i < size; i++)
        {
          backend = (DatabaseBackend) backends.get(i);
          if ((backend.isWriteEnabled() || backend.isDisabling())
              && backend.hasTable(table))
          {
            cm = backend.getConnectionManager(request.getLogin());
            break;
          }
        }
      }

      // Sanity check
      if (cm == null)
        throw new SQLException(Translate.get(
            "loadbalancer.backend.no.required.table", table));

      if (!backend.getDriverCompliance().supportGetGeneratedKeys())
        throw new SQLException(Translate.get(
            "loadbalancer.backend.autogeneratedkeys.unsupported", backend
                .getName()));

      // Ok, let's execute the query

      if (request.isAutoCommit())
      {
        // We do not execute request outside the already open transactions if we
        // are disabling the backend.
        if (!backend.canAcceptTasks(request))
          throw new SQLException(Translate.get(
              "loadbalancer.backend.is.disabling", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName()}));

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
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        GeneratedKeysResult result;
        try
        {
          result = executeStatementExecuteUpdateWithKeysOnBackend(request,
              backend, null, c, metadataCache);
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get("loadbalancer.request.failed",
              new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  e.getMessage()}));
        }
        finally
        {
          backend.removePendingRequest(request);
          cm.releaseConnectionInAutoCommit(request, c);
        }
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("loadbalancer.execute.on", new String[]{
              String.valueOf(request.getId()), backend.getName()}));
        return result;
      }
      else
      { // Inside a transaction
        Connection c;
        long tid = request.getTransactionId();

        try
        {
          c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(request,
              cm);
        }
        catch (UnreachableBackendException e1)
        {
          logger.error(Translate.get(
              "loadbalancer.backend.disabling.unreachable", backend.getName()));
          disableBackend(backend, true);
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }
        catch (NoTransactionStartWhenDisablingException e)
        {
          String msg = Translate.get("loadbalancer.backend.is.disabling",
              new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName()});
          logger.error(msg);
          throw new SQLException(msg);
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection", new String[]{
                  String.valueOf(tid), backend.getName()}));

        // Execute the query
        GeneratedKeysResult result;
        try
        {
          result = executeStatementExecuteUpdateWithKeysOnBackend(request,
              backend, null, cm.retrieveConnectionForTransaction(tid),
              metadataCache);
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get("loadbalancer.request.failed",
              new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  e.getMessage()}));
        }
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("loadbalancer.execute.on", new String[]{
              String.valueOf(request.getId()), backend.getName()}));
        success = true;
        return result;
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.fatal(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      vdb.releaseReadLockBackendLists(); // Release the lock
      if (!success)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());
    }
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
        "Statement.execute() is currently not supported with RAIDb-0");
  }

  /**
   * Execute a read request on the selected backend.
   * 
   * @param request the request to execute
   * @param backend the backend that will execute the request
   * @param metadataCache the metadataCache if any or null
   * @return the ControllerResultSet if (!success)
   *         recoveryLog.logRequestCompletion(request.getLogId(), false,
   *         request.getExecTimeInMs());
   * @throws SQLException if an error occurs
   */
  protected ControllerResultSet executeRequestOnBackend(SelectRequest request,
      DatabaseBackend backend, MetadataCache metadataCache) throws SQLException
  {
    // Handle macros
    handleMacros(request);

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
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backend.getName()));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        try
        {
          rs = executeStatementExecuteQueryOnBackend(request, backend, null, c
              .getConnection(), metadataCache);
          cm.releaseConnectionInAutoCommit(request, c);
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
        rs = executeStatementExecuteQueryOnBackend(request, backend, null, c,
            metadataCache);
      }
      catch (BadConnectionException e)
      { // Get rid of the bad connection
        cm.deleteConnection(tid);
        throw new SQLException(Translate
            .get("loadbalancer.connection.failed", new String[]{
                String.valueOf(tid), backend.getName(), e.getMessage()}));
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
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    throw new SQLException(
        "Stored procedure calls are not supported with RAIDb-0 load balancers.");
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(org.continuent.sequoia.controller.requests.StoredProcedure,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public ExecuteResult readOnlyCallableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    throw new SQLException(
        "Stored procedure calls are not supported with RAIDb-0 load balancers.");
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet callableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    throw new SQLException(
        "Stored procedure calls are not supported with RAIDb-0 load balancers.");
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecuteUpdate(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public ExecuteUpdateResult callableStatementExecuteUpdate(StoredProcedure proc)
      throws SQLException
  {
    throw new SQLException(
        "Stored procedure calls are not supported with RAIDb-0 load balancers.");
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecute(org.continuent.sequoia.controller.requests.StoredProcedure,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public ExecuteResult callableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws AllBackendsFailedException,
      SQLException
  {
    throw new SQLException(
        "Stored procedure calls are not supported with RAIDb-0 load balancers.");
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
    rollback(tm);
  }

  /**
   * Begins a new transaction.
   * 
   * @param tm the transaction marker metadata
   * @throws SQLException if an error occurs
   */
  public final void begin(TransactionMetaData tm) throws SQLException
  {
  }

  /**
   * Commits a transaction.
   * 
   * @param tm the transaction marker metadata
   * @throws SQLException if an error occurs
   */
  public void commit(TransactionMetaData tm) throws SQLException
  {
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logCommit(tm);

    // Acquire the lock
    String requestDescription = "commit " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

    // Build the list of backends that need to commit this transaction
    ArrayList commitList = new ArrayList(nbOfThreads);
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      if (backend.isStartedTransaction(lTid))
        commitList.add(backend);
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
        DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Check if someone had something to commit
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed

        // Notify failure in recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(logId, false, 0);

        List exceptions = task.getExceptions();
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
   * @throws SQLException if an error occurs
   */
  public void rollback(TransactionMetaData tm) throws SQLException
  {
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logRollback(tm);

    // Acquire the lock
    String requestDescription = "rollback " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

    // Build the list of backends that need to rollback this transaction
    ArrayList rollbackList = new ArrayList();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      if (backend.isStartedTransaction(lTid))
        rollbackList.add(backend);
    }

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

    // Check if someone had something to rollback
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed

        // Notify failure in recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(logId, false, 0);

        List exceptions = task.getExceptions();
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

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logRollbackToSavepoint(tm, savepointName);

    // Acquire the lock
    String requestDescription = "rollback " + savepointName + " " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

    // Build the list of backends that need to rollback this transaction
    ArrayList rollbackList = new ArrayList();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      if (backend.isStartedTransaction(lTid))
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
        DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Check if someone had something to rollback
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed

        // Notify failure in recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(logId, false, 0);

        List exceptions = task.getExceptions();
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

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logReleaseSavepoint(tm, savepointName);

    // Acquire the lock
    String requestDescription = "release savepoint " + savepointName + " "
        + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

    // Build the list of backends that need to rollback this transaction
    ArrayList savepointList = new ArrayList();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      if (backend.isStartedTransaction(lTid))
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
        DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
        backend.getTaskQueues().addTaskToBackendTotalOrderQueue(task);
      }
    }

    // Release the lock
    backendListLock.releaseRead();

    // Check if someone had something to release
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed

        // Notify failure in recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(logId, false, 0);

        List exceptions = task.getExceptions();
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
    Long lTid = new Long(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logSetSavepoint(tm, savepointName);

    // Acquire the lock
    String requestDescription = "set savepoint " + savepointName + " " + tid;
    int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

    // Build the list of backends that need to rollback this transaction
    ArrayList savepointList = new ArrayList();
    for (int i = 0; i < nbOfThreads; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) enabledBackends.get(i);
      if (backend.isStartedTransaction(lTid))
        savepointList.add(backend);
    }

    int nbOfSavepoints = savepointList.size();
    SavepointTask task = null;
    if (nbOfSavepoints != 0)
      task = new SavepointTask(getNbToWait(nbOfThreads), nbOfThreads, tm,
          savepointName);

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

    // Check if someone had something to release
    if (task == null)
      return;

    synchronized (task)
    {
      if (!task.hasCompleted())
        waitForTaskCompletion(tm.getTimeout(), requestDescription, task);

      if (task.getSuccess() == 0)
      { // All tasks failed

        // Notify failure in recovery log
        if (recoveryLog != null)
          recoveryLog.logRequestCompletion(logId, false, 0);

        List exceptions = task.getExceptions();
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
    ClosePersistentConnectionTask task = null;
    try
    {
      int nbOfThreads = acquireLockAndCheckNbOfThreads(null, requestDescription);

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
    }
    finally
    {
      if (task == null)
      { // Happens if we had a NoMoreBackendException
        task = new ClosePersistentConnectionTask(0, 0, login,
            persistentConnectionId);
      }
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
  public void enableBackend(DatabaseBackend db, boolean writeEnabled)
      throws SQLException
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
      if (forceDisable)
        db.shutdownConnectionManagers();
      db.terminateWorkerThreads();

      if (db.isInitialized())
        db.finalizeConnections();

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
        enabledBackends.remove(db);
        if (enabledBackends.isEmpty())
        {
          // Cleanup schema for any remaining locks
          this.vdb.getRequestManager().setDatabaseSchema(null, false);
        }
      }
      backendListLock.releaseWrite();
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#setWeight(String,
   *      int)
   */
  public void setWeight(String name, int w) throws SQLException
  {
    throw new SQLException("Weight is not supported with this load balancer");
  }

  /*
   * Debug/Monitoring
   */

  /**
   * Get information about the Request load balancer
   * 
   * @return <code>String</code> containing information
   */
  public String getInformation()
  {
    return "RAIDb-0 Request load balancer\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getXmlImpl
   */
  public String getXmlImpl()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RAIDb_0 + ">");
    createTablePolicy.getXml();
    info.append("</" + DatabasesXmlTags.ELT_RAIDb_0 + ">");
    return info.toString();
  }

}