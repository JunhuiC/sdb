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
 * Contributor(s): Vadim Kassin, Jaco Swart, Jean-Bernard van Zuylen.
 */

package org.continuent.sequoia.controller.loadbalancer.singledb;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;

import org.continuent.sequoia.common.exceptions.BadConnectionException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
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
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.requests.UnknownReadRequest;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * Single Database request load balancer.
 * <p>
 * The requests coming from the request controller are directly forwarded to the
 * single backend. This load balancer does not support multiple backends and
 * does not update the recovery log (it is not meant to be used with a recovery
 * log).
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:vadim@kase.kz">Vadim Kassin </a>
 * @author <a href="mailto:jaco.swart@iblocks.co.uk">Jaco Swart </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public class SingleDB extends AbstractLoadBalancer
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

  private DatabaseBackend backend;

  private static Trace    logger        = Trace
                                            .getLogger("org.continuent.sequoia.controller.loadbalancer.SingleDB");

  static Trace            endUserLogger = Trace
                                            .getLogger("org.continuent.sequoia.enduser");

  /*
   * Constructors
   */

  /**
   * Creates a new <code>SingleDB</code> instance.
   * 
   * @param vdb the <code>VirtualDatabase</code> this load balancer belongs to
   * @throws Exception if there is not exactly one backend attached to the
   *           <code>VirtualDatabase</code>
   */
  public SingleDB(VirtualDatabase vdb) throws Exception
  {
    // We don't need to parse the requests, just send them to the backend
    super(vdb, RAIDbLevels.SingleDB, ParsingGranularities.NO_PARSING);
    this.waitForCompletionPolicy = new WaitForCompletionPolicy(
        WaitForCompletionPolicy.ALL, false, false, ParsingGranularities.NO_PARSING);
  }

  /*
   * Request Handling
   */

  /**
   * Performs a read request. It is up to the implementation to choose to which
   * backend node(s) this request should be sent.
   * 
   * @param request an <code>SelectRequest</code>
   * @param metadataCache MetadataCache (null if none)
   * @return the corresponding <code>java.sql.ResultSet</code>
   * @exception SQLException if an error occurs
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request,
      MetadataCache metadataCache) throws SQLException
  {
    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.available", request.getId()));

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(request
          .getLogin());
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
            String backendName = backend.getName();
            String msg = Translate.get(
                "loadbalancer.backend.disabling.unreachable", backendName);
            logger.error(msg);
            endUserLogger.error(msg);
            disableBackend(backend, true);
            backend = null;
            throw new SQLException(Translate.get(
                "loadbalancer.backend.unreacheable", backendName));
          }

          // Sanity check
          if (c == null)
            throw new SQLException(Translate.get(
                "loadbalancer.backend.no.connection", backend.getName()));

          // Execute Query
          try
          {
            rs = executeStatementExecuteQueryOnBackend(request, backend, null,
                c.getConnection(), metadataCache);
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
            cm.releaseConnectionInAutoCommit(request, c);
            throw new SQLException(Translate.get(
                "loadbalancer.request.failed.on.backend", new String[]{
                    request.getSqlShortForm(vdb.getSqlShortFormLength()),
                    backend.getName(), e.getMessage()}));
          }
        }
        while (badConnection);
        return rs;
      }
      else
      {
        long tid = request.getTransactionId();
        // Re-use the connection used by this transaction
        PooledConnection c = cm.retrieveConnectionForTransaction(tid);

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection", new String[]{
                  String.valueOf(tid), backend.getName()}));

        // Execute Query
        ControllerResultSet rs = null;
        try
        {
          rs = executeStatementExecuteQueryOnBackend(request, backend, null, c
              .getConnection(), metadataCache);
        }
        catch (SQLException e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        catch (BadConnectionException e)
        { // Get rid of the bad connection
          cm.deleteConnection(tid);
          throw new SQLException(Translate.get(
              "loadbalancer.connection.failed", new String[]{
                  String.valueOf(tid), backend.getName(), e.getMessage()}));
        }
        catch (Throwable e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        return rs;
      }
    }
    catch (RuntimeException e)
    {
      String msg = "Request '"
          + request.getSqlShortForm(vdb.getSqlShortFormLength())
          + "' failed on backend " + backend.getURL() + " (" + e + ")";
      logger.fatal(msg, e);
      throw new SQLException(msg);
    }
  }

  /**
   * Performs a write request on the backend.
   * 
   * @param request an <code>AbstractWriteRequest</code>
   * @return number of rows affected by the request
   * @exception SQLException if an error occurs
   */
  public ExecuteUpdateResult statementExecuteUpdate(AbstractWriteRequest request)
      throws SQLException
  {
    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.available", request.getId()));

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(request
          .getLogin());
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
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        ExecuteUpdateResult result;
        try
        {
          result = executeStatementExecuteUpdateOnBackend(request, backend,
              null, c);
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        finally
        {
          cm.releaseConnectionInAutoCommit(request, c);
        }
        return result;
      }
      else
      { // Re-use the connection used by this transaction
        PooledConnection c = cm.retrieveConnectionForTransaction(request
            .getTransactionId());

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection",
              new String[]{String.valueOf(request.getTransactionId()),
                  backend.getName()}));

        // Execute Query
        try
        {
          ExecuteUpdateResult result = executeStatementExecuteUpdateOnBackend(
              request, backend, null, c);
          return result;
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      endUserLogger.fatal(msg);
      logger.fatal(msg, e);
      throw new SQLException(msg);
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
    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.available", request.getId()));

    if (!backend.getDriverCompliance().supportGetGeneratedKeys())
      throw new SQLException(Translate.get(
          "loadbalancer.backend.autogeneratedkeys.unsupported", backend
              .getName()));

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(request
          .getLogin());
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
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
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
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        finally
        {
          cm.releaseConnectionInAutoCommit(request, c);
        }
        return result;
      }
      else
      {
        // Re-use the connection used by this transaction
        PooledConnection c = cm.retrieveConnectionForTransaction(request
            .getTransactionId());

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection",
              new String[]{String.valueOf(request.getTransactionId()),
                  backend.getName()}));

        // Execute Query
        try
        {
          GeneratedKeysResult result = executeStatementExecuteUpdateWithKeysOnBackend(
              request, backend, null, c, metadataCache);
          return result;
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        finally
        {
          backend.removePendingRequest(request);
        }
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#statementExecute(org.continuent.sequoia.controller.requests.AbstractRequest,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public ExecuteResult statementExecute(AbstractRequest request,
      MetadataCache metadataCache) throws SQLException
  {
    if (backend == null)
      throw new SQLException("No available backend to execute request "
          + request.getId());

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(request
          .getLogin());
      if (request.isAutoCommit())
      { // Use a connection just for this request
        PooledConnection c = null;
        try
        {
          c = cm.retrieveConnectionInAutoCommit(request);
        }
        catch (UnreachableBackendException e1)
        {
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        ExecuteResult rs = null;
        try
        {
          rs = AbstractLoadBalancer.executeStatementExecuteOnBackend(request,
              backend, null, c, metadataCache);
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
        finally
        {
          cm.releaseConnectionInAutoCommit(request, c);
        }
        return rs;
      }
      else
      { // Re-use the connection used by this transaction
        PooledConnection c = cm.retrieveConnectionForTransaction(request
            .getTransactionId());

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection",
              new String[]{String.valueOf(request.getTransactionId()),
                  backend.getName()}));

        // Execute Query
        try
        {
          return AbstractLoadBalancer.executeStatementExecuteOnBackend(request,
              backend, null, c, metadataCache);
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend", new String[]{
                  request.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    return callableStatementExecuteQuery(proc, metadataCache);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(org.continuent.sequoia.controller.requests.StoredProcedure,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public ExecuteResult readOnlyCallableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    return callableStatementExecute(proc, metadataCache);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet callableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    if (backend == null)
      throw new SQLException(
          "No available backend to execute stored procedure " + proc.getId());

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(proc
          .getLogin());
      if (proc.isAutoCommit())
      { // Use a connection just for this request
        PooledConnection c = null;
        try
        {
          c = cm.retrieveConnectionInAutoCommit(proc);
        }
        catch (UnreachableBackendException e1)
        {
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        ControllerResultSet rs = null;
        try
        {
          rs = AbstractLoadBalancer
              .executeCallableStatementExecuteQueryOnBackend(proc, backend,
                  null, c.getConnection(), metadataCache);
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
        }
        return rs;
      }
      else
      { // Re-use the connection used by this transaction
        PooledConnection c = cm.retrieveConnectionForTransaction(proc
            .getTransactionId());

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection", new String[]{
                  String.valueOf(proc.getTransactionId()), backend.getName()}));

        // Execute Query
        try
        {
          ControllerResultSet result = AbstractLoadBalancer
              .executeCallableStatementExecuteQueryOnBackend(proc, backend,
                  null, c.getConnection(), metadataCache);
          return result;
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.storedprocedure.failed.on.backend", new String[]{
                  proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecuteUpdate(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public ExecuteUpdateResult callableStatementExecuteUpdate(StoredProcedure proc)
      throws SQLException
  {
    if (backend == null)
      throw new SQLException(
          "No available backend to execute stored procedure " + proc.getId());

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(proc
          .getLogin());
      if (proc.isAutoCommit())
      { // Use a connection just for this request
        PooledConnection c = null;
        try
        {
          c = cm.retrieveConnectionInAutoCommit(proc);
        }
        catch (UnreachableBackendException e1)
        {
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        ExecuteUpdateResult result;
        try
        {
          result = AbstractLoadBalancer
              .executeCallableStatementExecuteUpdateOnBackend(proc, backend,
                  null, c);
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
        }
        return result;
      }
      else
      { // Re-use the connection used by this transaction
        PooledConnection c = cm.retrieveConnectionForTransaction(proc
            .getTransactionId());

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection", new String[]{
                  String.valueOf(proc.getTransactionId()), backend.getName()}));

        // Execute Query
        try
        {
          ExecuteUpdateResult result = AbstractLoadBalancer
              .executeCallableStatementExecuteUpdateOnBackend(proc, backend,
                  null, c);
          return result;
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.storedprocedure.failed.on.backend", new String[]{
                  proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecute(StoredProcedure,
   *      MetadataCache)
   */
  public ExecuteResult callableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    if (backend == null)
      throw new SQLException(
          "No available backend to execute stored procedure " + proc.getId());

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(proc
          .getLogin());
      if (proc.isAutoCommit())
      { // Use a connection just for this request
        PooledConnection c = null;
        try
        {
          c = cm.retrieveConnectionInAutoCommit(proc);
        }
        catch (UnreachableBackendException e1)
        {
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
        }

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        ExecuteResult rs = null;
        try
        {
          rs = AbstractLoadBalancer.executeCallableStatementExecuteOnBackend(
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
        }
        return rs;
      }
      else
      { // Re-use the connection used by this transaction
        PooledConnection c = cm.retrieveConnectionForTransaction(proc
            .getTransactionId());

        // Sanity check
        if (c == null)
          throw new SQLException(Translate.get(
              "loadbalancer.unable.retrieve.connection", new String[]{
                  String.valueOf(proc.getTransactionId()), backend.getName()}));

        // Execute Query
        try
        {
          ExecuteResult result = AbstractLoadBalancer
              .executeCallableStatementExecuteOnBackend(proc, backend, null, c,
                  metadataCache);
          return result;
        }
        catch (Exception e)
        {
          throw new SQLException(Translate.get(
              "loadbalancer.storedprocedure.failed.on.backend", new String[]{
                  proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                  backend.getName(), e.getMessage()}));
        }
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getPreparedStatementGetMetaData(AbstractRequest)
   */
  public ControllerResultSet getPreparedStatementGetMetaData(
      AbstractRequest request) throws SQLException
  {
    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.available", request
              .getSqlOrTemplate()));

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(request
          .getLogin());
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
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
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
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend",
              new String[]{request.getSqlOrTemplate(), backend.getName(),
                  e.getMessage()}));
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
              "loadbalancer.request.failed.on.backend",
              new String[]{request.getSqlOrTemplate(), backend.getName(),
                  e.getMessage()}));
        }
      }
      while (badConnection);
      return rs;
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.getmetadata.failed",
          new String[]{request.getSqlOrTemplate(), backend.getURL(),
              e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getPreparedStatementGetParameterMetaData(AbstractRequest)
   *      TODO: factorize this code with at least
   *      {@link #getPreparedStatementGetMetaData(AbstractRequest)}
   *      (reconnection code makes it non-straightforward)
   */
  public ParameterMetaData getPreparedStatementGetParameterMetaData(
      AbstractRequest request) throws SQLException
  {
    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.available", request
              .getSqlOrTemplate()));

    try
    {
      AbstractConnectionManager cm = backend.getConnectionManager(request
          .getLogin());
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
          String backendName = backend.getName();
          String msg = Translate.get(
              "loadbalancer.backend.disabling.unreachable", backendName);
          logger.error(msg);
          endUserLogger.error(msg);
          disableBackend(backend, true);
          backend = null;
          throw new SQLException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
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
          throw new SQLException(Translate.get(
              "loadbalancer.request.failed.on.backend",
              new String[]{request.getSqlOrTemplate(), backend.getName(),
                  e.getMessage()}));
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
              "loadbalancer.request.failed.on.backend",
              new String[]{request.getSqlOrTemplate(), backend.getName(),
                  e.getMessage()}));
        }
      }
      while (badConnection);
      return pmd;
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.getmetadata.failed",
          new String[]{request.getSqlOrTemplate(), backend.getURL(),
              e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
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
   * @exception SQLException if an error occurs
   */
  public void begin(TransactionMetaData tm) throws SQLException
  {
    if (backend == null)
      throw new SQLException("No available backend to begin transaction "
          + tm.getTransactionId());

    // We do not accept new transactions if we are disabling the backend
    if (!backend.canAcceptTasks(null))
      throw new SQLException(Translate.get("loadbalancer.backend.is.disabling",
          new String[]{"begin transaction " + tm.getTransactionId(),
              backend.getName()}));

    try
    {
      PooledConnection c = backend.getConnectionManager(tm.getLogin())
          .getConnectionForTransaction(tm.getTransactionId());

      if (c == null)
        throw new SQLException(Translate.get(
            "loadbalancer.backend.no.connection", backend.getName()));

      c.getConnection().setAutoCommit(false);
    }
    catch (Exception e)
    {
      throw new SQLException("Begin of transaction " + tm.getTransactionId()
          + " failed on backend " + backend.getURL() + " (" + e + ")");
    }
  }

  /**
   * Commits a transaction.
   * 
   * @param tm the transaction marker metadata
   * @exception SQLException if an error occurs
   */
  public void commit(TransactionMetaData tm) throws SQLException
  {
    if (backend == null)
      throw new SQLException("No available backend to commit transaction "
          + tm.getTransactionId());

    try
    {
      AbstractConnectionManager cm = backend
          .getConnectionManager(tm.getLogin());
      PooledConnection pc = cm.retrieveConnectionForTransaction(tm
          .getTransactionId());

      if (pc == null)
        throw new SQLException("No connection found for transaction "
            + tm.getTransactionId());

      try
      {
        Connection c = pc.getConnection();
        c.commit();
        c.setAutoCommit(true);
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get("loadbalancer.commit.failed",
            new String[]{String.valueOf(tm.getTransactionId()),
                backend.getName(), e.getMessage()}));
      }
      finally
      {
        cm.releaseConnectionForTransaction(tm.getTransactionId());
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.commit.failed", new String[]{
          String.valueOf(tm.getTransactionId()), backend.getName(),
          e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
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
    if (backend == null)
      throw new SQLException("No available backend to rollback transaction "
          + tm.getTransactionId());

    try
    {
      AbstractConnectionManager cm = backend
          .getConnectionManager(tm.getLogin());
      PooledConnection pc = cm.retrieveConnectionForTransaction(tm
          .getTransactionId());

      if (pc == null)
        throw new SQLException("No connection found for transaction "
            + tm.getTransactionId());

      try
      {
        Connection c = pc.getConnection();
        c.rollback();
        c.setAutoCommit(true);
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get("loadbalancer.rollback.failed",
            new String[]{String.valueOf(tm.getTransactionId()),
                backend.getName(), e.getMessage()}));
      }
      finally
      {
        cm.releaseConnectionForTransaction(tm.getTransactionId());
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.rollback.failed", new String[]{
          String.valueOf(tm.getTransactionId()), backend.getName(),
          e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
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
    if (backend == null)
      throw new SQLException("No available backend to rollback transaction "
          + tm.getTransactionId());

    try
    {
      AbstractConnectionManager cm = backend
          .getConnectionManager(tm.getLogin());
      PooledConnection c = cm.retrieveConnectionForTransaction(tm
          .getTransactionId());

      if (c == null)
        throw new SQLException("No connection found for transaction "
            + tm.getTransactionId());

      Savepoint savepoint = backend.getSavepoint(
          new Long(tm.getTransactionId()), savepointName);

      if (savepoint == null)
        throw new SQLException("No savepoint with name " + savepointName
            + " has been found for transaction " + tm.getTransactionId());

      try
      {
        c.getConnection().rollback(savepoint);
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get(
            "loadbalancer.rollbacksavepoint.failed", new String[]{
                savepointName, String.valueOf(tm.getTransactionId()),
                backend.getName(), e.getMessage()}));
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.rollbacksavepoint.failed",
          new String[]{savepointName, String.valueOf(tm.getTransactionId()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
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
    if (backend == null)
      throw new SQLException("No available backend to release savepoint from "
          + " transaction " + tm.getTransactionId());

    try
    {
      AbstractConnectionManager cm = backend
          .getConnectionManager(tm.getLogin());
      PooledConnection c = cm.retrieveConnectionForTransaction(tm
          .getTransactionId());

      if (c == null)
        throw new SQLException("No connection found for transaction "
            + tm.getTransactionId());

      Savepoint savepoint = backend.getSavepoint(
          new Long(tm.getTransactionId()), savepointName);

      if (savepoint == null)
        throw new SQLException("No savepoint with name " + savepointName
            + " has been " + "found for transaction " + tm.getTransactionId());

      try
      {
        c.getConnection().releaseSavepoint(savepoint);
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get(
            "loadbalancer.releasesavepoint.failed", new String[]{savepointName,
                String.valueOf(tm.getTransactionId()), backend.getName(),
                e.getMessage()}));
      }
      finally
      {
        backend.removeSavepoint(new Long(tm.getTransactionId()), savepoint);
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.releasesavepoint.failed",
          new String[]{savepointName, String.valueOf(tm.getTransactionId()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * Set a savepoint to a transaction.
   * 
   * @param tm The transaction marker metadata
   * @param savepointName The name of the new savepoint
   * @throws AllBackendsFailedException if no backend succeeded in setting the
   *           savepoint.
   * @throws SQLException if an error occurs
   */
  public void setSavepoint(TransactionMetaData tm, String savepointName)
      throws AllBackendsFailedException, SQLException
  {
    if (backend == null)
      throw new SQLException("No available backend to set savepoint to "
          + " transaction " + tm.getTransactionId());

    try
    {
      AbstractConnectionManager cm = backend
          .getConnectionManager(tm.getLogin());
      PooledConnection c = cm.retrieveConnectionForTransaction(tm
          .getTransactionId());

      if (c == null)
        throw new SQLException("No connection found for transaction "
            + tm.getTransactionId());

      Savepoint savepoint = null;
      try
      {
        savepoint = c.getConnection().setSavepoint(savepointName);
      }
      catch (SQLException e)
      {
        throw new SQLException(Translate.get(
            "loadbalancer.setsavepoint.failed", new String[]{savepointName,
                String.valueOf(tm.getTransactionId()), backend.getName(),
                e.getMessage()}));
      }
      finally
      {
        if (savepoint != null)
          backend.addSavepoint(new Long(tm.getTransactionId()), savepoint);
      }
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.setsavepoint.failed",
          new String[]{savepointName, String.valueOf(tm.getTransactionId()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#closePersistentConnection(java.lang.String,
   *      long)
   */
  public void closePersistentConnection(String login,
      long persistentConnectionId) throws SQLException
  {
    AbstractConnectionManager cm = backend.getConnectionManager(login);
    if (cm != null)
    {
      // Release the connection if it exists
      cm.releasePersistentConnectionInAutoCommit(persistentConnectionId);
      backend.removePersistentConnection(persistentConnectionId);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#openPersistentConnection(String,
   *      long)
   */
  public void openPersistentConnection(String login, long persistentConnectionId)
      throws SQLException
  {
    AbstractConnectionManager cm = backend.getConnectionManager(login);
    if (cm == null)
    {
      throw new SQLException("No connection manager found for user " + login);
    }

    // Get a new connection
    AbstractRequest request = new UnknownReadRequest("", false, 0, "");
    request.setLogin(login);
    request.setPersistentConnection(true);
    request.setPersistentConnectionId(persistentConnectionId);
    try
    {
      PooledConnection c = cm.retrieveConnectionInAutoCommit(request);
      backend.addPersistentConnection(request.getPersistentConnectionId(), c);
    }
    catch (UnreachableBackendException e)
    {
      throw new SQLException(
          "Backend is not reachable to open persistent conenction "
              + persistentConnectionId);
    }
  }

  /*
   * Backends management
   */

  /**
   * Enables a backend that was previously disabled. Asks the corresponding
   * connection manager to initialize the connections if needed.
   * 
   * @param db the database backend to enable
   * @param writeEnabled True if the backend must be enabled for writes
   * @throws SQLException if an error occurs
   */
  public void enableBackend(DatabaseBackend db, boolean writeEnabled)
      throws SQLException
  {
    if (backend != null)
    {
      if (backend.isReadEnabled())
        throw new SQLException(
            "SingleDB load balancer accepts only one backend and "
                + backend.getName() + " is already enabled. Skipping "
                + db.getName() + " initialization.");
    }
    backend = db;
    logger.info(Translate.get("loadbalancer.backend.enabling", db.getName()));
    if (!backend.isInitialized())
      backend.initializeConnections();
    backend.enableRead();
    if (writeEnabled)
      backend.enableWrite();
  }

  /**
   * Disables a backend that was previously enabled. Asks the corresponding
   * connection manager to finalize the connections if needed.
   * 
   * @param db the database backend to disable
   * @param forceDisable true if disabling must be forced on the backend
   * @throws SQLException if an error occurs
   */
  public void disableBackend(DatabaseBackend db, boolean forceDisable)
      throws SQLException
  {
    if (backend.equals(db))
    {
      logger
          .info(Translate.get("loadbalancer.backend.disabling", db.getName()));
      backend.disable();
      if (backend.isInitialized())
        backend.finalizeConnections();
      backend = null;
    }
    else
    {
      String msg = "Trying to disable a non-existing backend " + db.getName();
      logger.warn(msg);
      throw new SQLException(msg);
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

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getNumberOfEnabledBackends()
   */
  public int getNumberOfEnabledBackends()
  {
    if (backend == null)
      return 0;
    else
      return 1;
  }

  /*
   * Debug/Monitoring
   */

  /**
   * Gets information about the request load balancer
   * 
   * @return <code>String</code> containing information
   */
  public String getInformation()
  {
    if (backend == null)
      return "SingleDB Request load balancer: !!!Warning!!! No enabled backend node found\n";
    else
      return "SingleDB Request load balancer using " + backend.getURL() + "\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getXmlImpl()
   */
  public String getXmlImpl()
  {
    return "<" + DatabasesXmlTags.ELT_SingleDB + "/>";
  }

}