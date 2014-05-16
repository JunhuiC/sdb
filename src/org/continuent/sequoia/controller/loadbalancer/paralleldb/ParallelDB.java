/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2006-2007 Continuent, Inc.
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


 * Free Software Foundation; either version 2.1 of the License, or any later
 * version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Jaco Swart, Jean-Bernard van Zuylen.
 */

package org.continuent.sequoia.controller.loadbalancer.paralleldb;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Hashtable;
import java.util.List;

import org.continuent.sequoia.common.exceptions.BadConnectionException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoTransactionStartWhenDisablingException;
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
 * This class defines a ParallelDB
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
/**
 * These are generic functions for all ParallelDB load balancers.
 * <p>
 * Read and write queries are load balanced on the backends without any
 * replication (assuming that the underlying parallel database takes care of
 * data replication). The load balancers provide failover for reads and writes.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jaco.swart@iblocks.co.uk">Jaco Swart </a>
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat </a>
 * @version 1.0
 */
public abstract class ParallelDB extends AbstractLoadBalancer
{
  // transaction id -> DatabaseBackend
  private Hashtable backendPerTransactionId;
  private int       numberOfEnabledBackends = 0;

  static Trace      endUserLogger           = Trace
                                                .getLogger("org.continuent.sequoia.enduser");

  /**
   * Creates a new <code>ParallelDB</code> load balancer with NO_PARSING and a
   * SingleDB RAIDb level.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @throws SQLException if an error occurs
   */
  public ParallelDB(VirtualDatabase vdb) throws SQLException
  {
    super(vdb, RAIDbLevels.SingleDB, ParsingGranularities.NO_PARSING);
    backendPerTransactionId = new Hashtable();
  }

  //
  // Request handling
  //

  /**
   * Choose a backend using the implementation specific load balancing algorithm
   * for read request execution.
   * 
   * @param request request to execute
   * @return the chosen backend
   * @throws SQLException if an error occurs
   */
  public abstract DatabaseBackend chooseBackendForReadRequest(
      AbstractRequest request) throws SQLException;

  /**
   * Choose a backend using the implementation specific load balancing algorithm
   * for write request execution.
   * 
   * @param request request to execute
   * @return the chosen backend
   * @throws SQLException if an error occurs
   */
  public abstract DatabaseBackend chooseBackendForWriteRequest(
      AbstractWriteRequest request) throws SQLException;

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#statementExecuteQuery(SelectRequest,
   *      MetadataCache)
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request,
      MetadataCache metadataCache) throws SQLException
  {
    DatabaseBackend backend;
    if (request.isAutoCommit())
      backend = chooseBackendForReadRequest(request);
    else
      backend = (DatabaseBackend) backendPerTransactionId.get(new Long(request
          .getTransactionId()));

    if (backend == null)
      throw new SQLException(Translate.get(
          "loadbalancer.execute.no.backend.found", request.getSqlShortForm(vdb
              .getSqlShortFormLength())));

    ControllerResultSet rs = null;
    // Execute the request on the chosen backend
    try
    {
      rs = executeStatementExecuteQueryOnBackend(request, backend,
          metadataCache);
    }
    catch (UnreachableBackendException urbe)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      // Try to execute query on different backend
      return statementExecuteQuery(request, metadataCache);
    }
    catch (SQLException se)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.request.failed", new String[]{
          String.valueOf(request.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (RuntimeException e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return rs;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#statementExecuteUpdate(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public ExecuteUpdateResult statementExecuteUpdate(AbstractWriteRequest request)
      throws AllBackendsFailedException, SQLException
  {
    // Handle macros
    handleMacros(request);

    // Log lazy begin if needed
    if (request.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          request.getTransactionId());

    // Log request
    if (recoveryLog != null)
      recoveryLog.logRequestExecuting(request);

    DatabaseBackend backend;
    if (request.isAutoCommit())
      backend = chooseBackendForWriteRequest(request);
    else
      backend = (DatabaseBackend) backendPerTransactionId.get(new Long(request
          .getTransactionId()));

    if (backend == null)
      throw new SQLException(Translate.get(
          "loadbalancer.execute.no.backend.found", request.getSqlShortForm(vdb
              .getSqlShortFormLength())));

    ExecuteUpdateResult result;
    // Execute the request on the chosen backend
    try
    {
      result = executeStatementExecuteUpdateOnBackend(request, backend);
    }
    catch (UnreachableBackendException urbe)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      // Try to execute query on different backend
      return statementExecuteUpdate(request);
    }
    catch (SQLException se)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.request.failed", new String[]{
          String.valueOf(request.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (RuntimeException e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return result;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#statementExecuteUpdateWithKeys(AbstractWriteRequest,
   *      MetadataCache)
   */
  public GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request, MetadataCache metadataCache)
      throws AllBackendsFailedException, SQLException
  {
    // Handle macros
    handleMacros(request);

    // Log lazy begin if needed
    if (request.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          request.getTransactionId());

    // Log request
    if (recoveryLog != null)
      recoveryLog.logRequestExecuting(request);

    DatabaseBackend backend;
    if (request.isAutoCommit())
      backend = chooseBackendForWriteRequest(request);
    else
      backend = (DatabaseBackend) backendPerTransactionId.get(new Long(request
          .getTransactionId()));

    if (backend == null)
      throw new SQLException(Translate.get(
          "loadbalancer.execute.no.backend.found", request.getSqlShortForm(vdb
              .getSqlShortFormLength())));

    GeneratedKeysResult rs;
    // Execute the request on the chosen backend
    try
    {
      rs = executeStatementExecuteUpdateWithKeysOnBackend(request, backend,
          metadataCache);
    }
    catch (UnreachableBackendException urbe)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      // Try to execute query on different backend
      return statementExecuteUpdateWithKeys(request, metadataCache);
    }
    catch (SQLException se)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.request.failed", new String[]{
          String.valueOf(request.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (RuntimeException e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(request.getLogId(), false, request
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return rs;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#statementExecute(AbstractRequest,
   *      MetadataCache)
   */
  public ExecuteResult statementExecute(AbstractRequest request,
      MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException
  {
    // Handle macros
    handleMacros(request);

    // Log lazy begin if needed
    if (request.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          request.getTransactionId());

    // Log request
    if (recoveryLog != null)
      recoveryLog.logRequestExecuting(request);

    DatabaseBackend backend;
    if (request.isAutoCommit())
      backend = chooseBackendForReadRequest(request);
    else
      backend = (DatabaseBackend) backendPerTransactionId.get(new Long(request
          .getTransactionId()));

    if (backend == null)
      throw new SQLException(Translate.get(
          "loadbalancer.storedprocedure.no.backend.found", request
              .getSqlShortForm(vdb.getSqlShortFormLength())));

    ExecuteResult rs = null;
    // Execute the request on the chosen backend
    try
    {
      rs = executeStatementExecuteOnBackend(request, backend, metadataCache);
    }
    catch (UnreachableBackendException urbe)
    {
      // Notify failure in recovery log
      recoveryLog.logRequestCompletion(request.getLogId(), false, request
          .getExecTimeInMs());

      // Try to execute query on different backend
      return statementExecute(request, metadataCache);
    }
    catch (SQLException se)
    {
      // Notify failure in recovery log
      recoveryLog.logRequestCompletion(request.getLogId(), false, request
          .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(request.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (RuntimeException e)
    {
      // Notify failure in recovery log
      recoveryLog.logRequestCompletion(request.getLogId(), false, request
          .getExecTimeInMs());

      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return rs;
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
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(StoredProcedure,
   *      MetadataCache)
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
    // Handle macros
    handleMacros(proc);

    // Log lazy begin if needed
    if (proc.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          proc.getTransactionId());

    // Log request
    if (recoveryLog != null)
      recoveryLog.logRequestExecuting(proc);

    DatabaseBackend backend;
    if (proc.isAutoCommit())
      backend = chooseBackendForReadRequest(proc);
    else
      backend = (DatabaseBackend) backendPerTransactionId.get(new Long(proc
          .getTransactionId()));

    if (backend == null)
      throw new SQLException(Translate.get(
          "loadbalancer.storedprocedure.no.backend.found", proc
              .getSqlShortForm(vdb.getSqlShortFormLength())));

    ControllerResultSet rs = null;
    // Execute the request on the chosen backend
    try
    {
      rs = executeCallableStatementExecuteQueryOnBackend(proc, backend,
          metadataCache);
    }
    catch (UnreachableBackendException urbe)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      // Try to execute query on different backend
      return callableStatementExecuteQuery(proc, metadataCache);
    }
    catch (SQLException se)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(proc.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (RuntimeException e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return rs;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecuteUpdate(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public ExecuteUpdateResult callableStatementExecuteUpdate(StoredProcedure proc)
      throws SQLException
  {
    // Handle macros
    handleMacros(proc);

    // Log lazy begin if needed
    if (proc.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          proc.getTransactionId());

    // Log request
    if (recoveryLog != null)
      recoveryLog.logRequestExecuting(proc);

    DatabaseBackend backend;
    if (proc.isAutoCommit())
      backend = chooseBackendForReadRequest(proc);
    else
      backend = (DatabaseBackend) backendPerTransactionId.get(new Long(proc
          .getTransactionId()));

    if (backend == null)
      throw new SQLException(Translate.get(
          "loadbalancer.storedprocedure.no.backend.found", proc
              .getSqlShortForm(vdb.getSqlShortFormLength())));

    ExecuteUpdateResult result;
    // Execute the request on the chosen backend
    try
    {
      result = executeCallableStatementExecuteUpdateOnBackend(proc, backend);
    }
    catch (UnreachableBackendException urbe)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      // Try to execute query on different backend
      return callableStatementExecuteUpdate(proc);
    }
    catch (SQLException se)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(proc.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (RuntimeException e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return result;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#callableStatementExecute(StoredProcedure,
   *      MetadataCache)
   */
  public ExecuteResult callableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    // Handle macros
    handleMacros(proc);

    // Log lazy begin if needed
    if (proc.isLazyTransactionStart())
      this.vdb.getRequestManager().logLazyTransactionBegin(
          proc.getTransactionId());

    // Log request
    if (recoveryLog != null)
      recoveryLog.logRequestExecuting(proc);

    DatabaseBackend backend;
    if (proc.isAutoCommit())
      backend = chooseBackendForReadRequest(proc);
    else
      backend = (DatabaseBackend) backendPerTransactionId.get(new Long(proc
          .getTransactionId()));

    if (backend == null)
      throw new SQLException(Translate.get(
          "loadbalancer.storedprocedure.no.backend.found", proc
              .getSqlShortForm(vdb.getSqlShortFormLength())));

    ExecuteResult rs = null;
    // Execute the request on the chosen backend
    try
    {
      rs = executeCallableStatementExecuteOnBackend(proc, backend,
          metadataCache);
    }
    catch (UnreachableBackendException urbe)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      // Try to execute query on different backend
      ExecuteResult result = callableStatementExecute(proc, metadataCache);

      return result;
    }
    catch (SQLException se)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      String msg = Translate.get("loadbalancer.storedprocedure.failed",
          new String[]{String.valueOf(proc.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (RuntimeException e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(proc.getLogId(), false, proc
            .getExecTimeInMs());

      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return rs;
  }

  /**
   * Execute a read request on the selected backend.
   * 
   * @param request the request to execute
   * @param backend the backend that will execute the request
   * @param metadataCache MetadataCache (null if none)
   * @return the ControllerResultSet
   * @throws SQLException if an error occurs
   */
  private ControllerResultSet executeStatementExecuteQueryOnBackend(
      SelectRequest request, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException,
      UnreachableBackendException
  {
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
        PooledConnection pc = null;
        try
        {
          pc = cm.retrieveConnectionInAutoCommit(request);
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
        if (pc == null)
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.connection", backend.getName()));

        // Execute Query
        try
        {
          rs = executeStatementExecuteQueryOnBackend(request, backend, null, pc
              .getConnection(), metadataCache);
          cm.releaseConnectionInAutoCommit(request, pc);
        }
        catch (BadConnectionException e)
        { // Get rid of the bad connection
          cm.deleteConnection(pc);
          badConnection = true;
        }
        catch (Throwable e)
        {
          cm.releaseConnectionInAutoCommit(request, pc);
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
        rs = executeStatementExecuteQueryOnBackend(request, backend, null, c,
            metadataCache);
      }
      catch (BadConnectionException e)
      { // Connection failed, so did the transaction
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
   * Execute a write request on the selected backend.
   * 
   * @param request the request to execute
   * @param backend the backend that will execute the request
   * @return the number of modified rows
   * @throws SQLException if an error occurs
   */
  private ExecuteUpdateResult executeStatementExecuteUpdateOnBackend(
      AbstractWriteRequest request, DatabaseBackend backend)
      throws SQLException, UnreachableBackendException
  {
    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.available", request.getId()));

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
          throw new UnreachableBackendException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
        }

        // Sanity check
        if (c == null)
          throw new UnreachableBackendException(Translate.get(
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
        ExecuteUpdateResult result;
        try
        {
          result = executeStatementExecuteUpdateOnBackend(request, backend,
              null, c);
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
      logger.fatal(msg, e);
      throw new SQLException(msg);
    }
  }

  /**
   * Execute a write request on the selected backend and return the
   * autogenerated keys.
   * 
   * @param request the request to execute
   * @param backend the backend that will execute the request
   * @param metadataCache MetadataCache (null if none)
   * @return the ResultSet containing the auto-generated keys
   * @throws SQLException if an error occurs
   */
  private GeneratedKeysResult executeStatementExecuteUpdateWithKeysOnBackend(
      AbstractWriteRequest request, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException,
      UnreachableBackendException
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
          throw new UnreachableBackendException(Translate.get(
              "loadbalancer.backend.unreacheable", backendName));
        }

        // Sanity check
        if (c == null)
          throw new UnreachableBackendException(Translate.get(
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
          return executeStatementExecuteUpdateWithKeysOnBackend(request,
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
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.fatal(msg, e);
      endUserLogger.fatal(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * Execute a request that return multiple results on the selected backend.
   * 
   * @param request the request to execute
   * @param backend the backend that will execute the request
   * @param metadataCache MetadataCache (null if none)
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   */
  private ExecuteResult executeStatementExecuteOnBackend(
      AbstractRequest request, DatabaseBackend backend,
      MetadataCache metadataCache) throws SQLException,
      UnreachableBackendException
  {
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
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.request.on", new String[]{
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
      ExecuteResult rs;
      try
      {
        rs = AbstractLoadBalancer.executeStatementExecuteOnBackend(request,
            backend, null, cm.retrieveConnectionForTransaction(tid),
            metadataCache);
      }
      catch (Exception e)
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
   * Execute a stored procedure on the selected backend.
   * 
   * @param proc the stored procedure to execute
   * @param backend the backend that will execute the request
   * @param metadataCache MetadataCache (null if none)
   * @return the ControllerResultSet
   * @throws SQLException if an error occurs
   */
  private ControllerResultSet executeCallableStatementExecuteQueryOnBackend(
      StoredProcedure proc, DatabaseBackend backend, MetadataCache metadataCache)
      throws SQLException, UnreachableBackendException
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
      // Use a connection just for this request
      PooledConnection c = null;
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
      if (c == null)
        throw new UnreachableBackendException(Translate.get(
            "loadbalancer.backend.no.connection", backend.getName()));

      // Execute Query
      ControllerResultSet rs = null;
      try
      {
        rs = AbstractLoadBalancer
            .executeCallableStatementExecuteQueryOnBackend(proc, backend, null,
                c.getConnection(), metadataCache);
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
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.storedprocedure.on",
            new String[]{String.valueOf(proc.getId()), backend.getName()}));
      return rs;
    }
    else
    { // Inside a transaction
      Connection c;
      long tid = proc.getTransactionId();

      try
      {
        c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(proc, cm);
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
            new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
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
      ControllerResultSet rs;
      try
      {
        rs = AbstractLoadBalancer
            .executeCallableStatementExecuteQueryOnBackend(proc, backend, null,
                c, metadataCache);
      }
      catch (Exception e)
      {
        throw new SQLException(Translate.get(
            "loadbalancer.storedprocedure.failed.on.backend", new String[]{
                proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(proc.getId()),
                backend.getName()}));
      return rs;
    }
  }

  /**
   * Execute a stored procedure on the selected backend.
   * 
   * @param proc the stored procedure to execute
   * @param backend the backend that will execute the request
   * @return the ResultSet
   * @throws SQLException if an error occurs
   */
  private ExecuteUpdateResult executeCallableStatementExecuteUpdateOnBackend(
      StoredProcedure proc, DatabaseBackend backend) throws SQLException,
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
      // Use a connection just for this request
      PooledConnection c = null;
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
      if (c == null)
        throw new UnreachableBackendException(Translate.get(
            "loadbalancer.backend.no.connection", backend.getName()));

      // Execute Query
      ExecuteUpdateResult result;
      try
      {
        result = AbstractLoadBalancer
            .executeCallableStatementExecuteUpdateOnBackend(proc, backend,
                null, c);

        // Warning! No way to detect if schema has been modified unless
        // we ask the backend again using DatabaseMetaData.getTables().
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
            new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
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
      ExecuteUpdateResult result;
      try
      {
        result = AbstractLoadBalancer
            .executeCallableStatementExecuteUpdateOnBackend(proc, backend,
                null, cm.retrieveConnectionForTransaction(tid));

        // Warning! No way to detect if schema has been modified unless
        // we ask the backend again using DatabaseMetaData.getTables().
      }
      catch (Exception e)
      {
        throw new SQLException(Translate.get(
            "loadbalancer.storedprocedure.failed.on.backend", new String[]{
                proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                backend.getName(), e.getMessage()}));
      }
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(proc.getId()),
                backend.getName()}));
      return result;
    }
  }

  /**
   * Execute a stored procedure that return multiple results on the selected
   * backend.
   * 
   * @param proc the stored procedure to execute
   * @param backend the backend that will execute the request
   * @param metadataCache MetadataCache (null if none)
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   */
  private ExecuteResult executeCallableStatementExecuteOnBackend(
      StoredProcedure proc, DatabaseBackend backend, MetadataCache metadataCache)
      throws SQLException, UnreachableBackendException
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
      // Use a connection just for this request
      PooledConnection c = null;
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
      if (c == null)
        throw new UnreachableBackendException(Translate.get(
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
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.storedprocedure.on",
            new String[]{String.valueOf(proc.getId()), backend.getName()}));
      return rs;
    }
    else
    { // Inside a transaction
      Connection c;
      long tid = proc.getTransactionId();

      try
      {
        c = backend.getConnectionForTransactionAndLazyBeginIfNeeded(proc, cm);
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
            new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
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
      ExecuteResult rs;
      try
      {
        rs = AbstractLoadBalancer.executeCallableStatementExecuteOnBackend(
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
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.transaction.on",
            new String[]{String.valueOf(tid), String.valueOf(proc.getId()),
                backend.getName()}));
      return rs;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getPreparedStatementGetMetaData(org.continuent.sequoia.controller.requests.AbstractRequest)
   */
  public ControllerResultSet getPreparedStatementGetMetaData(
      AbstractRequest request) throws SQLException
  {
    DatabaseBackend backend = chooseBackendForReadRequest(request);

    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.enabled", request.getId()));

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
      { // Connection failed, so did the transaction
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
    DatabaseBackend backend = chooseBackendForReadRequest(request);

    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.enabled", request.getId()));

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
      { // Connection failed, so did the transaction
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

  //
  // Transaction Management
  //

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#abort(org.continuent.sequoia.controller.requestmanager.TransactionMetaData)
   */
  public void abort(TransactionMetaData tm) throws SQLException
  {
    rollback(tm);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#begin(org.continuent.sequoia.controller.requestmanager.TransactionMetaData)
   */
  public void begin(TransactionMetaData tm) throws SQLException
  {
    Long lTid = new Long(tm.getTransactionId());
    if (backendPerTransactionId.containsKey(lTid))
      throw new SQLException(Translate.get(
          "loadbalancer.transaction.already.started", lTid.toString()));

    DatabaseBackend backend = chooseBackendForReadRequest(new UnknownReadRequest(
        "begin", false, 0, "\n"));
    backendPerTransactionId.put(lTid, backend);
    backend.startTransaction(lTid);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#commit(org.continuent.sequoia.controller.requestmanager.TransactionMetaData)
   */
  public void commit(TransactionMetaData tm) throws SQLException
  {
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);
    DatabaseBackend db = (DatabaseBackend) backendPerTransactionId.remove(lTid);

    AbstractConnectionManager cm = db.getConnectionManager(tm.getLogin());
    PooledConnection pc = cm.retrieveConnectionForTransaction(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logCommit(tm);

    // Sanity check
    if (pc == null)
    { // Bad connection
      db.stopTransaction(lTid);

      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.connection", new String[]{
              String.valueOf(tid), db.getName()}));
    }

    // Execute Query
    try
    {
      Connection c = pc.getConnection();
      c.commit();
      c.setAutoCommit(true);
    }
    catch (Exception e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(logId, false, 0);

      String msg = Translate.get("loadbalancer.commit.failed", new String[]{
          String.valueOf(tid), db.getName(), e.getMessage()});
      logger.error(msg);
      throw new SQLException(msg);
    }
    finally
    {
      cm.releaseConnectionForTransaction(tid);
      db.stopTransaction(lTid);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#rollback(org.continuent.sequoia.controller.requestmanager.TransactionMetaData)
   */
  public void rollback(TransactionMetaData tm) throws SQLException
  {
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);
    DatabaseBackend db = (DatabaseBackend) backendPerTransactionId.remove(lTid);

    AbstractConnectionManager cm = db.getConnectionManager(tm.getLogin());
    PooledConnection pc = cm.retrieveConnectionForTransaction(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logRollback(tm);

    // Sanity check
    if (pc == null)
    { // Bad connection
      db.stopTransaction(lTid);

      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.connection", new String[]{
              String.valueOf(tid), db.getName()}));
    }

    // Execute Query
    try
    {
      Connection c = pc.getConnection();
      c.rollback();

      c.setAutoCommit(true);
    }
    catch (Exception e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(logId, false, 0);

      String msg = Translate.get("loadbalancer.rollback.failed", new String[]{
          String.valueOf(tid), db.getName(), e.getMessage()});
      logger.error(msg);
      throw new SQLException(msg);
    }
    finally
    {
      cm.releaseConnectionForTransaction(tid);
      db.stopTransaction(lTid);
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
    DatabaseBackend db = (DatabaseBackend) backendPerTransactionId.remove(lTid);

    AbstractConnectionManager cm = db.getConnectionManager(tm.getLogin());
    PooledConnection c = cm.retrieveConnectionForTransaction(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logRollbackToSavepoint(tm, savepointName);

    // Sanity check
    if (c == null)
    { // Bad connection
      db.stopTransaction(lTid);

      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.connection", new String[]{
              String.valueOf(tid), db.getName()}));
    }

    // Retrieve savepoint
    Savepoint savepoint = db.getSavepoint(lTid, savepointName);
    if (savepoint == null)
    {
      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.savepoint", new String[]{savepointName,
              String.valueOf(tid), db.getName()}));
    }

    // Execute Query
    try
    {
      c.getConnection().rollback(savepoint);
    }
    catch (Exception e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(logId, false, 0);

      String msg = Translate.get("loadbalancer.rollbacksavepoint.failed",
          new String[]{savepointName, String.valueOf(tid), db.getName(),
              e.getMessage()});
      logger.error(msg);
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
    long tid = tm.getTransactionId();
    Long lTid = new Long(tid);

    DatabaseBackend db = (DatabaseBackend) backendPerTransactionId.get(lTid);
    AbstractConnectionManager cm = db.getConnectionManager(tm.getLogin());
    PooledConnection c = cm.retrieveConnectionForTransaction(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logReleaseSavepoint(tm, savepointName);

    // Sanity check
    if (c == null)
    { // Bad connection
      db.stopTransaction(lTid);

      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.connection", new String[]{
              String.valueOf(tid), db.getName()}));
    }

    // Retrieve savepoint
    Savepoint savepoint = db.getSavepoint(lTid, savepointName);
    if (savepoint == null)
    {
      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.savepoint", new String[]{
              String.valueOf(tid), savepointName, db.getName()}));
    }

    // Execute Query
    try
    {
      c.getConnection().releaseSavepoint(savepoint);
    }
    catch (Exception e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(logId, false, 0);

      String msg = Translate.get("loadbalancer.releasesavepoint.failed",
          new String[]{savepointName, String.valueOf(tid), db.getName(),
              e.getMessage()});
      logger.error(msg);
      throw new SQLException(msg);
    }
    finally
    {
      db.removeSavepoint(lTid, savepoint);
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

    DatabaseBackend db = (DatabaseBackend) backendPerTransactionId.get(lTid);
    AbstractConnectionManager cm = db.getConnectionManager(tm.getLogin());
    PooledConnection c = cm.retrieveConnectionForTransaction(tid);

    long logId = 0;
    // Log the request
    if (recoveryLog != null)
      logId = recoveryLog.logSetSavepoint(tm, savepointName);

    // Sanity check
    if (c == null)
    { // Bad connection
      db.stopTransaction(lTid);

      throw new SQLException(Translate.get(
          "loadbalancer.unable.retrieve.connection", new String[]{
              String.valueOf(tid), db.getName()}));
    }

    // Execute Query
    Savepoint savepoint = null;
    try
    {
      savepoint = c.getConnection().setSavepoint(savepointName);
    }
    catch (Exception e)
    {
      // Notify failure in recovery log
      if (recoveryLog != null)
        recoveryLog.logRequestCompletion(logId, false, 0);

      String msg = Translate.get("loadbalancer.setsavepoint.failed",
          new String[]{savepointName, String.valueOf(tid), db.getName(),
              e.getMessage()});
      logger.error(msg);
      throw new SQLException(msg);
    }
    finally
    {
      if (savepoint != null)
        db.addSavepoint(lTid, savepoint);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#closePersistentConnection(java.lang.String,
   *      long)
   */
  public void closePersistentConnection(String login,
      long persistentConnectionId) throws SQLException
  {
    try
    {
      vdb.acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
    }
    int size = vdb.getBackends().size();
    List backends = vdb.getBackends();
    for (int i = 0; i < size; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) backends.get(i);
      AbstractConnectionManager cm = backend.getConnectionManager(login);
      if (cm != null)
      {
        cm.releasePersistentConnectionInAutoCommit(persistentConnectionId);
        backend.removePersistentConnection(persistentConnectionId);
      }
    }
    vdb.releaseReadLockBackendLists();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#openPersistentConnection(String,
   *      long)
   */
  public void openPersistentConnection(String login, long persistentConnectionId)
      throws SQLException
  {
    // Fake request to call the method that creates persistent connections
    AbstractRequest request = new UnknownReadRequest("", false, 0, "");
    request.setLogin(login);
    request.setPersistentConnection(true);
    request.setPersistentConnectionId(persistentConnectionId);

    try
    {
      vdb.acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
    }
    int size = vdb.getBackends().size();
    List backends = vdb.getBackends();
    for (int i = 0; i < size; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) backends.get(i);
      AbstractConnectionManager cm = backend.getConnectionManager(login);
      if (cm == null)
      {
        logger.warn("Failed to open persistent connection "
            + persistentConnectionId + " on backend " + backend.getName());
        continue;
      }
      try
      {
        // Create the persistent connection
        PooledConnection c = cm.retrieveConnectionInAutoCommit(request);
        backend.addPersistentConnection(request.getPersistentConnectionId(), c);
      }
      catch (UnreachableBackendException e)
      {
        logger.warn("Failed to open persistent connection "
            + persistentConnectionId + " on backend " + backend.getName(), e);
      }
    }
    vdb.releaseReadLockBackendLists();
  }

  /**
   * Enables a backend that was previously disabled. Asks the corresponding
   * connection manager to initialize the connections if needed.
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
    logger.info(Translate.get("loadbalancer.backend.enabling", db.getName()));
    if (!db.isInitialized())
      db.initializeConnections();
    db.enableRead();
    if (writeEnabled)
      db.enableWrite();
    numberOfEnabledBackends++;
  }

  /**
   * Disables a backend that was previously enabled. Asks the corresponding
   * connection manager to finalize the connections if needed.
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
    logger.info(Translate.get("loadbalancer.backend.disabling", db.getName()));
    numberOfEnabledBackends--;
    db.disable();
    if (db.isInitialized())
      db.finalizeConnections();
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getNumberOfEnabledBackends()
   */
  public int getNumberOfEnabledBackends()
  {
    return numberOfEnabledBackends;
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
    info.append("<" + DatabasesXmlTags.ELT_ParallelDB + ">");
    info.append(getParallelDBXml());
    info.append("</" + DatabasesXmlTags.ELT_ParallelDB + ">");
    return info.toString();
  }

  /**
   * Return the XML tags of the ParallelDB load balancer implementation.
   * 
   * @return content of ParallelDB xml
   */
  public abstract String getParallelDBXml();

}