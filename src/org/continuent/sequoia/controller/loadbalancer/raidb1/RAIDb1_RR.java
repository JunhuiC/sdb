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

package org.continuent.sequoia.controller.loadbalancer.raidb1;

import java.sql.SQLException;
import java.util.ArrayList;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-1 Round Robin load balancer
 * <p>
 * The read requests coming from the Request Manager are sent in a round robin
 * to the backend nodes. Write requests are broadcasted to all backends.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @version 1.0
 */
public class RAIDb1_RR extends RAIDb1
{
  /*
   * How the code is organized ? 1. Member variables 2. Constructor(s) 3.
   * Request handling 4. Debug/Monitoring
   */

  private int index; // index in the backend vector the Round-Robin

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-1 Round Robin request load balancer.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy How many backends must complete before
   *          returning the result?
   * @throws Exception if an error occurs
   */
  public RAIDb1_RR(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy) throws Exception
  {
    super(vdb, waitForCompletionPolicy);
    index = -1;
  }

  /*
   * Request Handling
   */

  /**
   * Selects the backend using a simple round-robin algorithm and executes the
   * read request.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#statementExecuteQuery(SelectRequest,
   *      MetadataCache)
   */
  public ControllerResultSet execSingleBackendReadRequest(
      SelectRequest request, MetadataCache metadataCache) throws SQLException
  {
    return (ControllerResultSet) executeRoundRobinRequest(request,
        STATEMENT_EXECUTE_QUERY, "Request ", metadataCache);
  }

  /**
   * Selects the backend using a simple round-robin algorithm and executes the
   * read request.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    return (ControllerResultSet) executeRoundRobinRequest(proc,
        CALLABLE_STATEMENT_EXECUTE_QUERY, "Stored procedure ", metadataCache);
  }

  /**
   * Selects the backend using a simple round-robin algorithm. The backend that
   * has the shortest queue of currently executing queries is chosen to execute
   * this stored procedure.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(StoredProcedure,
   *      MetadataCache)
   */
  public ExecuteResult readOnlyCallableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    return (ExecuteResult) executeRoundRobinRequest(proc,
        CALLABLE_STATEMENT_EXECUTE, "Stored procedure ", metadataCache);
  }

  /**
   * Common code to execute a SelectRequest or a StoredProcedure on a backend
   * chosen using a round-robin algorithm.
   * 
   * @param request a <code>SelectRequest</code> or
   *          <code>StoredProcedure</code>
   * @param callType one of STATEMENT_EXECUTE_QUERY,
   *          CALLABLE_STATEMENT_EXECUTE_QUERY or CALLABLE_STATEMENT_EXECUTE
   * @param errorMsgPrefix the error message prefix, usually "Request " or
   *          "Stored procedure " ... failed because ...
   * @param metadataCache a metadataCache if any or null
   * @return a <code>ResultSet</code>
   * @throws SQLException if an error occurs
   */
  private Object executeRoundRobinRequest(AbstractRequest request,
      int callType, String errorMsgPrefix, MetadataCache metadataCache)
      throws SQLException
  {
    // Choose a backend
    try
    {
      vdb.acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
      throw new SQLException(msg);
    }

    // The backend that will execute the query
    DatabaseBackend backend = null;

    // Note that vdb lock is released in the finally clause of this try/catch
    // block
    try
    {
      ArrayList backends = vdb.getBackends();
      int size = backends.size();

      if (size == 0)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.available", request.getId()));

      // Take the next backend
      int maxTries = size;
      synchronized (this)
      {
        do
        {
          index = (index + 1) % size;
          backend = (DatabaseBackend) backends.get(index);
          maxTries--;
        }
        while ((!backend.isReadEnabled() && maxTries >= 0));
      }

      if (maxTries < 0)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.enabled", request.getId()));
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.execute.find.backend.failed",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      vdb.releaseReadLockBackendLists();
    }

    // Execute the request on the chosen backend
    try
    {
      switch (callType)
      {
        case STATEMENT_EXECUTE_QUERY :
          return executeRequestOnBackend((SelectRequest) request, backend,
              metadataCache);
        case CALLABLE_STATEMENT_EXECUTE_QUERY :
          return executeStoredProcedureOnBackend((StoredProcedure) request,
              true, backend, metadataCache);
        case CALLABLE_STATEMENT_EXECUTE :
          return executeStoredProcedureOnBackend((StoredProcedure) request,
              false, backend, metadataCache);
        default :
          throw new RuntimeException("Unhandled call type " + callType
              + " in executeRoundRobin");
      }
    }
    catch (UnreachableBackendException urbe)
    {
      // Try to execute query on different backend
      return executeRoundRobinRequest(request, callType, errorMsgPrefix,
          metadataCache);
    }
    catch (SQLException se)
    {
      String msg = Translate.get("loadbalancer.something.failed", new String[]{
          errorMsgPrefix, String.valueOf(request.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw se;
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.something.failed.on",
          new String[]{errorMsgPrefix,
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }
  }

  /*
   * Debug/Monitoring
   */

  /**
   * Gets information about the request load balancer.
   * 
   * @return <code>String</code> containing information
   */
  public String getInformation()
  {
    // We don't lock since we don't need a top accurate value
    int size = vdb.getBackends().size();

    if (size == 0)
      return "RAIDb-1 Round-Robin Request load balancer: !!!Warning!!! No backend nodes found\n";
    else
      return "RAIDb-1 Round-Robin Request load balancer (" + size
          + " backends)\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#getRaidb1Xml
   */
  public String getRaidb1Xml()
  {
    return "<" + DatabasesXmlTags.ELT_RAIDb_1_RoundRobin + "/>";
  }

}