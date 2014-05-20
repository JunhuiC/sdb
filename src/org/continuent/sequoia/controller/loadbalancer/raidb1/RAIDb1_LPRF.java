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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): _______________________
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
 * RAIDb-1 Round Robin load balancer featuring (Least Pending Requests First
 * load balancing algorithm).
 * <p>
 * The read requests coming from the Request Manager are sent to the node that
 * has the least pending read requests among the nodes that can execute the
 * request. Write requests are broadcasted to all backends.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class RAIDb1_LPRF extends RAIDb1
{
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Debug/Monitoring

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-1 Round Robin request load balancer.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy how many backends must complete before
   *          returning the result?
   * @throws Exception if an error occurs
   */
  public RAIDb1_LPRF(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy) throws Exception
  {
    super(vdb, waitForCompletionPolicy);
  }

  /*
   * Request Handling
   */

  /**
   * Selects the backend using a least pending request first policy. The backend
   * that has the shortest queue of currently executing queries is chosen to
   * execute this query.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#statementExecuteQuery(SelectRequest,
   *      MetadataCache)
   */
  public ControllerResultSet execSingleBackendReadRequest(
      SelectRequest request, MetadataCache metadataCache) throws SQLException
  {
    return (ControllerResultSet) executeLPRF(request, STATEMENT_EXECUTE_QUERY,
        "Request ", metadataCache);
  }

  /**
   * Selects the backend using a least pending request first policy. The backend
   * that has the shortest queue of currently executing queries is chosen to
   * execute this stored procedure.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    return (ControllerResultSet) executeLPRF(proc,
        CALLABLE_STATEMENT_EXECUTE_QUERY, "Stored procedure ", metadataCache);
  }

  /**
   * Selects the backend using a least pending request first policy. The backend
   * that has the shortest queue of currently executing queries is chosen to
   * execute this stored procedure.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(StoredProcedure,
   *      MetadataCache)
   */
  public ExecuteResult readOnlyCallableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    return (ExecuteResult) executeLPRF(proc, CALLABLE_STATEMENT_EXECUTE,
        "Stored procedure ", metadataCache);
  }

  /**
   * Common code to execute a SelectRequest or a StoredProcedure on a backend
   * chosen using a LPRF algorithm.
   * 
   * @param request a <code>SelectRequest</code> or
   *          <code>StoredProcedure</code>
   * @param callType one of STATEMENT_EXECUTE_QUERY,
   *          CALLABLE_STATEMENT_EXECUTE_QUERY or CALLABLE_STATEMENT_EXECUTE
   * @param errorMsgPrefix the error message prefix, usually "Request " or
   *          "Stored procedure " ... failed because ...
   * @param metadataCache the metadataCache if any or null
   * @return a <code>ControllerResultSet</code> or an
   *         <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   */
  private Object executeLPRF(AbstractRequest request, int callType,
      String errorMsgPrefix, MetadataCache metadataCache) throws SQLException
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

    /*
     * The backend that will execute the query
     */
    DatabaseBackend backend = null;

    // Note that vdb lock is released in the finally clause of this try/catch
    // block
    try
    {
      ArrayList<?> backends = vdb.getBackends();
      int size = backends.size();

      // Choose the backend that has the least pending requests
      int leastRequests = 0;
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend b = (DatabaseBackend) backends.get(i);
        if (b.isReadEnabled())
        {
          int pending = b.getPendingRequests().size();
          if ((backend == null) || (pending < leastRequests))
          {
            backend = b;
            if (pending == 0)
              break; // Stop here we will never find a less loaded node
            else
              leastRequests = pending;
          }
        }
      }

    }
    catch (Throwable e)
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

    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.enabled", request.getId()));

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
              + " in executeLPRF");
      }
    }
    catch (UnreachableBackendException urbe)
    {
      // Try to execute query on different backend
      return executeLPRF(request, callType, errorMsgPrefix, metadataCache);
    }
    catch (SQLException se)
    {
      if (logger.isDebugEnabled())
      {
        String msg = Translate.get("loadbalancer.something.failed", new String[]{
            errorMsgPrefix, String.valueOf(request.getId()), se.getMessage()});
        logger.debug(msg);
      }
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
      return "RAIDb-1 Least Pending Request First load balancer: !!!Warning!!! No backend nodes found\n";
    else
      return "RAIDb-1 Least Pending Request First load balancer (" + size
          + " backends)\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#getRaidb1Xml
   */
  public String getRaidb1Xml()
  {
    return "<" + DatabasesXmlTags.ELT_RAIDb_1_LeastPendingRequestsFirst + "/>";
  }

}
