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
 * Contributor(s): Julie Marguerite, Gaetano Mazzeo.
 */

package org.continuent.sequoia.controller.loadbalancer.raidb1;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.loadbalancer.WeightedBalancer;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-1 Weighted Round Robin load balancer
 * <p>
 * The read requests coming from the request manager are sent to the backend
 * nodes using a weighted round robin. Write requests are broadcasted to all
 * backends.
 * <p>
 * The weighted round-robin works as follows. If the backend weight is set to 0,
 * no read requests are sent to this backend unless it is the last one available
 * on this controller. The load balancer maintains a current weight that is
 * increased by one each time a new read request is executed. <br>
 * If backend1 has a weight of 5 and backend2 a weight of 10, backend1 will
 * receive the 5 first requests and backend2 the next 10 requests. Then we
 * restart with backend1. Be careful that large weight values will heavily load
 * backends in turn but will probably not balance the load in an effective way.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class RAIDb1_WRR extends RAIDb1
{
  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Debug/Monitoring
  //

  private HashMap<String, Integer> weights = new HashMap<String, Integer>();
  private int     currentWeight;

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-1 Weighted Round Robin request load balancer.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy How many backends must complete before
   *          returning the result?
   * @throws Exception if an error occurs
   */
  public RAIDb1_WRR(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy) throws Exception
  {
    super(vdb, waitForCompletionPolicy);
    currentWeight = 0;
  }

  /*
   * Request Handling
   */

  /**
   * Selects the backend using a weighted round-robin algorithm and executes the
   * read request.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#statementExecuteQuery(SelectRequest,
   *      MetadataCache)
   */
  public ControllerResultSet execSingleBackendReadRequest(
      SelectRequest request, MetadataCache metadataCache) throws SQLException
  {
    return (ControllerResultSet) executeWRR(request, STATEMENT_EXECUTE_QUERY,
        "Request ", metadataCache);
  }

  /**
   * Selects the backend using a weighted round-robin algorithm. The backend
   * that has the shortest queue of currently executing queries is chosen to
   * execute this stored procedure.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    return (ControllerResultSet) executeWRR(proc,
        CALLABLE_STATEMENT_EXECUTE_QUERY, "Stored procedure ", metadataCache);
  }

  /**
   * Selects the backend using a weighted round-robin algorithm. The backend
   * that has the shortest queue of currently executing queries is chosen to
   * execute this stored procedure.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(StoredProcedure,
   *      MetadataCache)
   */
  public ExecuteResult readOnlyCallableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    return (ExecuteResult) executeWRR(proc, CALLABLE_STATEMENT_EXECUTE,
        "Stored procedure ", metadataCache);
  }

  /**
   * Common code to execute a SelectRequest or a StoredProcedure on a backend
   * chosen using a weighted round-robin algorithm.
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
  private Object executeWRR(AbstractRequest request, int callType,
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

    DatabaseBackend backend = null;

    // Note that vdb lock is released in the finally clause of this try/catch
    // block
    try
    {
      ArrayList<?> backends = vdb.getBackends();
      int size = backends.size();

      if (size == 0)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.available", request.getId()));

      // Choose the backend (WRR algorithm starts here)
      int w = 0; // cumulative weight
      boolean backendFound = false;
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend b = (DatabaseBackend) backends.get(i);
        if (b.isReadEnabled())
        {
          if (backend == null)
            backend = b; // Fallback if no backend found

          // Add the weight of this backend
          Integer weight = (Integer) weights.get(b.getName());
          if (weight == null)
            logger.error("No weight defined for backend " + b.getName());
          else
          {
            int backendWeight = weight.intValue();
            if (backendWeight == 0)
              continue; // Weight of 0, avoid using that backend
            else
              w += backendWeight;
          }

          // Ok we reached the needed weight, take this backend
          if (currentWeight < w)
          {
            backend = b;
            backendFound = true;
            break;
          }
        }
      }

      if (backend == null)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.enabled", request.getId()));

      // We are over the total weight and we are using the
      // first available node. Let's reset the index to 1
      // since we used this first node (0++).
      if (backendFound)
        currentWeight++; // Next time take the next
      else
        currentWeight = 1;
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
      return executeWRR(request, callType, errorMsgPrefix, metadataCache);
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
   * Backends management
   */

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#setWeight(String,
   *      int)
   */
  public void setWeight(String name, int w) throws SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("loadbalancer.weight.set", new String[]{
          String.valueOf(w), name}));

    weights.put(name, new Integer(w));
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
      return "RAIDb-1 with Weighted Round Robin Request load balancer: !!!Warning!!! No backend nodes found\n";
    else
      return "RAIDb-1 Weighted Round-Robin Request load balancer (" + size
          + " backends)\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#getRaidb1Xml
   */
  public String getRaidb1Xml()
  {
    return WeightedBalancer.getRaidbXml(weights,
        DatabasesXmlTags.ELT_RAIDb_1_WeightedRoundRobin);
  }

}
