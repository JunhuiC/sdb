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

package org.continuent.sequoia.controller.loadbalancer.raidb2;

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
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTablePolicy;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-2 Weighted Round Robin load balancer.
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
 * @version 1.0
 */
public class RAIDb2_WRR extends RAIDb2
{
  private HashMap weights;
  private int     currentWeight;

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-2 Weighted Round Robin request load balancer.
   * 
   * @param vdb The virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy How many backends must complete before
   *          returning the result?
   * @param createTablePolicy The policy defining how 'create table' statements
   *          should be handled
   * @exception Exception if an error occurs
   */
  public RAIDb2_WRR(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy,
      CreateTablePolicy createTablePolicy) throws Exception
  {
    super(vdb, waitForCompletionPolicy, createTablePolicy);
    currentWeight = -1;
  }

  /*
   * Request Handling
   */

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#statementExecuteQuery(org.continuent.sequoia.controller.requests.SelectRequest,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request,
      MetadataCache metadataCache) throws SQLException
  {
    return null;
  }

  /**
   * Chooses the node to execute the stored procedure using a round-robin
   * algorithm. If the next node has not the needed stored procedure, we try the
   * next one and so on until a suitable backend is found.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    return (ControllerResultSet) callStoredProcedure(proc,
        CALLABLE_STATEMENT_EXECUTE_QUERY, metadataCache);
  }

  /**
   * Chooses the node to execute the stored procedure using a round-robin
   * algorithm. If the next node has not the needed stored procedure, we try the
   * next one and so on until a suitable backend is found.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(org.continuent.sequoia.controller.requests.StoredProcedure,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public ExecuteResult readOnlyCallableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    return (ExecuteResult) callStoredProcedure(proc,
        CALLABLE_STATEMENT_EXECUTE, metadataCache);
  }

  /**
   * Common code to execute a StoredProcedure using executeQuery or
   * executeUpdate on a backend chosen using a LPRF algorithm.
   * 
   * @param proc a <code>StoredProcedure</code>
   * @param callType one of CALLABLE_STATEMENT_EXECUTE_QUERY or
   *          CALLABLE_STATEMENT_EXECUTE
   * @param metadataCache the metadataCache if any or null
   * @return a <code>ControllerResultSet</code> or an
   *         <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   */
  private Object callStoredProcedure(StoredProcedure proc, int callType,
      MetadataCache metadataCache) throws SQLException
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
      ArrayList backends = vdb.getBackends();
      int size = backends.size();

      if (size == 0)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.available", proc.getId()));

      // Choose the backend (WRR algorithm starts here)
      int w = 0; // cumulative weight
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend b = (DatabaseBackend) backends.get(i);
        if (b.isReadEnabled()
            && b.hasStoredProcedure(proc.getProcedureKey(), proc
                .getNbOfParameters()))
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
          if (currentWeight <= w)
          {
            backend = b;
            currentWeight++; // Next time take the next
            break;
          }
        }
      }

      if (backend == null)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.enabled", proc.getId()));

      // We are over the total weight and we are using the
      // first available node. Let's reset the index to 1
      // since we used this first node (0++).
      if (currentWeight > w)
        currentWeight = 1;
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.execute.find.backend.failed",
          new String[]{proc.getSqlShortForm(vdb.getSqlShortFormLength()),
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
        case CALLABLE_STATEMENT_EXECUTE_QUERY :
          return executeStoredProcedureOnBackend(proc, true, backend,
              metadataCache);
        case CALLABLE_STATEMENT_EXECUTE :
          return executeStoredProcedureOnBackend(proc, false, backend,
              metadataCache);
        default :
          throw new RuntimeException("Unhandled call type " + callType
              + " in executeRoundRobin");
      }
    }
    catch (UnreachableBackendException urbe)
    {
      // Try to execute query on different backend
      return callStoredProcedure(proc, callType, metadataCache);
    }
    catch (SQLException se)
    {
      String msg = Translate.get("loadbalancer.something.failed", new String[]{
          "Stored procedure ", String.valueOf(proc.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw se;
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.something.failed.on",
          new String[]{"Stored procedure ",
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
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
    throw new SQLException("Weight is not supported with this load balancer");
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
    if (weights == null)
      return "RAIDb-2 Weighted Round Robin Request load balancer: !!!Warning!!! No backend nodes found\n";
    else
      return "RAIDb-2 Weighted Round Robin Request load balancer balancing over "
          + weights.size() + " nodes\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2#getRaidb2Xml
   */
  public String getRaidb2Xml()
  {
    return WeightedBalancer.getRaidbXml(weights,
        DatabasesXmlTags.ELT_RAIDb_2_WeightedRoundRobin);
  }
}