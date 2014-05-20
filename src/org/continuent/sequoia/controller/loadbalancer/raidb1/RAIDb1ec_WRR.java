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
import java.util.HashMap;

import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.loadbalancer.WeightedBalancer;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-1 Weighted Round Robin load balancer with error checking.
 * <p>
 * This load balancer tolerates byzantine failures of databases. The read
 * requests coming from the Request Manager are sent to multiple backend nodes
 * and the results are compared. Write requests are broadcasted to all backends.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @version 1.0
 */
public class RAIDb1ec_WRR extends RAIDb1ec
{
  /*
   * How the code is organized ? 1. Member variables 2. Constructor(s) 3.
   * Request handling 4. Debug/Monitoring
   */

  // private int index; // index in the backend vector the Round-Robin
  private HashMap<String, Integer> weights;

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-1 Weighted Round Robin with error checking request load
   * balancer.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy How many backends must complete before
   *          returning the result?
   * @param errorCheckingPolicy policy to apply for error checking.
   * @param nbOfConcurrentReads number of concurrent reads allowed
   * @exception Exception if an error occurs
   */
  public RAIDb1ec_WRR(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy,
      ErrorCheckingPolicy errorCheckingPolicy, int nbOfConcurrentReads)
      throws Exception
  {
    super(vdb, waitForCompletionPolicy, errorCheckingPolicy,
        nbOfConcurrentReads);
    // index = -1;
  }

  /*
   * Request Handling
   */

  /**
   * Not implemented.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#statementExecuteQuery(SelectRequest,
   *      MetadataCache)
   */
  public ControllerResultSet execSingleBackendReadRequest(
      SelectRequest request, MetadataCache metadataCache) throws SQLException
  {
    throw new NotImplementedException(this.getClass().getName()
        + ":execSingleBackendReadRequest");
  }

  /**
   * Not implemented.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecuteQuery(StoredProcedure,
   *      MetadataCache)
   */
  public ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException
  {
    throw new NotImplementedException(this.getClass().getName()
        + ":readOnlyCallableStatementExecuteQuery");
  }

  /**
   * Not implemented.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(org.continuent.sequoia.controller.requests.StoredProcedure,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public ExecuteResult readOnlyCallableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws SQLException
  {
    throw new NotImplementedException(this.getClass().getName()
        + ":readOnlyCallableStatementExecute");
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
      return "RAIDb-1 Error Checking with Weighted Round-Robin Request load balancer: "
          + "!!!Warning!!! No backend nodes found\n";
    else
      return "RAIDb-1 Error Checking with Weighted Round-Robin Request load balancer ("
          + size + " backends)\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1#getRaidb1Xml
   */
  public String getRaidb1Xml()
  {
    return WeightedBalancer.getRaidbXml(weights,
        DatabasesXmlTags.ELT_RAIDb_1ec_WeightedRoundRobin);
  }
}