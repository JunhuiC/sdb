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
import java.util.HashMap;

import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.loadbalancer.WeightedBalancer;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTablePolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-2 Weighted Round Robin load balancer with error checking.
 * <p>
 * This load balancer tolerates byzantine failures of databases. The read
 * requests coming from the request manager are sent to multiple backend nodes
 * and the results are compared. Write requests are broadcasted to all backends.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @version 1.0
 */
public class RAIDb2ec_WRR extends RAIDb2ec
{
  /*
   * How the code is organized ? 1. Member variables 2. Constructor(s) 3.
   * Request handling 4. Debug/Monitoring
   */

  private HashMap backends;

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-2 weighted round robin with error checking request load
   * balancer.
   * 
   * @param vdb The virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy How many backends must complete before
   *          returning the result?
   * @param createTablePolicy The policy defining how 'create table' statements
   *          should be handled
   * @param errorCheckingPolicy Policy to apply for error checking.
   * @param nbOfConcurrentReads Number of concurrent reads allowed
   * @exception Exception if an error occurs
   */
  public RAIDb2ec_WRR(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy,
      CreateTablePolicy createTablePolicy,
      ErrorCheckingPolicy errorCheckingPolicy, int nbOfConcurrentReads)
      throws Exception
  {
    super(vdb, waitForCompletionPolicy, createTablePolicy, errorCheckingPolicy,
        nbOfConcurrentReads);
  }

  /*
   * Request Handling
   */

  /**
   * Performs a read request. It is up to the implementation to choose to which
   * backend node(s) this request should be sent.
   * 
   * @param request an <code>SelectRequest</code>
   * @param metadataCache cached metadata to use to construct the result set
   * @return the corresponding <code>java.sql.ResultSet</code>
   * @exception SQLException if an error occurs
   * @see org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2#statementExecuteQuery(SelectRequest,
   *      MetadataCache)
   */
  public ControllerResultSet statementExecuteQuery(SelectRequest request,
      MetadataCache metadataCache) throws SQLException
  {
    throw new NotImplementedException(this.getClass().getName()
        + ":statementExecuteQuery");
  }

  /**
   * Not implemented.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(StoredProcedure,
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
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#readOnlyCallableStatementExecute(StoredProcedure,
   *      MetadataCache)
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
    if (backends == null)
      return "RAIDb-2 Error Checking with Weighted Round Robin Request load balancer: "
          + "!!!Warning!!! No backend nodes found\n";
    else
      return "RAIDb-2 Error Checking with Weighted Round Robin Request load balancer balancing over "
          + backends.size() + " nodes\n";
  }

  /**
   * @see RAIDb2#getRaidb2Xml()
   */
  public String getRaidb2Xml()
  {
    return WeightedBalancer.getRaidbXml(backends,
        DatabasesXmlTags.ELT_RAIDb_2ec_WeightedRoundRobin);
  }
}