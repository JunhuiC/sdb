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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.loadbalancer.raidb2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTablePolicy;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-2 Round Robin load balancer featuring (Least Pending Requests First
 * load balancing algorithm).
 * <p>
 * The read requests coming from the request manager are sent to the node that
 * has the Least pending read requests among the nodes that can execute the
 * request.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class RAIDb2_LPRF extends RAIDb2
{

  /*
   * How the code is organized ? 1. Member variables 2. Constructor(s) 3.
   * Request handling 4. Debug/Monitoring
   */

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-2 Round Robin request load balancer.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @param waitForCompletionPolicy how many backends must complete before
   *          returning the result?
   * @param createTablePolicy the policy defining how 'create table' statements
   *          should be handled
   * @exception Exception if an error occurs
   */
  public RAIDb2_LPRF(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy,
      CreateTablePolicy createTablePolicy) throws Exception
  {
    super(vdb, waitForCompletionPolicy, createTablePolicy);
  }

  /*
   * Request Handling
   */

  /**
   * Chooses the node to execute the request using a round-robin algorithm. If
   * the next node has not the tables needed to execute the requests, we try the
   * next one and so on until a suitable backend is found.
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

    DatabaseBackend backend = null; // The backend that will execute the query

    // Note that vdb lock is released in the finally clause of this try/catch
    // block
    try
    {
      ArrayList backends = vdb.getBackends();
      int size = backends.size();

      if (size == 0)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.available", request.getId()));

      // Choose the backend that has the least pending requests
      int leastRequests = 0;
      int enabledBackends = 0;
      Collection tables = request.getFrom();
      if (tables == null)
        throw new SQLException(Translate.get("loadbalancer.from.not.found",
            request.getSqlShortForm(vdb.getSqlShortFormLength())));

      for (int i = 0; i < size; i++)
      {
        DatabaseBackend b = (DatabaseBackend) backends.get(i);
        if (b.isReadEnabled())
        {
          enabledBackends++;
          if (b.hasTables(tables))
          {
            int pending = b.getPendingRequests().size();
            if (((backend == null) || (pending < leastRequests)))
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

      if (backend == null)
      {
        if (enabledBackends == 0)
          throw new NoMoreBackendException(Translate.get(
              "loadbalancer.execute.no.backend.enabled", request.getId()));
        else
          throw new SQLException(Translate.get(
              "loadbalancer.backend.no.required.tables", tables.toString()));
      }

    }
    catch (RuntimeException e)
    {
      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      vdb.releaseReadLockBackendLists();
    }

    // Execute the request on the chosen backend
    ControllerResultSet rs = null;
    try
    {
      rs = executeReadRequestOnBackend(request, backend, metadataCache);
    }
    catch (UnreachableBackendException se)
    {
      // Try on another backend
      return statementExecuteQuery(request, metadataCache);
    }
    catch (SQLException se)
    {
      String msg = Translate.get("loadbalancer.request.failed", new String[]{
          String.valueOf(request.getId()), se.getMessage()});
      if (logger.isInfoEnabled())
        logger.info(msg);
      throw new SQLException(msg);
    }
    catch (Throwable e)
    {
      String msg = Translate.get("loadbalancer.request.failed.on.backend",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }

    return rs;
  }

  /**
   * Chooses the node to execute the stored procedure using a LPRF algorithm. If
   * the next node has not the needed stored procedure, we try the next one and
   * so on until a suitable backend is found.
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
   * Chooses the node to execute the stored procedure using a LPRF algorithm. If
   * the next node has not the needed stored procedure, we try the next one and
   * so on until a suitable backend is found.
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

    DatabaseBackend backend = null; // The backend that will execute the query

    // Note that vdb lock is released in the finally clause of this try/catch
    // block
    try
    {
      DatabaseBackend failedBackend = null;
      SQLException failedException = null;
      Object result = null;
      do
      {
        ArrayList backends = vdb.getBackends();
        int size = backends.size();

        if (size == 0)
          throw new NoMoreBackendException(Translate.get(
              "loadbalancer.execute.no.backend.available", proc.getId()));

        // Choose the backend that has the least pending requests
        int leastRequests = 0;
        int enabledBackends = 0;

        for (int i = 0; i < size; i++)
        {
          DatabaseBackend b = (DatabaseBackend) backends.get(i);
          if (b.isReadEnabled())
          {
            enabledBackends++;
            if (b.hasStoredProcedure(proc.getProcedureKey(), proc
                .getNbOfParameters()))
            {
              int pending = b.getPendingRequests().size();
              if ((b != failedBackend)
                  && ((backend == null) || (pending < leastRequests)))
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

        if (backend == null)
        {
          if (enabledBackends == 0)
            throw new SQLException(Translate
                .get("loadbalancer.storedprocedure.backend.no.enabled", proc
                    .getId()));
          else if (failedBackend == null)
            throw new SQLException(Translate.get(
                "loadbalancer.backend.no.required.storedprocedure", proc
                    .getProcedureKey()));
          else
            // Bad query, the only backend that could execute it has failed
            throw failedException;
        }

        // Execute the request on the chosen backend
        boolean toDisable = false;
        try
        {
          switch (callType)
          {
            case CALLABLE_STATEMENT_EXECUTE_QUERY :
              result = executeStoredProcedureOnBackend(proc, true, backend,
                  metadataCache);
              break;
            case CALLABLE_STATEMENT_EXECUTE :
              result = executeStoredProcedureOnBackend(proc, false, backend,
                  metadataCache);
              break;
            default :
              throw new RuntimeException("Unhandled call type " + callType
                  + " in executeLPRF");
          }
          if (failedBackend != null)
          { // Previous backend failed
            if (logger.isWarnEnabled())
              logger.warn(Translate.get("loadbalancer.storedprocedure.status",
                  new String[]{String.valueOf(proc.getId()), backend.getName(),
                      failedBackend.getName()}));
            toDisable = true;
          }
        }
        catch (UnreachableBackendException se)
        {
          // Retry on an other backend.
          continue;
        }
        catch (SQLException se)
        {
          if (failedBackend != null)
          { // Bad query, no backend can execute it
            String msg = Translate.get(
                "loadbalancer.storedprocedure.failed.twice", new String[]{
                    String.valueOf(proc.getId()), se.getMessage()});
            if (logger.isInfoEnabled())
              logger.info(msg);
            throw new SQLException(msg);
          }
          else
          { // We are the first to fail on this query
            failedBackend = backend;
            failedException = se;
            if (logger.isInfoEnabled())
              logger.info(Translate.get(
                  "loadbalancer.storedprocedure.failed.on.backend",
                  new String[]{
                      proc.getSqlShortForm(vdb.getSqlShortFormLength()),
                      backend.getName(), se.getMessage()}));
            continue;
          }
        }

        if (toDisable)
        { // retry has succeeded and we need to disable the first node that
          // failed
          try
          {
            if (logger.isWarnEnabled())
              logger.warn(Translate.get("loadbalancer.backend.disabling",
                  failedBackend.getName()));
            disableBackend(failedBackend, true);
          }
          catch (SQLException ignore)
          {
          }
          finally
          {
            failedBackend = null; // to exit the do{}while
          }
        }
      }
      while (failedBackend != null);
      return result;
    }
    catch (RuntimeException e)
    {
      String msg = Translate.get(
          "loadbalancer.storedprocedure.failed.on.backend", new String[]{
              proc.getSqlShortForm(vdb.getSqlShortFormLength()),
              backend.getName(), e.getMessage()});
      logger.fatal(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      vdb.releaseReadLockBackendLists();
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
    // We don't lock since we don't need a completely accurate value
    int size = vdb.getBackends().size();

    if (size == 0)
      return "RAIDb-2 Least Pending Requests First load balancer: !!!Warning!!! No backend nodes found\n";
    else
      return "RAIDb-2 Least Pending Requests First load balancer (" + size
          + " backends)\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2#getRaidb2Xml
   */
  public String getRaidb2Xml()
  {
    return "<" + DatabasesXmlTags.ELT_RAIDb_2_LeastPendingRequestsFirst + "/>";
  }
}