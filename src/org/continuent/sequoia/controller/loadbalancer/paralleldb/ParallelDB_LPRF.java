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

package org.continuent.sequoia.controller.loadbalancer.paralleldb;

import java.sql.SQLException;
import java.util.ArrayList;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * This class defines a ParallelDB_LPRF load balancer. This load balancer
 * chooses the node that has the least pending queries for read and write
 * queries execution.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ParallelDB_LPRF extends ParallelDB
{

  /**
   * Creates a new <code>ParallelDB_LPRF</code> object.
   * 
   * @param vdb the virtual database this load balancer belongs to.
   * @throws Exception if an error occurs
   */
  public ParallelDB_LPRF(VirtualDatabase vdb) throws Exception
  {
    super(vdb);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.paralleldb.ParallelDB#chooseBackendForReadRequest(org.continuent.sequoia.controller.requests.AbstractRequest)
   */
  public DatabaseBackend chooseBackendForReadRequest(AbstractRequest request)
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

    DatabaseBackend backend = null; // The backend that will execute the query

    // Note that vdb lock is released in the finally clause of this try/catch
    // block
    try
    {
      ArrayList<?> backends = vdb.getBackends();
      int size = backends.size();

      if (size == 0)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.available", request.getId()));

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

      if (backend == null)
        throw new SQLException(Translate.get(
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

    return backend;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.paralleldb.ParallelDB#chooseBackendForWriteRequest(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public DatabaseBackend chooseBackendForWriteRequest(
      AbstractWriteRequest request) throws SQLException
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
      ArrayList<?> backends = vdb.getBackends();
      int size = backends.size();

      if (size == 0)
        throw new NoMoreBackendException(Translate.get(
            "loadbalancer.execute.no.backend.available", request.getId()));

      // Choose the backend that has the least pending requests
      int leastRequests = 0;
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend b = (DatabaseBackend) backends.get(i);
        if (b.isWriteEnabled() && !b.isDisabling())
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

      if (backend == null)
      {
        throw new SQLException(Translate.get(
            "loadbalancer.execute.no.backend.enabled", request.getId()));
      }
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

    return backend;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getInformation()
   */
  public String getInformation()
  {
    // We don't lock since we don't need a top accurate value
    int size = vdb.getBackends().size();

    if (size == 0)
      return "ParallelDB Least Pending Request First Request load balancer: !!!Warning!!! No backend nodes found\n";
    else
      return "ParallelDB Least Pending Request First Request load balancer ("
          + size + " backends)\n";
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.paralleldb.ParallelDB#getParallelDBXml()
   */
  public String getParallelDBXml()
  {
    return "<" + DatabasesXmlTags.ELT_ParallelDB_LeastPendingRequestsFirst
        + "/>";
  }
}