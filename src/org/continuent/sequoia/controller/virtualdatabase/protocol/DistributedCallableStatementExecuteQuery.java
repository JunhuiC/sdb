/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.requests.StoredProcedureCallResult;

/**
 * This class defines a distributed call to CallableStatement.executeQuery()
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class DistributedCallableStatementExecuteQuery
    extends DistributedRequest
{
  private static final long serialVersionUID = 8634424524848530342L;

  /**
   * Creates a new <code>CallableStatementExecuteQuery</code> object to
   * execute a read stored procedure on multiple controllers.
   * 
   * @param proc the stored procedure to execute
   */
  public DistributedCallableStatementExecuteQuery(StoredProcedure proc)
  {
    super(proc);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#scheduleRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Object scheduleRequest(DistributedRequestManager drm)
      throws SQLException
  {
    LinkedList totalOrderQueue = drm.getVirtualDatabase().getTotalOrderQueue();
    synchronized (totalOrderQueue)
    {
      totalOrderQueue.addLast(request);
    }
    return request;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#executeScheduledRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Serializable executeScheduledRequest(DistributedRequestManager drm)
      throws SQLException
  {
    Serializable result = null;
    StoredProcedure proc = (StoredProcedure) request;
    boolean hasBeenScheduled = false;
    try
    {
      drm.getLoadBalancer().waitForSuspendWritesToComplete(request);

      // Even if we do not execute this query, we have to log the begin if any
      drm.lazyTransactionStart(request);
      drm.scheduleStoredProcedure((StoredProcedure) request);
      hasBeenScheduled = true;

      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get("requestmanager.read.stored.procedure", new String[]{
                String.valueOf(request.getId()),
                request.getSqlShortForm(drm.getVirtualDatabase()
                    .getSqlShortFormLength())}));


      DatabaseProcedureSemantic semantic = proc.getSemantic();
      if (proc.isReadOnly() || ((semantic != null) && (semantic.isReadOnly())))
      {
        // Need to remove the query from the total order queue because
        // loadBalancer.readOnlyCallableStatementExecuteQuery does not do it.
        // Fixes SEQUOIA-413
        drm.getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(request);
      }
      
      result = new StoredProcedureCallResult(proc, drm
          .loadBalanceCallableStatementExecuteQuery(proc));


      if (drm.storeRequestResult(request, result))
        result = DistributedRequestManager.SUCCESSFUL_COMPLETION;

      drm.updateRecoveryLogFlushCacheAndRefreshSchema(proc);

      // Notify scheduler of completion
      drm.getScheduler().storedProcedureCompleted(proc);

      return result;
    }
    catch (NoMoreBackendException e)
    {
      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get(
                "virtualdatabase.distributed.read.procedure.logging.only",
                request.getSqlShortForm(drm.getVirtualDatabase()
                    .getSqlShortFormLength())));

      DatabaseProcedureSemantic semantic = proc.getSemantic();
      if (proc.isReadOnly() || ((semantic != null) && (semantic.isReadOnly())))
      {
        // Need to remove the query from the total order queue because
        // loadBalancer.readOnlyCallableStatementExecuteQuery does not do it.
        // Fixes SEQUOIA-413
        drm.getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(request);
      }

      // Add to failed list, the scheduler will be notified when the response
      // will be received from the other controllers.
      drm.addFailedOnAllBackends(request, hasBeenScheduled);
      throw e;
    }
    catch (AllBackendsFailedException e)
    {
      // Add to failed list, the scheduler will be notified when the response
      // will be received from the other controllers.
      drm.addFailedOnAllBackends(request, hasBeenScheduled);
      if (drm.getLogger().isDebugEnabled())
        drm
            .getLogger()
            .debug(
                Translate
                    .get(
                        "virtualdatabase.distributed.read.procedure.all.backends.locally.failed",
                        request.getSqlShortForm(drm.getVirtualDatabase()
                            .getSqlShortFormLength())));
      return e;
    }
    catch (SQLException e)
    {
      // Add to failed list, the scheduler will be notified when the response
      // will be received from the other controllers.
      drm.addFailedOnAllBackends(request, hasBeenScheduled);
      drm.getLogger().warn(
          Translate.get(
              "virtualdatabase.distributed.read.procedure.sqlexception", e
                  .getMessage()), e);
      return e;
    }
    catch (RuntimeException re)
    {
      // Something bad more likely happened during the notification.
      // Add to failed list, the scheduler will be notified when the response
      // will be received from the other controllers.
      drm.addFailedOnAllBackends(request, hasBeenScheduled);
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.read.procedure.exception",
              re.getMessage()), re);
      return new SQLException(re.getMessage());
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "S " + request.getId() + " " + request.getTransactionId() + " "
        + request.getUniqueKey();
  }

}