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
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;

/**
 * Execute a distributed call to Statement.executeUpdate(sql,
 * Statement.RETURN_GENERATED_KEYS) between several controllers.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class DistributedStatementExecuteUpdateWithKeys
    extends DistributedRequest
{
  private static final long serialVersionUID = -7075254412706395576L;

  /**
   * @param request write request to execute
   */
  public DistributedStatementExecuteUpdateWithKeys(AbstractWriteRequest request)
  {
    super(request);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#scheduleRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Object scheduleRequest(DistributedRequestManager drm)
      throws SQLException
  {
    LinkedList<Object> totalOrderQueue = drm.getVirtualDatabase().getTotalOrderQueue();
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
    boolean hasBeenScheduled = false;

    try
    {
      drm.getLoadBalancer().waitForSuspendWritesToComplete(request);
      // This call will trigger a lazyTransactionStart as well
      drm.scheduleExecWriteRequest((AbstractWriteRequest) request);
      hasBeenScheduled = true;

      Serializable execWriteRequestResult = null;
      execWriteRequestResult = drm
          .loadBalanceStatementExecuteUpdateWithKeys((AbstractWriteRequest) request);
      int updateCount = ((GeneratedKeysResult) execWriteRequestResult)
          .getUpdateCount();

      drm.storeRequestResult(request, execWriteRequestResult);

      drm.updateAndNotifyExecWriteRequest((AbstractWriteRequest) request,
          updateCount);

      return execWriteRequestResult;
    }
    catch (NoMoreBackendException e)
    {
      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get("virtualdatabase.distributed.write.logging.only",
                request.getSqlShortForm(drm.getVirtualDatabase()
                    .getSqlShortFormLength())));

      // Add to failed list, the scheduler will be notified when the response
      // will be received from the other controllers.
      drm.addFailedOnAllBackends(request, hasBeenScheduled);
      return e;
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
                        "virtualdatabase.distributed.write.all.backends.locally.failed",
                        request.getSqlShortForm(drm.getVirtualDatabase()
                            .getSqlShortFormLength())));
      return e;
    }
    catch (SQLException e)
    {
      // Something bad more likely happened during the update but the scheduler
      // has already been notified.
      drm.addFailedOnAllBackends(request, hasBeenScheduled);
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.write.sqlexception", e
              .getMessage()), e);
      return e;
    }
    catch (RuntimeException re)
    {
      // Something bad more likely happened during the update but the scheduler
      // has already been notified.
      drm.addFailedOnAllBackends(request, hasBeenScheduled);
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.write.exception", re
              .getMessage()), re);
      return new SQLException(re.getMessage());
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "W " + request.getId() + " " + request.getTransactionId() + " "
        + request.getUniqueKey();
  }

}