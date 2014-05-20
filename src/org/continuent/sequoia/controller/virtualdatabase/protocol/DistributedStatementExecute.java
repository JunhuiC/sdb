/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Continuent, Inc.
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
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;

/**
 * This class defines a distributed call to Statement.execute()
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class DistributedStatementExecute extends DistributedRequest
{
  private static final long serialVersionUID = 8634424524848530342L;

  /**
   * Creates a new <code>DistributedStatementExecute</code> object to execute
   * a stored procedure on multiple controllers.
   * 
   * @param request the request to execute
   */
  public DistributedStatementExecute(AbstractRequest request)
  {
    super(request);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#scheduleRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Object scheduleRequest(DistributedRequestManager drm)
      throws SQLException
  {
    // Create a fake stored procedure
    LinkedList<Object> totalOrderQueue = drm.getVirtualDatabase().getTotalOrderQueue();
    synchronized (totalOrderQueue)
    {
      totalOrderQueue.addLast(request);
    }
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#executeScheduledRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Serializable executeScheduledRequest(DistributedRequestManager drm)
      throws SQLException
  {
    Serializable result = null;
    AbstractWriteRequest abstractWriteRequest = (AbstractWriteRequest) request;
    boolean hasBeenScheduled = false;
    try
    {
      drm.getLoadBalancer()
          .waitForSuspendWritesToComplete(abstractWriteRequest);
      // Even if we do not execute this query, we have to log the begin if any
      drm.lazyTransactionStart(abstractWriteRequest);
      drm.scheduleExecWriteRequest(abstractWriteRequest);
      hasBeenScheduled = true;

      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get("requestmanager.read.stored.procedure", new String[]{
                String.valueOf(abstractWriteRequest.getId()),
                abstractWriteRequest.getSqlShortForm(drm.getVirtualDatabase()
                    .getSqlShortFormLength())}));

      result = drm.loadBalanceStatementExecute(abstractWriteRequest);

      if (drm.storeRequestResult(abstractWriteRequest, result))
        result = DistributedRequestManager.SUCCESSFUL_COMPLETION;

      drm.updateAndNotifyExecWriteRequest(abstractWriteRequest, -1);

      return result;
    }
    catch (NoMoreBackendException e)
    {
      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get(
                "virtualdatabase.distributed.read.procedure.logging.only",
                abstractWriteRequest.getSqlShortForm(drm.getVirtualDatabase()
                    .getSqlShortFormLength())));

      drm.addFailedOnAllBackends(abstractWriteRequest, hasBeenScheduled);
      return e;
    }
    catch (AllBackendsFailedException e)
    {
      // Add to failed list, the scheduler will be notified when the response
      // will be received from the other controllers.
      drm.addFailedOnAllBackends(abstractWriteRequest, hasBeenScheduled);
      if (drm.getLogger().isDebugEnabled())
        drm
            .getLogger()
            .debug(
                Translate
                    .get(
                        "virtualdatabase.distributed.read.procedure.all.backends.locally.failed",
                        abstractWriteRequest.getSqlShortForm(drm
                            .getVirtualDatabase().getSqlShortFormLength())));
      return e;
    }
    catch (SQLException e)
    {
      // Something bad more likely happened during the notification. Let's
      // notify the scheduler (possibly again) to be safer.
      drm.addFailedOnAllBackends(abstractWriteRequest, hasBeenScheduled);
      drm.getLogger().warn(
          Translate.get(
              "virtualdatabase.distributed.read.procedure.sqlexception", e
                  .getMessage()), e);
      return e;
    }
    catch (RuntimeException re)
    {
      // Something bad more likely happened during the notification.
      drm.addFailedOnAllBackends(abstractWriteRequest, hasBeenScheduled);
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
    return "E " + request.getId() + " " + request.getTransactionId() + " "
        + request.getUniqueKey();
  }

}