/**
 * Sequoia: Database clustering technology.
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
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * Execute a distributed read request such as SELECT FOR ... UPDATE.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class DistributedStatementExecuteQuery extends DistributedRequest
{
  private static final long serialVersionUID = -7183844510032678987L;

  /**
   * Creates a new <code>ExecRemoteStatementExecuteQuery</code> object.
   * 
   * @param request select request to execute
   */
  public DistributedStatementExecuteQuery(SelectRequest request)
  {
    super(request);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#scheduleRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Object scheduleRequest(DistributedRequestManager drm)
      throws SQLException
  {
    if (((SelectRequest) request).isMustBroadcast())
    { // Total order queue is only needed for broadcasted select statements
      LinkedList<Object> totalOrderQueue = drm.getVirtualDatabase()
          .getTotalOrderQueue();
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.addLast(request);
      }
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
    SelectRequest selectRequest = ((SelectRequest) request);
    try
    {
      if (selectRequest.isMustBroadcast() || (!selectRequest.isAutoCommit()))
      {
        drm.getLoadBalancer().waitForSuspendWritesToComplete(request);
        // Lazily start the transaction and log begin since this request will be
        // logged as well. Else let the transaction be started lazily by the
        // load balancer without logging the transaction begin in the recovery
        // log.
        drm.lazyTransactionStart(request);
      }

      result = drm.execLocalStatementExecuteQuery(selectRequest);

      if (drm.storeRequestResult(request, result))
        result = DistributedRequestManager.SUCCESSFUL_COMPLETION;

      return result;
    }
    catch (NoMoreBackendException e)
    {
      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get("virtualdatabase.distributed.write.logging.only",
                request.getSqlShortForm(drm.getVirtualDatabase()
                    .getSqlShortFormLength())));

      /**
       * Add the failed request to the failedOnAllBackends list for later
       * notification (@see
       * DistributedRequestManager#completeFailedOnAllBackends(AbstractRequest,
       * boolean)). The request will be logged later if it was successful on
       * other controllers.
       * <p>
       * The code in drm.execLocalStatementExecuteQuery always notify the
       * scheduler (even in the presence of failures), so there is no need for
       * further scheduler notification (this explains the false in
       * drm.addFailedOnAllBackends(request, false)).
       */
      drm.addFailedOnAllBackends(request, false);

      return e;
    }
    catch (SQLException e)
    {
      /**
       * Add the failed request to the failedOnAllBackends list for later
       * notification (@see
       * DistributedRequestManager#completeFailedOnAllBackends(AbstractRequest,
       * boolean)). The request will be logged later if it was successful on
       * other controllers.
       * <p>
       * The code in drm.execLocalStatementExecuteQuery always notify the
       * scheduler (even in the presence of failures), so there is no need for
       * further scheduler notification (this explains the false in
       * drm.addFailedOnAllBackends(request, false)).
       */
      drm.addFailedOnAllBackends(request, false);
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.read.sqlexception", e
              .getMessage()), e);
      if (selectRequest.isMustBroadcast())
      {
        // Be sure to remove the query from the total order queue if failure
        // occured before removal from the queue
        drm.getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(request);
      }
      return e;
    }
    catch (RuntimeException re)
    {
      /**
       * Add the failed request to the failedOnAllBackends list for later
       * notification (@see
       * DistributedRequestManager#completeFailedOnAllBackends(AbstractRequest,
       * boolean)). The request will be logged later if it was successful on
       * other controllers.
       * <p>
       * The code in drm.execLocalStatementExecuteQuery always notify the
       * scheduler (even in the presence of failures), so there is no need for
       * further scheduler notification (this explains the false in
       * drm.addFailedOnAllBackends(request, false)).
       */
      drm.addFailedOnAllBackends(request, false);
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.read.exception", re
              .getMessage()), re);
      if (selectRequest.isMustBroadcast())
      {
        // Be sure to remove the query from the total order queue if failure
        // occured before removal from the queue
        drm.getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(request);
      }
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