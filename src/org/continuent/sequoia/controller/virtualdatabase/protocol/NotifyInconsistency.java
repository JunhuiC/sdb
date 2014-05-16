/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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

import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This class defines a inconsistency notification command that is sent to
 * controllers that reported a result that is inconsistent with other
 * controllers. As a consequence, all backends of the inconsistent controllers
 * are disabled.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class NotifyInconsistency extends DistributedRequest
{
  private static final long serialVersionUID = -5744767614853747783L;

  /**
   * Creates a new <code>NotifyInconsistency</code> object
   * 
   * @param request the request that generated the inconsistency
   */
  public NotifyInconsistency(AbstractRequest request)
  {
    super(request);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#scheduleRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public final Object scheduleRequest(DistributedRequestManager drm)
      throws SQLException
  {
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#executeScheduledRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public final Serializable executeScheduledRequest(
      DistributedRequestManager drm) throws SQLException
  {
    try
    {
      drm.getLogger()
          .warn(
              "Disabling all backends after an inconsistency was detected for request "
                  + request.getId()
                  + (request.isAutoCommit() ? "" : " "
                      + request.getTransactionId()) + " " + request);
      drm.getVirtualDatabase().disableAllBackends(true);
    }
    catch (VirtualDatabaseException e)
    {
      drm
          .getLogger()
          .error(
              "An error occured while disabling all backends after an inconsistency was detected for request "
                  + request.getId()
                  + (request.isAutoCommit() ? "" : " "
                      + request.getTransactionId()) + " " + request, e);
    }
    return null;
  }

}