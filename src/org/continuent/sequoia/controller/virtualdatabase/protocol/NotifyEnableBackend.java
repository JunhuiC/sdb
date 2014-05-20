/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): Damian Arregui.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * Send the information that the sender wants to enable the specified backend.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Damian.Arregui@continuent.com">Damian Arregui </a>
 * @version 1.0
 */
public class NotifyEnableBackend extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = 5783006534850234709L;

  private BackendInfo       backendInfo;

  /**
   * Creates a new EnableBackend object with the specified backend information.
   * 
   * @param backendInfo information on the backend to enable
   */
  public NotifyEnableBackend(BackendInfo backendInfo)
  {
    this.backendInfo = backendInfo;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    Trace logger = dvdb.getLogger();
    List<DatabaseBackend> remoteBackends = (List<DatabaseBackend>) dvdb.getBackendsPerController().get(sender);
    if (remoteBackends == null)
    { // This case was reported by Alessandro Gamboz on April 1, 2005.
      // It looks like the EnableBackend message arrives before membership
      // has been properly updated.
      logger.warn("No information has been found for remote controller "
          + sender);
      remoteBackends = new ArrayList<DatabaseBackend>();
      dvdb.addBackendPerController(sender, remoteBackends);
    }
    DatabaseBackend enabledBackend = backendInfo.getDatabaseBackend();
    int size = remoteBackends.size();
    boolean backendFound = false;
    for (int i = 0; i < size; i++)
    {
      DatabaseBackend remoteBackend = (DatabaseBackend) remoteBackends.get(i);
      if (remoteBackend.equals(enabledBackend))
      {
        logger.info("Backend " + remoteBackend.getName()
            + " enabled on controller " + sender);
        remoteBackends.set(i, enabledBackend);
        backendFound = true;
        break;
      }
    }
    if (!backendFound)
    {
      logger.warn("Updating backend list with unknown backend "
          + enabledBackend.getName() + " enabled on controller " + sender);
      remoteBackends.add(enabledBackend);
    }
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    return null;
  }

}