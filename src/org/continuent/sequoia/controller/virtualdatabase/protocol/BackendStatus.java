/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
import java.util.List;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * Send the status of local backends to remote controllers.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class BackendStatus extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = -537987250588460222L;

  private List             /* <BackendInfo> */<BackendInfo> backendList;
  private long              controllerId;

  /**
   * Build a new BackendStatus object
   * 
   * @param backends a List&lt;BackendInfo&gt;
   * @param controllerId the sending controller identifier
   * @see org.continuent.sequoia.common.jmx.management.BackendInfo
   */
  public BackendStatus(List/* <BackendInfo> */<BackendInfo> backends, long controllerId)
  {
    backendList = backends;
    this.controllerId = controllerId;
  }

  /**
   * Get the list of backends info.
   * 
   * @return a List&lt;BackendInfo&gt; of the remote controller BackendInfo
   */
  public List/* <BackendInfo> */<BackendInfo> getBackendInfos()
  {
    return backendList;
  }

  /**
   * Returns the controllerId value.
   * 
   * @return Returns the controllerId.
   */
  public final long getControllerId()
  {
    return controllerId;
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

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    // Update backend list from sender
    List<DatabaseBackend> remoteBackends = BackendInfo.toDatabaseBackends(backendList);
    dvdb.addRemoteControllerId(sender, controllerId);
    dvdb.addBackendPerController(sender, remoteBackends);
    return new BackendStatus(
        DatabaseBackend.toBackendInfos(dvdb.getBackends()),
        ((DistributedRequestManager) dvdb.getRequestManager())
            .getControllerId());
  }

}