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
 * Initial developer(s): Damian Arregui.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.util.List;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * Transports the response to a <code>VirtualDatabaseConfiguration</code>
 * message.
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damián Arregui</a>
 * @version 1.0
 */
public class VirtualDatabaseConfigurationResponse
    extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = -6800865688275066016L;

  private long              controllerId;
  private List              additionalVdbUsers;

  /**
   * Creates a new <code>VirtualDatabaseConfigurationResponse</code> object
   * 
   * @param controllerId the responsding controller id or
   *          <code>DistributedVirtualDatabase.INCOMPATIBLE_CONFIGURATION</code>
   *          if the configurations are not compatible.
   * @param additionalVdbUsers a list of <code>VirtualDatabaseUser</code>
   *          objects if the responding controller has additional vdb users,
   *          null otherwise.
   */
  public VirtualDatabaseConfigurationResponse(long controllerId,
      List additionalVdbUsers)
  {
    this.controllerId = controllerId;
    this.additionalVdbUsers = additionalVdbUsers;
  }

  /**
   * Returns the additional vdb users in the local configuration with respect to
   * the remote configuration.
   * 
   * @return a <code>List</code> of VirtualDatabaseUser objects or null if
   *         there are no additional vdb users.
   */
  public List getAdditionalVdbUsers()
  {
    return additionalVdbUsers;
  }

  /**
   * Returns the local controller id.
   * 
   * @return the local controller id or
   *         <code>DistributedVirtualDatabase.INCOMPATIBLE_CONFIGURATION</code>
   *         if the configurations are not compatible.
   */
  public long getControllerId()
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
    // TODO Auto-generated method stub
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
    // TODO Auto-generated method stub
    return null;
  }

}
