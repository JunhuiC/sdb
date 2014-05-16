/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Damian Arregui.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a IsValidUserForAllBackends command.
 * 
 * @author <a href="mailto:Damian.Arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public class IsValidUserForAllBackends
    extends DistributedVirtualDatabaseMessage
{
  private static final long   serialVersionUID = -1267300635779611796L;

  private VirtualDatabaseUser vdbUser;

  /**
   * Creates a new <code>IsValidUserForAllBackends</code> object
   * 
   * @param vdbUser user to be checked on backends.
   */
  public IsValidUserForAllBackends(VirtualDatabaseUser vdbUser)
  {
    this.vdbUser = vdbUser;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    return (Boolean) handleMessageSingleThreadedResult;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    // Checking authenticationManager.isTransparentLoginEnabled() is redundant
    // if the request has been issued through this controller: the same check is
    // performed at the vdb worker thread level.
    return new Boolean(dvdb.getAuthenticationManager()
        .isTransparentLoginEnabled()
        && dvdb.isValidUserForAllBackends(vdbUser));
  }

}
