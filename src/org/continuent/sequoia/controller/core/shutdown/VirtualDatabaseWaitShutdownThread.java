/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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

package org.continuent.sequoia.controller.core.shutdown;

import org.continuent.sequoia.common.exceptions.ShutdownException;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * This thread waits for all clients to terminate their connections before
 * shutting down the virtual database.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class VirtualDatabaseWaitShutdownThread
    extends VirtualDatabaseShutdownThread
{

  /**
   * Creates a new <code>VirtualDatabaseSafeShutdownThread</code> object
   * 
   * @param vdb the VirtualDatabase to shutdown
   */
  public VirtualDatabaseWaitShutdownThread(VirtualDatabase vdb)
  {
    super(vdb, Constants.SHUTDOWN_WAIT);
  }

  /**
   * @see org.continuent.sequoia.controller.core.shutdown.ShutdownThread#shutdown()
   */
  public void shutdown() throws ShutdownException
  {
    this.waitForClientsToDisconnect();
    this.terminateVirtualDatabaseWorkerThreads();
    this.disableAllBackendsWithCheckpoint();
    virtualDatabase.setShutdownCheckpoint();
    this.shutdownCacheRecoveryLogAndGroupCommunication();
  }

}