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

package org.continuent.sequoia.controller.virtualdatabase.management;

import org.continuent.sequoia.common.exceptions.NotImplementedException;

/**
 * This class defines a TransferDumpOperation
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class TransferDumpOperation extends AbstractAdminOperation
{
  private String dumpName;
  private String controllerDestination;

  /**
   * Creates a new <code>TransferDumpOperation</code> object
   * 
   * @param dumpName the name of the transferred dump
   * @param remoteControllerName name of the destination controller
   */
  public TransferDumpOperation(String dumpName, String remoteControllerName)
  {
    this.dumpName = dumpName;
    this.controllerDestination = remoteControllerName;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.management.AbstractAdminOperation#cancel()
   */
  public void cancel() throws NotImplementedException
  {
    throw new NotImplementedException(
        "Cancel operation is not implemented for TransferDumpOperation");
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.management.AbstractAdminOperation#getDescription()
   */
  public String getDescription()
  {
    return "Transferring dump " + dumpName + " to controller"
        + controllerDestination;
  }

}
