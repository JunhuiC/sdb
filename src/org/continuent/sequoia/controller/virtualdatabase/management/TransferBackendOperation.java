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
 * This class defines a TransferBackendOperation
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class TransferBackendOperation extends AbstractAdminOperation
{
  private String backendName;
  private String controllerDestination;

  /**
   * Creates a new <code>TransferBackendOperation</code> object
   * 
   * @param backendName name of the backend being transferred
   * @param controllerDestination controller the backend is being transferred to
   */
  public TransferBackendOperation(String backendName,
      String controllerDestination)
  {
    this.backendName = backendName;
    this.controllerDestination = controllerDestination;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.management.AbstractAdminOperation#cancel()
   */
  public void cancel() throws NotImplementedException
  {
    throw new NotImplementedException(
        "Cancel operation is not implemented for TransferBackendOperation");
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.management.AbstractAdminOperation#getDescription()
   */
  public String getDescription()
  {
    return "Transferring backend " + backendName + " to controller"
        + controllerDestination;
  }

}
