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

package org.continuent.sequoia.driver.connectpolicy;

import org.continuent.sequoia.common.exceptions.NoMoreControllerException;
import org.continuent.sequoia.driver.ControllerInfo;
import org.continuent.sequoia.driver.SequoiaUrl;

/**
 * This class defines a RoundRobinConnectPolicy used when the Sequoia URL has
 * the following form:
 * jdbc:sequoia://node1,node2,node3/myDB?preferredController=roundRobin
 * <p>
 * Round robin starts with the first node in URL.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class RoundRobinConnectPolicy extends AbstractControllerConnectPolicy
{
  private int index = -1;

  /**
   * Creates a new <code>RandomConnectPolicy</code> object
   * 
   * @param controllerList the controller list on which the policy applies
   * @param pingDelayInMs Interval in milliseconds between two pings of a
   *          controller
   * @param controllerTimeoutInMs timeout in milliseconds after which a
   *          controller is considered as dead if it did not respond to pings
   * @param debugLevel the debug level to use
   * @see org.continuent.sequoia.driver.SequoiaUrl#DEBUG_LEVEL_OFF
   */
  public RoundRobinConnectPolicy(ControllerInfo[] controllerList,
      int pingDelayInMs, int controllerTimeoutInMs, int debugLevel)
  {
    super(controllerList, pingDelayInMs, controllerTimeoutInMs, debugLevel);
  }

  /**
   * @see org.continuent.sequoia.driver.connectpolicy.AbstractControllerConnectPolicy#getController()
   */
  public synchronized ControllerInfo getController()
      throws NoMoreControllerException
  {
    if (aliveControllers.isEmpty())
      throw new NoMoreControllerException();
    index++;
    if (index >= aliveControllers.size())
      index = 0;
    ControllerInfo selectedController = super.getControllerByNum(index);
    if (debugLevel == SequoiaUrl.DEBUG_LEVEL_DEBUG)
      System.out.println("Selected controller: " + selectedController);
    return selectedController;
  }
}
