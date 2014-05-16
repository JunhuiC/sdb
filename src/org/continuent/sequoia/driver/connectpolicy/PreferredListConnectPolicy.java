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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.exceptions.NoMoreControllerException;
import org.continuent.sequoia.driver.ControllerInfo;
import org.continuent.sequoia.driver.SequoiaUrl;
import org.continuent.sequoia.driver.connectpolicy.AbstractControllerConnectPolicy.ControllerAndVdbState;

/**
 * This class defines a PreferredListConnectPolicy
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class PreferredListConnectPolicy extends AbstractControllerConnectPolicy
{
  private int       index = -1;
  private int       preferredIndex = -1;
  private ArrayList preferredControllers;
  private ArrayList originalPreferredControllers;

  /**
   * Creates a new <code>PreferredListConnectPolicy</code> object
   * 
   * @param controllerList the controller list on which the policy applies
   * @param preferredControllerList comma separated list of preferred
   *          controllers
   * @param pingDelayInMs Interval in milliseconds between two pings of a
   *          controller
   * @param controllerTimeoutInMs timeout in milliseconds after which a
   *          controller is considered as dead if it did not respond to pings
   * @param debugLevel the debug level to use
   * @see org.continuent.sequoia.driver.SequoiaUrl#DEBUG_LEVEL_OFF
   */
  public PreferredListConnectPolicy(ControllerInfo[] controllerList,
      String preferredControllerList, int pingDelayInMs,
      int controllerTimeoutInMs, int debugLevel)
  {
    super(controllerList, pingDelayInMs, controllerTimeoutInMs, debugLevel);

    // Check the validity of each controller in the list
    StringTokenizer controllers = new StringTokenizer(preferredControllerList,
        ",", true);
    int tokenNumber = controllers.countTokens();
    preferredControllers = new ArrayList(tokenNumber - 1);
    int i = 0;
    String s;
    boolean lastTokenWasComma = false;
    while (controllers.hasMoreTokens())
    {
      s = controllers.nextToken().trim();
      if (s.equals(","))
      {
        if (lastTokenWasComma || (i == 0) || (i == tokenNumber - 1))
          // ',' cannot be the first or the last token
          // another ',' cannot follow a ','
          throw new RuntimeException(
              "Syntax error in controller list for preferredController attribute '"
                  + preferredControllerList + "'");
        else
        {
          lastTokenWasComma = true;
          continue;
        }
      }
      lastTokenWasComma = false;
      try
      {
        // ensure that the preferred controller is in the list of all
        // controllers
        ControllerInfo pref = SequoiaUrl.parseController(s);
        boolean found = false;
        for (int idx = 0; idx < controllerList.length; idx++)
          if (controllerList[idx].equals(pref))
          {
            found = true;
            break;
          }
        if (!found)
        {
          throw new RuntimeException("Preferred controller " + pref
              + " is not in the list of controllers (" + aliveControllers + ")");
        }
        preferredControllers.add(pref);
      }
      catch (SQLException e)
      {
        throw new RuntimeException("Invalid controller " + s
            + " in controller list for preferredController attribute");
      }
      i++;
    }
    // make a copy of the preferred controllers to be able to find them back
    // (see controllerUp())
    originalPreferredControllers = new ArrayList(preferredControllers);
  }

  /**
   * @see org.continuent.sequoia.driver.connectpolicy.AbstractControllerConnectPolicy#controllerUp(org.continuent.sequoia.driver.ControllerInfo)
   */
  public synchronized void controllerUp(ControllerInfo controller)
  {
    super.controllerUp(controller);
    // re integrate controller in preferred list if it was in the original list
    if (originalPreferredControllers.contains(controller))
    {
      // determine where to put the controller in the preferred list
      int prefIndex = originalPreferredControllers.indexOf(controller);
      if (prefIndex > preferredControllers.size())
        prefIndex = preferredControllers.size() - 1;
      preferredControllers.add(prefIndex, controller);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.connectpolicy.AbstractControllerConnectPolicy#controllerDown(org.continuent.sequoia.driver.ControllerInfo)
   */
  public synchronized void controllerDown(ControllerInfo controller)
  {
    super.controllerDown(controller);
    while (preferredControllers.remove(controller))
      ;
  }

  /**
   * Gets the controllers as follow:
   * <ul>
   * <li> if there are preferred controllers left, get one of them in round
   * robin mode
   * <li> else, get one of the other controllers in round robin mode
   * </ul>
   */
  public synchronized ControllerInfo getController()
      throws NoMoreControllerException
  {
    // preferredController is a subset of aliveControllers => no double check
    if (aliveControllers.isEmpty())
      throw new NoMoreControllerException();

    ControllerInfo selectedController = null;
    int nbPreferredTested = 0;
    // first, try in the preferred controllers list
    while (selectedController == null && !preferredControllers.isEmpty()
        && nbPreferredTested < preferredControllers.size())
    {
      preferredIndex++;
      if (preferredIndex >= preferredControllers.size())
        preferredIndex = 0;
      selectedController = (ControllerInfo) preferredControllers.get(preferredIndex);
      // check that the selected controller can be used (ie. its vdb is up)
      if (selectedController != null
          && !super.isVdbUpOnController(selectedController))
      {
        selectedController = null;
        nbPreferredTested++;
      }
    }
    if (selectedController == null)
    {
      // No preferred => find 1st available controller in round-robin mode.
      // No problem re-using the index because it is "modded" (%) with the list
      // size before getting it
      index++;
      if (index >= aliveControllers.size())
        index = 0;
      selectedController = super.getControllerByNum(index);
    }
    if (debugLevel == SequoiaUrl.DEBUG_LEVEL_DEBUG)
      System.out.println("Selected controller: " + selectedController);
    return selectedController;
  }
}
