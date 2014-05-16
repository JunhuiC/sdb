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

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

import org.continuent.sequoia.common.exceptions.NoMoreControllerException;
import org.continuent.sequoia.driver.ControllerInfo;
import org.continuent.sequoia.driver.ControllerWatcher;
import org.continuent.sequoia.driver.SequoiaUrl;
import org.continuent.sequoia.driver.SocketKillerCallBack;

/**
 * This class defines an AbstractControllerConnectPolicy used by the driver to
 * choose a controller to connect to.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public abstract class AbstractControllerConnectPolicy
{
  /** Holds a controller and its last vdb failure */
  protected class ControllerAndVdbState
  {
    ControllerInfo controller;
    long           lastVdbFailure = 0;

    /**
     * Constructs a <code>ControllerAndVdbState</code> object with the given
     * controller, considering last vdb failure is 0 (ie. no failure)
     * 
     * @param ctrl controller to refer to
     */
    ControllerAndVdbState(ControllerInfo ctrl)
    {
      controller = ctrl;
    }

    /**
     * Gives the state of the vdb. Returns true if the last vdb failure is older
     * than 5 seconds (refering to given time), false if the vdb failed during
     * the last 5 seconds
     * 
     * @param now the time to compare last vdb failure to
     * @return tru is the vdb can be considered as up, false otherwise
     */
    boolean isVdbUp(long now)
    {
      if ((now - 5000) < lastVdbFailure)
        return false;
      return true;
    }

    /**
     * Gives a string representation of this object: prints controller + state
     */
    public String toString()
    {
      return controller + " ("
          + (isVdbUp(System.currentTimeMillis()) ? "VDB up)" : "VDB down)");
    }
  }

  /**
   * Up-to-date list of controllers that respond to pings with associated
   * lastVdbFailure time stamp
   */
  protected ArrayList            aliveControllers;
  /** Controller watcher thread */
  protected ControllerWatcher    watcher;
  /** Callback for controller failures and come backs */
  protected SocketKillerCallBack callback;
  /** Level of logging (will be done on stdout) */
  protected int                  debugLevel = SequoiaUrl.DEBUG_LEVEL_OFF;

  /**
   * Creates a new <code>AbstractControllerConnectPolicy</code> object
   * 
   * @param controllerList the controller list on which the policy applies
   * @param pingDelayInMs Interval in milliseconds between two pings of a
   *          controller
   * @param controllerTimeoutInMs timeout in milliseconds after which a
   *          controller is considered as dead if it did not respond to pings
   * @param debugLevel the debug level to use
   * @see org.continuent.sequoia.driver.SequoiaUrl#DEBUG_LEVEL_OFF
   */
  public AbstractControllerConnectPolicy(ControllerInfo[] controllerList,
      int pingDelayInMs, int controllerTimeoutInMs, int debugLevel)
  {
    if (controllerList == null)
      throw new NullPointerException(
          "Invalid null controller list in connect policy constructor");
    if (controllerList.length == 0)
      throw new RuntimeException(
          "Invalid empty controller list in connect policy constructor");
    this.aliveControllers = new ArrayList(controllerList.length);
    for (int i = 0; i < controllerList.length; i++)
    {
      // vdbLastFailure will be initialized at 0, ie. no vdb failure
      this.aliveControllers.add(new ControllerAndVdbState(controllerList[i]));
    }
    this.debugLevel = debugLevel;

    // Create the objects for controller watcher
    this.callback = new SocketKillerCallBack(this, debugLevel);
    try
    {
      watcher = new ControllerWatcher(controllerList, callback, pingDelayInMs,
          controllerTimeoutInMs, debugLevel);
      watcher.setDaemon(true);
      watcher.start();
    }
    catch (IOException cause)
    {
      watcher = null;
      if (debugLevel >= SequoiaUrl.DEBUG_LEVEL_INFO)
        System.out
            .println("Controller watcher creation failed, failover won't work!");
    }
  }

  protected void finalize() throws Throwable
  {
    watcher.terminateBoth();
    super.finalize();
  }

  /**
   * Get a controller using the implementation specific policy
   * 
   * @return <code>ControllerInfo</code> of the selected controller
   * @throws NoMoreControllerException if no controller in the controller list
   *           is reachable
   */
  public abstract ControllerInfo getController()
      throws NoMoreControllerException;

  /**
   * Registers the given socket to the callback so it can kill this socket when
   * the controller is detected as failed
   * 
   * @param controller the controller to which the socket is connected
   * @param socket the socket to register
   */
  public synchronized void registerSocket(ControllerInfo controller,
      Socket socket)
  {
    callback.registerSocket(controller, socket);
  }

  /**
   * Tell the watcher that a failure has been detected on the given controller.<br>
   * This function should be called when a connection error occurs on the given
   * controller. We don't update the aliveController list here: the watcher
   * callback will call controllerDown() for us
   * 
   * @param controller the controller suspected of failure
   */
  public synchronized void forceControllerDown(ControllerInfo controller)
  {
    if (watcher != null)
      watcher.forceControllerDown(controller);
  }

  /**
   * Removes this controller from the list of alive ones. <b>Warning:</b> This
   * function should never be called directly, only the callback should make us
   * of this.
   * 
   * @param controller the suspect controller
   */
  public synchronized void controllerDown(ControllerInfo controller)
  {
    for (Iterator iter = aliveControllers.iterator(); iter.hasNext();)
    {
      ControllerAndVdbState ctrl = (ControllerAndVdbState) iter.next();
      if (ctrl.controller.equals(controller))
      {
        iter.remove();
        return;
      }
    }
  }

  /**
   * Adds the specified controller to the list of alive ones at the specified
   * position. <b>Warning:</b> This function should never be called directly,
   * only the callback should make us of this.
   * 
   * @param controller the controller that came back
   * @param index the index to put the controller back at
   */
  public synchronized void controllerUp(ControllerInfo controller)
  {
    int index = watcher.getOriginalIndexOf(controller);
    // check bounds
    if (aliveControllers.isEmpty())
      // no alive controllers, add the controller at position 0
      index = 0;
    else if (index >= aliveControllers.size())
      // index bigger than current list size, put the controller at the end
      index = aliveControllers.size() - 1;
    // add the controller and suppose that vdb is present
    aliveControllers.add(index, new ControllerAndVdbState(controller));
  }

  /**
   * Informs that the given controller's vdb is no more available
   */
  public synchronized void setVdbDownOnController(ControllerInfo controller)
  {
    for (Iterator iter = aliveControllers.iterator(); iter.hasNext();)
    {
      ControllerAndVdbState ctrl = (ControllerAndVdbState) iter.next();
      if (ctrl.controller.equals(controller))
      {
        ctrl.lastVdbFailure = System.currentTimeMillis();
        return;
      }
    }
  }

  /**
   * Returns the number of controllers still known to be alive
   */
  public synchronized int numberOfAliveControllers()
  {
    return aliveControllers.size();
  }

  /**
   * Tells if the given controller VDB is available.<br>
   * 
   * @return false if the lastVdbFailure associated to the given controller is
   *         not older than ??? milliseconds, true otherwise
   */
  protected synchronized boolean isVdbUpOnController(ControllerInfo controller)
  {
    if (controller == null)
      return false;
    for (Iterator iter = aliveControllers.iterator(); iter.hasNext();)
    {
      ControllerAndVdbState ctrl = (ControllerAndVdbState) iter.next();
      if (ctrl.controller.equals(controller))
        return ctrl.isVdbUp(System.currentTimeMillis());
    }
    return false;
  }

  /**
   * Returns the first controller that is alive and with a running vdb starting
   * at rank wishedIndex in the list. wishedIndex = 0 will return the first
   * alive controller. If the wishedIndex controller's vdb at is not available,
   * will return the next controller in a round robin way, until finding one
   * suitable. If no suitable controller can be found, throws a
   * NoMoreControllerException
   * 
   * @param wishedIndex index of the controller in the list
   * @return a controller that is alive with its vdb up
   * @throws NoMoreControllerException if no controller with vdb up could be
   *           found
   */
  protected synchronized ControllerInfo getControllerByNum(int wishedIndex)
      throws NoMoreControllerException
  {
    int nbOfControllers = aliveControllers.size();
    if (nbOfControllers == 0)
      throw new NoMoreControllerException();
    // make sure given index is valid
    if (wishedIndex >= nbOfControllers)
      wishedIndex = 0;
    // to stop iteration when all controllers have been tested
    int nbTested = 0;
    // if the vdb was not available for the whished controller, we get the next
    // one in a round robin manner
    ControllerAndVdbState ctrl = (ControllerAndVdbState) aliveControllers
        .get(wishedIndex);
    while (nbTested < nbOfControllers)
    {
      if (ctrl.isVdbUp(System.currentTimeMillis()))
      {
        return ctrl.controller;
      }
      // try next controller
      wishedIndex++;
      if (wishedIndex >= nbOfControllers)
        wishedIndex = 0;
      nbTested++;
      ctrl = (ControllerAndVdbState) aliveControllers.get(wishedIndex);
    }
    // all controllers tested, no more available
    throw new NoMoreControllerException();
  }
}
