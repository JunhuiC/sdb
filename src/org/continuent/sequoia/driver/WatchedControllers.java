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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.driver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Provides a list of controllers and maintains their state (up or down),
 * launching appropriatly methods from callback given callback instance.<br>
 * To each controller is associated a lastTimeSeen value corresponding to the
 * last time the controller responded to a ping. Their state is updated each
 * time the lastTimeSeen value is accessed (read or written). At init time, all
 * controllers are considered as up (responding to pings). Controllers can also
 * be forced as 'no more responding to pings' (ie. down)
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class WatchedControllers
{
  /**
   * Holds a controller list index, lastTimeSeen variable and state (up or down)
   */
  private class ControllerIndexAndState
  {
    /** This is the index of the controller in the original list */
    int     index;
    /** Consider that a controller is up at init time */
    boolean respondsToPings = true;
    /**
     * Init to time the object is constructed and updated at each ping received
     */
    long    lastTimeSeen;

    ControllerIndexAndState(int idx, long initTime)
    {
      index = idx;
      lastTimeSeen = initTime;
    }

    public void updateTime(long newTime)
    {
      lastTimeSeen = newTime;
    }

    public int getIndex()
    {
      return index;
    }

    public void setIndex(int idx)
    {
      index = idx;
    }

    public boolean isUp()
    {
      return respondsToPings;
    }

    public long getLastTimeSeen()
    {
      return lastTimeSeen;
    }

    public void setUp()
    {
      respondsToPings = true;
    }

    public void setDown()
    {
      respondsToPings = false;
    }
  }

  /** All controllers with their corresponding state */
  // This list is, for now, static. In the future, it could support dynamic
  // insertions and suppressions
  // The list will be heavily accessed for searches given a ControllerInfo
  // (see setControllerResponsed), that's why we use a hash map
  final HashMap                  allControllersAndStates;

  /**
   * Delay after which a controller will be considered as failing if it did not
   * respond to pings
   */
  int                            controllerTimeout;
  /** Actions to take when a controller goes down or back-alive */
  ControllerStateChangedCallback callback;

  /**
   * Creates a new list of watched controllers with the given list of
   * controllers and an initial lastTimeSeen value.<br>
   * 
   * @param controllers list of all controllers identified by their
   *          <code>ControllerInfo</code>
   * @param initTime initial lastTimeSeen value, typically
   *          <code>System.currentTimeMillis()</code>
   * @param callback Callback implementation to call when a controller state
   *          changes
   * @param controllerTimeout delay after which a controller will be considered
   *          as failing if it did not respond to pings
   */
  public WatchedControllers(ControllerInfo[] controllers, long initTime,
      int controllerTimeout, ControllerStateChangedCallback callback)
  {
    int numberOfControllers = 0; // if caller wants to add controllers later
    if (controllers != null)
      numberOfControllers = controllers.length;
    allControllersAndStates = new HashMap(numberOfControllers);
    // we are inside a contructor, so we can safely operate on the
    // list without worrying of concurrent modifications
    for (int i = 0; i < numberOfControllers; i++)
    {
      // controllers can't be null here, due to for loop clause
      this.allControllersAndStates.put(controllers[i],
          new ControllerIndexAndState(i, initTime)); // keep the original index
    }
    this.controllerTimeout = controllerTimeout;
    this.callback = callback;
  }

  /**
   * Associates given lastTimeSeen value to specified controller and update its
   * state: if the controller was considered as down, calls the
   * {@link ControllerStateChangedCallback#onControllerUp(ControllerInfo)}
   * 
   * @see ControllerStateChangedCallback
   */
  public void setControllerResponsed(ControllerInfo ctrl, long newTime)
  {
    synchronized (allControllersAndStates)
    {
      ControllerIndexAndState state = (ControllerIndexAndState) allControllersAndStates
          .get(ctrl);
      if (state == null)
      {
        // Well, this should never happen, this is a bug
        System.err.println("ERROR: Unknown controller " + ctrl
            + " responded to ping! (list=" + allControllersAndStates + ")");
        return;
      }
      // if the controller was down, we launch the callback
      if (!state.isUp())
      {
        state.setUp();
        callback.onControllerUp(ctrl);
      }
      // Set the new time
      state.updateTime(newTime);
    }
  }

  /**
   * Updates all controllers state according to the given time.<br>
   * Iterates through the controller list and (if the controller was not already
   * considered as down) if the lastTimeSeen value is older than the given time
   * minus {@link #controllerTimeout}, the controller will the be considered as
   * failing, and
   * {@link ControllerStateChangedCallback#onControllerDown(ControllerInfo)}
   * will be called.
   */
  public void lookForDeadControllers(long currentTime)
  {
    for (Iterator iter = getControllerIterator(); iter.hasNext();)
    {
      ControllerInfo ctrl = (ControllerInfo) iter.next();
      ControllerIndexAndState state = (ControllerIndexAndState) allControllersAndStates
          .get(ctrl);
      if (state.isUp()
          && currentTime - state.getLastTimeSeen() > controllerTimeout)
      {
        state.setDown();
        callback.onControllerDown(ctrl);
      }
    }
  }

  /**
   * Forces the given controller to be considered as down.<br>
   * If the given controller was already down, does nothing. Otherwise, marks it
   * as dead and calls
   * {@link ControllerStateChangedCallback#onControllerDown(ControllerInfo)}
   */
  public synchronized void setControllerDown(ControllerInfo ctrl)
  {
    ControllerIndexAndState state = (ControllerIndexAndState) allControllersAndStates
        .get(ctrl);
    if (state.isUp()) // don't do the job twice
    {
      state.setDown();
      callback.onControllerDown(ctrl);
    }
  }

  /**
   * Returns a 'safe' read-only iterator on controllers' ControllerInfo.<br>
   * Creates a copy of the controller hashmap keys and returns a iterator on it.
   * This way, the iterator will not be affected by hashmap operations
   */
  public final Iterator getControllerIterator()
  {
    HashMap copy = getControllersClone();
    return (new HashSet(copy.keySet())).iterator();
  }

  /**
   * Creates and returns a copy of the controller hashmap member variable for
   * thread-safe read operations purpose.
   */
  private HashMap getControllersClone()
  {
    HashMap copy = null;
    synchronized (allControllersAndStates)
    {
      copy = (HashMap) (allControllersAndStates).clone();
    }
    return copy;
  }

  /**
   * Returns the given controllers index in the original list
   * 
   * @param controller the controller to get index of
   * @return original index of the given controller
   */
  public int getOriginalIndexOf(ControllerInfo controller)
  {
    if (!allControllersAndStates.containsKey(controller))
    {
      // this is a bug => return valid index anyway
      System.err.println("ERROR: Unknown controller " + controller
          + " to get index of! (list=" + allControllersAndStates + ")");
      return 0;
    }
    return ((ControllerIndexAndState) allControllersAndStates.get(controller)).index;
  }

  /**
   * <b>NOT IMPLEMENTED YET</b><br>
   * Adds a controller to the list and associates the given lastTimeSeenValue to
   * it.<br>
   * Note that this function is not used for now, but is present for future
   * dynamic controller addition<br>
   * This operation is thread-safe, thus can slow-down other concurrent
   * operations like {@link #removeController(ControllerInfo)},
   * {@link #setControllerResponsed(ControllerInfo, long)},
   */
  public void addController(ControllerInfo controller, int index,
      long lastTimeSeen)
  {
    synchronized (allControllersAndStates)
    {
      // TODO: we have to move the indexes of all controllers with index greater
      // or equal to the given index
      // ie. iterate through the list, if (ctrlIdx >= index) ctrlIdx++
      // then we can add the controller like this:
      // allControllersAndStates.put(controller, new ControllerIndexAndState(
      // index, lastTimeSeen));
      throw new RuntimeException(
          "Dynamic addition of controllers not implemented yet");
    }
  }

  /**
   * <b>NOT IMPLEMENTED YET</b><br>
   * Removes the selected controller from the list of controllers to watch
   * 
   * @param controller controller to remove
   * @see #addController(ControllerInfo, long)
   */
  public void removeController(ControllerInfo controller)
  {
    synchronized (allControllersAndStates)
    {
      // TODO: we have to move the indexes of all controllers with index greater
      // or equal to the given index
      // ie. iterate through the list, if (ctrlIdx >= index) ctrlIdx--
      // then we can add the controller like this:
      // allControllersAndStates.remove(controller);
      throw new RuntimeException(
          "Dynamic suppression of controllers not implemented yet");
    }
  }

  public String toString()
  {
    StringBuffer result = new StringBuffer("{");

    Iterator ctrls = getControllerIterator();
    if (ctrls.hasNext())
      result.append(ctrls.next());
      
    while (ctrls.hasNext())
      result.append(", " + ctrls.next());

    result.append("}");

    return result.toString();
  }
}
