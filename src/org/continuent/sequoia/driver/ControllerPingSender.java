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
 * Contributor(s): Marc Herbert
 */

package org.continuent.sequoia.driver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Date;
import java.util.Iterator;

import org.continuent.sequoia.common.util.Constants;

/**
 * This thread sends a ping to all controllers in a given list at a given
 * frequency
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @author <a href="mailto:marc.herbert@continuent.com">Marc Herbert</a>
 * @version 1.0
 */
public class ControllerPingSender extends Thread
{
  /** List of controllers to send pings to */
  final WatchedControllers        controllers;
  /** Time to wait between 2 successive pings to a controller */
  final int                       pingDelayInMs;
  /** Channel for outgoing pings */
  final DatagramChannel           sendChannel;
  /** Packet sent as a ping */
  final ByteBuffer                pingPacket = ByteBuffer.allocate(1);
  /** Level of logging (logs are printed to stdout, see {@link SequoiaUrl}) */
  final int                       logLevel;

  private final ControllerWatcher fatherWatcher;

  private boolean                 terminated = false;

  /**
   * Creates a new <code>ControllerPingSender</code> on the given controller
   * list with the given frequency
   * 
   * @param controllerList controllers to ping. Should never be null
   * @param channel <code>DatagramChannel</code> to use for sending pings
   * @param pingDelayInMs time to wait between two successive pings
   * @param logLevel level of logging to use (logs are printed to stdout)
   */
  public ControllerPingSender(ControllerWatcher father,
      WatchedControllers controllerList, DatagramChannel channel,
      int pingDelayInMs, int logLevel)
  {
    super("ControllerPingSender");
    this.fatherWatcher = father;
    this.controllers = controllerList;
    this.pingDelayInMs = pingDelayInMs;
    this.logLevel = logLevel;
    // sendChannel = new DatagramSocket();
    sendChannel = channel;
    pingPacket.put(Constants.CONTROLLER_PING_VERSION);
  }

  /**
   * Starts pinging controllers.<br>
   * Controllers will be ping with an interval of at least pingDelayInMs. But as
   * this function is synchronized on the controller list it can be slowed down
   * if controllers are added/removed, raising up the ping delay
   * 
   * @see java.lang.Thread#run()
   */
  public void run()
  {
    if (logLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
      System.out.println(new Date() + " " + this + " started.");

    long oldTime = System.currentTimeMillis();

    while (!terminated)
    {
      try
      {
        for (Iterator<?> it = controllers.getControllerIterator(); it.hasNext();)
        {
          ControllerInfo ctrl = (ControllerInfo) it.next();
          sendPingTo(ctrl);
        }
        // Wait for pingDelayInMs milliseconds before next ping
        sleep(pingDelayInMs);
        // detect violent clock changes (user hardly set new date/time)
        long newTime = System.currentTimeMillis();
        long timeShift = newTime - (oldTime + pingDelayInMs);
        if (Math.abs(timeShift) > fatherWatcher.controllerTimeout / 2)
        {
          System.err.println(timeShift / 1000 + "s time shift detected, from "
              + new Date(oldTime) + " to " + new Date(newTime));
          System.err
              .println("Brutal changes of date/time can lead to erroneous controller failure detections!");
        }
        oldTime = newTime;
      }
      catch (IOException e)
      {
        System.err.println(new Date() + " " + this
            + " IOException, trying to restart both daemons");
        e.printStackTrace(System.err);
        fatherWatcher.restartBothDaemons();
      }
      catch (InterruptedException e)
      {
        System.err.println(new Date() + " " + this
            + " InterruptedException, trying to restart both daemons");
        e.printStackTrace(System.err);
        fatherWatcher.restartBothDaemons();
      } // try
    } // while (!terminated)

    if (logLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
      System.out.println(new Date() + " " + this + " terminated");

  }

  private void sendPingTo(ControllerInfo ctrl) throws IOException
  {
    pingPacket.rewind();
    sendChannel.send(pingPacket, ctrl);
  }

  void terminate()
  {
    terminated = true;
  }

  public String toString()
  {
    // Add our address, useful as an ID. Stolen from Object.toString()
    return super.toString() + "@" + Integer.toHexString(hashCode()) + " "
        + controllers;
  }
}
