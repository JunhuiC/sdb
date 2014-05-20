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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Date;
import java.util.Iterator;

import org.continuent.sequoia.common.util.Constants;

/**
 * Main class for controller ping monitoring.<br>
 * Launches a controller pinger thread and creates a list of controllers with
 * their states. Reads controllers responses (pongs) and updates state
 * accordingly
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @author <a href="mailto:marc.herbert@continuent.com">Marc Herbert</a>
 * @version 1.0
 */
public class ControllerWatcher extends Thread
{
  /** List of controllers to send pings to */
  final WatchedControllers   controllers;
  /** Thread that sends pings */
  final ControllerPingSender pinger;
  /** Selector for incomming pings */
  final Selector             selector;
  /** Level of logging (logs are printed to stdout, see {@link SequoiaUrl}) */
  final int                  logLevel;
  /**
   * Time to wait between 2 successive pings to a controller, also used for
   * select timeout
   */
  final int                  pingDelayInMs;
  /**
   * Delay after which a controller will be considered as failing if it did not
   * respond to pings. Used here as a timeout for select()
   */
  final int                  controllerTimeout;

  private boolean            terminated = false;

  /**
   * Creates a controller watcher with the given controllers to watch.<br>
   * Pings will be sent to all given controllers
   * 
   * @param controllerList controllers to ping
   * @param callback Callback implementation to call when a controller state
   *          changes
   * @param pingDelayInMs time to wait between two successive pings
   * @param controllerTimeout delay after which a controller will be considered
   *          as failing if it did not respond to pings
   * @param logLevel level of logging to use (logs are printed to stdout)
   * @exception IOException if the socket selector could not be opened
   */
  public ControllerWatcher(ControllerInfo[] controllerList,
      ControllerStateChangedCallback callback, int pingDelayInMs,
      int controllerTimeout, int logLevel) throws IOException
  {
    this(new WatchedControllers(controllerList, System.currentTimeMillis(),
        controllerTimeout, callback), pingDelayInMs, controllerTimeout,
        logLevel);
  }

  private ControllerWatcher(WatchedControllers watchedControllers,
      int pingDelayInMs, int controllerTimeout, int logLevel)
      throws IOException

  {
    super("ControllerWatcher");

    this.controllers = watchedControllers;
    this.pingDelayInMs = pingDelayInMs;
    this.controllerTimeout = controllerTimeout;
    this.logLevel = logLevel;
    this.selector = Selector.open();

    this.pinger = new ControllerPingSender(this, this.controllers,
        newRegisteredChannel(), pingDelayInMs, logLevel);
    this.pinger.setDaemon(true);

  }

  /**
   * @return a new DatagramChannel configured and registered at this.selector
   */
  private DatagramChannel newRegisteredChannel() throws IOException
  {
    // Register channel
    // Open the channel, set it to non-blocking, initiate connect
    DatagramChannel channel = DatagramChannel.open();
    channel.configureBlocking(false);
    channel.register(this.selector, SelectionKey.OP_READ);
    return channel;

  }

  private void processAnswers() throws IOException
  {
    long timeAnswerWasReceived = System.currentTimeMillis();
    for (Iterator<?> i = selector.selectedKeys().iterator(); i.hasNext();)
    {
      // Retrieve the next key and remove it from the set
      SelectionKey sk = (SelectionKey) i.next();
      i.remove();
      DatagramChannel channel = (DatagramChannel) sk.channel();
      ByteBuffer buf = ByteBuffer.allocate(1);

      InetSocketAddress addr = (InetSocketAddress) channel.receive(buf);
      if (addr != null)
      {
        // creates the controller info from the received SocketAddress
        ControllerInfo from = new ControllerInfo(addr);
        byte data[] = buf.array();
        if (data[0] == Constants.CONTROLLER_PING_VERSION)
        {
          controllers.setControllerResponsed(from, timeAnswerWasReceived);
        }
        else if (logLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
        {
          System.out.println(new Date() + " " + this + ": controller " + from
              + " ping protocol does not match ours, was: " + data[0]
              + ", expected:" + Constants.CONTROLLER_PING_VERSION);
        }
      }
    }
  }

  /**
   * Starts watching controllers.<br>
   * Reads ping responses from controllers and updates corresponding controller
   * data in the <code>ControllerList</code>.
   * 
   * @see java.lang.Thread#run()
   */
  public void run()
  {
    if (logLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
      System.out.println(new Date() + " " + this + " started.");

    // Start ping
    pinger.start();
    while (!terminated)
    {
      try
      {
        int nbOfAnswers = selector.select(2 * pingDelayInMs);
        if (nbOfAnswers > 0)
          processAnswers();
        // This will start callbacks according to new states
        controllers.lookForDeadControllers(System.currentTimeMillis());
      }
      catch (IOException ioe)
      {
        System.err.println(new Date() + " " + this
            + ": Error while reading controller answers to ping");
        ioe.printStackTrace(System.err);

        restartBothDaemons();
      } // try
    } // while (!terminated)

    if (logLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
      System.out.println(new Date() + " " + this + " terminated");

  }

  /**
   * Forces a given controller to be considered as not responding
   * 
   * @see WatchedControllers#setControllerDown(ControllerInfo)
   */
  public void forceControllerDown(ControllerInfo c)
  {
    controllers.setControllerDown(c);
  }

  /**
   * Returns the given controllers index in the original list
   * 
   * @param controller the controller to get index of
   * @return original index of the given controller
   */
  public int getOriginalIndexOf(ControllerInfo controller)
  {
    return controllers.getOriginalIndexOf(controller);
  }

  /**
   * Terminates both pinger and watcher thread TODO: terminateBoth definition.
   */
  public void terminateBoth()
  {
    pinger.terminate();
    terminated = true;
    try
    {
      // workaround for SEQUOIA-942 (Selector.open() leaks file descriptors)
      selector.close();
    }
    catch (IOException e)
    {
      e.printStackTrace(System.err); // ignore
    }
  }

  void restartBothDaemons()
  {
    synchronized (this)
    {
      // don't restart multiple times
      if (terminated)
        return;

      // tell old threads (including Thread.this!) to die ASAP
      this.terminateBoth();
    }

    boolean failed = true;

    // just for logging
    ControllerWatcher newWatcher = null; // compiler is not clever enough

    while (failed)
    {
      try
      // to start over with 2 brand new threads (and thus a brand new,
      // non-interrupted, non-closed channel)
      {
        Thread.sleep(pingDelayInMs);
        newWatcher = new ControllerWatcher(controllers, pingDelayInMs,
            controllerTimeout, logLevel);
        newWatcher.start();
        failed = false; // success, exit try loop
      }
      catch (InterruptedException ie)
      {
        // loop over
        ie.printStackTrace(System.err);
      }
      catch (IOException e)
      {
        // loop over
        e.printStackTrace(System.err);
      }
    }

    System.err.println(new Date() + " " + this
        + " successfully restarted both daemons.");
    System.err.println("New watcher = " + newWatcher);

  }

  public String toString()
  {
    // Add our address, useful as an ID. Stolen from Object.toString()
    return super.toString() + "@" + Integer.toHexString(hashCode()) + " "
        + " " + controllers;
  }

}
