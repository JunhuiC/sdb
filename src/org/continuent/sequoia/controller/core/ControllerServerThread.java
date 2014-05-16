/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Emmanuel Cecchet. 
 * Contributor(s): Duncan Smith.
 */

package org.continuent.sequoia.controller.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import javax.net.ServerSocketFactory;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.net.SSLException;
import org.continuent.sequoia.common.net.SocketFactoryFactory;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;

/**
 * A <code>ControllerServerThread</code> listens for Sequoia driver
 * connections. It accepts the connection and give them to
 * <code>ControllerWorkerThreads</code>.
 * 
 * @see org.continuent.sequoia.controller.core.ControllerWorkerThread
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:duncan@mightybot.com">Duncan Smith </a>
 * @version 1.0
 */
public class ControllerServerThread extends Thread
{
  private ServerSocket  serverSocket;
  private boolean       isShuttingDown                     = false;
  protected Controller  controller;
  /** Thread that responds to client pings */
  private PingResponder pingResponder;
  /** Pending queue of client (driver) socket connections */
  protected ArrayList   controllerServerThreadPendingQueue = new ArrayList();
  /**
   * Number of idle <code>ControllerWorkerThread</code>. Access to this
   * variable must be synchronized using pendingQueue.
   */
  protected int         idleWorkerThreads                  = 0;

  /** Logger instance. */
  static Trace          logger                             = Trace
                                                               .getLogger("org.continuent.sequoia.controller.core.Controller");

  static Trace          endUserLogger                      = Trace
                                                               .getLogger("org.continuent.sequoia.enduser");

  /**
   * Creates a new ControllerServerThread that listens on the given port.
   * 
   * @param controller The controller which created this thread.
   */
  public ControllerServerThread(Controller controller)
  {
    super("ControllerServerThread");
    this.controller = controller;

    try
    {
      InetAddress bindAddress = null;

      // Determine if a specific IP address has been requested.
      /**
       * @see org.continuent.sequoia.controller.xml.ControllerParser#configureController(Attributes)
       *      for how controller's IPAddress is set by default.
       */
      if (!controller.getIPAddress().equals(
          InetAddress.getLocalHost().getHostAddress()))
      {
        // Non-default value: an IP has been specified that is not localhost...
        // If the user has *asked for* getLocalHost().getHostAddress(), bad luck
        // we lose.
        bindAddress = InetAddress.getByName(controller.getIPAddress());
      }
      // else, default value: no specific IP was requested or was left as the
      // default. Create a basic local socket by specifying null bind address

      // Build an InetAddress by passing the requested IP address to the
      // InetAddress class constructor. This will validate the sanity of the
      // IP by either accepting the requested value or throwing a
      // BindException.

      if (controller.isSecurityEnabled()
          && controller.getSecurity().isSSLEnabled())
      {
        ServerSocketFactory sslFact = SocketFactoryFactory
            .createServerFactory(controller.getSecurity().getSslConfig());
        serverSocket = sslFact.createServerSocket(controller.getPortNumber(),
            controller.getBacklogSize(), bindAddress);
      }
      else
      {
        serverSocket = new ServerSocket(controller.getPortNumber(), controller
            .getBacklogSize(), bindAddress);
      }
    }
    catch (java.net.BindException e)
    { // Thrown if an illegal IP address was specified.
      String msg = Translate.get("controller.server.thread.illegal.ip",
          new String[]{controller.getIPAddress(), e.getMessage()});
      logger.fatal(msg);
      endUserLogger.fatal(msg);
      controller.endOfController(e);
    }
    catch (IOException e)
    {
      String msg = Translate.get("controller.server.thread.socket.failed",
          new String[]{String.valueOf(controller.getPortNumber()),
              e.getMessage()});
      if (logger.isDebugEnabled())
        logger.fatal(msg, e);
      else
        logger.fatal(msg);
      endUserLogger.fatal(msg);
      controller.endOfController(e);
    }
    catch (SSLException e)
    {
      String msg = Translate.get("controller.server.thread.socket.failed",
          new String[]{String.valueOf(controller.getPortNumber()),
              e.getMessage()});
      if (logger.isDebugEnabled())
        logger.fatal(msg, e);
      else
        logger.fatal(msg);
      endUserLogger.fatal(msg);
      controller.endOfController(e);
    }

    // Create the UDP ping responder with the same port
    try
    {
      pingResponder = new PingResponder(controller.getPortNumber());
      // no special clean-up to be done, let the jvm kill the thread at exit
      pingResponder.setDaemon(true);
    }
    catch (IOException e)
    {
      pingResponder = null;
      if (logger.isErrorEnabled())
      {
        logger.error(Translate.get("controller.ping.responder.creation.failed",
            e.getLocalizedMessage()));
      }
      controller.endOfController(e);
    }

    if (logger.isInfoEnabled())
    {
      logger.info(Translate.get("controller.server.thread.waiting.connections",
          new String[]{serverSocket.getInetAddress().getHostAddress(),
              String.valueOf(serverSocket.getLocalPort())}));
      logger.debug(Translate.get("controller.server.thread.backlog.size", ""
          + controller.getBacklogSize()));
    }
  }

  /**
   * Accepts connections from drivers, read the virtual database name and
   * returns the connection point.
   */
  public void run()
  {
    if (controller == null)
    {
      logger.error(Translate.get("controller.server.thread.controller.null"));
      isShuttingDown = true;
    }
    // Start the ping responder
    if (pingResponder != null)
      pingResponder.start();

    // Start processing connections
    Socket clientSocket = null;
    while (!isShuttingDown)
    {
      try
      { // Accept a connection
        clientSocket = serverSocket.accept();
        if (isShuttingDown)
          break;
        if (controller.isSecurityEnabled()
            && !controller.getSecurity().allowConnection(clientSocket))
        {
          String errmsg = Translate.get(
              "controller.server.thread.connection.refused", clientSocket
                  .getInetAddress().getHostName());
          logger.warn(errmsg);
          DriverBufferedOutputStream out = new DriverBufferedOutputStream(
              clientSocket);
          out.writeBoolean(false);
          out.writeLongUTF(errmsg);
          out.flush(); // FIXME: should we .close() instead ?
          clientSocket = null;
          continue;
        }
        else
        {
          if (logger.isDebugEnabled())
            logger.debug(Translate.get(
                "controller.server.thread.connection.accept", clientSocket
                    .getInetAddress().getHostName()));
        }
        boolean createThread = false;
        if (isShuttingDown)
          break;
        synchronized (controllerServerThreadPendingQueue)
        {
          // Add the connection to the queue
          controllerServerThreadPendingQueue.add(clientSocket);
          // Check if we need to create a new thread or just wake up an
          // existing one
          if (idleWorkerThreads == 0)
            createThread = true;
          else
            // Here we notify all threads else if one thread doesn't wake up
            // after the first notify() we will send a second notify() and
            // one signal will be lost. So the safe way is to wake up everybody
            // and that worker threads go back to sleep if there is no job.
            controllerServerThreadPendingQueue.notifyAll();
        }
        if (createThread)
        { // Start a new worker thread if needed
          ControllerWorkerThread thread = new ControllerWorkerThread(this);
          thread.start();
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("controller.server.thread.starting"));
        }
      }
      catch (IOException e)
      {
        if (!isShuttingDown)
        {
          logger.warn(Translate.get(
              "controller.server.thread.new.connection.error", e), e);
        }
      }
    }
    if (logger.isInfoEnabled())
      logger.info(Translate.get("controller.server.thread.terminating"));
  }

  /**
   * Refuse new connection to clients and finish transaction
   */
  public void shutdown()
  {
    isShuttingDown = true;
    // Shutting down server thread
    try
    {
      serverSocket.close();
    }
    catch (Exception e)
    {
      logger.warn(Translate.get("controller.shutdown.server.socket.exception"),
          e);
    }
    /*
     * Close pending connections (not yet served by any ControllerWorkerThread)
     * and wake up idle ControllerWorkerThreads.
     */
    Object lock = controllerServerThreadPendingQueue;
    synchronized (lock)
    {
      // close pending connections
      int nbSockets = controllerServerThreadPendingQueue.size();
      Socket socket = null;
      for (int i = 0; i < nbSockets; i++)
      {
        socket = (Socket) controllerServerThreadPendingQueue.get(i);
        logger.info(Translate.get("controller.shutdown.client.socket", socket
            .getInetAddress().toString()));

        try
        {
          socket.close();
        }
        catch (Exception e)
        {
          logger.warn(Translate
              .get("controller.shutdown.client.socket.exception"), e);
        }
      }

      // wake up idle ControllerWorkerThreads,
      // asking them to die (controllerServerThreadPendingQueue=null)
      this.controllerServerThreadPendingQueue = null;
      lock.notifyAll();
    }
    
    // Terminate the PingResponder thread.  
    pingResponder.shutdown();
  }

  /**
   * @return Returns the controllerServerThreadPendingQueue size.
   */
  public int getControllerServerThreadPendingQueueSize()
  {
    synchronized (controllerServerThreadPendingQueue)
    {
      return controllerServerThreadPendingQueue.size();
    }
  }

  /**
   * @return Returns the idleWorkerThreads.
   */
  public int getIdleWorkerThreads()
  {
    synchronized (controllerServerThreadPendingQueue)
    {
      return idleWorkerThreads;
    }
  }

  /**
   * Returns the isShuttingDown value.
   * 
   * @return Returns the isShuttingDown.
   */
  public boolean isShuttingDown()
  {
    return isShuttingDown;
  }
}