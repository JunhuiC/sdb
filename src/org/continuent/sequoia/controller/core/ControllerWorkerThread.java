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
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.controller.core;

import java.io.IOException;
import java.io.OptionalDataException;
import java.net.Socket;
import java.util.ArrayList;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.protocol.Commands;
import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;

/**
 * The <code>ControllerWorkerThread</code> handles a connection with a Sequoia
 * driver. It reads a String containing the virtual database name from the
 * driver and sends back the corresponding <code>ConnectionPoint</code>.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ControllerWorkerThread extends Thread
{
  private ControllerServerThread serverThread;
  private boolean                isKilled = false;

  /** Logger instance. */
  static Trace                   logger   = Trace
                                              .getLogger("org.continuent.sequoia.controller.core.Controller");

  /*
   * Constructor
   */

  /**
   * Creates a new <code>ControllerWorkerThread</code> instance.
   * 
   * @param serverThread the <code>ControllerServerThread</code> that created
   *          us.
   */
  public ControllerWorkerThread(ControllerServerThread serverThread)
  {
    super("ControllerWorkerThread");
    this.serverThread = serverThread;
  }

  /**
   * Gets a connection from the connection queue and process it.
   */
  public void run()
  {
    Socket clientSocket;

    if (serverThread == null)
    {
      logger.error(Translate.get("controller.workerthread.null.serverthread"));
      isKilled = true;
    }
    else if (serverThread.controllerServerThreadPendingQueue == null)
    {
      logger.error(Translate.get("controller.workerthread.null.pendingqueue"));
      isKilled = true;
    }

    // Main loop
    while (!isKilled)
    {
      if (serverThread.isShuttingDown())
        break;
      // Get a connection from the pending queue
      synchronized (serverThread.controllerServerThreadPendingQueue)
      {
        while (serverThread.controllerServerThreadPendingQueue.isEmpty())
        {
          // Nothing to do, let's sleep ...
          serverThread.idleWorkerThreads++;
          boolean timeout = false;
          try
          {
            long before = System.currentTimeMillis();
            serverThread.controllerServerThreadPendingQueue
                .wait(ControllerConstants.DEFAULT_CONTROLLER_WORKER_THREAD_SLEEP_TIME);
            long now = System.currentTimeMillis();
            // Check if timeout has expired
            timeout = now - before >= ControllerConstants.DEFAULT_CONTROLLER_WORKER_THREAD_SLEEP_TIME;
          }
          catch (InterruptedException ignore)
          {
          }
          serverThread.idleWorkerThreads--;
          // We are shutting down
          if (serverThread.controllerServerThreadPendingQueue == null)
          {
            isKilled = true;
            break;
          }
          if (timeout
              && serverThread.controllerServerThreadPendingQueue.isEmpty())
          {
            // Nothing to do, let's die.
            isKilled = true;
            break;
          }
        }

        if (isKilled)
          break;

        // Get a connection
        clientSocket = (Socket) serverThread.controllerServerThreadPendingQueue
            .remove(0);
      } // synchronized (serverThread.controllerServerThreadPendingQueue)

      if (clientSocket == null)
      {
        logger.error(Translate.get("controller.workerthread.null.socket"));
        continue;
      }
      else if (logger.isDebugEnabled())
        logger.debug(Translate.get("controller.workerthread.connection.from",
            new String[]{clientSocket.getInetAddress().toString(),
                String.valueOf(clientSocket.getPort())}));

      try
      {
        // Disable Nagle algorithm else small messages are not sent
        // (at least under Linux) even if we flush the output stream.
        clientSocket.setTcpNoDelay(true);

        // Handle connection
        DriverBufferedInputStream in = new DriverBufferedInputStream(
            clientSocket);
        DriverBufferedOutputStream out = new DriverBufferedOutputStream(
            clientSocket);

        // Check protocol version for driver compatibility
        int driverVersion = in.readInt();

        if (driverVersion != Commands.ProtocolVersion)
        {
          if (driverVersion != Commands.Ping)
          {
            // If only minor versions differ, we can accept 'old versions' of
            // drivers, they will still be compatible: they just won't know
            // newer commands.
            // In that case (Driver.minor < Controller.minor), we just inform
            // that an old driver is connecting to us
            String versionMismatch = Translate
                .get(
                    "controller.workerthread.protocol.versions",
                    new Object[]{
                        Integer.toString(Commands
                            .getProtocolMajorVersion(driverVersion))
                            + "."
                            + Commands.getProtocolMinorVersion(driverVersion),
                        Commands
                            .getProtocolMajorVersion(Commands.ProtocolVersion)
                            + "."
                            + Commands
                                .getProtocolMinorVersion(Commands.ProtocolVersion)});
            if (Commands.getProtocolMajorVersion(driverVersion) != Commands
                .getProtocolMajorVersion(Commands.ProtocolVersion)
                || Commands.getProtocolMinorVersion(driverVersion) > Commands
                    .getProtocolMinorVersion(Commands.ProtocolVersion))
            {
              // null = we don't want to read user/pass
              abortConnectionEstablishement(null, out, Translate.get(
                  "controller.workerthread.protocol.incompatible",
                  versionMismatch));
              continue;
            }
            // Reduced to debug to prevent flood of messages in log. 
            // (SEQUOIA-999)
            if (logger.isDebugEnabled())
              logger.debug(Translate.get(
                  "controller.workerthread.protocol.old.driver",
                  versionMismatch));
          }
          else
          {
            if (logger.isDebugEnabled())
              logger.debug("Controller pinged");
            try
            {
              // Close the socket
              clientSocket.close();
            }
            catch (Exception ignore)
            {
            }
            continue;
          }
        }
        // Driver version OK
        String virtualDatabaseName = in.readLongUTF();

        // Read the virtual database name
        VirtualDatabase vdb = serverThread.controller
            .getVirtualDatabase(virtualDatabaseName);
        if (vdb == null)
        {
          // Tell the driver about the error (otherwise the driver just gets a
          // closed socket, with no explicit reason)
          abortConnectionEstablishement(in, out, Translate.get(
              "virtualdatabase.not.found", virtualDatabaseName));
          continue;
        }
        if (vdb.isShuttingDown())
        {
          String msg = Translate.get("virtualdatabase.shutting.down",
              virtualDatabaseName);
          logger.warn(msg);
          abortConnectionEstablishement(in, out, msg);
          continue;
        }

        // At this point we have the virtual database the driver wants to
        // connect to and we have to give the job to a
        // VirtualDatabaseWorkerThread
        ArrayList vdbActiveThreads = vdb.getActiveThreads();
        ArrayList vdbPendingQueue = vdb.getPendingConnections();

        if (vdbActiveThreads == null)
        {
          logger.error(Translate
              .get("controller.workerthread.null.active.thread"));
          isKilled = true;
        }
        if (vdbPendingQueue == null)
        {
          logger
              .error(Translate.get("controller.workerthread.null.connection"));
          isKilled = true;
        }

        // Start minimum number of worker threads
        boolean tooManyConnections;
        synchronized (vdbActiveThreads)
        {
          while (vdb.getCurrentNbOfThreads() < vdb.getMinNbOfThreads())
          {
            forkVirtualDatabaseWorkerThread(vdb, Translate
                .get("controller.workerthread.starting.thread.for.minimum"));
          }

          // Check if maximum number of concurrent connections has been
          // reached
          tooManyConnections = (vdb.getMaxNbOfConnections() > 0)
              && vdbActiveThreads.size() + vdbPendingQueue.size() > vdb
                  .getMaxNbOfConnections();
        }
        if (tooManyConnections)
        {
          abortConnectionEstablishement(in, out, Translate
              .get("controller.workerthread.too.many.connections"));
          continue;
        }

        /**
         * We successfully found the virtual database and we are handing over
         * the connection to a virtual database worker thread (VDWT).
         * Acknowledge success (for the vdb) to the driver, the VDWT will
         * perform the user authentication.
         * <p>
         * Let the VDWT flush when it sends its boolean for auth. result.
         */
        out.writeBoolean(true);

        // Put the connection in the queue
        synchronized (vdbPendingQueue)
        {
          vdbPendingQueue.add(in);
          vdbPendingQueue.add(out);
          // Nullify the socket else it is closed in the finally block
          clientSocket = null;
          synchronized (vdbActiveThreads)
          { // Is a thread available?
            if (vdb.getIdleThreads() < vdbPendingQueue.size() / 2)
            { // No
              if ((vdb.getCurrentNbOfThreads() <= vdb.getMaxNbOfThreads())
                  || (vdb.getMaxNbOfThreads() == 0))
              {
                forkVirtualDatabaseWorkerThread(vdb, Translate
                    .get("controller.workerthread.starting.thread"));
              }
              else if (logger.isInfoEnabled())
                logger.info(Translate.get(
                    "controller.workerthread.maximum.thread", vdb
                        .getMaxNbOfThreads()));
            }
            else
            {
              if (logger.isDebugEnabled())
                logger.debug(Translate
                    .get("controller.workerthread.notify.thread"));
              // Here we notify all threads else if one thread doesn't wake
              // up after the first notify() we will send a second notify()
              // and one signal will be lost. So the safe way is to wake up
              // everybody and that worker threads go back to sleep if there
              // is no job.
              vdbPendingQueue.notifyAll();
            }
          }
        }
      }
      // }
      catch (OptionalDataException e)
      {
        logger
            .error(Translate.get("controller.workerthread.protocol.error", e));
      }
      catch (IOException e)
      {
        logger.error(Translate.get("controller.workerthread.io.error", e));
      }
      finally
      {
        try
        {
          if (clientSocket != null)
          {
            if (logger.isDebugEnabled())
              logger.debug(Translate
                  .get("controller.workerthread.connection.closing"));
            clientSocket.close();
          }
        }
        catch (IOException ignore)
        {
        }
      }
    }

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("controller.workerthread.terminating"));
  }

  /**
   * Early aborts a connection establishement process: reads user/pass if
   * appropriate, then sends a negative acknowledgement to the driver, then the
   * abort reason and logs a warning. <br>
   * This function has been introduced in order to get more info <b>at the
   * driver side</b>, when a connection fails. At this point (before the fork
   * of the <code>VirtualDatabaseWorkerThread</code>), if we close the
   * connection, the driver won't get any reason why the connection failed (only
   * a closed socket). When calling this function, the driver has probably
   * already send the remaining connection establishement info. These data are
   * ignored (kept in the input socket buffer). FIXME: there is a race condition
   * here. If the driver is too slow, it will try to send to a closed socket and
   * fail without ever receiving this error message.
   * 
   * @param in input socket to read user and pass. If null, won't read them
   * @param reason string message indicating why the connection establishement
   *          has been aborted
   * @param out output socket to the driver
   */
  private void abortConnectionEstablishement(DriverBufferedInputStream in,
      DriverBufferedOutputStream out, String reason)
  {
    if (in != null)
    {
      try
      {
        in.readLongUTF(); // discard user
        in.readLongUTF(); // discard password
      }
      catch (IOException ignored)
      {
      }
    }
    if (logger.isWarnEnabled())
      logger.warn(reason);
    try
    {
      out.writeBoolean(false); // =connection failed
      out.writeLongUTF(reason);
      out.flush();
    }
    // ignore any stream error: we are closing the connection anyway !
    catch (IOException ignored)
    {
    }
  }

  /**
   * Fork a new worker thread.
   * 
   * @param vdb VirtualDatabase to be served
   * @param debugmesg debug message for the controller log
   */
  private void forkVirtualDatabaseWorkerThread(VirtualDatabase vdb,
      String debugmesg)
  {
    if (logger.isDebugEnabled())
      logger.debug(debugmesg);
    VirtualDatabaseWorkerThread thread;

    thread = new VirtualDatabaseWorkerThread(serverThread.controller, vdb);

    vdb.addVirtualDatabaseWorkerThread(thread);
    thread.start();
  }
}