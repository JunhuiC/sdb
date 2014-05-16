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

package org.continuent.sequoia.controller.core;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.util.Constants;

/**
 * Thread that responds to pings sent by the driver.<br>
 * When receiving a ping, replies immediatly by a 'pong' to the sender.
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class PingResponder extends Thread
{
  /** Socket for incomming pings */
  DatagramSocket         socket         = null;
  /** Incomming data buffer. Pings are only one byte long */
  byte[]                 buffer         = new byte[10];
  /** Ping packet received */
  DatagramPacket         incomingPacket = new DatagramPacket(buffer,
                                            buffer.length);
  /** Data that will be sent in the pong packet */
  static final byte      pongData[]     = {Constants.CONTROLLER_PING_VERSION};
  /** Packet sent as a pong */
  DatagramPacket         pongPacket     = new DatagramPacket(pongData,
                                            pongData.length);
  /** Logger instance. */
  static protected Trace logger         = Trace
                                            .getLogger("org.continuent.sequoia.controller.core.PingResponder");

  /** Boolean to control loop and let us know when to stop. */
  private boolean loop = true;
  
  /**
   * Creates a new <code>PingResponder</code> object that will receive pings
   * on the given UDP port
   * 
   * @param port UDP port to receive pings on
   * @throws SocketException
   */
  public PingResponder(int port) throws SocketException
  {
    super("PingResponder");
    socket = new DatagramSocket(port);
  }

  /**
   * Main endless loop iterating on blocking ping reception + pong answer
   */
  public void run()
  {
    logger.info(Translate.get("controller.ping.responder.started"));
    while (loop)
    {
      byte[] data = null;
      // 1. Receive ping. The receive function will not return until data is
      // available on the socket
      try
      {
        socket.receive(incomingPacket);
        data = incomingPacket.getData();
        if (logger.isDebugEnabled())
        {
          String msg = Translate.get("controller.ping.responder.received.ping",
              incomingPacket.getAddress().toString());
          logger.debug(msg);
        }
        if (data[0] != Constants.CONTROLLER_PING_VERSION)
        {
          if (logger.isDebugEnabled())
          {
            logger.debug(Translate.get(
                "controller.ping.responder.wrong.protocol", new String[]{
                    Byte.toString(data[0]),
                    Byte.toString(Constants.CONTROLLER_PING_VERSION)}));
          }
          data = null; // prevents from responding to unknown protocol
        }
      }
      catch (IOException e)
      {
        if (logger.isDebugEnabled())
        {
          String msg = Translate
              .get("controller.ping.responder.received.error");
          logger.debug(msg, e);
        }
      }
      // 2. Send pong as response
      if (data != null)
      {
        pongPacket.setAddress(incomingPacket.getAddress());
        pongPacket.setPort(incomingPacket.getPort());
        try
        {
          socket.send(pongPacket);
          if (logger.isDebugEnabled())
          {
            String msg = Translate.get("controller.ping.responder.send.pong",
                incomingPacket.getAddress().toString());
            logger.debug(msg);
          }
        }
        catch (IOException e)
        {
          if (logger.isInfoEnabled())
          {
            logger.info(Translate
                .get("controller.ping.responder.send.error", e));
          }
        }
      }
    }
    
    logger.info(Translate.get("controller.ping.responder.ended"));
  }
  
  /**
   * Cancel the PingResponder by clearing the loop variable and 
   * sending an interrupt to terminate any i/o.  Wait briefly for 
   * the thread to die then close the socket.  This should ensure 
   * resources are properly freed. 
   */
  public synchronized void shutdown()
  {
    // Only shut down if we are still up and running. 
    if (this.isAlive())
    {
      loop = false;
      this.interrupt();
      try
      {
        this.join(1000);
      }
      catch (InterruptedException e)
      {
        if (this.isAlive())
          logger.warn(Translate.get("controller.ping.responder.not.terminated"));
      }
      socket.close();
    }
  }
}