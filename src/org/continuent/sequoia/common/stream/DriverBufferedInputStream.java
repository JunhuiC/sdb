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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): Emmanuel Cecchet. Marc Herbert.
 */

package org.continuent.sequoia.common.stream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.Socket;

import org.continuent.sequoia.driver.SequoiaUrl;

/**
 * LongUTFDataInputStream subclass used between the controller and the driver.
 * <p>
 * Creates a buffered LongUTFDataInputStream upon a given
 * {@link java.net.Socket} and adds its creation date (as a <code>long</code>)
 * for statistics purposes on the socket (eg.
 * {@link org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#retrieveClientData()})
 * This class is now an implementation detail and references to it should be
 * replaced by references to the more abstract LongUTFDataInputStream.
 * Ultimately this class could/should be moved out of the protocol package,
 * closer to the very few classes that really need to reference it.
 * 
 * @see org.continuent.sequoia.common.stream.LongUTFDataInputStream
 * @see org.continuent.sequoia.common.stream.DriverBufferedOutputStream
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @author <a href="mailto:Gilles.Rayrat@emicnetworks.com">Gilles Rayrat</a>
 */
public class DriverBufferedInputStream
    extends LongUTFDataInputStream
{
  private Socket    socket;
  private long      dateCreated;
  private final int debugLevel;

  /**
   * @see java.lang.Object#finalize()
   */
  protected void finalize() throws Throwable
  {
    try
    {
      /*
       * This is just an extra safety net: this socket must be ALREADY closed at
       * this time (check finalize()-related litterature)
       */
      if (!socket.isClosed())
      {
        if (debugLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
        {
          System.err
              .println("Socket was not closed, either someone forgot to"
                  + " call Connection.close() on " + socket);
          System.err.println("or a finally { close(); } block is missing");
        }

        socket.close();
      }

    }
    finally
    {
      super.finalize();
    }
  }

  /**
   * Controller has a different logging scheme: debugLevel = OFF
   * 
   * @see #DriverBufferedInputStream(Socket, int)
   */
  public DriverBufferedInputStream(Socket clientSocket) throws IOException,
      StreamCorruptedException
  {
    this(clientSocket, SequoiaUrl.DEBUG_LEVEL_OFF);
  }

  /**
   * Creates a buffered Stream on a socket and sets the creation date to the
   * current system time.
   * 
   * @param socket socket for this stream
   * @param debugLevel debug level
   * @throws IOException if an error occurs
   * @throws StreamCorruptedException if an error occurs
   */
  public DriverBufferedInputStream(Socket socket, int debugLevel)
      throws IOException, StreamCorruptedException
  {
    super(new BufferedInputStream(socket.getInputStream()));
    this.socket = socket;
    this.debugLevel = debugLevel;
    dateCreated = System.currentTimeMillis();
  }

  /**
   * @return Returns the socket.
   */
  public Socket getSocket()
  {
    return socket;
  }

  /**
   * @return Returns the creation date.
   */
  public long getDateCreated()
  {
    return dateCreated;
  }
}