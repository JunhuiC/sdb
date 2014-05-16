/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Marc Wick.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

/**
 * This class defines a AuthenticatedSSLSocketFactory
 * <p>
 * It is a wrapper around the socket factory in the constructor and sets the
 * setNeedClientAuth to true to enforce client authentication with the public
 * key
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class AuthenticatedServerSocketFactory extends SSLServerSocketFactory

{

  private SSLServerSocketFactory factory;

  /**
   * Creates a new <code>AuthenticatedSSLSocketFactory.java</code> object
   * 
   * @param factory - the factory
   */
  public AuthenticatedServerSocketFactory(SSLServerSocketFactory factory)
  {
    this.factory = factory;
  }

  /**
   * @see javax.net.ServerSocketFactory#createServerSocket(int)
   */
  public ServerSocket createServerSocket(int port) throws IOException,
      UnknownHostException
  {
    SSLServerSocket socket = (SSLServerSocket) factory.createServerSocket(port);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.ServerSocketFactory#createServerSocket(int,int)
   */
  public ServerSocket createServerSocket(int port, int backlog)
      throws IOException, UnknownHostException
  {
    SSLServerSocket socket = (SSLServerSocket) factory.createServerSocket(port,
        backlog);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.ServerSocketFactory#createServerSocket(int, int,
   *      java.net.InetAddress)
   */
  public ServerSocket createServerSocket(int port, int backlog,
      InetAddress ifAddress) throws IOException, UnknownHostException
  {
    SSLServerSocket socket = (SSLServerSocket) factory.createServerSocket(port,
        backlog, ifAddress);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.ssl.SSLServerSocketFactory#getDefaultCipherSuites()
   */
  public String[] getDefaultCipherSuites()
  {
    return factory.getDefaultCipherSuites();
  }

  /**
   * @see javax.net.ssl.SSLServerSocketFactory#getSupportedCipherSuites()
   */
  public String[] getSupportedCipherSuites()
  {
    return factory.getDefaultCipherSuites();
  }

}