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
 * Initial developer(s): Marc Wick.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.net;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

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
public class AuthenticatedSocketFactory extends SSLSocketFactory
    implements
      Serializable
{

  private static final long serialVersionUID = 3408254276587727154L;

  private SSLSocketFactory  factory;

  /**
   * Creates a new <code>AuthenticatedSSLSocketFactory.java</code> object
   * 
   * @param factory - the factory
   */
  public AuthenticatedSocketFactory(SSLSocketFactory factory)
  {
    this.factory = factory;
  }

  /**
   * @see javax.net.SocketFactory#createSocket(java.lang.String, int)
   */
  public Socket createSocket(String host, int port) throws IOException,
      UnknownHostException
  {
    SSLSocket socket = (SSLSocket) factory.createSocket(host, port);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int)
   */
  public Socket createSocket(InetAddress host, int port) throws IOException
  {
    SSLSocket socket = (SSLSocket) factory.createSocket(host, port);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.SocketFactory#createSocket(java.lang.String, int,
   *      java.net.InetAddress, int)
   */
  public Socket createSocket(String host, int port, InetAddress localAddress,
      int localPort) throws IOException, UnknownHostException
  {
    SSLSocket socket = (SSLSocket) factory.createSocket(host, port,
        localAddress, localPort);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int,
   *      java.net.InetAddress, int)
   */
  public Socket createSocket(InetAddress address, int port,
      InetAddress localAddress, int localPort) throws IOException
  {
    SSLSocket socket = (SSLSocket) factory.createSocket(address, port,
        localAddress, localPort);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.ssl.SSLSocketFactory#createSocket(java.net.Socket,
   *      java.lang.String, int, boolean)
   */
  public Socket createSocket(Socket s, String host, int port, boolean autoClose)
      throws IOException
  {
    SSLSocket socket = (SSLSocket) factory.createSocket(s, host, port,
        autoClose);
    socket.setNeedClientAuth(true);
    return socket;
  }

  /**
   * @see javax.net.ssl.SSLSocketFactory#getDefaultCipherSuites()
   */
  public String[] getDefaultCipherSuites()
  {
    return factory.getDefaultCipherSuites();
  }

  /**
   * @see javax.net.ssl.SSLSocketFactory#getSupportedCipherSuites()
   */
  public String[] getSupportedCipherSuites()
  {
    return factory.getDefaultCipherSuites();
  }

}
