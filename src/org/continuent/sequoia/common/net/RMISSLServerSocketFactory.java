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
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocket;

/**
 * This class defines a RMISSLServerSocketFactory
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class RMISSLServerSocketFactory
    implements
      RMIServerSocketFactory,
      Serializable
{
  private static final long serialVersionUID = -1173753000488037655L;

  ServerSocketFactory       factory;

  /**
   * Creates a new <code>RMISSLServerSocketFactory.java</code> object
   * 
   * @param socketFactory - the factory to be used
   */
  public RMISSLServerSocketFactory(ServerSocketFactory socketFactory)
  {
    this.factory = socketFactory;
  }

  /**
   * @see java.rmi.server.RMIServerSocketFactory#createServerSocket(int)
   */
  public ServerSocket createServerSocket(int port) throws IOException
  {
    SSLServerSocket socket = (SSLServerSocket) factory.createServerSocket(port);
    return socket;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   *      <p>
   *      http://developer.java.sun.com/developer/bugParade/bugs/4492317.html
   */
  public boolean equals(Object obj)
  {
    if (obj == null)
      return false;
    if (this == obj)
      return true;
    if (factory == null)
      return false;
    return (getClass() == obj.getClass() && factory.equals(factory));
  }

  /**
   * @see java.lang.Object#hashCode()
   *      <p>
   *      http://developer.java.sun.com/developer/bugParade/bugs/4492317.html
   */
  public int hashCode()
  {
    return factory.hashCode();
  }

}
