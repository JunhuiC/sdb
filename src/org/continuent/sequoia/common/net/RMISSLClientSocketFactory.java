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
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * This class defines a RMISSLClientSocketFactory
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class RMISSLClientSocketFactory
    implements
      RMIClientSocketFactory,
      Serializable
{
  private static final long serialVersionUID = -5994304413561755872L;

  /**
   * @see java.rmi.server.RMIClientSocketFactory#createSocket(java.lang.String,
   *      int)
   */
  public Socket createSocket(String host, int port) throws IOException
  {
    SSLSocket socket = (SSLSocket) SSLSocketFactory.getDefault().createSocket(
        host, port);
    if (System.getProperty("javax.net.ssl.trustStore") != null)
      socket.setNeedClientAuth(true);

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
    return getClass() == obj.getClass();
  }

  /**
   * @see java.lang.Object#hashCode()
   *      <p>
   *      http://developer.java.sun.com/developer/bugParade/bugs/4492317.html
   */
  public int hashCode()
  {
    return 13;
  }
}
