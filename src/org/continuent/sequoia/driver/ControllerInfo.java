/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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
 * Contributor(s): Gilles Rayrat.
 */

package org.continuent.sequoia.driver;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Controller related information, namely the host address and the port on which
 * the controller is running.<br>
 * This class actually extends <code>InetSocketAddress</code> with similar
 * constructor from name and port, which catches name resolution issues to throw
 * an UnknownHostException.<br>
 * A second constructor from <code>InetSocketAddress</code> is available to
 * create a ControllerInfo from (for example) a udp packet.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class ControllerInfo extends InetSocketAddress
{
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new <code>ControllerInfo</code> object from a hostname and a
   * port number by trying to resolve given hostname. If resolution fails,
   * throws and {@link UnknownHostException}
   * 
   * @param hostname the controller host name
   * @param port the controller port
   * @throws UnknownHostException if the given name could not be resolved
   * @see InetSocketAddress#InetSocketAddress(String, int)
   */
  public ControllerInfo(String hostname, int port) throws UnknownHostException
  {
    super(hostname, port);
    if (isUnresolved())
    {
      throw new UnknownHostException("Hostname " + hostname
          + " could not be resolved");
    }
  }

  /**
   * Creates a new <code>ControllerInfo</code> object from a given
   * <code>InetSocketAddress</code>. This constructor actually is a wrapper
   * of {@link InetSocketAddress#InetSocketAddress(java.net.InetAddress, int)}
   */
  public ControllerInfo(InetSocketAddress addr)
  {
    super(addr.getAddress(), addr.getPort());
  }
}
