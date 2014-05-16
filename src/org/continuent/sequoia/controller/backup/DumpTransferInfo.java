/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 EmicNetworks.
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
 * Initial developer(s): Olivier Fambon.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backup;

import java.io.Serializable;
import java.net.SocketAddress;

/**
 * DumpTransferInfo is used to store the necessary information for the client
 * backuper to fetch a dump from server backuper.
 * 
 * @author <a href="mailto:olivier.fambon@emicnetworks.com">Olivier Fambon</a>
 * @version 1.0
 */
public class DumpTransferInfo implements Serializable
{
  private static final long serialVersionUID = -7714674074423697782L;

  private long              sessionKey;
  private SocketAddress     backuperServerAddress;

  DumpTransferInfo(SocketAddress backuperServerAddress, long sessionKey)
  {
    this.sessionKey = sessionKey;
    this.backuperServerAddress = backuperServerAddress;
  }

  /**
   * Returns the Backuper server address that clients should use to fetch a
   * dump. The Backuper server at this address will ask for the session key.
   * 
   * @return the Backuper server address.
   */
  public SocketAddress getBackuperServerAddress()
  {
    return backuperServerAddress;
  }

  /**
   * Returns a SessionKey to be used as authentication token by the client when
   * fetching a dump.
   * 
   * @return a SessionKey to be used as authentication token.
   */
  public long getSessionKey()
  {
    return sessionKey;
  }
}
