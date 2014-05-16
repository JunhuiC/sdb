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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend.result;

import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.requests.AbstractRequest;

public class AutocommitConnectionResource implements ConnectionResource
{
  AbstractRequest request;
  PooledConnection connection;
  AbstractConnectionManager connectionManager;

  public AutocommitConnectionResource(AbstractRequest request, 
      PooledConnection connection, AbstractConnectionManager connectionManager)
  {
    this.request = request;
    this.connection = connection;
    this.connectionManager = connectionManager;
  }

  /**
   * Frees the connection using autocommit method. 
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.backend.result.ConnectionResource#release()
   */
  public synchronized void release()
  {
    if (connection != null)
    {
      connectionManager.releaseConnectionInAutoCommit(request, connection);
      connection = null;
    }
  }

}
