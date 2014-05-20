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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): 
 */

package org.continuent.sequoia.common.jmx.monitoring.client;

import java.util.ArrayList;

import org.continuent.sequoia.common.exceptions.DataCollectorException;
import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.monitoring.datacollector.DataCollector;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;

/**
 * Collects information about Sequoia clients.
 * <p>
 * TODO: Implements proper client data collection. This is not used at the
 * moment.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public abstract class AbstractClientDataCollector extends AbstractDataCollector
{
  /**
	 * 
	 */
	private static final long serialVersionUID = -7431987841647096336L;
private String virtualDatabaseName;
  private String clientId;
  private int    clientIndex;

  /**
   * @param virtualDatabaseName of the virtualdatabase
   * @param clientId for the client
   * @throws DataCollectorException if cannot access client
   */
  public AbstractClientDataCollector(String virtualDatabaseName, String clientId)
      throws DataCollectorException
  {
    super();
    this.virtualDatabaseName = virtualDatabaseName;
    this.clientId = clientId;
    setClientIndex();
  }

  private Object setClientIndex() throws DataCollectorException
  {
    VirtualDatabase vdb = ((Controller) controller)
        .getVirtualDatabase(virtualDatabaseName);
    ArrayList<?> activeThreads = vdb.getActiveThreads();
    int size = activeThreads.size();
    VirtualDatabaseWorkerThread client = null;
    int index = 0;
    for (index = 0; index < size; index++)
    {
      client = ((VirtualDatabaseWorkerThread) activeThreads.get(index));
      if (client.getUser().equals(clientId))
        break;
      else
        client = null;
    }

    if (client == null)
      throw new DataCollectorException(DataCollector.CLIENT_NOT_FOUND);
    else
    {
      this.clientIndex = index;
      return client;
    }
  }

  private Object checkClientIndex() throws DataCollectorException
  {
    VirtualDatabase vdb = ((Controller) controller)
        .getVirtualDatabase(virtualDatabaseName);
    ArrayList<?> activeThreads = vdb.getActiveThreads();
    VirtualDatabaseWorkerThread client = (VirtualDatabaseWorkerThread) activeThreads
        .get(clientIndex);
    if (client.getUser().equals(clientId))
      return client;
    else
    {
      return setClientIndex();
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public long collectValue() throws DataCollectorException
  {
    VirtualDatabaseWorkerThread client = (VirtualDatabaseWorkerThread) checkClientIndex();
    return this.getValue(client);
  }

  /**
   * We have the client object so let's get the value we want from ot
   * 
   * @param client as an object to allow it through RMI, but IS a
   *          <code>VirtualDatabaseWorkerThread</code>
   * @return the collected value
   */
  public abstract long getValue(Object client);

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getTargetName()
   */
  public String getTargetName()
  {
    return clientId;
  }
}
