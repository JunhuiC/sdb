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

package org.continuent.sequoia.common.jmx.monitoring.backend;

import org.continuent.sequoia.common.exceptions.DataCollectorException;
import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.monitoring.datacollector.DataCollector;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * Abstract class to factor code for collecting data from backends
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public abstract class AbstractBackendDataCollector
    extends AbstractDataCollector
{
  private String backendName;
  private String virtualDatabaseName;

  /**
   * Create new collector
   * 
   * @param backendName of the backend to get data from
   * @param virtualDatabaseName that contains reference to this backend
   */
  public AbstractBackendDataCollector(String backendName,
      String virtualDatabaseName)
  {
    super();
    this.backendName = backendName;
    this.virtualDatabaseName = virtualDatabaseName;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public long collectValue() throws DataCollectorException
  {
    try
    {
      VirtualDatabase vdb = ((Controller) controller)
          .getVirtualDatabase(virtualDatabaseName);
      DatabaseBackend db = vdb.getAndCheckBackend(backendName,
          VirtualDatabase.NO_CHECK_BACKEND);
      return this.getValue(db);
    }
    catch (Exception e)
    {
      throw new DataCollectorException(DataCollector.BACKEND_NOT_ACCESSIBLE);
    }
  }

  /**
   * get the proper collected value when we have instace of the backend
   * 
   * @param backend <code>DatabaseBackend</code> instance
   * @return collected value
   */
  public abstract long getValue(Object backend);

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getTargetName()
   */
  public String getTargetName()
  {
    return backendName;
  }
}
