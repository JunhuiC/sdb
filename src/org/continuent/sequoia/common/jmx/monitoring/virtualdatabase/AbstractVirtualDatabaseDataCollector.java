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
package org.continuent.sequoia.common.jmx.monitoring.virtualdatabase;

import org.continuent.sequoia.common.exceptions.DataCollectorException;
import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * Abstract class for virtual databases collectors
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 */
public abstract class AbstractVirtualDatabaseDataCollector
    extends
      AbstractDataCollector
{
  private String virtualDatabaseName;

  /**
   * abstract collector contructor
   * @param virtualDatabaseName to collect data from
   */
  public AbstractVirtualDatabaseDataCollector(String virtualDatabaseName)
  {
    super();
    this.virtualDatabaseName = virtualDatabaseName;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public long collectValue() throws DataCollectorException
  {
    VirtualDatabase vdb = ((Controller)controller).getVirtualDatabase(
        virtualDatabaseName);
    return this.getValue(vdb);
  }

  /**
   * We have the database object so let's get the value we want from ot
   * @param database as an object to allow it through RMI, but IS a <code>VirtualDatabase</code>
   * @return the collected value 
   */
  public abstract long getValue(Object database);
  
  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getTargetName()
   */
  public String getTargetName()
  {
    return virtualDatabaseName;
  }
}
