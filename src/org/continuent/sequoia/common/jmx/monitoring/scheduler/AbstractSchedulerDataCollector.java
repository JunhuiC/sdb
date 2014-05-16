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
package org.continuent.sequoia.common.jmx.monitoring.scheduler;

import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * Abstract class to factor code for scheduler collectors
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 */
public abstract class AbstractSchedulerDataCollector
    extends
      AbstractDataCollector
{
  private String virtualDatabaseName;

  /**
   * create a new collector
   * 
   * @param virtualDatabaseName database accessed to get data
   */
  public AbstractSchedulerDataCollector(String virtualDatabaseName)
  {
    super();
    this.virtualDatabaseName = virtualDatabaseName;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public long collectValue()
  {
    VirtualDatabase vdb = ((Controller)controller).getVirtualDatabase(
        virtualDatabaseName);
    AbstractScheduler scheduler = vdb.getRequestManager().getScheduler();
    return this.getValue(scheduler);
  }

  /**
   * Get information on the scheduler retrieved by <code>collectValue()</code>
   * 
   * @param scheduler to get value from
   * @return collected value
   */
  public abstract long getValue(Object scheduler);
  
  

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getTargetName()
   */
  public String getTargetName()
  {
    return virtualDatabaseName;
  }
}
