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

package org.continuent.sequoia.common.jmx.monitoring.controller;

import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;
import org.continuent.sequoia.controller.core.Controller;

/**
 * Abstract data collector to factor code for the controller collectors
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public abstract class AbstractControllerDataCollector
    extends
      AbstractDataCollector
{
  private String controllerName;

  /**
   * Default constructors
   */
  public AbstractControllerDataCollector()
  {
    //controllerName = ControllerConstants.DEFAULT_IP;
  }
  
  /**
   * Create a new collector for controller and set the name
   *@param controller attached to the collector
   */
  public AbstractControllerDataCollector(Object controller)
  {
    controllerName = ((Controller)controller).getIPAddress();
    this.controller = controller;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public abstract long collectValue();

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getTargetName()
   */
  public String getTargetName()
  {
    return controllerName;
  }
}
