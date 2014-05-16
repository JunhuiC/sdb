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

package org.continuent.sequoia.common.jmx.monitoring;

import java.io.Serializable;

import org.continuent.sequoia.common.exceptions.DataCollectorException;

/**
 * This defines the abstract hierachy to collect monitoring information. All
 * monitored information from the controller should extends this class.
 * <code>collectValue</code> can therefore NOT be called directly on the
 * client side. Instead, the client should be only given the returned result.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 */
public abstract class AbstractDataCollector implements Serializable
{
  protected transient Object controller;

  /**
   * This is used on the controller side to collect information
   * 
   * @return the value collected by this collectorsardes@inrialpes.fr
   * @throws DataCollectorException if fails to collect the information
   */
  public abstract long collectValue() throws DataCollectorException;

  /**
   * Get a string description for this collector
   * 
   * @return translated string
   */
  public abstract String getDescription();

  /**
   * Return the name of the target of this collector
   * 
   * @return target name
   */
  public abstract String getTargetName();

  /**
   * associated a controller to this data collector
   * 
   * @param controller to associate
   */
  public void setController(Object controller)
  {
    this.controller = controller;
  }

}
