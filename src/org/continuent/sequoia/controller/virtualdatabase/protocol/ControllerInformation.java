/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a ControllerInformation class to send to new group members
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 */
public class ControllerInformation extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = -2380753151132303045L;

  private String            controllerName;
  private String            jmxName;
  private long              controllerId;

  /**
   * Creates a new <code>ControllerInformation</code> object
   * 
   * @param controllerName the controller name
   * @param controllerJmxName the jmx name of the controller
   * @param controllerId the controller identifier
   */
  public ControllerInformation(String controllerName, String controllerJmxName,
      long controllerId)
  {
    this.controllerName = controllerName;
    this.jmxName = controllerJmxName;
    this.controllerId = controllerId;
  }

  /**
   * Returns the controllerId value.
   * 
   * @return Returns the controllerId.
   */
  public final long getControllerId()
  {
    return controllerId;
  }

  /**
   * @return Returns the controllerName.
   */
  public String getControllerName()
  {
    return controllerName;
  }

  /**
   * @param controllerName The controllerName to set.
   */
  public void setControllerName(String controllerName)
  {
    this.controllerName = controllerName;
  }

  /**
   * Returns the jmxName value.
   * 
   * @return Returns the jmxName.
   */
  public String getJmxName()
  {
    return jmxName;
  }

  /**
   * Sets the jmxName value.
   * 
   * @param jmxName The jmxName to set.
   */
  public void setJmxName(String jmxName)
  {
    this.jmxName = jmxName;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    dvdb.addRemoteControllerJmxName(sender, jmxName);
    dvdb.addRemoteControllerId(sender, controllerId);
    return Boolean.TRUE;
  }
}