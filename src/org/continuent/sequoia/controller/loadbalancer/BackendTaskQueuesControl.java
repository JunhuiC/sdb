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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.loadbalancer;

import javax.management.NotCompliantMBeanException;

import org.continuent.sequoia.common.jmx.mbeans.BackendTaskQueuesControlMBean;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;

/**
 * BackendTaskQueuesControlMBean implementation.
 */
public class BackendTaskQueuesControl extends AbstractStandardMBean
    implements
      BackendTaskQueuesControlMBean
{

  private BackendTaskQueues managedTaskQueues;

  /**
   * Creates a new <code>BackendTaskQueuesControl</code> object
   * 
   * @param taskQueues the managed BackendTaskQueues
   * @throws NotCompliantMBeanException if the mbean is not compliant
   */
  public BackendTaskQueuesControl(BackendTaskQueues taskQueues)
      throws NotCompliantMBeanException
  {
    super(BackendTaskQueuesControlMBean.class);
    this.managedTaskQueues = taskQueues;
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "backend task queues";
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.BackendTaskQueuesControlMBean#dump()
   */
  public String dump()
  {
    return managedTaskQueues.dump();
  }
}
