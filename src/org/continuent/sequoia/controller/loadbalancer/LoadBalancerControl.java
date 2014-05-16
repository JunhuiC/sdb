/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Emic Networks.
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

import org.continuent.sequoia.common.jmx.mbeans.LoadBalancerControlMBean;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;

/**
 * LoadBalancerControlMBean implemementation.<br />
 * Used to manage a AbstractLoadBalancer.
 * 
 * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer
 */
public class LoadBalancerControl extends AbstractStandardMBean
    implements
      LoadBalancerControlMBean
{

  private AbstractLoadBalancer managedLoadBalancer;

  /**
   * Creates a new <code>LoadBalancerControl</code> object
   * 
   * @param managedLoadBalancer the managed load balancer
   * @throws NotCompliantMBeanException if this mbean is not compliant
   */
  public LoadBalancerControl(AbstractLoadBalancer managedLoadBalancer)
      throws NotCompliantMBeanException
  {
    super(LoadBalancerControlMBean.class);
    this.managedLoadBalancer = managedLoadBalancer;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.LoadBalancerControlMBean#getRAIDbLevel()
   */
  public int getRAIDbLevel()
  {
    return managedLoadBalancer.getRAIDbLevel();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.LoadBalancerControlMBean#getParsingGranularity()
   */
  public int getParsingGranularity()
  {
    return managedLoadBalancer.getParsingGranularity();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.LoadBalancerControlMBean#getInformation()
   */
  public String getInformation()
  {
    return managedLoadBalancer.getInformation();
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "loadbalancer"; //$NON-NLS-1$
  }
}
