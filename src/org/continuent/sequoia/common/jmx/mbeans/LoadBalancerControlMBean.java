/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.jmx.mbeans;

/**
 * This class defines a AbstractLoadBalancerMBean
 * 
 */
public interface LoadBalancerControlMBean
{
  /**
   * Return the load balancer RAIDb level
   * 
   * @return the RAIDb level
   */
  int getRAIDbLevel();

  /**
   * Get the needed query parsing granularity.
   * 
   * @return needed query parsing granularity
   */
  int getParsingGranularity();

  /**
   * Return generic information about the load balancer.
   * 
   * @return load balancer information
   */
  String getInformation();
}