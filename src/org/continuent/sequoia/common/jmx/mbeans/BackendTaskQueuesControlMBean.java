/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.jmx.mbeans;

/**
 * MBean Interface to manage task queues associated to a backend.
 * 
 * @see org.continuent.sequoia.common.jmx.JmxConstants#getBackendTaskQueuesObjectName(String,
 *      String)
 */
public interface BackendTaskQueuesControlMBean
{
  /**
   * Returns a <code>String</code> representing a dump of the state of the
   * backend task queues.
   * 
   * @return a String representing a dump of the state of the backend task
   *         queues.
   */
  String dump();
}