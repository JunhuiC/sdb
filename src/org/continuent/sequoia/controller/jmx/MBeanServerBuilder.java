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
 * Initial developer(s): Marc Wick.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.jmx;

import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;

/**
 * This class is a custom implementation of MBeanServerBuilder, it builds a
 * MBeanServer decorated with an AuthenticatingMBeanServer.
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class MBeanServerBuilder extends ChainedMBeanServerBuilder
{
  /**
   * Creates a new <code>MBeanServerBuilder.java</code> object
   */
  public MBeanServerBuilder()
  {
    super(new javax.management.MBeanServerBuilder());
  }

  /**
   * @see javax.management.MBeanServerBuilder#newMBeanServer(java.lang.String,
   *      javax.management.MBeanServer, javax.management.MBeanServerDelegate)
   */
  public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer,
      MBeanServerDelegate delegate)
  {
    AuthenticatingMBeanServer extern = new AuthenticatingMBeanServer();
    MBeanServer nested = getMBeanServerBuilder().newMBeanServer(defaultDomain,
        outer == null ? extern : outer, delegate);
    extern.setMBeanServer(nested);
    return extern;
  }
}
