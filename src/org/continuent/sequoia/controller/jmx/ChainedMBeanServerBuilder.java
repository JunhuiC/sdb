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
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;

/**
 * Base class for chained MBeanServerBuilders. <br>
 * By default this class delegates all method calls to the nested
 * MBeanServerBuilder. <br>
 * See the MX4J documentation on how to use correctly this class. <br>
 * <br>
 * Example implementation:
 * 
 * <pre>
 * 
 *  
 *   
 *    public class LoggingBuilder extends ChainedMBeanServerBuilder
 *    {
 *       public LoggingBuilder()
 *       {
 *          super(new MBeanServerBuilder());
 *       }
 *   
 *       public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate)
 *       {
 *          LoggingMBeanServer external = new LoggingMBeanServer();
 *          MBeanServer nested = getBuilder().newMBeanServer(defaultDomain, outer == null ? external : outer, delegate);
 *          external.setMBeanServer(nested);
 *          return external;
 *       }
 *    }
 *   
 *    public class LoggingMBeanServer extends ChainedMBeanServer
 *    {
 *       protected void setMBeanServer(MBeanServer server)
 *       {
 *          super.setMBeanServer(server);
 *       }
 *   
 *       public Object getAttribute(ObjectName objectName, String attribute)
 *               throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException
 *       {
 *          Object value = super.getAttribute(objectName, attribute);
 *          System.out.println(&quot;Value is: &quot; + value);
 *          return value;
 *       }
 *   
 *       ...
 *    }
 *    
 *   
 *  
 * </pre>
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class ChainedMBeanServerBuilder extends javax.management.MBeanServerBuilder
{
  private final MBeanServerBuilder builder;

  /**
   * Creates a new chained MBeanServerBuilder
   * 
   * @param builder The MBeanServerBuilder this object delegates to.
   */
  public ChainedMBeanServerBuilder(MBeanServerBuilder builder)
  {
    if (builder == null)
      throw new IllegalArgumentException();
    this.builder = builder;
  }

  /**
   * Forwards the call to the chained builder.
   * 
   * @see MBeanServerBuilder#newMBeanServerDelegate
   */
  public MBeanServerDelegate newMBeanServerDelegate()
  {
    return getMBeanServerBuilder().newMBeanServerDelegate();
  }

  /**
   * Forwards the call to the chained builder.
   * 
   * @see MBeanServerBuilder#newMBeanServer(java.lang.String,
   *      javax.management.MBeanServer, javax.management.MBeanServerDelegate)
   */
  public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer,
      MBeanServerDelegate delegate)
  {
    return getMBeanServerBuilder().newMBeanServer(defaultDomain, outer,
        delegate);
  }

  /**
   * Returns the chained MBeanServerBuilder this object delegates to.
   */
  protected MBeanServerBuilder getMBeanServerBuilder()
  {
    return builder;
  }

}
