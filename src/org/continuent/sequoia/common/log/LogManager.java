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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): __________________.
 */

package org.continuent.sequoia.common.log;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Logger manager.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LogManager
{
  /**
   * Retrieves a logger by its name.
   * 
   * @param name logger name
   * @return trace a <code>trace</code>
   */
  static Trace getLogger(String name)
  {
    return new Trace(Logger.getLogger(name));
  }

  /**
   * Configures log4j according to the given property file name.
   * 
   * @param propertyFileName the log4j property file name
   */
  public static void configure(String propertyFileName)
  {
    PropertyConfigurator.configure(propertyFileName);
  }
}
