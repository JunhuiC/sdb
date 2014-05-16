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

/**
 * This is a wrapper where debug logging has been statically disabled. It
 * should improve the performance if one wants to completely disable debug
 * traces.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class StaticNoDebugLogger extends Trace
{

  /**
   * Creates a new <code>StaticNoDebugLogger</code> object from a given log4j
   * <code>Logger</code>.
   * 
   * @param log4jLogger the log4j <code>Logger</code>
   */
  public StaticNoDebugLogger(Logger log4jLogger)
  {
    super(log4jLogger);
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see Trace#debug(Object, Throwable)
   */
  public void debug(Object message, Throwable t)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see Trace#debug(Object)
   */
  public void debug(Object message)
  {
  }

  /**
   * @return <code>false</code>
   * @see org.continuent.sequoia.common.log.Trace#isDebugEnabled()
   */
  public boolean isDebugEnabled()
  {
    return false;
  }

}
