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
 * This is a wrapper where logging has been statically disabled. It should
 * improve the performance if one wants to completely disable traces.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class StaticDisabledLogger extends Trace
{

  /**
   * Creates a new <code>StaticDisabledLogger</code> object from a given
   * log4j <code>Logger</code>.
   * 
   * @param log4jLogger the log4j <code>Logger</code>
   */
  public StaticDisabledLogger(Logger log4jLogger)
  {
    super(log4jLogger);
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#debug(Object, Throwable)
   */
  public void debug(Object message, Throwable t)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#debug(Object)
   */
  public void debug(Object message)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#error(Object, Throwable)
   */
  public void error(Object message, Throwable t)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#error(Object)
   */
  public void error(Object message)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#fatal(Object, Throwable)
   */
  public void fatal(Object message, Throwable t)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#fatal(Object)
   */
  public void fatal(Object message)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#info(Object, Throwable)
   */
  public void info(Object message, Throwable t)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#info(Object)
   */
  public void info(Object message)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#warn(Object, Throwable)
   */
  public void warn(Object message, Throwable t)
  {
  }

  /**
   * This method is overriden with an empty body.
   * 
   * @see org.continuent.sequoia.common.log.Trace#warn(Object)
   */
  public void warn(Object message)
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

  /**
   * @return <code>false</code>
   * @see org.continuent.sequoia.common.log.Trace#isErrorEnabled()
   */
  public boolean isErrorEnabled()
  {
    return false;
  }

  /**
   * @return <code>false</code>
   * @see org.continuent.sequoia.common.log.Trace#isFatalEnabled()
   */
  public boolean isFatalEnabled()
  {
    return false;
  }

  /**
   * @return <code>false</code>
   * @see org.continuent.sequoia.common.log.Trace#isInfoEnabled()
   */
  public boolean isInfoEnabled()
  {
    return false;
  }

  /**
   * @return <code>false</code>
   * @see org.continuent.sequoia.common.log.Trace#isWarnEnabled()
   */
  public boolean isWarnEnabled()
  {
    return false;
  }
}
