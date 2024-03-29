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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This a wrapper to the log4j logging system. We provide additional features to
 * statically remove tracing.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class Trace
{
  /** Log4j logger instance. */
  private Logger log4jLogger;

  /**
   * Creates a new <code>Trace</code> object from a given log4j
   * <code>Logger</code>.
   * 
   * @param log4jLogger the log4j <code>Logger</code>
   */
  protected Trace(Logger log4jLogger)
  {
    this.log4jLogger = log4jLogger;
  }

  /**
   * Retrieves a logger by its name.
   * 
   * @param name logger name
   * @return trace a <code>Trace</code> instance
   */
  public static Trace getLogger(String name)
  {
    return LogManager.getLogger(name);
  }

  /**
   * Logs a message object with the <code>DEBUG</code> <code>Level</code>.
   * 
   * @param message the message object to log
   */
  public void debug(Object message)
  {
    log4jLogger.debug(message);
  }

  /**
   * Logs a message object with the <code>DEBUG</code> <code>Level</code>
   * including the stack trace of the {@link Throwable}<code>error</code>
   * passed as parameter.
   * 
   * @param message the message object to log
   * @param error the exception to log, including its stack trace
   */
  public void debug(Object message, Throwable error)
  {
    log4jLogger.debug(message, error);
  }

  /**
   * Logs a message object with the <code>ERROR</code> <code>Level</code>.
   * 
   * @param message the message object to log
   */
  public void error(Object message)
  {
    log4jLogger.error(message);
  }

  /**
   * Logs a message object with the <code>ERROR</code> <code>Level</code>
   * including the stack trace of the {@link Throwable}<code>error</code>
   * passed as parameter.
   * 
   * @param message the message object to log.
   * @param error the exception to log, including its stack trace.
   */
  public void error(Object message, Throwable error)
  {
    log4jLogger.error(message, error);
  }

  /**
   * Logs a message object with the <code>FATAL</code> <code>Level</code>.
   * 
   * @param message the message object to log.
   */
  public void fatal(Object message)
  {
    log4jLogger.fatal(message);
  }

  /**
   * Logs a message object with the <code>FATAL</code> <code>Level</code>
   * including the stack trace of the {@link Throwable}<code>error</code>
   * passed as parameter.
   * 
   * @param message the message object to log.
   * @param error the exception to log, including its stack trace.
   */
  public void fatal(Object message, Throwable error)
  {
    log4jLogger.fatal(message, error);
  }

  /**
   * Logs a message object with the <code>INFO</code> <code>Level</code>.
   * 
   * @param message the message object to log.
   */
  public void info(Object message)
  {
    log4jLogger.info(message);
  }

  /**
   * Logs a message object with the <code>INFO</code> <code>Level</code>
   * including the stack trace of the {@link Throwable}<code>error</code>
   * passed as parameter.
   * 
   * @param message the message object to log.
   * @param error the exception to log, including its stack trace.
   */
  public void info(Object message, Throwable error)
  {
    log4jLogger.info(message, error);
  }

  /**
   * Logs a message object with the <code>WARN</code> <code>Level</code>.
   * 
   * @param message the message object to log.
   */
  public void warn(Object message)
  {
    log4jLogger.warn(message);
  }

  /**
   * Logs a message object with the <code>WARN</code> <code>Level</code>
   * including the stack trace of the {@link Throwable}<code>error</code>
   * passed as parameter.
   * 
   * @param message the message object to log.
   * @param error the exception to log, including its stack trace.
   */
  public void warn(Object message, Throwable error)
  {
    log4jLogger.warn(message, error);
  }

  /**
   * Checks whether this category is enabled for the
   * <code>DEBUG</code> <code>Level</code>.
   * <p>
   * This function is intended to lessen the computational cost of disabled log
   * debug statements.
   * <p>
   * For some <code>cat</code> Category object, when you write,
   * 
   * <pre>
   *  cat.debug("This is entry number: " + i );
   * </pre>
   * 
   * <p>
   * You incur the cost constructing the message, concatenatiion in this case,
   * regardless of whether the message is logged or not.
   * <p>
   * If you are worried about speed, then you should write
   * 
   * <pre>
   *  if(cat.isDebugEnabled()) { cat.debug("This is entry number: " + i ); }
   * </pre>
   * 
   * <p>
   * This way you will not incur the cost of parameter construction if debugging
   * is disabled for <code>cat</code>. On the other hand, if the
   * <code>cat</code> is debug enabled, you will incur the cost of evaluating
   * whether the category is debug enabled twice. Once in
   * <code>isDebugEnabled</code> and once in the <code>debug</code>. This
   * is an insignificant overhead since evaluating a category takes about 1%% of
   * the time it takes to actually log.
   * 
   * @return <code>true</code> if this category is debug enabled,
   *         <code>false</code> otherwise.
   */
  public boolean isDebugEnabled()
  {
    return log4jLogger.isDebugEnabled();
  }

  /**
   * Checks whether this category is enabled for the <code>INFO</code>
   * <code>Level</code>.
   * 
   * @return <code>true</code> if this category is enabled for
   *         <code>Level</code> <code>INFO</code>, <code>false</code>
   *         otherwise.
   * @see #isDebugEnabled()
   */
  public boolean isInfoEnabled()
  {
    return log4jLogger.isInfoEnabled();
  }

  /**
   * Checks whether this category is enabled for the <code>WARN</code> 
   * <code>Level</code>.
   * 
   * @return <code>true</code> if this category is enabled for
   *         <code>WARN</code> <code>Level</code>, <code>false</code>
   *         otherwise.
   * @see #isDebugEnabled()
   */
  public boolean isWarnEnabled()
  {
    return log4jLogger.isEnabledFor(Level.WARN);
  }

  /**
   * Checks whether this category is enabled for the <code>ERROR</code>
   * <code>Level</code>.
   * 
   * @return <code>true</code> if this category is enabled for
   *         <code>ERROR</code> <code>Level</code>, <code>false</code>
   *         otherwise.
   * @see #isDebugEnabled()
   */
  public boolean isErrorEnabled()
  {
    return log4jLogger.isEnabledFor(Level.ERROR);
  }

  /**
   * Checks whether this category is enabled for the <code>FATAL</code> 
   * <code>Level</code>.
   * 
   * @return <code>true</code> if this category is enabled for
   *         <code>FATAL</code> <code>Level</code>, <code>false</code>
   *         otherwise.
   * @see #isDebugEnabled()
   */
  public boolean isFatalEnabled()
  {
    return log4jLogger.isEnabledFor(Level.FATAL);
  }
}
