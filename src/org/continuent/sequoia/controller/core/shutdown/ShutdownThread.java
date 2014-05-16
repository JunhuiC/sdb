/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): Nicolas Modrzyk.
 */

package org.continuent.sequoia.controller.core.shutdown;

import java.util.Date;

import org.continuent.sequoia.common.exceptions.ShutdownException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;

/**
 * Skeleton for shutdown threads. This includes <code>Controller</code>,
 * <code>VirtualDatabase</code> and <code>DatabaseBackend</code> shutdown
 * threads.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public abstract class ShutdownThread implements Runnable
{
  /** Group to join onto when shutting down */
  public ThreadGroup shutdownGroup = new ThreadGroup("shutdown" + new Date());

  protected int      shutdownLevel;

  /** Logger instance. */
  Trace              logger        = Trace
                                       .getLogger("org.continuent.sequoia.controller.shutdown");

  /**
   * Create a new shutdown thread
   * 
   * @param level Constants.SHUTDOWN_WAIT, Constants.SHUTDOWN_SAFE or
   *          Constants.SHUTDOWN_FORCE
   */
  public ShutdownThread(int level)
  {
    this.shutdownLevel = level;
    logger = Trace.getLogger("org.continuent.sequoia.controller.shutdown");
  }

  /**
   * Returns the shutdownGroup value.
   * 
   * @return Returns the shutdownGroup.
   */
  public ThreadGroup getShutdownGroup()
  {
    return shutdownGroup;
  }

  /**
   * Get shutdown level
   * 
   * @return level
   */
  public int getShutdownLevel()
  {
    return this.shutdownLevel;
  }

  /**
   * Execute the shutdown
   * 
   * @see java.lang.Runnable#run()
   */
  public void run()
  {
    try
    {
      shutdown();
    }
    catch (ShutdownException se)
    {
      se.printStackTrace();
      abortShutdown(se);
    }
  }

  /**
   * If shutdown fails ...
   * 
   * @param cause why shutdown was aborted
   */
  public void abortShutdown(Exception cause)
  {
    logger.info(Translate.get("controller.shutdown.aborted", cause));
  }

  /**
   * Specific implementation of the shutdown method.
   * 
   * @throws ShutdownException if fails
   */
  public abstract void shutdown() throws ShutdownException;

}