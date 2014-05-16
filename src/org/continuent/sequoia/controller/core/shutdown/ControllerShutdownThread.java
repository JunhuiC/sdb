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
 * Contributor(s): Mathieu Peltier, Nicolas Modrzyk.
 */

package org.continuent.sequoia.controller.core.shutdown;

import java.io.File;

import org.continuent.sequoia.common.exceptions.ShutdownException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.core.ControllerServerThread;
import org.continuent.sequoia.controller.core.ReportManager;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;

/**
 * Class to shut down a controller
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 */
public class ControllerShutdownThread extends ShutdownThread
{
  protected Controller controller;

  /**
   * Prepare the thread for shutting down.
   * 
   * @param controller the controller to shutdown
   */
  public ControllerShutdownThread(Controller controller)
  {
    super(Constants.SHUTDOWN_SAFE);
    this.controller = controller;
  }

  /**
   * @see org.continuent.sequoia.controller.core.shutdown.ShutdownThread#shutdown()
   */
  public void shutdown() throws ShutdownException
  {
    logger.info("Starting controller shutdown");
    generateReportIfNeeded();
    shutdownJmxAgent();
    shutdownServerConnectionThread(0);
    logger.info("Controller shutdown completed");
  }

  /**
   * Shutdown the JMX Agent.
   */
  protected void shutdownJmxAgent()
  {
    logger.info("Shutting down Jmx Agent");
    try
    {
      if (controller.getJmxEnable())
        MBeanServerManager.setJmxEnabled(false);
    }
    catch (Exception jme)
    {
      logger.error(Translate.get("controller.shutdown.jmx.error", jme
          .getMessage()), jme);
      // throw new ShutdownException(jme);
    }
  }

  /**
   * Shutdown the ControllerServerThread and its attached connection to reject
   * new incoming connections.
   * 
   * @param joinTimeoutInMillis timeout in milliseconds to wait for controller
   *          server thread termination. A timeout of 0 means wait forever.
   * @throws ShutdownException if an error occurs
   */
  protected void shutdownServerConnectionThread(int joinTimeoutInMillis)
      throws ShutdownException
  {
    if (logger.isDebugEnabled())
      logger.debug("Shutting down ControllerServerThread");
    try
    {
      // Shutdown Server Connections Thread
      ControllerServerThread thread = controller.getConnectionThread();
      if (thread != null && !thread.isShuttingDown())
      {
        thread.shutdown();
        logger.info("Waiting for controller thread termination.");
        thread.join(joinTimeoutInMillis);
      }
    }
    catch (Exception e)
    {
      throw new ShutdownException(e);
    }
  }

  /**
   * Generate a controller report if it has been enabled in the config file.
   */
  protected void generateReportIfNeeded()
  {
    ReportManager report = controller.getReport();
    if (report != null && report.isGenerateOnShutdown())
    {
      report.generate(true);
      logger.info(Translate.get("fatal.report.generated", report
          .getReportLocation()
          + File.separator + ControllerConstants.REPORT_FILE));
    }
  }

}