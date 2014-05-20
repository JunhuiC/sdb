/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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

package org.continuent.sequoia.controller.management;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.mbeans.ControllerMBean;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;

/**
 * This class defines the management interface of a Controller
 * 
 * @author <a href="mailto:jeff@emicnetworks.com">Jeff Mesnil</a>
 * @version 1.0
 */
public class Controller extends AbstractStandardMBean
    implements
      ControllerMBean
{

  private org.continuent.sequoia.controller.core.Controller controller;
  static Trace                                              endUserLogger = Trace
                                                                              .getLogger("org.continuent.sequoia.enduser");

  /**
   * Creates a new <code>Controller</code> object based on a
   * <code>managedController</code>
   * 
   * @param managedController the controller to manage
   * @throws NotCompliantMBeanException if this instance is not a compliant
   *           MBean
   */
  public Controller(
      org.continuent.sequoia.controller.core.Controller managedController)
      throws NotCompliantMBeanException
  {
    super(ControllerMBean.class);
    this.controller = managedController;
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "controller"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#addVirtualDatabases(java.lang.String)
   */
  public void addVirtualDatabases(String xml) throws ControllerException
  {
    try
    {
      endUserLogger.info(Translate.get("controller.add.virtualdatabases"));
      controller.addVirtualDatabases(xml);
      endUserLogger.info(Translate.get("controller.virtualdatabases.added"));
    }
    catch (ControllerException ex)
    {
      endUserLogger.error(ex.getMessage());
      throw ex;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#initializeVirtualDatabases(java.lang.String, boolean)
   */
  public void addVirtualDatabases(String vdbXmlSpec, boolean force) throws ControllerException
  {
    if (!force)
    {
      addVirtualDatabases(vdbXmlSpec);
      return; 
    }
    
    try
    {
      endUserLogger.info(Translate.get("controller.add.virtualdatabases") + " (force)");
      controller.addVirtualDatabases(vdbXmlSpec, null, ControllerConstants.AUTO_ENABLE_FORCE_LOAD,
          null);
      endUserLogger.info(Translate.get("controller.virtualdatabases.added") + " (force)");
    }
    catch (ControllerException ex)
    {
      endUserLogger.error(ex.getMessage());
      throw ex;
    }
  }
  
  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#initializeVirtualDatabases(java.lang.String)
   */
  public void initializeVirtualDatabases(String vdbXmlSpec) throws ControllerException
  {
    try
    {
      endUserLogger.info(Translate.get("controller.add.virtualdatabases") + " (init)");
      controller.addVirtualDatabases(vdbXmlSpec, null, ControllerConstants.AUTO_ENABLE_INIT,
          null);
      endUserLogger.info(Translate.get("controller.virtualdatabases.added") + " (init)");
    }
    catch (ControllerException ex)
    {
      endUserLogger.error(ex.getMessage());
      throw ex;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getVirtualDatabaseNames()
   */
  public List<?> getVirtualDatabaseNames()
  {
    return controller.getVirtualDatabaseNames();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#addDriver(byte[])
   */
  public void addDriver(byte[] bytes) throws Exception
  {
    try
    {
      controller.addDriver(bytes);
    }
    catch (ControllerException e)
    {
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#saveConfiguration()
   */
  public String saveConfiguration() throws Exception
  {
    String msg = controller.saveConfiguration();
    endUserLogger.info(msg);
    return msg;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#shutdown()
   */
  public void shutdown() throws ControllerException
  {
    try
    {
      endUserLogger.info(Translate.get("controller.shutdown", this
          .getControllerName()));
      controller.shutdown();
      endUserLogger.info(Translate.get("controller.shutdown.completed",
          this.getControllerName()));
    }
    catch (ControllerException ex)
    {
      endUserLogger.error(ex.getMessage());
      throw ex;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getBacklogSize()
   */
  public int getBacklogSize()
  {
    return controller.getBacklogSize();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getControllerName()
   */
  public String getControllerName()
  {
    return controller.getControllerName();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getJmxName()
   */
  public String getJmxName()
  {
    return controller.getJmxName();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getPortNumber()
   */
  public int getPortNumber()
  {
    return controller.getPortNumber();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getVersionNumber()
   */
  public String getVersionNumber() throws RemoteException
  {
    return controller.getVersionNumber();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getXml()
   */
  public String getXml()
  {
    return controller.getXml();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#isShuttingDown()
   */
  public boolean isShuttingDown()
  {
    return controller.isShuttingDown();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#setBacklogSize(int)
   */
  public void setBacklogSize(int size)
  {
    controller.setBacklogSize(size);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#refreshLogConfiguration()
   */
  public void refreshLogConfiguration() throws ControllerException
  {
    try
    {
      controller.refreshLogConfiguration();
      endUserLogger
          .info(Translate.get("controller.refresh.log.success"));
    }
    catch (ControllerException ex)
    {
      endUserLogger.error(ex.getMessage());
      throw ex;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#updateLogConfigurationFile(java.lang.String)
   */
  public void updateLogConfigurationFile(String newConfiguration)
      throws IOException, ControllerException
  {
    try
    {
      controller.updateLogConfigurationFile(newConfiguration);
      endUserLogger.info(Translate.get("controller.update.log.success"));
    }
    catch (ControllerException ex)
    {
      endUserLogger.error(ex.getMessage());
      throw ex;
    }
    catch (IOException ioex)
    {
      endUserLogger.error(ioex.getMessage());
      throw ioex;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#viewLogConfigurationFile()
   */
  public String viewLogConfigurationFile() throws IOException
  {
    return controller.viewLogConfigurationFile();
  }

}
