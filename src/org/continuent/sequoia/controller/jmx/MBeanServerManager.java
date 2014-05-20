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
 * Contributor(s): Peter Royal
 */

package org.continuent.sequoia.controller.jmx;

import java.util.Iterator;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxException;
import org.continuent.sequoia.common.log.Trace;

/**
 * The MBeanServerManager (Singleton) creates a single MBeanServer in an JVM.
 * The server can be accessed with the getInstance() method.
 * <p>
 * The server is created with
 * org.continuent.sequoia.controller.jmx.MBeanServerBuilder
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class MBeanServerManager
{

  static Trace               logger       = Trace
                                              .getLogger("org.continuent.sequoia.controller.jmx.MBeanServer");

  private static MBeanServer mbs;
  private static boolean     isJmxEnabled = true;

  /**
   * creating a MBeanServer, if it does not exist, otherwise a reference to the
   * MBeanServer is returned
   * 
   * @return the mbeanserver instance, null if jmx is disabled
   */
  public static synchronized MBeanServer getInstance()
  {

    if (!isJmxEnabled)
    {
      return null;
    }

    if (mbs != null)
    {
      return mbs;
    }

    String defaultServerBuilder = System
        .getProperty("javax.management.builder.initial");

    if (!MBeanServerBuilder.class.getName().equals(defaultServerBuilder))
    {
      if (defaultServerBuilder != null)
        logger.error("property javax.management.builder.initial was "
            + defaultServerBuilder);

      logger.debug("setting property javax.management.builder.initial");
      System.setProperty("javax.management.builder.initial",
          org.continuent.sequoia.controller.jmx.MBeanServerBuilder.class
              .getName());

    }

    // try to use the MBeanServer provided by Java 5
    createJava5PlatformMBeanServer();
    // otherwise rely on the classpath to create the MBeanServer
    if (null == mbs)
    {
      mbs = MBeanServerFactory.createMBeanServer();
      logger.debug(Translate.get("jmx.server.from.classpath")); //$NON-NLS-1$
    }

    return mbs;
  }

  /**
   * Retrieve the MBeanServer provided by Java 5. This is done by reflection to
   * avoid creating hard-coded dependencies to Java 5-only classes.
   */
  private static void createJava5PlatformMBeanServer()
  {
    Class<?> clazz;
    try
    {
      clazz = Class.forName("java.lang.management.ManagementFactory"); //$NON-NLS-1$
    }
    catch (ClassNotFoundException e)
    {
      // Java 5-only class is not available: do nothing
      return;
    }
    try
    {
      mbs = (MBeanServer) clazz
          .getMethod("getPlatformMBeanServer", ( Class<?>[] )null).invoke(( Object[] )null, ( Object[] )null); //$NON-NLS-1$
      logger.debug(Translate.get("jmx.server.from.java5")); //$NON-NLS-1$
    }
    catch (Exception e)
    {
      logger.debug(Translate.get("jmx.server.not.java5"), e); //$NON-NLS-1$
    }
  }

  /**
   * Returns the isJmxEnabled value.
   * 
   * @return Returns the isJmxEnabled.
   */
  public static boolean isJmxEnabled()
  {
    return isJmxEnabled;
  }

  /**
   * enable or disable jmx
   * 
   * @param isJmxEnabled The isJmxEnabled to set.
   * @throws JmxException an exception
   */
  public static void setJmxEnabled(boolean isJmxEnabled) throws JmxException
  {
    if (MBeanServerManager.isJmxEnabled != isJmxEnabled && !isJmxEnabled && mbs!=null)
    {
      // stop rmi connectors
      List<?> list = RmiConnector.getRmiConnectors();
      for (Iterator<?> it = list.iterator(); it.hasNext();)
      {
        RmiConnector rmi = (RmiConnector) it.next();
        rmi.stop();
      }

      // stop http adaptors
      list = HttpAdaptor.getHttpAdaptors();
      for (Iterator<?> it = list.iterator(); it.hasNext();)
      {
        HttpAdaptor http = (HttpAdaptor) it.next();
        http.stop();
      }
      // Stop mbean server
      MBeanServerFactory.releaseMBeanServer(mbs);
      mbs = null;
    }
    // set jmx enabled to its value
    MBeanServerManager.isJmxEnabled = isJmxEnabled;
  }

  /**
   * Registers an MBean with the MBean server if jmx is enabled, otherwise it
   * returns null.
   * <p>
   * This method is equivalend to
   * 
   * <pre>
   * MBeanServer server = MBeanServerManager.getInstance();
   * if (server != null)
   * {
   *   server.registerMBean(object, name);
   * }
   * </pre>
   * 
   * @param object The MBean to be registered as an MBean.
   * @param name The object name of the MBean. May be null.
   * @return An ObjectInstance, containing the ObjectName and the Java class
   *         name of the newly registered MBean. If the contained ObjectName is
   *         n, the contained Java class name is getMBeanInfo(n).getClassName().
   *         Or null if jmx is disabled
   * @throws JmxException the object could not be registered
   */
  public static ObjectInstance registerMBean(Object object, ObjectName name)
      throws JmxException
  {
    MBeanServer server = getInstance();
    try
    {

      if (server != null)
      {
        logger.debug(Translate.get("jmx.register.mbean", new String[]{
            object.getClass().toString(), name.getCanonicalName()}));

        ObjectInstance objInstance = null;
        if (!server.isRegistered(name))
        {
          objInstance = server.registerMBean(object, name);
        }
        else
        {
          logger.error(Translate.get("jmx.register.mbean.already.exist",
              new String[]{name.getCanonicalName()}));
          try
          {
            server.unregisterMBean(name);
          }
          catch (Exception e)
          {
            logger.error(Translate.get("jmx.delete.mbean.failed", new String[]{
                name.toString(), e.getMessage()}));
          }
          objInstance = server.registerMBean(object, name);
        }

        logger.debug(Translate.get("jmx.server.mbean.count", ""
            + server.getMBeanCount()));
        return objInstance;
      }
      return null;
    }
    catch (Exception e)
    {
      logger.error(Translate.get("jmx.register.mbean.failed",
          new String[]{object.getClass().toString(), e.getMessage(),
              e.getClass().toString()}));
      e.printStackTrace();
      e.getCause().printStackTrace();
      throw new JmxException(e);
    }
  }

  /**
   * unregister an mbean.
   * 
   * @param name the name of the bean to unregister
   * @throws JmxException problems
   */
  public static void unregister(ObjectName name) throws JmxException
  {
    MBeanServer server = getInstance();
    if (server != null)
    {
      try
      {
        // unregister the MBean
        server.unregisterMBean(name);
        logger.debug(Translate.get("jmx.server.mbean.count", ""
            + server.getMBeanCount()));

      }
      catch (Exception e)
      {
        logger.error(Translate.get("jmx.register.mbean.failed", new String[]{
            name.getCanonicalName(), e.getMessage()}));
        throw new JmxException(e);
      }
    }
  }

}