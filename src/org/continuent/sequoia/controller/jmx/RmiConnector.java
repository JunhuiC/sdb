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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import javax.management.Notification;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import org.continuent.sequoia.common.authentication.PasswordAuthenticator;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxException;
import org.continuent.sequoia.common.jmx.notifications.JmxNotification;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.net.RMISSLClientSocketFactory;
import org.continuent.sequoia.common.net.RMISSLServerSocketFactory;
import org.continuent.sequoia.common.net.SSLConfiguration;
import org.continuent.sequoia.common.net.SocketFactoryFactory;

/**
 * This class defines a RmiConnector
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class RmiConnector
{
  static Trace               logger        = Trace
                                               .getLogger("org.continuent.sequoia.controller.jmx");

  private String             controllerName;
  private String             hostName;
  private int                registryPort;
  private int                serverPort;
  private JMXAuthenticator   authenticator;
  private SSLConfiguration   sslConfig;

  /**
   * we have to keep a reference to the server to avoid it from being garbage
   * collected otherwise the client will throw a java.rmi.NoSuchObjectException
   * (problem experienced with ssl connections)
   */
  private JMXConnectorServer connection;
  private Remote             rmiRegistry;

  private static List        rmiConnectors = new ArrayList();

  /**
   * Creates a new <code>RmiConnector.java</code> object
   * 
   * @param controllerName for reference when sending notification
   * @param hostName the name of the host we bind to, if null the default
   *          InetAddress.getLocalHost().getHostName() is used
   * @param port the port the rmi registry is listening on
   * @param authenticator the jmxauthenticator used for the connection
   * @param sslConfig ssl configuration
   * @throws JmxException the name of the localhost could not be determined
   */
  public RmiConnector(String controllerName, String hostName, int registryPort,
      int serverPort, JMXAuthenticator authenticator, SSLConfiguration sslConfig)
      throws JmxException
  {
    if (hostName != null)
    {
      this.hostName = hostName;
    }
    else
    {
      try
      {
        /** TODO: dssmith - determine applicability of getLocalHost() */
        this.hostName = InetAddress.getLocalHost().getHostName();
      }
      catch (UnknownHostException ex)
      {
        throw new JmxException(ex);
      }
    }
    this.controllerName = controllerName;
    this.registryPort = registryPort;
    this.serverPort = serverPort;
    this.authenticator = authenticator;
    this.sslConfig = sslConfig;

    addRmiConnector(this);
  }

  /**
   * Returns the authenticator value.
   * 
   * @return Returns the authenticator.
   */
  public JMXAuthenticator getAuthenticator()
  {
    return authenticator;
  }

  /**
   * Sets the authenticator value.
   * 
   * @param authenticator The authenticator to set.
   */
  public void setAuthenticator(JMXAuthenticator authenticator)
  {
    this.authenticator = authenticator;
  }

  /**
   * Returns the port value.
   * 
   * @return Returns the port.
   */
  public int getPort()
  {
    return registryPort;
  }

  /**
   * Sets the port value.
   * 
   * @param port The port to set.
   */
  public void setPort(int port)
  {
    this.registryPort = port;
  }

  /**
   * Returns the sslConfig value.
   * 
   * @return Returns the sslConfig.
   */
  public SSLConfiguration getSslConfig()
  {
    return sslConfig;
  }

  /**
   * Sets the sslConfig value.
   * 
   * @param sslConfig The sslConfig to set.
   */
  public void setSslConfig(SSLConfiguration sslConfig)
  {
    this.sslConfig = sslConfig;
  }

  /**
   * Returns the connection value.
   * 
   * @return Returns the connection.
   */
  public JMXConnectorServer getConnection()
  {
    return connection;
  }

  /**
   * start the rmi connector and the rmi naming service
   * 
   * @throws JmxException an exception
   */
  public void start() throws JmxException
  {
    createNamingService();
    createJRMPAdaptor();
  }

  /**
   * stop the rmi connector and the rmi registry
   * 
   * @throws JmxException an exception
   */
  public synchronized void stop() throws JmxException
  {
    try
    {
      if (connection != null)
        connection.stop();
      if (rmiRegistry != null)
        UnicastRemoteObject.unexportObject(rmiRegistry, true);
    }
    catch (Exception e)
    {
      throw new JmxException(e);
    }
    finally
    {
      connection = null;
      rmiRegistry = null;
    }
  }

  /**
   * Create naming service and starts rmi
   * 
   * @throws JmxException if creation fails
   */
  private void createNamingService() throws JmxException
  {
    try
    {
      // create and start the naming service
      logger.info(Translate.get("jmx.create.naming.service", new String[]{""
          + registryPort}));
      rmiRegistry = LocateRegistry.createRegistry(registryPort);
    }
    catch (Exception e)
    {
      throw new JmxException(e);
    }
  }

  private void createJRMPAdaptor() throws JmxException
  {
    try
    {
      // create the JRMP adaptator
      logger.info(Translate.get("jmx.create.jrmp.adaptor", "" + registryPort));

      // Set the jndi name with which it will be registered
      // JNDI properties
      logger.debug(Translate.get("jmx.prepare.jndi"));

      String rmiServer = "";
      if (0 != serverPort)
      {
        logger.info("Use RMI-ServerPort " + serverPort);
        rmiServer = "localhost:" + serverPort;
      }
      JMXServiceURL address = new JMXServiceURL("service:jmx:rmi://"
          + rmiServer + "/jndi/rmi://" + hostName + ":" + registryPort
          + "/jmxrmi");

      java.util.Map environment = new java.util.HashMap();

      if (authenticator == null)
      {
        authenticator = PasswordAuthenticator.NO_AUTHENICATION;
      }

      if (authenticator != null)
      {
        environment.put(JMXConnectorServer.AUTHENTICATOR, authenticator);
      }

      // ssl enabled ?
      if (sslConfig != null)
      {
        logger.info(Translate.get("jmx.create.jrmp.ssl.enabled"));

        RMISSLClientSocketFactory csf = new RMISSLClientSocketFactory();
        RMISSLServerSocketFactory ssf = new RMISSLServerSocketFactory(
            SocketFactoryFactory.createServerFactory(sslConfig));
        environment.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
            csf);
        environment.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
            ssf);
      }

      connection = javax.management.remote.JMXConnectorServerFactory
          .newJMXConnectorServer(address, environment, MBeanServerManager
              .getInstance());

      connection.start();
    }
    catch (Exception e)
    {
      throw new JmxException(e);
    }
  }

  /**
   * Returns a list of rmiConnectors .
   * 
   * @return Returns list of RmiConnector.
   */
  public static List getRmiConnectors()
  {
    return rmiConnectors;
  }

  /**
   * Adds an rmiConnector to the list.
   * 
   * @param pRmiConnector The rmiConnector to add.
   */
  private static synchronized void addRmiConnector(RmiConnector pRmiConnector)
  {
    rmiConnectors.add(pRmiConnector);
  }

  /**
   * @return Returns the controllerName.
   */
  public String getControllerName()
  {
    return controllerName;
  }

  /**
   * @return Returns the hostName.
   */
  public String getHostName()
  {
    return hostName;
  }

  private Date            myDate;
  private long            time;
  private JmxNotification sequoiaNotification;
  private Notification    notification;
  private static long     sequence = 0;

  /**
   * This method sends notification to all client registered to an instance of
   * the <code>RmiConnector</code> class. The <code>JmxNotification</code>
   * class is used here to create an object with all the information gathered in
   * parameters, and then is serialized in xml for interaction on the client
   * side.
   * 
   * @see JmxNotification
   * @param mbean the mbean that is generating the notification
   * @param type the type as seen in <code>SequoiaNotificationList</code>
   * @param priority notification level as seen in
   *          <code>SequoiaNotificationList</code>
   * @param description a string description of the notification
   * @param data a hashtable of data that can be used to give more information
   *          on the notification
   */
  public synchronized void sendNotification(AbstractStandardMBean mbean,
      String type, String priority, String description, Hashtable data)
  {

    myDate = new Date();
    time = myDate.getTime();

    sequoiaNotification = new JmxNotification(priority, "" + sequence, type,
        description, "" + time, controllerName, mbean.getClass().getName(),
        "mbeanName", hostName, "" + registryPort, data);
    notification = new Notification(type, mbean, sequence, myDate.getTime(),
        description);
    notification.setUserData(sequoiaNotification.toString());
    mbean.sendNotification(notification);
  }

  /**
   * Broadcast a jmx notification to any client connected to any RmiConnector
   * registered in the static list. The method is static because it is sending
   * notifications to all rmi connectors.
   * 
   * @param mbean the mbean that is generating the notification
   * @param type the type as seen in <code>SequoiaNotificationList</code>
   * @param priority notification level as seen in
   *          <code>SequoiaNotificationList</code>
   * @param description a string description of the notification
   * @param data a hashtable of data that can be used to give more information
   *          on the notification
   */
  public static void broadcastNotification(AbstractStandardMBean mbean,
      String type, String priority, String description, Hashtable data)
  {
    sequence++;
    logger.info("Sending notification:" + description + "(Message No:"
        + sequence + ")");
    Iterator iter = rmiConnectors.iterator();
    RmiConnector rmi;
    while (iter.hasNext())
    {
      rmi = ((RmiConnector) iter.next());
      rmi.sendNotification(mbean, type, priority, description, data);
    }
  }
}