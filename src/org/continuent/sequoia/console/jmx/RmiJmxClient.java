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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.jmx;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.Attribute;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.monitor.StringMonitor;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.Context;
import javax.security.auth.Subject;

import org.continuent.sequoia.common.authentication.PasswordAuthenticator;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean;
import org.continuent.sequoia.common.jmx.mbeans.BackendTaskQueuesControlMBean;
import org.continuent.sequoia.common.jmx.mbeans.ControllerMBean;
import org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean;
import org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean;
import org.continuent.sequoia.common.jmx.mbeans.ParsingCacheMBean;
import org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean;
import org.continuent.sequoia.common.jmx.mbeans.RequestManagerMBean;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.users.AdminUser;

/**
 * This class defines a RmiJmxClient that uses Jmx 2.0 specifications to connect
 * to the RmiSever
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class RmiJmxClient
{
  private JMXConnector         connector;
  private Object               credentials;
  private String               remoteHostAddress;
  private String               remoteHostPort;

  private NotificationListener notificationListener;

  // List of last used MBeans
  private ControllerMBean      controllerMBean;
  private DatabaseBackendMBean backendMBean;
  private DataCollectorMBean   dataMBean;

  private Trace                logger = Trace
                                          .getLogger("org.continuent.sequoia.console.jmx");

  /**
   * Returns the notificationListener value.
   * 
   * @return Returns the notificationListener.
   */
  public NotificationListener getNotificationListener()
  {
    return notificationListener;
  }

  /**
   * Sets the notificationListener value.
   * 
   * @param notificationListener The notificationListener to set.
   */
  public void setNotificationListener(NotificationListener notificationListener)
  {
    this.notificationListener = notificationListener;
  }

  /**
   * Returns the credentials value.
   * 
   * @return Returns the credentials.
   */
  public Object getCredentials()
  {
    return credentials;
  }

  /**
   * Creates a new <code>RmiJmxClient.java</code> object
   * 
   * @param port the port of the host to connect to
   * @param host the host name to connect to
   * @param jmxUser the jmxUser if one, to be authenticated with
   * @param jmxPassword the jmxPassword if one, to be authenticated with
   * @throws IOException if cannot connect
   */
  public RmiJmxClient(String port, String host, String jmxUser,
      String jmxPassword) throws IOException
  {
    this(port, host, PasswordAuthenticator.createCredentials(jmxUser,
        jmxPassword));
  }

  /**
   * Creates a new <code>RmiJmxClient</code> object
   * 
   * @param url the jmx connector url
   * @param credentials to use for the connection
   * @throws IOException if connect fails
   */
  public RmiJmxClient(String url, Object credentials) throws IOException
  {
    int index = url.indexOf(":");
    String ip = url.substring(0, index);
    String port = url.substring(index + 1);
    connect(port, ip, credentials);
  }

  /**
   * Creates a new <code>RmiJmxClient.java</code> object
   * 
   * @param port the port of the host to connect to
   * @param host the host name to connect to
   * @param credentials to use for the connection
   * @throws IOException if connect fails
   */
  public RmiJmxClient(String port, String host, Object credentials)
      throws IOException
  {
    connect(port, host, credentials);
  }

  /**
   * Connect to the MBean server
   * 
   * @param port the port of the host to connect to
   * @param host the host name to connect to
   * @param credentials to use for the connection
   * @throws IOException if connect fails
   */
  public void connect(String port, String host, Object credentials)
      throws IOException
  {

    JMXServiceURL address = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");

    Map environment = new HashMap();

    // use username and password for authentication of connections
    // with the controller, the values are compared to the ones
    // specified in the controller.xml config file.
    if (credentials != null)
    {
      // this line is not required if no username/password has been configered
      environment.put(JMXConnector.CREDENTIALS, credentials);
    }

    this.credentials = credentials;

    connector = JMXConnectorFactory.connect(address, environment);
    remoteHostAddress = host;
    remoteHostPort = port;
    invalidateMBeans();
  }

  /**
   * Invalidate all MBeans. When connecting to a new Controller, all the local
   * MBean instances must be invalidated (since they refered to the previous
   * Controller and its associated MBean server).
   */
  private void invalidateMBeans()
  {
    controllerMBean = null;
    dataMBean = null;
    backendMBean = null;
  }

  /**
   * List of all the mbean on the current server
   * 
   * @return a set of <tt>ObjectInstance</tt>
   * @throws Exception if fails
   */
  public Set listSequoiaMBeans() throws Exception
  {
    Set set = connector.getMBeanServerConnection().queryMBeans(
        new ObjectName("sequoia:*"), null);
    return set;
  }

  /**
   * Get the mbean information
   * 
   * @param mbean the <tt>ObjectName</tt> of the mbean to access
   * @return <tt>MBeanInfo</tt> object
   * @throws Exception if fails
   */
  public MBeanInfo getMBeanInfo(ObjectName mbean) throws Exception
  {
    return connector.getMBeanServerConnection().getMBeanInfo(mbean);
  }

  /**
   * Get the value of an attribute on the given mbean
   * 
   * @param mbean the <tt>ObjectName</tt> of the mbean to access
   * @param attribute the attribute name
   * @return <tt>Object</tt> being the value returned by the get <Attribute>
   *         method
   * @throws Exception if fails
   */
  public Object getAttributeValue(ObjectName mbean, String attribute)
      throws Exception
  {
    return connector.getMBeanServerConnection().getAttribute(mbean, attribute);
  }

  /**
   * Change an attribute value
   * 
   * @param mbean the <tt>ObjectName</tt> of the mbean to access
   * @param attribute the attribute name
   * @param value the attribute new value
   * @throws Exception if fails
   */
  public void setAttributeValue(ObjectName mbean, String attribute, Object value)
      throws Exception
  {
    Attribute att = new Attribute(attribute, value);
    connector.getMBeanServerConnection().setAttribute(mbean, att);
  }

  /**
   * Set the current subject for authentication
   * 
   * @param user the user login
   * @param password the user password
   */
  private Subject getSubject(String user, String password)
  {
    // we build a subject for authentication
    AdminUser dbUser = new AdminUser(user, password);
    Set principals = new HashSet();
    principals.add(dbUser);
    return new Subject(true, principals, new HashSet(), new HashSet());
  }

  /**
   * Get a reference to the virtualdatabaseMbean with the given authentication
   * 
   * @param database the virtual database name
   * @param user the user recognized as the <code>VirtualDatabaseUser</code>
   * @param password the password for the <code>VirtualDatabaseUser</code>
   * @return <code>VirtualDatabaseMBean</code> instance
   * @throws IOException if cannot connect to MBean
   * @throws InstanceNotFoundException if cannot locate MBean
   * @throws VirtualDatabaseException if virtual database fails
   */
  public VirtualDatabaseMBean getVirtualDatabaseProxy(String database,
      String user, String password) throws InstanceNotFoundException,
      IOException, VirtualDatabaseException
  {
    if (!isValidConnection())
    {
      try
      {
        reconnect();
      }
      catch (Exception e)
      {
        throw new IOException(ConsoleTranslate
            .get("jmx.server.connection.lost")); //$NON-NLS-1$
      }
    }
    ObjectName db;
    try
    {
      db = JmxConstants.getVirtualDataBaseObjectName(database);
    }
    catch (MalformedObjectNameException e)
    {
      throw new VirtualDatabaseException(e);
    }
    // we open a connection for this subject, all subsequent calls with this
    // connection will be executed on the behalf of our subject.
    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(getSubject(user, password));

    if (!delegateConnection.isRegistered(db))
    {
      throw new VirtualDatabaseException(ConsoleTranslate
          .get("virtualdatabase.mbean.not.accessible")); //$NON-NLS-1$
    }

    // we create a proxy to the virtual database
    VirtualDatabaseMBean local = (VirtualDatabaseMBean) MBeanServerInvocationHandler
        .newProxyInstance(delegateConnection, db, VirtualDatabaseMBean.class,
            false);
    checkAccessible(local);
    // Check authentication
    boolean authenticated = false;
    try
    {
      authenticated = local.checkAdminAuthentication(user, password);
    }
    catch (Exception e)
    {
      logger.warn(
          "Exception while checking virtual database admin authentication", e);
      throw new VirtualDatabaseException(
          "Could not check authentication. MBean is not accessible.");
    }
    if (!authenticated)
      throw new VirtualDatabaseException("Authentication Failed");

    // Add notification listener
    if (notificationListener != null)
    {
      delegateConnection.addNotificationListener(db, notificationListener,
          null, null);

      // CounterMonitor cm = new CounterMonitor();
      // cm.setNotify(true);
      // cm.setGranularityPeriod(100);
      // cm.setObservedObject(db);
      // cm.setObservedAttribute("currentNbOfThreads");
      // cm.setThreshold(new Integer(6));
      // cm.start();
    }

    return local;
  }

  /**
   * Check that the VirtualDatabaseMBean is accessible. if
   * <code>virtualDbMBean</code> is <code>null</code>, do nothing
   * 
   * @param virtualDbMBean the VirtualDatabaseMBean to check
   * @throws VirtualDatabaseException if the VirtualDatabaseMBean is not
   *           accessible
   */
  private void checkAccessible(VirtualDatabaseMBean virtualDbMBean)
      throws VirtualDatabaseException
  {
    if (virtualDbMBean == null)
    {
      return;
    }
    // we try to get the name of the virtual database from the mbean to
    // check if it is accessible
    try
    {
      virtualDbMBean.getVirtualDatabaseName();
    }
    catch (Exception e)
    {
      throw new VirtualDatabaseException(ConsoleTranslate
          .get("virtualdatabase.mbean.not.accessible")); //$NON-NLS-1$
    }
  }

  /**
   * Get a proxy to the ControllerMBean
   * 
   * @return <code>ControllerMBean</code> instance
   * @throws IOException if cannot connect to MBean
   */
  public ControllerMBean getControllerProxy() throws IOException
  {

    if (controllerMBean != null && isValidConnection())
    {
      return controllerMBean;
    }
    else
    {
      if (!isValidConnection())
      {
        try
        {
          reconnect();
        }
        catch (Exception e)
        {
          throw new IOException(ConsoleTranslate
              .get("jmx.server.connection.lost")); //$NON-NLS-1$
        }
      }
      ObjectName db;
      try
      {
        db = JmxConstants.getControllerObjectName();
      }
      catch (MalformedObjectNameException e)
      {
        throw new IOException(e.getMessage());
      }

      // we create a new proxy to the controller
      controllerMBean = (ControllerMBean) MBeanServerInvocationHandler
          .newProxyInstance(connector.getMBeanServerConnection(), db,
              ControllerMBean.class, false);

      // Add notification listener
      if (notificationListener != null)
      {
        try
        {
          connector.getMBeanServerConnection().addNotificationListener(db,
              notificationListener, null, null);
        }
        catch (Exception e)
        {
          throw new IOException("Could not register listener on the mbean");
        }
      }

      return controllerMBean;
    }
  }

  /**
   * Get a proxy to the DataCollectorMBean
   * 
   * @return <code>DataCollectorMBean</code> instance
   * @throws IOException if fails
   */
  public DataCollectorMBean getDataCollectorProxy() throws IOException
  {

    if (dataMBean != null && isValidConnection())
    {
      return dataMBean;
    }
    else
    {
      if (!isValidConnection())
        reconnect();
      ObjectName db = JmxConstants.getDataCollectorObjectName();

      // we create a new proxy to the data collector
      dataMBean = (DataCollectorMBean) MBeanServerInvocationHandler
          .newProxyInstance(connector.getMBeanServerConnection(), db,
              DataCollectorMBean.class, false);
      return dataMBean;
    }
  }

  /**
   * Get a proxy to the DatabaseBackendMBean
   * 
   * @return <code>DatabaseBackendMBean</code> instance
   * @param vdb virtual database name
   * @param backend backend name
   * @param user user name
   * @param password password name
   * @throws IOException if cannot connect to MBean
   * @throws InstanceNotFoundException if cannot locate MBean
   */
  public DatabaseBackendMBean getDatabaseBackendProxy(String vdb,
      String backend, String user, String password)
      throws InstanceNotFoundException, IOException
  {
    if (backendMBean != null && isValidConnection())
    {
      try
      {
        if (backendMBean.getName().equals(backend))
          return backendMBean;
      }
      catch (Exception e)
      {
        // backend is no more there
      }
    }

    if (!isValidConnection())
      reconnect();

    ObjectName backendObjectName;
    try
    {
      backendObjectName = JmxConstants.getDatabaseBackendObjectName(vdb,
          backend);
    }
    catch (MalformedObjectNameException e)
    {
      throw new IOException(e.getMessage());
    }

    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(getSubject(user, password));

    if (notificationListener != null)
    {
      delegateConnection.addNotificationListener(backendObjectName,
          notificationListener, null, null);
      StringMonitor sm = new StringMonitor();
      sm.setObservedObject(backendObjectName);
      sm.setObservedAttribute("LastKnownCheckpoint");
      sm.setStringToCompare("hello");
      sm.setGranularityPeriod(100);
      sm.setNotifyDiffer(true);
      sm.addNotificationListener(notificationListener, null, null);
      sm.start();
    }

    // we create a proxy to the database backend
    backendMBean = (DatabaseBackendMBean) MBeanServerInvocationHandler
        .newProxyInstance(delegateConnection, backendObjectName,
            DatabaseBackendMBean.class, false);
    return backendMBean;
  }

  /**
   * Returns a proxy on a BackendTaskQueuesControlMBean.
   * 
   * @param vdb name of the virtual database
   * @param backend name of the backend
   * @param user user login for the virtual database
   * @param password user password fort the virtual database
   * @return a proxy on a BackendTaskQueuesControlMBean
   * @throws IOException if an I/O exception occured
   */
  public BackendTaskQueuesControlMBean getBackendTaskQueues(String vdb,
      String backend, String user, String password) throws IOException
  {
    if (!isValidConnection())
      reconnect();

    ObjectName taskQueuesObjectName;
    try
    {
      taskQueuesObjectName = JmxConstants.getBackendTaskQueuesObjectName(vdb,
          backend);
    }
    catch (MalformedObjectNameException e)
    {
      throw new IOException(e.getMessage());
    }

    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(getSubject(user, password));

    // we create a proxy to the database backend
    return (BackendTaskQueuesControlMBean) MBeanServerInvocationHandler
        .newProxyInstance(delegateConnection, taskQueuesObjectName,
            BackendTaskQueuesControlMBean.class, false);
  }

  /**
   * Returns a proxy on a RecoveryLogControlMBean.
   * 
   * @param vdb name of the virtual databaseTODO: getRecoveryLog definition.
   * @param user user login for the virtual database
   * @param password user password fort the virtual database
   * @return a proxy on a RecoveryLogControlMBean
   * @throws IOException if an I/O exception occured
   */
  public RecoveryLogControlMBean getRecoveryLog(String vdb, String user,
      String password) throws IOException
  {
    if (!isValidConnection())
      reconnect();

    ObjectName recoveryLogObjectName;
    try
    {
      recoveryLogObjectName = JmxConstants.getRecoveryLogObjectName(vdb);
    }
    catch (MalformedObjectNameException e)
    {
      throw new IOException(e.getMessage());
    }

    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(getSubject(user, password));

    // we create a proxy to the database backend
    return (RecoveryLogControlMBean) MBeanServerInvocationHandler
        .newProxyInstance(delegateConnection, recoveryLogObjectName,
            RecoveryLogControlMBean.class, false);
  }

  /**
   * Returns a proxy on the given vdb Scheduler.
   * 
   * @param vdb name of the virtual database
   * @param user user login for the virtual database
   * @param password user password fort the virtual database
   * @return a proxy on an AbstractScheduler
   * @throws IOException if an I/O exception occured
   */
  public AbstractSchedulerControlMBean getAbstractScheduler(String vdb,
      String user, String password) throws IOException
  {
    if (!isValidConnection())
      reconnect();

    ObjectName recoveryLogObjectName;
    try
    {
      recoveryLogObjectName = JmxConstants.getAbstractSchedulerObjectName(vdb);
    }
    catch (MalformedObjectNameException e)
    {
      throw new IOException(e.getMessage());
    }

    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(getSubject(user, password));

    // we create a proxy to the database backend
    return (AbstractSchedulerControlMBean) MBeanServerInvocationHandler
        .newProxyInstance(delegateConnection, recoveryLogObjectName,
            AbstractSchedulerControlMBean.class, false);
  }

  /**
   * Returns a proxy on the given vdb Scheduler parsing cache
   * 
   * @param vdb name of the virtual database
   * @param user user login for the virtual database
   * @param password user password fort the virtual database
   * @return a proxy on a ParsingCache
   * @throws IOException if an I/O exception occured
   */
  public ParsingCacheMBean getParsingCache(String vdb, String user,
      String password) throws IOException
  {
    if (!isValidConnection())
      reconnect();

    ObjectName parsingCacheObjectName;
    try
    {
      parsingCacheObjectName = JmxConstants.getParsingCacheObjectName(vdb);
    }
    catch (MalformedObjectNameException e)
    {
      throw new IOException(e.getMessage());
    }

    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(getSubject(user, password));

    // we create a proxy to the database backend
    return (ParsingCacheMBean) MBeanServerInvocationHandler.newProxyInstance(
        delegateConnection, parsingCacheObjectName, ParsingCacheMBean.class,
        false);
  }

  /**
   * Returns a proxy on the RequestManager of the given virtual database.
   * 
   * @param vdb name of the virtual database
   * @param user user login for the virtual database
   * @param password user password fort the virtual database
   * @return a proxy on a RequestManager
   * @throws IOException if an I/O exception occured
   */
  public RequestManagerMBean getRequestManager(String vdb, String user,
      String password) throws IOException
  {
    if (!isValidConnection())
      reconnect();

    ObjectName requestManagerObjectName;
    try
    {
      requestManagerObjectName = JmxConstants.getRequestManagerObjectName(vdb);
    }
    catch (MalformedObjectNameException e)
    {
      throw new IOException(e.getMessage());
    }

    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(getSubject(user, password));

    // we create a proxy to the database backend
    return (RequestManagerMBean) MBeanServerInvocationHandler.newProxyInstance(
        delegateConnection, requestManagerObjectName,
        RequestManagerMBean.class, false);
  }

  /**
   * Get the controller name used for jmx connection This is
   * [hostname]:[jmxServerPort]
   * 
   * @return <code>remoteHostName+":"+remoteHostPort</code>
   */
  public String getRemoteName()
  {
    return remoteHostAddress + ":" + remoteHostPort;
  }

  /**
   * Returns the remoteHostAddress value.
   * 
   * @return Returns the remoteHostAddress.
   */
  public String getRemoteHostAddress()
  {
    return remoteHostAddress;
  }

  /**
   * Returns the remoteHostPort value.
   * 
   * @return Returns the remoteHostPort.
   */
  public String getRemoteHostPort()
  {
    return remoteHostPort;
  }

  /**
   * Reconnect to the same mbean server
   * 
   * @throws IOException if reconnection failed
   */
  public void reconnect() throws IOException
  {
    connect(remoteHostPort, remoteHostAddress, credentials);
  }

  /**
   * Test if the connection with the mbean server is still valid
   * 
   * @return true if it is
   */
  public boolean isValidConnection()
  {
    try
    {
      connector.getMBeanServerConnection().getMBeanCount();
      return true;
    }
    catch (Exception e)
    {
      controllerMBean = null;
      backendMBean = null;
      dataMBean = null;
      return false;
    }
  }
}