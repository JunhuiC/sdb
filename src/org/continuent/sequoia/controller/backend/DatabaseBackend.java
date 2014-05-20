/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Mathieu Peltier, Sara Bouchenak, Jean-Bernard van Zuylen, Guillaume Smet.
 */

package org.continuent.sequoia.controller.backend;

import java.io.StringReader;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.Vector;

import javax.management.AttributeChangeNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.continuent.sequoia.common.exceptions.NoTransactionStartWhenDisablingException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.jmx.management.BackendState;
import org.continuent.sequoia.common.jmx.monitoring.backend.BackendStatistics;
import org.continuent.sequoia.common.jmx.notifications.SequoiaNotificationList;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.metadata.MetadataContainer;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedure;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.backend.rewriting.AbstractRewritingRule;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.DriverManager;
import org.continuent.sequoia.controller.connection.FailFastPoolConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.connection.RandomWaitPoolConnectionManager;
import org.continuent.sequoia.controller.connection.SimpleConnectionManager;
import org.continuent.sequoia.controller.connection.VariablePoolConnectionManager;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.interceptors.impl.InterceptorManagerAdapter;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueues;
import org.continuent.sequoia.controller.loadbalancer.BackendWorkerThread;
import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;
import org.continuent.sequoia.controller.loadbalancer.tasks.KillThreadTask;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.AlterRequest;
import org.continuent.sequoia.controller.requests.CreateRequest;
import org.continuent.sequoia.controller.requests.DropRequest;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * A <code>DatabaseBackend</code> represents a real database backend that will
 * have to be bound to a virtual Sequoia database. All connections opened will
 * use the same url but possibly different login/password.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Sara.Bouchenak@epfl.ch">Sara Bouchenak </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:guillaume.smet@gmail.com">Guillaume Smet</a>
 * @version 1.0
 */
public final class DatabaseBackend implements XmlComponent
{
  //
  // How the code is organized?
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Connection management
  // 4. Transaction management
  // 5. State management
  // 6. Schema management
  // 7. Monitoring
  // 8. Getter/Setter (possibly in alphabetical order)
  // 9. Worker threads management
  //

  /** Logical name assigned to this backend. */
  private String                         name;

  /** Path for driver */
  private String                         driverPath;

  /** Database native JDBC driver class name. */
  private String                         driverClassName;

  /** Driver compliance to Sequoia requirements */
  private transient DriverCompliance     driverCompliance;

  /** Real URL to access the database (JDBC URL). */
  private String                         url;

  /** Name of the virtual database this backend is attached to */
  private String                         virtualDatabaseName;

  /** A boolean to know if we should allow this backend to be enabled for write */
  private boolean                        writeCanBeEnabled;

  /** SQL statement used to check if a connection is still valid */
  private String                         connectionTestStatement;

  /**
   * The schema of the database. This should be accessed in a synchronized(this)
   * block since it can be updated dynamically.
   */
  private transient DatabaseSchema       schema;

  /** <code>true</code> if schema is static. */
  private boolean                        schemaIsStatic           = false;

  /**
   * <code>true</code> if the backend must maintain its schema dynamically for
   * the virtual database needs
   */
  private boolean                        schemaIsNeededByVdb      = true;

  /** <code>true</code> if schema is no more up-to-date and needs a refresh */
  private boolean                        schemaIsDirty            = true;

  /** Connection managers for this backend. */
  private transient Map<String, AbstractConnectionManager>                  connectionManagers;

  /** List of persistent connections active for this backend (<persistentConnectionId,PooledConnection>) */
  private Map<Long, PooledConnection>                            persistentConnections;

  /** Logger instance. */
  protected transient Trace              logger;

  /**
   * ArrayList&lt;Long&gt; of active transactions on this backend (as opposed to
   * the transactions started on the virtual database managing this backend).
   */
  private transient ArrayList<Long>            activeTransactions       = new ArrayList<Long>();

  /** List of savepoints for each transaction */
  private transient Map<Long, List<Savepoint>>                  savepoints               = new HashMap<Long, List<Savepoint>>();

  /** List of pending requests. */
  private transient Vector<AbstractRequest>               pendingRequests          = new Vector<AbstractRequest>();

  /** List of pending tasks. */
  private transient Vector<AbstractTask>               pendingTasks             = new Vector<AbstractTask>();

  /** Task queues to handle writes in RAIDb-1 or RAIDb-2 */
  private transient BackendTaskQueues    taskQueues               = null;
  /** List of BackendWorkerThread to execute write queries in RAIDb-1 or RAIDb-2 */
  private int                            nbOfWorkerThreads        = 5;
  private ArrayList<BackendWorkerThread>                      workerThreads            = null;
  private final Object                   workerThreadSync         = new Object();

  /** Monitoring Values */
  private int                            totalRequest;
  private int                            totalWriteRequest;
  private int                            totalReadRequest;
  private int                            totalTransactions;

  /** List of <code>AbstractRewritingRule</code> objects. */
  private ArrayList<AbstractRewritingRule>                      rewritingRules;

  /** For metadata information generation */
  private int                            dynamicPrecision;
  private boolean                        gatherSystemTables       = false;
  private DatabaseProcedureSemantic      defaultStoredProcedureSemantic;
  private HashMap<String, DatabaseProcedureSemantic>                        storedProcedureSemantics = new HashMap<String, DatabaseProcedureSemantic>();

  private String                         schemaName               = null;

  /** Short form of SQL statements to include in traces and exceptions */
  private int                            sqlShortFormLength       = 40;

  private String                         lastKnownCheckpoint;

  /**
   * The current state of the backend
   * 
   * @see org.continuent.sequoia.common.jmx.management.BackendState
   */
  private int                            state                    = BackendState.DISABLED;

  private transient BackendStateListener stateListener;

  private NotificationBroadcasterSupport notificationBroadcaster;

  private int                            notificationSequence     = 0;

  private AbstractConnectionManager      defaultConnectionManager = null;

  /* end user logger */
  static Trace                           endUserLogger            = Trace
                                                                      .getLogger("org.continuent.sequoia.enduser");

  /**
   * Creates a new <code>DatabaseBackend</code> instance.
   * 
   * @param name logical name assigned to this backend
   * @param driverPath path for driver
   * @param driverClassName class name of the database native JDBC driver to
   *          load
   * @param url URL to access the database
   * @param vdbName Name of the virtual database this backend is attached to
   * @param writeCanBeEnabled if writes can be enabled on this backend
   * @param connectionTestStatement SQL statement used to check if a connection
   *          is still valid
   * @param nbOfWorkerThreads defines the number of BackendWorkerThread that
   *          should process writes in parallel (minimum is 2)
   */
  public DatabaseBackend(String name, String driverPath,
      String driverClassName, String url, String vdbName,
      boolean writeCanBeEnabled, String connectionTestStatement,
      int nbOfWorkerThreads)
  {
    if (name == null)
      throw new IllegalArgumentException(Translate
          .get("backend.null.backend.name"));

    if (driverClassName == null)
      throw new IllegalArgumentException(Translate.get("backend.null.driver"));

    if (url == null)
      throw new IllegalArgumentException(Translate.get("backend.null.url"));

    if (vdbName == null)
      throw new IllegalArgumentException(Translate
          .get("backend.null.virtualdatabase.name"));

    if (connectionTestStatement == null)
      throw new IllegalArgumentException(Translate
          .get("backend.null.connection.test"));

    logger = Trace.getLogger(DatabaseBackend.class.getName() + "." + vdbName
        + "." + name);

    if (nbOfWorkerThreads < 2)
    {
      nbOfWorkerThreads = 2;
      logger.warn("Invalid number of worker threads (" + nbOfWorkerThreads
          + "), re-adjusting to the minimum (2).");
    }

    this.name = name;
    this.writeCanBeEnabled = writeCanBeEnabled;
    this.driverPath = driverPath;
    this.driverClassName = driverClassName;
    this.url = url;
    this.virtualDatabaseName = vdbName;
    this.connectionTestStatement = connectionTestStatement;
    this.nbOfWorkerThreads = nbOfWorkerThreads;
    this.connectionManagers = new HashMap<String, AbstractConnectionManager>();
    this.persistentConnections = new HashMap<Long, PooledConnection>();
    this.driverCompliance = ControllerConstants.CONTROLLER_FACTORY
        .getDriverComplianceFactory().createDriverCompliance(logger);
    totalRequest = 0;
    dynamicPrecision = DatabaseBackendSchemaConstants.DynamicPrecisionAll;
  }

  /**
   * Creates a new <code>DatabaseBackend</code> object
   * 
   * @param info a backend info object to create a database backend object from
   */
  public DatabaseBackend(BackendInfo info)
  {
    this(info.getName(), info.getDriverPath(), info.getDriverClassName(), info
        .getUrl(), info.getVirtualDatabaseName(), true, info
        .getConnectionTestStatement(), info.getNbOfWorkerThreads());
    setDynamicPrecision(info.getDynamicPrecision(),
        info.isGatherSystemTables(), info.getSchemaName());
    try
    {
      String xml = info.getXml();
      StringReader sreader = new StringReader(xml);
      SAXReader reader = new SAXReader();
      Document document = reader.read(sreader);
      Element root = document.getRootElement();
      Iterator<?> iter1 = root.elementIterator();
      while (iter1.hasNext())
      {
        Element elem = (Element) iter1.next();
        if (elem.getName().equals(DatabasesXmlTags.ELT_ConnectionManager))
        {
          String vuser = elem.valueOf("@" + DatabasesXmlTags.ATT_vLogin);
          String rlogin = elem.valueOf("@" + DatabasesXmlTags.ATT_rLogin);
          String rpassword = elem.valueOf("@" + DatabasesXmlTags.ATT_rPassword);
          Iterator<?> iter2 = elem.elementIterator();
          while (iter2.hasNext())
          {
            Element connectionManager = (Element) iter2.next();
            String cname = connectionManager.getName();
            if (cname
                .equals(DatabasesXmlTags.ELT_VariablePoolConnectionManager))
            {
              int minPoolSize = Integer.parseInt(connectionManager.valueOf("@"
                  + DatabasesXmlTags.ATT_minPoolSize));
              int maxPoolSize = Integer.parseInt(connectionManager.valueOf("@"
                  + DatabasesXmlTags.ATT_maxPoolSize));
              int idleTimeout = Integer.parseInt(connectionManager.valueOf("@"
                  + DatabasesXmlTags.ATT_idleTimeout));
              int waitTimeout = Integer.parseInt(connectionManager.valueOf("@"
                  + DatabasesXmlTags.ATT_waitTimeout));
              this.addConnectionManager(vuser,
                  new VariablePoolConnectionManager(url, name, rlogin,
                      rpassword, driverPath, driverClassName, minPoolSize,
                      maxPoolSize, idleTimeout, waitTimeout));
            }
            else if (cname.equals(DatabasesXmlTags.ELT_SimpleConnectionManager))
            {
              this.addConnectionManager(vuser, new SimpleConnectionManager(url,
                  name, rlogin, rpassword, driverPath, driverClassName));
            }
            else if (cname
                .equals(DatabasesXmlTags.ELT_RandomWaitPoolConnectionManager))
            {
              int poolSize = Integer.parseInt(connectionManager.valueOf("@"
                  + DatabasesXmlTags.ATT_poolSize));
              int timeout = Integer.parseInt(connectionManager.valueOf("@"
                  + DatabasesXmlTags.ATT_timeout));
              this
                  .addConnectionManager(vuser,
                      new RandomWaitPoolConnectionManager(url, name, rlogin,
                          rpassword, driverPath, driverClassName, poolSize,
                          timeout)); /* Lookup functions */

            }
            else if (cname
                .equals(DatabasesXmlTags.ELT_FailFastPoolConnectionManager))
            {
              int poolSize = Integer.parseInt(connectionManager.valueOf("@"
                  + DatabasesXmlTags.ATT_poolSize));
              this.addConnectionManager(vuser,
                  new FailFastPoolConnectionManager(url, name, rlogin,
                      rpassword, driverPath, driverClassName, poolSize));
            }
          }
        }
      }

    }
    catch (Exception e)
    {
      logger
          .error(Translate.get("backend.add.connection.manager.failed", e), e);
    }
  }

  /**
   * Additionnal constructor for setting a different dynamic schema level.
   * Default was to gather all information Creates a new
   * <code>DatabaseBackend</code> instance.
   * 
   * @param name logical name assigned to this backend
   * @param driverPath path for driver
   * @param driverClassName class name of the database native JDBC driver to
   *          load
   * @param url URL to access the database
   * @param vdbName Name of the virtual database this backend is attached to
   * @param connectionTestStatement SQL statement used to check if a connection
   *          is still valid
   * @param nbOfWorkerThreads defines the number of BackendWorkerThread that
   *          should process writes in parallel (minimum is 2)
   * @param dynamicSchemaLevel for dynamically gathering schema from backend
   */
  public DatabaseBackend(String name, String driverPath,
      String driverClassName, String url, String vdbName,
      String connectionTestStatement, int nbOfWorkerThreads,
      String dynamicSchemaLevel)
  {
    this(name, driverPath, driverClassName, url, vdbName, true,
        connectionTestStatement, nbOfWorkerThreads);
    this.dynamicPrecision = DatabaseBackendSchemaConstants
        .getDynamicSchemaLevel(dynamicSchemaLevel);
  }

  /**
   * Returns a deeply copied clone of this backend. Will use the same rewriting
   * rules and will get new instance of connection managers with the same
   * configuration
   * 
   * @param newName the new name for this new backend
   * @param parameters a set of parameters to use to replace values from the
   *          copied backend. <br>
   *          The different parameters are: <br>
   *          <ul>
   *          <li><tt>driverPath</tt>: the path to the driver</li>
   *          <li><tt>driver</tt>: the driver class name</li>
   *          <li><tt>url</tt>: the url to connect to the database</li>
   *          <li><tt>connectionTestStatement</tt>: the query to test the
   *          connection</li>
   *          </ul>
   *          <br>
   * @return <code>DatabaseBackend</code> instance
   * @throws Exception if cannot proceed the copy
   */
  public DatabaseBackend copy(String newName, Map<?, ?> parameters) throws Exception
  {
    // Get the parameters from the backend if they are not specified, or take
    // them from the map of parameters otherwise.
    String fromDriverPath = parameters
        .containsKey(DatabasesXmlTags.ATT_driverPath) ? (String) parameters
        .get(DatabasesXmlTags.ATT_driverPath) : this.getDriverPath();

    String fromDriverClassName = parameters
        .containsKey(DatabasesXmlTags.ATT_driver) ? (String) parameters
        .get(DatabasesXmlTags.ATT_driver) : this.getDriverClassName();

    String fromUrl = parameters.containsKey(DatabasesXmlTags.ATT_url)
        ? (String) parameters.get(DatabasesXmlTags.ATT_url) /* Lookup functions */

        : this.getURL();

    String fromConnectionTestStatement = parameters
        .containsKey(DatabasesXmlTags.ATT_connectionTestStatement)
        ? (String) parameters.get(DatabasesXmlTags.ATT_connectionTestStatement)
        : this.getConnectionTestStatement();

    if (getURL().equals(fromUrl)
        && getDriverClassName().equals(fromDriverClassName))
      throw new VirtualDatabaseException(
          "It is not allowed to clone a backend with the same URL and driver class name");

    // Create the new backend object
    DatabaseBackend newBackend = new DatabaseBackend(newName, fromDriverPath,
        fromDriverClassName, fromUrl, virtualDatabaseName, writeCanBeEnabled,
        fromConnectionTestStatement, nbOfWorkerThreads);

    // Clone dynamic precision
    newBackend.setDynamicPrecision(this.dynamicPrecision,
        this.gatherSystemTables, this.schemaName);

    // Set the rewriting rules and the connection managers as the backend we
    // are copying
    newBackend.rewritingRules = this.rewritingRules;

    // Set Connection managers
    Map<String, AbstractConnectionManager> fromConnectionManagers = this.connectionManagers;
    Iterator<String> iter = fromConnectionManagers.keySet().iterator();

    String vlogin = null;
    AbstractConnectionManager connectionManager;
    while (iter.hasNext())
    {
      vlogin = (String) iter.next();
      connectionManager = (AbstractConnectionManager) fromConnectionManagers
          .get(vlogin);
      newBackend.addConnectionManager(vlogin, connectionManager.copy(fromUrl,
          newName));
    }

    return newBackend;
  }

  /**
   * Two database backends are considered equal if they have the same name, URL
   * and driver class name.
   * 
   * @param other an object
   * @return a <code>boolean</code> value
   */
  public boolean equals(Object other)
  {
    if ((other == null) || (!(other instanceof DatabaseBackend)))
      return false;
    else
    {
      DatabaseBackend b = (DatabaseBackend) other;
      return name.equals(b.getName())
          && driverClassName.equals(b.getDriverClassName())
          && url.equals(b.getURL());
    }
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return name.hashCode();
  }

  //
  // Connection management
  //

  /**
   * Adds a <code>ConnectionManager</code> to this backend. Note that the
   * <code>ConnectionManager</code> is not initialized in this method.
   * 
   * @param vLogin the virtual login corresponding to this connection manager
   * @param connectionManager the <code>ConnectionManager</code> to add
   */
  public void addConnectionManager(String vLogin,
      AbstractConnectionManager connectionManager)
  {
    if (connectionManager == null)
      throw new IllegalArgumentException(Translate.get(
          "backend.null.connection.manager", new String[]{name, url}));
    if (logger.isInfoEnabled())
      logger.info(Translate.get("backend.add.connection.manager.for.user",
          vLogin));
    connectionManager.setVLogin(vLogin);
    connectionManager.setConnectionTestStatement(getConnectionTestStatement());
    connectionManagers.put(vLogin, connectionManager);
  }

  /**
   * Adds a default connection manager on this backend for the specified user.
   * The default connection manager should be specified in the vdb config file.
   * 
   * @param vdbUser user for whom the connnection manager will be added.
   * @throws SQLException if connection manager could not be initialized.
   */
  public void addDefaultConnectionManager(VirtualDatabaseUser vdbUser)
      throws SQLException
  {
    if (defaultConnectionManager == null)
    {
      if (logger.isWarnEnabled())
      {
        logger
            .warn("Default connection manager undefined in backend configuration, setting to a VariablePoolConnectionManager");
      }
      defaultConnectionManager = new VariablePoolConnectionManager(url, name,
          vdbUser.getLogin(), vdbUser.getPassword(), driverPath,
          driverClassName, 20, 5, 0, 180, 0);
    }
    AbstractConnectionManager connectionManager = defaultConnectionManager
        .clone(vdbUser.getLogin(), vdbUser.getPassword());
    connectionManager.initializeConnections();
    addConnectionManager(vdbUser.getLogin(), connectionManager);
  }

  /**
   * Removes a <code>ConnectionManager</code> to this backend. Note that the
   * <code>ConnectionManager</code> is finalized in this method.
   * 
   * @param vdbUser the virtual user corresponding to this connection manager
   * @throws SQLException if connection manager could not be finalized.
   */
  public void removeConnectionManager(VirtualDatabaseUser vdbUser)
      throws SQLException
  {
    ((AbstractConnectionManager) connectionManagers.get(vdbUser))
        .finalizeConnections();
    connectionManagers.remove(vdbUser);
  }

  /**
   * Check if the driver used by this backend is compliant with Sequoia needs.
   * 
   * @throws SQLException if the driver is not compliant
   */
  public void checkDriverCompliance() throws SQLException
  {
    if (connectionManagers.isEmpty())
      throw new SQLException(Translate.get("backend.null.connection.manager",
          new String[]{name, url}));

    AbstractConnectionManager connectionManager;
    Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
    connectionManager = (AbstractConnectionManager) iter.next();

    try
    {
      if (!driverCompliance.complianceTest(url, connectionManager.getLogin(),
          connectionManager.getPassword(), connectionManager.getDriverPath(),
          connectionManager.getDriverClassName(), connectionTestStatement))
        throw new SQLException(Translate.get("backend.driver.not.compliant",
            driverClassName, ControllerConstants.PRODUCT_NAME));
    }
    catch (ConnectException e)
    {
      throw (SQLException) new SQLException(Translate.get(
          "backend.cannot.connect.to", e)).initCause(e);
    }
  }

  /**
   * Sets the default connection manager used for the transparent login feature.
   * 
   * @param defaultConnectionManager connection manager
   */
  public void setDefaultConnectionManager(
      AbstractConnectionManager defaultConnectionManager)
  {
    this.defaultConnectionManager = defaultConnectionManager;
  }

  /**
   * Initializes the connection managers' connections. The caller must ensure
   * that the driver has already been loaded else an exception will be thrown.
   * 
   * @exception SQLException if an error occurs
   */
  public synchronized void initializeConnections() throws SQLException
  {
    if (connectionManagers.isEmpty())
      throw new SQLException(Translate.get("backend.not.defined", new String[]{
          name, url}));

    AbstractConnectionManager connectionManager;
    Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
    while (iter.hasNext())
    {
      connectionManager = (AbstractConnectionManager) iter.next();
      if (!connectionManager.isInitialized())
        connectionManager.initializeConnections();
    }
  }

  /**
   * Releases all the connections to the database held by the connection
   * managers.
   * 
   * @throws SQLException if an error occurs
   */
  public synchronized void finalizeConnections() throws SQLException
  {
    /*
     * Remove our references to persistentConnections so that they can garbage
     * collected.
     */
    synchronized (persistentConnections)
    {
      persistentConnections.clear();
    }
    if (connectionManagers.isEmpty())
      throw new SQLException(Translate.get("backend.not.defined", new String[]{
          name, url}));

    AbstractConnectionManager connectionManager;
    Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
    while (iter.hasNext())
    {
      connectionManager = (AbstractConnectionManager) iter.next();
      if (connectionManager.isInitialized())
        connectionManager.finalizeConnections();
    }
  }

  /**
   * Force all connections to be renewed when they are used next. This just sets
   * a flag and connections will be lazily replaced as needed.
   */
  public void flagAllConnectionsForRenewal()
  {
    AbstractConnectionManager connectionManager;
    Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
    while (iter.hasNext())
    {
      connectionManager = (AbstractConnectionManager) iter.next();
      if (connectionManager.isInitialized())
        connectionManager.flagAllConnectionsForRenewal();
    }
  }

  /**
   * mutex used only as a synchronization point for getting a connection in
   * getConnection...IfNeeded() method.<br />
   * This method uses a synchronized block on this mutex rather than
   * synchronizing on <code>this</code> so that we won't hang all the system
   * if the method blocks because no connections are available.
   * 
   * @see #getConnectionForTransactionAndLazyBeginIfNeeded(AbstractRequest,
   *      AbstractConnectionManager)
   */
  private final Object connectionMutex = new Object();

  /**
   * Add a persistent connection to this backend
   * 
   * @param persistentConnectionId id of the persistent connection to add
   * @param c the persistent connection to add
   */
  public void addPersistentConnection(long persistentConnectionId,
      PooledConnection c)
  {
    synchronized (persistentConnections)
    {
      persistentConnections.put(new Long(persistentConnectionId), c);
    }
  }

  /**
   * Returns <code>true</code> if this DatabaseBackend has persistent
   * connections, <code>false</code> else.
   * 
   * @return <code>true</code> if this DatabaseBackend has persistent
   *         connections, <code>false</code> else
   */
  public boolean hasPersistentConnections()
  {
    return (persistentConnections.size() > 0);
  }

  /**
   * Remove a persistent connection from this backend
   * 
   * @param persistentConnectionId id of the persistent connection to remove
   */
  public void removePersistentConnection(long persistentConnectionId)
  {
    synchronized (persistentConnections)
    {
      persistentConnections.remove(new Long(persistentConnectionId));
      if (persistentConnections.isEmpty())
      {
        persistentConnections.notifyAll();
      }
    }
  }

  /**
   * Retrieve a connection for a given transaction or create a new connection
   * and start a new transaction. <br>
   * This method is internally synchronized on a mutex so that concurrent writes
   * within the same transaction that are allowed to execute out of order will
   * not open separate connection if they race on transaction begin.
   * 
   * @param request request that will execute (must carry transaction id and
   *          transaction isolation level (does nothing if equals to
   *          Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL))
   * @param cm connection manager to get the connection from
   * @return the connection for the given transaction id
   * @throws UnreachableBackendException if the backend is no more reachable
   * @throws NoTransactionStartWhenDisablingException if a new transaction
   *           needed to be started but the backend is in the disabling state
   * @throws SQLException if another error occurs
   * @see #connectionMutex
   */
  public Connection getConnectionForTransactionAndLazyBeginIfNeeded(
      AbstractRequest request, AbstractConnectionManager cm)
      throws UnreachableBackendException,
      NoTransactionStartWhenDisablingException, SQLException
  {
    Long tid = new Long(request.getTransactionId());
    /*
     * An existing transaction that already has a connection must not be blocked
     * by transactions that are waiting for connections. Transactions that have
     * started and have a connection are past the point where concurrent
     * requests are a problem.
     */
    if (isStartedTransaction(tid))
    {
      PooledConnection pc = cm.retrieveConnectionForTransaction(request
          .getTransactionId());
      if (pc != null)
      {
        return pc.getConnection();
      }
    }
    synchronized (connectionMutex)
    {
      /*
       * Repeat the test from above, because a concurrent write request may have
       * changed the transaction status while this request was waiting for the
       * mutex.
       */
      if (isStartedTransaction(tid))
      { // Transaction has already been started, retrieve connection
        PooledConnection pc = cm.retrieveConnectionForTransaction(request
            .getTransactionId());
        if ((pc == null) && isDisabling())
        {
          /**
           * The backend is disabling and this transaction was added at
           * disabling time because it was already logged but we didn't lazily
           * start it yet, so let's do the lazy start now.
           * 
           * @see RequestManager#disableBackendStoreCheckpointAndSetDisabling(DatabaseBackend,
           *      String)
           */
          Connection c = AbstractLoadBalancer.getConnectionAndBeginTransaction(
              this, cm, request);
          if (c == null)
          {
            if (logger.isWarnEnabled())
            {
              logger
                  .warn("Null connection returned from AbstractLoadBalancer.getConnectionAndBeginTransaction() (backend is disabling)");
            }
          }
          return c;
        }
        if ((pc == null) && !isDisabling())
        { // Sanity check, should never happen
          if (logger.isErrorEnabled())
          {
            logger.error("Null connection [tid = " + request.getTransactionId()
                + ", backend state = " + state + ", active tx = "
                + activeTransactions + "]");
          }
          return null;
        }
        return pc.getConnection();
      }
      else
      {
        if (!canAcceptTasks(request))
          throw new NoTransactionStartWhenDisablingException();

        Connection c = null;

        // Transaction has not been started yet, this is a lazy begin
        c = AbstractLoadBalancer.getConnectionAndBeginTransaction(this, cm,
            request);
        // begin transaction
        startTransaction(tid);

        if (c == null)
        {
          if (logger.isWarnEnabled())
          {
            logger
                .warn("Null connection returned from AbstractLoadBalancer.getConnectionAndBeginTransaction() [state = "
                    + state + "]");
          }
        }
        return c;
      }
    } // end of synchronized (connectionMutex)
  }

  /**
   * Returns the <code>ConnectionManager</code> associated to this backend for
   * a given virtual login.
   * 
   * @param vLogin the virtual login
   * @return an <code>AbstractConnectionManager</code> instance
   */
  public AbstractConnectionManager getConnectionManager(String vLogin)
  {
    return (AbstractConnectionManager) connectionManagers.get(vLogin);
  }

  /**
   * Returns the SQL statement to use to check the connection validity.
   * 
   * @return a <code>String</code> containing a SQL statement
   */
  public String getConnectionTestStatement()
  {
    return connectionTestStatement;
  }

  /**
   * Retrieves the SQLWarning chained to the given persistent connection
   * 
   * @param connId persistent connection id to retrieve warnings from
   * @exception SQLException if a database access error occurs or this method is
   *              called on a closed connection
   * @return the warnings on the given connection or null
   */
  public SQLWarning getPersistentConnectionWarnings(long connId)
      throws SQLException
  {
    if (persistentConnections != null)
    {
      PooledConnection pc = (PooledConnection) persistentConnections
          .get(new Long(connId));
      if (pc != null)
        return pc.getConnection().getWarnings();
    }
    return null;
  }

  /**
   * Clears the SQLWarning chained to the given persistent connection
   * 
   * @param connId persistent connection id to retrieve warnings from
   * @exception SQLException if a database access error occurs
   */
  public void clearPersistentConnectionWarnings(long connId)
      throws SQLException
  {
    if (persistentConnections != null)
    {
      PooledConnection pc = (PooledConnection) persistentConnections
          .get(new Long(connId));
      if (pc != null)
        pc.getConnection().clearWarnings();
    }
  }

  /**
   * Check if the given connection is valid or not. This function issues the
   * connectionTestStatement query on the connection and if it succeeds then the
   * connection is declared valid. If an exception occurs, the connection is
   * declared invalid.
   * 
   * @param connection the connection to test
   * @return true if the connection is valid
   */
  public boolean isValidConnection(Connection connection)
  {
    Statement s = null;
    try
    {
      s = connection.createStatement();
      s.executeQuery(connectionTestStatement);
    }
    catch (SQLException e)
    {
      if ("25P02".equals(e.getSQLState())
          || (e.getMessage() != null && e.getMessage().indexOf(
              "ignored until end of transaction block") > 0))
      {
        // see bug item #300873 on the forge for details
        // PostgreSQL throws an exception if a query is issued after a request
        // has failed within a transaction, we now have to check for this
        // exception as it means the connection is valid
        //
        // PostgreSQL versions after 7.4 will return the SQLState if version 3
        // of the protocol is used (it is the default version unless you use
        // protocolVersion parameter in your JDBC url), whereas PostgreSQL
        // versions prior to 7.4 (version 2 of the protocol) will have to be
        // checked for the message text
        return true;
      }
      return false;
    }
    finally
    {
      if (s != null)
      {
        try
        {
          s.close();
        }
        catch (SQLException ignore)
        {
        }
      }
    }
    return true;
  }

  /**
   * Tell all the connection managers to stop handing out new Connections.
   */
  public synchronized void shutdownConnectionManagers()
  {
    for (Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator(); iter.hasNext();)
    {
      AbstractConnectionManager cm = (AbstractConnectionManager) iter.next();
      if (cm.isInitialized())
      {
        cm.shutdown();
      }
    }
  }

  //
  // Transaction management
  //

  /**
   * Adds a savepoint to a given transaction
   * 
   * @param tid transaction identifier
   * @param savepoint savepoint to add
   */
  public void addSavepoint(Long tid, Savepoint savepoint)
  {
    synchronized (savepoints)
    {
      List<Savepoint> savepointList = (List<Savepoint>) savepoints.get(tid);
      if (savepointList == null)
      { // Lazy list creation
        savepointList = new ArrayList<Savepoint>();
        savepoints.put(tid, savepointList);
      }
      savepointList.add(savepoint);
    }
  }

  /**
   * Returns a Savepoint identified its name for a given transaction or
   * <code>null</code> if no such Savepoint exists.
   * 
   * @param tid transaction identifier
   * @param savepointName name of the savepoint
   * @return a savepoint or <code>null</code> if there is no savepoint of the
   *         given <code>savePointName</code> for the transaction identified
   *         by the <code>tid</code>
   */
  public Savepoint getSavepoint(Long tid, String savepointName)
  {
    synchronized (savepoints)
    {
      List<?> savepointList = (List<?>) savepoints.get(tid);
      if (savepointList == null)
        return null; // No checkpoint for that transaction

      Iterator<?> i = savepointList.iterator();
      while (i.hasNext())
      {
        try
        {
          Savepoint savepoint = (Savepoint) i.next();
          if (savepointName.equals(savepoint.getSavepointName()))
            return savepoint;
        }
        catch (SQLException ignore)
        {
          // We should never get here because we always use named savepoints
          // on backends
        }
      }
    }

    // No savepoint has been found for given savepoint name
    return null;
  }

  /**
   * Returns <code>true</code> if the specified transaction has been started
   * on this backend (a connection has been allocated for this transaction).
   * 
   * @param tid transaction identifier
   * @return <code>true</code> if the transaction has been started
   */
  public boolean isStartedTransaction(Long tid)
  {
    synchronized (activeTransactions)
    {
      return activeTransactions.contains(tid);
    }
  }

  /**
   * Removes a savepoint for a given transaction
   * 
   * @param tid transaction identifier
   * @param savepoint savepoint to remove
   */
  public void removeSavepoint(Long tid, Savepoint savepoint)
  {
    synchronized (savepoints)
    {
      List<?> savepointList = (List<?>) savepoints.get(tid);
      if (savepointList == null)
        logger.error("No savepoints found for transaction " + tid);
      else
        savepointList.remove(savepoint);
    }
  }

  /**
   * Signals that a transaction has been started on this backend. It means that
   * a connection has been allocated for this transaction.
   * 
   * @param tid transaction identifier
   */
  public void startTransaction(Long tid)
  {
    synchronized (activeTransactions)
    {
      totalTransactions++;
      activeTransactions.add(tid);
    }
  }

  /**
   * Signals that a transaction has been stopped on this backend. It means that
   * the connection has been released for this transaction.
   * 
   * @param tid transaction identifier
   */
  public void stopTransaction(Long tid)
  {
    synchronized (activeTransactions)
    {
      if (!activeTransactions.remove(tid))
        throw new IllegalArgumentException(Translate.get(
            "backend.transaction.not.started", new String[]{"" + tid, name}));
      // If this was the last open transaction, we notify people possibly
      // waiting on waitForAllTransactionsToComplete()
      if (activeTransactions.isEmpty())
      {
        activeTransactions.notifyAll();
      }
    }

    synchronized (savepoints)
    {
      savepoints.remove(tid);
    }
  }

  /**
   * This method waits until all currently open transactions and persistent
   * connections on this backend complete. If no transaction are currently
   * running on this backend or no persistent connection is open, this method
   * immediately returns.
   */
  public void waitForAllTransactionsAndPersistentConnectionsToComplete()
  {
    // Wait for active transactions to complete
    synchronized (activeTransactions)
    {
      if (!activeTransactions.isEmpty())
      {
        if (logger.isInfoEnabled())
          logger.info("Backend " + name + " wait for "
              + activeTransactions.size()
              + " transactions to complete before disabling completely.");

        try
        {
          activeTransactions.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }
    }

    // Wait for active persistent connections to close
    synchronized (persistentConnections)
    {
      if (!persistentConnections.isEmpty())
      {
        if (logger.isInfoEnabled())
          logger
              .info("Backend "
                  + name
                  + " wait for "
                  + persistentConnections.size()
                  + " persistent connections to close before disabling completely.");

        try
        {
          persistentConnections.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }
    }
  }

  //
  // State Management
  //

  /**
   * Returns true if the backend is in a state that allows it to accept tasks or
   * the execution of the specified request.
   * 
   * @param request the request that is going to execute in that task (null if
   *          not applicable).
   * @return Returns true if backend isReadEnabled() or isWriteEnabled() or
   *         isReplaying()
   */
  public boolean canAcceptTasks(AbstractRequest request)
  {
    if (request != null)
      return canAcceptTasks(request.isPersistentConnection(), request
          .getPersistentConnectionId());
    else
      return canAcceptTasks(false, -1);
  }

  /**
   * Returns true if the backend is in a state that allows it to accept tasks or
   * the execution for the specified persistent connection.
   * 
   * @param persistentConnectionId the persistent connection id of the request
   *          that is going to execute .
   * @return Returns true if backend isReadEnabled() or isWriteEnabled() or
   *         isReplaying()
   */
  public boolean canAcceptTasks(long persistentConnectionId)
  {
    return canAcceptTasks(true, persistentConnectionId);
  }

  private boolean canAcceptTasks(boolean isPersistentConnection,
      long persistentConnectionId)
  {
    // return isReadEnabled() || isWriteEnabled() || isReplaying();
    boolean acceptTask = state == BackendState.READ_ENABLED_WRITE_DISABLED
        || state == BackendState.READ_ENABLED_WRITE_ENABLED
        || state == BackendState.READ_DISABLED_WRITE_ENABLED
        || state == BackendState.REPLAYING;

    if (!acceptTask && isPersistentConnection)
    {
      // Check if the request is on one of our active persistent connections in
      // which case we have to execute it.
      synchronized (persistentConnections)
      {
        return persistentConnections.containsKey(new Long(
            persistentConnectionId));
      }
    }
    return acceptTask;
  }

  /**
   * Cleans transactions and pending requests / tasks states
   */
  private void cleanBackendStates()
  {
    activeTransactions.clear();
    savepoints.clear();
    pendingRequests.clear();
    pendingTasks.clear();
    if (!isSchemaStatic())
    {
      setSchemaIsDirty(true, null);
      // make sure locks from old transactions are not carried over
      schema = null;
    }
  }

  /**
   * Sets the database backend state to disable. This state is just an
   * indication and it has no semantic effect. It is up to the request manager
   * (especially the load balancer) to ensure that no more requests are sent to
   * this backend.
   * 
   * @return false if the backend was already in the disabled state, true
   *         otherwise
   */
  public synchronized boolean disable()
  {
    if (getStateValue() == BackendState.DISABLED)
    {
      return false;
    }
    setState(BackendState.DISABLED);

    return true;
  }

  /**
   * Disables the database backend for reads. This does not affect write ability
   */
  public synchronized void disableRead()
  {
    if (isWriteEnabled())
      setState(BackendState.READ_DISABLED_WRITE_ENABLED);
    else
      setState(BackendState.DISABLED);
  }

  /**
   * Disables the database backend for writes. This does not affect read ability
   * although the backend will not be coherent anymore as soon as a write as
   * occured. This should be used in conjunction with a checkpoint to recover
   * missing writes.
   */
  public synchronized void disableWrite()
  {
    if (isReadEnabled())
      setState(BackendState.READ_ENABLED_WRITE_DISABLED);
    else
      setState(BackendState.DISABLED);
  }

  /**
   * Enables the database backend for reads. This method should only be called
   * when the backend is synchronized with the others.
   */
  public synchronized void enableRead()
  {
    if (isWriteEnabled())
      setState(BackendState.READ_ENABLED_WRITE_ENABLED);
    else
      setState(BackendState.READ_ENABLED_WRITE_DISABLED);
  }

  /**
   * Enables the database backend for writes. This method should only be called
   * when the backend is synchronized with the others.
   */
  public synchronized void enableWrite()
  {
    // Remove last known checkpoint since backend will now be modified and no
    // more synchronized with the checkpoint.
    setLastKnownCheckpoint(null);
    if (isReadEnabled())
      setState(BackendState.READ_ENABLED_WRITE_ENABLED);
    else
      setState(BackendState.READ_DISABLED_WRITE_ENABLED);
  }

  /**
   * Returns the lastKnownCheckpoint value.
   * 
   * @return Returns the lastKnownCheckpoint.
   */
  public String getLastKnownCheckpoint()
  {
    return lastKnownCheckpoint;
  }

  /**
   * Retrieve the state of the backend.
   * 
   * @see SequoiaNotificationList#VIRTUALDATABASE_BACKEND_DISABLED
   * @see SequoiaNotificationList#VIRTUALDATABASE_BACKEND_RECOVERING
   * @see SequoiaNotificationList#VIRTUALDATABASE_BACKEND_BACKINGUP
   * @see SequoiaNotificationList#VIRTUALDATABASE_BACKEND_DISABLING
   * @see SequoiaNotificationList#VIRTUALDATABASE_BACKEND_ENABLED
   * @see SequoiaNotificationList#VIRTUALDATABASE_BACKEND_DISABLED
   * @return one of the above
   */
  // FIXME should not return key for i18n but i18n translation instead
  public String getState()
  {
    switch (state)
    {
      case BackendState.READ_ENABLED_WRITE_DISABLED :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_ENABLED;
      case BackendState.READ_ENABLED_WRITE_ENABLED :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_READ_ENABLED_WRITE_ENABLED;
      case BackendState.READ_DISABLED_WRITE_ENABLED :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_READ_DISABLED_WRITE_ENABLED;
      case BackendState.DISABLING :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_DISABLING;
      case BackendState.BACKUPING :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_BACKINGUP;
      case BackendState.RESTORING :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_RECOVERING;
      case BackendState.REPLAYING :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_REPLAYING;
      case BackendState.DISABLED :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_DISABLED;
      case BackendState.UNKNOWN :
        return SequoiaNotificationList.VIRTUALDATABASE_BACKEND_UNKNOWN;
      default :
        throw new IllegalArgumentException("Unknown backend state:" + state);
    }
  }

  /**
   * Return the integer value corresponding to the state of the backend. The
   * values are defined in <code>BackendState</code>
   * 
   * @return <tt>int</tt> value
   * @see BackendState
   */
  public int getStateValue()
  {
    return state;
  }

  /**
   * Returns the isBackuping value.
   * 
   * @return Returns the isBackuping.
   */
  public boolean isBackuping()
  {
    return state == BackendState.BACKUPING;
  }

  /**
   * Is the backend completely disabled ? This usually means it has a known
   * state with a checkpoint associated to it.
   * 
   * @return <code>true</code> if the backend is disabled
   */
  public boolean isDisabled()
  {
    return state == BackendState.DISABLED;
  }

  /**
   * Returns the isDisabling value.
   * 
   * @return Returns the isDisabling.
   */
  public boolean isDisabling()
  {
    return state == BackendState.DISABLING;
  }

  /**
   * Tests if this backend is initialized
   * 
   * @return <code>true</code> if this backend is initialized
   * @throws SQLException if an error occurs
   */
  public synchronized boolean isInitialized() throws SQLException
  {
    if (connectionManagers.isEmpty())
      throw new SQLException(Translate.get("backend.null.connection.manager",
          new String[]{name, url}));
    Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
    while (iter.hasNext())
    {
      if (!((AbstractConnectionManager) iter.next()).isInitialized())
        return false;
    }
    return true;
  }

  /**
   * Is the backend accessible ?
   * 
   * @return <tt>true</tt> if a jdbc connection is still possible from the
   *         controller
   */
  public synchronized boolean isJDBCConnected()
  {
    Connection con = null;
    Statement s = null;
    try
    {
      if (connectionManagers.isEmpty())
        throw new SQLException(Translate.get("backend.null.connection.manager",
            new String[]{name, url}));

      AbstractConnectionManager connectionManager;
      Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
      connectionManager = (AbstractConnectionManager) iter.next();

      con = connectionManager.getConnectionFromDriver();
      if (con == null)
      {
        return false;
      }
      s = con.createStatement();
      s.execute(this.connectionTestStatement);
      return true;
    }
    catch (Exception e)
    {
      String msg = Translate.get("loadbalancer.backend.unreacheable", name);
      logger.warn(msg, e);
      return false;
    }
    finally
    {
      if (s != null)
      {
        try
        {
          s.close();
        }
        catch (SQLException ignore)
        {
        }
      }
      if (con != null)
      {
        try
        {
          con.close();
        }
        catch (SQLException e)
        {
          return false;
        }
      }
    }
  }

  /**
   * Returns true if the backend cannot be used anymore
   * 
   * @return Returns true if the backend was removed from activity by the load
   *         balancer
   */
  // TODO nobody uses this method. Should be removed
  public boolean isKilled()
  {
    return state == BackendState.UNKNOWN;
  }

  /**
   * Tests if this backend is read enabled (active and synchronized).
   * 
   * @return <code>true</code> if this backend is enabled.
   */
  public synchronized boolean isReadEnabled()
  {
    return state == BackendState.READ_ENABLED_WRITE_DISABLED
        || state == BackendState.READ_ENABLED_WRITE_ENABLED;
  }

  /**
   * Returns true if the backend is in the BackendState.RECOVERING state
   * 
   * @return Returns true if the backend is restoring a dump
   */
  // TODO nobody uses this method. Should be removed
  public boolean isRestoring()
  {
    return state == BackendState.RESTORING;
  }

  /**
   * Returns true if the backend is in the BackendState.REPLAYING state
   * 
   * @return Returns true if the backend is replaying the recovery
   */
  public boolean isReplaying()
  {
    return state == BackendState.REPLAYING;
  }

  /**
   * Tests if this backend is write enabled (active and synchronized).
   * 
   * @return <code>true</code> if this backend is enabled.
   */
  public synchronized boolean isWriteEnabled()
  {
    return state == BackendState.READ_ENABLED_WRITE_ENABLED
        || state == BackendState.READ_DISABLED_WRITE_ENABLED;
  }

  /**
   * Returns the writeCanBeEnabled value.
   * 
   * @return Returns the writeCanBeEnabled.
   */
  public boolean isWriteCanBeEnabled()
  {
    return writeCanBeEnabled;
  }

  /**
   * Sets the NotificationBroadcasterSupport that this backend can use to send
   * JMX notification.
   * 
   * @param notificationBroadcaster a NotificationBroadcasterSupport
   */
  public void setNotificationBroadcaster(
      NotificationBroadcasterSupport notificationBroadcaster)
  {
    this.notificationBroadcaster = notificationBroadcaster;
  }

  /**
   * Notify JMX NotificationListener that the state of this DatabaseBackend has
   * changed. The emmitted Notification is a
   * <code>AttributeChangeNotification</code> with an attributeName set to
   * <code>"StateValue"</code> and an attributeType set to
   * <code>Integer</code>
   * 
   * @param message a message about the backend state change
   * @param oldState the previous state of the backend
   * @param currentState the current state of the backend
   * @see BackendState
   * @see #getStateValue()
   */
  private void notifyJMXStateChanged(String message, int oldState,
      int currentState)
  {
    try
    {
      Notification attrChangeNotification = new AttributeChangeNotification(
          JmxConstants.getDatabaseBackendObjectName(virtualDatabaseName, name),
          notificationSequence++, new Date().getTime(), message,
          "StateValue", "Integer", //$NON-NLS-1$ //$NON-NLS-2$
          new Integer(oldState), new Integer(currentState));
      sendNotification(attrChangeNotification);
    }
    catch (MalformedObjectNameException e)
    {
      logger.warn("Unable to send JMX notification", e);
    }
  }

  /**
   * Sends JMX notification
   * 
   * @param type notification type
   * @see SequoiaNotificationList
   */
  public void notifyJmx(String type)
  {
    notifyJmx(type, SequoiaNotificationList.NOTIFICATION_LEVEL_INFO, Translate
        .get(type, getName()));
  }

  /**
   * Sends JMX error notification
   * 
   * @param e <tt>Exception</tt> object. Only the message will be used
   * @param type notification type
   * @see SequoiaNotificationList
   */
  public void notifyJmxError(String type, Exception e)
  {
    notifyJmx(type, SequoiaNotificationList.NOTIFICATION_LEVEL_ERROR, Translate
        .get(type, new String[]{getName(), e.getMessage()}));
  }

  /**
   * Sends a JMX notification.
   * 
   * @param type Type of JMX notification
   * @param level unused parameter (will be deprecated)
   * @param message Message of the notification
   */
  private void notifyJmx(String type, String level, String message)
  {
    try
    {
      Notification notification = (new Notification(type, JmxConstants
          .getDatabaseBackendObjectName(virtualDatabaseName, name),
          notificationSequence++, message));

      sendNotification(notification);
    }
    catch (MalformedObjectNameException e)
    {
      logger.warn("Unable to send JMX notification", e);
    }
  }

  /**
   * Sends a JMX notification.
   * 
   * @param notification a JMX Notification to send
   */
  private void sendNotification(Notification notification)
  {
    if (MBeanServerManager.isJmxEnabled())
    {
      notificationBroadcaster.sendNotification(notification);
    }
  }

  /**
   * Notify the state of the backend has changed.<br />
   * This notification triggers the update of the state of the backend stored in
   * the recovery log.
   */
  private void notifyStateListener()
  {
    if (stateListener != null)
      stateListener.stateChanged(this);
  }

  /**
   * Sets the stateListener value.
   * 
   * @param stateListener The stateListener to set.
   */
  public void setStateListener(BackendStateListener stateListener)
  {
    this.stateListener = stateListener;
  }

  /**
   * This is used when the backend must be disabled but currently open
   * transactions must terminate. This is a transitional state. When disabling
   * is complete the caller must set the backend state to disabled.
   * <p>
   * Reads are no more allowed on the backend and the state is updated so that
   * isReadEnabled() returns false.
   * 
   * @see #disable()
   * @see #isReadEnabled()
   * @deprecated not used anymore. Please use the setState method instead
   */
  // TODO nobody uses this method. Should be removed
  public void setDisabling()
  {
    setState(BackendState.DISABLING);
  }

  /**
   * setLastKnownCheckpoint for this backend
   * 
   * @param checkpoint the checkpoint
   */
  public void setLastKnownCheckpoint(String checkpoint)
  {
    this.lastKnownCheckpoint = checkpoint;
    notifyStateListener(); // triggers recovery log update
  }

  /**
   * Set the state of a backend
   * 
   * @param state see BackendState for a possible list of the different state
   * @see org.continuent.sequoia.common.jmx.management.BackendState
   */
  // FIXME should use a type-safe enum to represent backend state instead of
  // ints
  public synchronized void setState(int state)
  {
    switch (state)
    {
      case BackendState.UNKNOWN :
        lastKnownCheckpoint = null;
        break;
      case BackendState.RESTORING :
      case BackendState.REPLAYING :
        cleanBackendStates();
        break;
      case BackendState.READ_ENABLED_WRITE_DISABLED :
      case BackendState.READ_ENABLED_WRITE_ENABLED :
      case BackendState.READ_DISABLED_WRITE_ENABLED :
      case BackendState.DISABLING :
      case BackendState.BACKUPING :
      case BackendState.DISABLED :
        break;
      default :
        throw new IllegalArgumentException("Unknown backend state:" + state);
    }
    int oldState = this.state;
    this.state = state;
    int currentState = this.state;
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("backend.state.changed", new String[]{
          getQualifiedName(), getState()}));
    endUserLogger.info(Translate.get("backend.state.changed", new String[]{
        getQualifiedName(), BackendState.description(currentState)}));

    notifyStateListener();
    notifyJMXStateChanged(Translate.get(getState(), getQualifiedName()),
        oldState, currentState);
  }

  //
  // Schema manipulation
  //

  /**
   * Add a specific semantic to a particular stored procedure
   * 
   * @param procedureName the stored procedure name
   * @param parameterCount the number of parameters of the stored procedure
   * @param semantic the semantic information
   */
  public void addStoredProcedureSemantic(String procedureName,
      int parameterCount, DatabaseProcedureSemantic semantic)
  {
    storedProcedureSemantics.put(DatabaseProcedure.buildKey(procedureName,
        parameterCount), semantic);
  }

  /**
   * Checks that the current database schema is compatible with all schema
   * gathered from each connection manager.
   * <p>
   * If no schema has been defined, the first gathered schema is used as the
   * current database schema.
   * <p>
   * For each schema that is not compatible with the current schema, a warning
   * is issued on the logger for that backend
   * 
   * @param c optional connection from which the schema should be fetched (null
   *          if not applicable)
   * @return true if compatible, false otherwise
   */
  @SuppressWarnings("unchecked")
public synchronized boolean checkDatabaseSchema(Connection c)
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("backend.dynamic.schema",
          DatabaseBackendSchemaConstants
              .getDynamicSchemaLevel(dynamicPrecision)));

    boolean checked = true;
    if (c == null)
    {
      AbstractConnectionManager connectionMananger;
      Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
      while (iter.hasNext())
      {
        connectionMananger = (AbstractConnectionManager) iter.next();

        // Gather the database schema from this connection manager
        DatabaseBackendMetaData meta = new DatabaseBackendMetaData(
            connectionMananger, logger, dynamicPrecision, gatherSystemTables,
            virtualDatabaseName, schemaName);

        DatabaseSchema metaSchema;
        try
        {
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("backend.gathering.database.schema"));
          metaSchema = meta.getDatabaseSchema();
        }
        catch (SQLException e)
        {
          if (logger.isWarnEnabled())
            logger.warn(Translate.get("backend.gather.schema.failed", e));
          return false;
        }
        if (schema == null)
        {
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("backend.use.gathered.schema.as.new"));
          schema = metaSchema;
        }
        else
        {
          if (dynamicPrecision == DatabaseBackendSchemaConstants.DynamicPrecisionStatic)
          {
            if (logger.isInfoEnabled())
              logger
                  .info(Translate.get("backend.schema.static.no.check", name));
          }
          else
          {
            if (logger.isInfoEnabled())
              logger.info(Translate.get("backend.check.schema.compatibility"));
            if (schema.isCompatibleSubset(metaSchema))
              logger.info(Translate.get("backend.schema.compatible.for.login",
                  connectionMananger.getLogin()));
            else
            {
              checked = false;
              logger.warn(Translate.get(
                  "backend.schema.not.compatible.for.login", connectionMananger
                      .getLogin()));
            }
          }
        }
      }
    }
    else
    { // Fetch schema from given connection
      try
      {
        schema = new DatabaseSQLMetaData(logger, c, dynamicPrecision,
            gatherSystemTables, schemaName)
            .createDatabaseSchema(virtualDatabaseName);
      }
      catch (SQLException e)
      {
        if (logger.isInfoEnabled())
          logger.info("Failed to fetch schema from given connection " + c, e);
        return checkDatabaseSchema(null);
      }
    }

    setSchemaIsDirty(false, null);

    if (schema != null)
    {
      // Update stored procedure information

      if (defaultStoredProcedureSemantic != null)
      { // Apply default semantic to all stored procedures
        for (Iterator<?> iterator = schema.getProcedures().values().iterator(); iterator
            .hasNext();)
        {
          DatabaseProcedure proc = (DatabaseProcedure) iterator.next();
          if (proc.getSemantic() == null)
            proc.setSemantic(defaultStoredProcedureSemantic);
        }
      }

      // Apply specific stored procedure information
      for (Iterator<String> iterator = storedProcedureSemantics.keySet().iterator(); iterator
          .hasNext();)
      {
        String procedureKey = (String) iterator.next();
        DatabaseProcedureSemantic semantic = (DatabaseProcedureSemantic) storedProcedureSemantics
            .get(procedureKey);
        DatabaseProcedure proc = schema.getProcedure(procedureKey);
        if (proc != null)
        { // Update semantic information for this proc found in the schema
          proc.setSemantic(semantic);
        }
        else
        { // Add the stored procedure not found in the schema
          int parenthesis = procedureKey.indexOf("(");
          String procName = procedureKey.substring(0, parenthesis);
          proc = new DatabaseProcedure(procName, "",
              DatabaseProcedure.ProcedureResultUnknown);
          int paramCount = Integer.valueOf(
              procedureKey.substring(parenthesis + 1, procedureKey.indexOf(")",
                  parenthesis))).intValue();
          @SuppressWarnings("rawtypes")
		ArrayList params = new ArrayList(paramCount);
          for (int i = 0; i < paramCount; i++)
            params.add("Param" + i);
          proc.setParameters(params);
          proc.setSemantic(semantic);
          schema.addProcedure(proc);
        }
      }
    }
    return checked;
  }

  /**
   * Returns the schema of this database.
   * 
   * @return the schema of this database. Returns <code>null</code> if the
   *         schema has not been set.
   * @see #setDatabaseSchema(DatabaseSchema, boolean)
   */
  public synchronized DatabaseSchema getDatabaseSchema()
  {
    if (schemaIsNeededByVdb && schemaIsDirty && !schemaIsStatic)
      refreshSchema(null);
    return schema;
  }

  /**
   * Get the Database static metadata from this backend using a connection from
   * the first available connection manager.
   * 
   * @return Static metadata information
   */
  public MetadataContainer getDatabaseStaticMetadata()
  {
    AbstractConnectionManager connectionMananger;
    Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
    if (iter.hasNext())
    {
      connectionMananger = (AbstractConnectionManager) iter.next();
      // Gather the static metadata from the first connection manager
      DatabaseBackendMetaData meta = new DatabaseBackendMetaData(
          connectionMananger, logger, dynamicPrecision, gatherSystemTables,
          virtualDatabaseName, schemaName);
      try
      {
        return meta.retrieveDatabaseMetadata();
      }
      catch (SQLException e)
      {
        return null;
      }
    }
    else
      return null;
  }

  /**
   * @return Returns the dynamicPrecision.
   */
  public int getDynamicPrecision()
  {
    return dynamicPrecision;
  }

  /**
   * Returns the schemaName value.
   * 
   * @return Returns the schemaName.
   */
  public String getSchemaName()
  {
    return schemaName;
  }

  /**
   * Returns the default Stored Procedure Semantic value.
   * 
   * @return Returns the defaultStoredProcedureSemantic.
   */
  // TODO nobody uses this method. Should be removed
  public DatabaseProcedureSemantic getDefaultStoredProcedureSemantic()
  {
    return defaultStoredProcedureSemantic;
  }

  /**
   * Set the default Stored Procedure semantic definition.
   * 
   * @param defaultSemantic the default semantic
   */
  public void setDefaultStoredProcedureSemantic(
      DatabaseProcedureSemantic defaultSemantic)
  {
    this.defaultStoredProcedureSemantic = defaultSemantic;
    this.defaultStoredProcedureSemantic.setUseDefaultSemantic(true);
  }

  /**
   * Get all the names of tables of this database <b>NOTE</b>: The returned
   * collection will contain two entries per actual table: one with the table
   * name alone, the other prefixed by the schema name + ".".
   * 
   * @see org.continuent.sequoia.common.sql.schema.DatabaseSchema#addTable(DatabaseTable)
   * @return <code>Collection</code> of <code>DatabaseTable</code>
   */
  public Collection<? extends Object> getTables()
  {
    DatabaseSchema schemaPtr = getDatabaseSchema();
    if (schemaPtr == null)
      throw new NullPointerException(Translate.get("backend.schema.not.set"));
    return schemaPtr.getTables().values();
  }

  /**
   * Returns <code>true</code> if this backend has the given table in its
   * schema. The caller must ensure that the database schema has been defined,
   * using the {@link #setDatabaseSchema(DatabaseSchema, boolean)}or
   * {@link #checkDatabaseSchema()}
   * 
   * @param table The table name to look for
   * @return <code>true</code> if tables is found in the schema
   */
  public boolean hasTable(String table)
  {
    DatabaseSchema schemaPtr = getDatabaseSchema();
    if (schemaPtr == null)
      throw new NullPointerException(Translate.get("backend.schema.not.set"));

    return schemaPtr.hasTable(table);
  }

  /**
   * Returns <code>true</code> if this backend has the given list of tables in
   * its schema. The caller must ensure that the database schema has been
   * defined, using the {@link #setDatabaseSchema(DatabaseSchema, boolean)}or
   * {@link #checkDatabaseSchema()}methods.
   * 
   * @param tables the list of table names (<code>Collection</code> of
   *          <code>String</code>) to look for
   * @return <code>true</code> if all the tables are found
   */
  public boolean hasTables(Collection<?> tables)
  {
    DatabaseSchema schemaPtr = getDatabaseSchema();
    if (schemaPtr == null)
      throw new NullPointerException(Translate.get("backend.schema.not.set"));

    if (tables == null)
      throw new IllegalArgumentException(Translate.get("backend.null.tables"));

    for (Iterator<?> iter = tables.iterator(); iter.hasNext();)
    {
      if (!schemaPtr.hasTable((String) iter.next()))
        return false;
    }
    return true;
  }

  /**
   * Returns <code>true</code> if this backend has the given stored procedure
   * in its schema. The caller must ensure that the database schema has been
   * defined, using the {@link #setDatabaseSchema(DatabaseSchema, boolean)}or
   * {@link #checkDatabaseSchema()}
   * 
   * @param procedureName The stored procedure name to look for
   * @param nbOfParameters number of parameters of the stored procecdure
   * @return <code>true</code> if the procedure has been found
   */
  public boolean hasStoredProcedure(String procedureName, int nbOfParameters)
  {
    DatabaseSchema schemaPtr = getDatabaseSchema();
    if (schemaPtr == null)
      throw new NullPointerException(Translate.get("backend.schema.not.set"));

    return schemaPtr.hasProcedure(procedureName, nbOfParameters);
  }

  /**
   * Returns the gatherSystemTables value.
   * 
   * @return Returns the gatherSystemTables.
   */
  public boolean isGatherSystemTables()
  {
    return gatherSystemTables;
  }

  /**
   * Returns the schemaIsDirty value.
   * 
   * @return Returns true if the backend database schema is dirty and needs a
   *         refresh.
   */
  // TODO nobody uses this method. Should be removed
  public boolean isSchemaDirty()
  {
    return schemaIsDirty;
  }

  /**
   * @return Returns the schemaIsStatic.
   */
  public boolean isSchemaStatic()
  {
    return schemaIsStatic;
  }

  /**
   * Erase the current schema and force a re-fetch of all the meta data
   * 
   * @param c optional connection from which the schema should be fetched (null
   *          if not applicable)
   */
  private synchronized void refreshSchema(Connection c)
  {
    if (isSchemaStatic())
      return;
    DatabaseSchema oldSchema = schema;
    setDatabaseSchema(null, isSchemaStatic());
    checkDatabaseSchema(c); // set dirty to false as well
    // Schema is null if we failed to refresh it
    if ((schema != null) && (oldSchema != null))
    {
      schema.setLocks(oldSchema);
    }
  }

  /**
   * Sets the database schema.
   * 
   * @param databaseSchema the schema to set
   * @param isStatic <code>true</code> if the schema should be static
   * @see #getDatabaseSchema()
   */
  public synchronized void setDatabaseSchema(DatabaseSchema databaseSchema,
      boolean isStatic)
  {
    if (schema == null)
    {
      schemaIsStatic = isStatic;
      schema = databaseSchema;
    }
    else
    {
      if (!isStatic)
        schema = databaseSchema;
    }
  }

  /**
   * Set the amount of information that must be gathered when fetching database
   * schema information.
   * 
   * @param dynamicPrecision The dynamicPrecision to set.
   * @param gatherSystemTables True if we must gather system tables
   * @param schemaName Schema name to use to gather tables
   */
  public void setDynamicPrecision(int dynamicPrecision,
      boolean gatherSystemTables, String schemaName)
  {
    this.dynamicPrecision = dynamicPrecision;
    this.gatherSystemTables = gatherSystemTables;
    this.schemaName = schemaName;
  }

  /**
   * Sets the schemaIsDirty value if the backend schema needs to be refreshed.
   * 
   * @param schemaIsDirty The schemaIsDirty to set.
   * @param request optional request information used to retrieve the connection
   *          from which the schema should be fetched (null if not applicable)
   */
  public void setSchemaIsDirty(boolean schemaIsDirty, AbstractRequest request)
  {
    if (request == null)
    {
      this.schemaIsDirty = schemaIsDirty;
      return;
    }

    // Try to retrieve the connection corresponding to the persistent connection
    // or transaction if applicable
    Connection c = null;
    PooledConnection pc = null;
    if (request.isPersistentConnection())
    {
      AbstractConnectionManager cm = getConnectionManager(request.getLogin());
      if (cm != null)
        try
        {
          pc = cm.retrieveConnectionInAutoCommit(request);
          if (pc != null)
            c = pc.getConnection();
        }
        catch (UnreachableBackendException ignore)
        {
        }
    }
    else if (!request.isAutoCommit())
    {
      AbstractConnectionManager cm = getConnectionManager(request.getLogin());
      if (cm != null)
        pc = cm.retrieveConnectionForTransaction(request.getTransactionId());
      if (pc != null)
        c = pc.getConnection();
    }

    if (c == null)
      this.schemaIsDirty = schemaIsDirty;
    else
      // refresh schema right away
      refreshSchema(c);
  }

  /**
   * Sets the schemaIsNeededByVdb value.
   * 
   * @param schemaIsNeededByVdb The schemaIsNeededByVdb to set.
   */
  public void setSchemaIsNeededByVdb(boolean schemaIsNeededByVdb)
  {
    this.schemaIsNeededByVdb = schemaIsNeededByVdb;
  }

  /**
   * Update the DatabaseBackend schema definition according to the successful
   * execution of the provided request. Note that the schema is only updated it
   * the provided request is a DDL statement.
   * <p>
   * TODO: a full refresh is forced on CREATE request to be sure to properly
   * handle foreign keys (see SEQUOIA-581). An improvement would be not to
   * refresh the whole schema on each create request - just add the new table
   * and fetch its dependencies (foreign keys) by reloading exported keys of
   * existing tables (see SEQUOIA-xxx). Similar optimization may be done on
   * ALTER requests but it would require to be able to parse the request -
   * currently we just force a full schema refresh. Note that concerning DROP
   * request, we remove the table from dependending tables of other tables in
   * addition to the removal of the table from the schema (given t1 and t2
   * tables, if t2 has a fk referencing t1, the table t1 will have the table t2
   * in its dependending tables). In case of rollback, these modification will
   * be cancelled because the schema will be refreshed.
   * 
   * @param request the request that possibly updates the schema
   */
  public void updateDatabaseBackendSchema(AbstractWriteRequest request)
  {
    if (!request.altersSomething() || !request.altersDatabaseSchema())
      return;

    // Update schema
    if (schemaIsNeededByVdb)
    {
      if (request.isCreate())
      { // Add the table to the schema
        DatabaseSchema dbs = getDatabaseSchema();
        if (dbs != null)
        {
          CreateRequest createRequest = (CreateRequest) request;
          if (createRequest.altersDatabaseSchema())
          {
            if (createRequest.getDatabaseTable() != null)
            {
              DatabaseTable t = new DatabaseTable(createRequest
                  .getDatabaseTable());
              dbs.addTable(t);
              if (logger.isDebugEnabled())
                logger.debug("Added table '" + t.getName()
                    + "' to backend database schema");

              /*
               * Set the inverse relationships for the depending tables
               */
              if (t.getDependingTables() != null)
              {
                for (Iterator<?> i = t.getDependingTables().iterator(); i
                    .hasNext();)
                {
                  String rtn = (String) i.next();
                  DatabaseTable rt = dbs.getTable(rtn);
                  if (rt != null)
                  {
                    rt.addDependingTable(t.getName());
                  }
                }
              }
            }
          }
        }
        if (ControllerConstants.FORCE_SCHEMA_REFRESH_ON_CREATE_STATEMENT)
        {
          setSchemaIsDirty(true, request);
        }
      }
      else if (request.isAlter())
      {
        DatabaseSchema dbs = getDatabaseSchema();
        if (dbs != null)
        {
          AlterRequest alterRequest = (AlterRequest) request;
          if (alterRequest.altersDatabaseSchema())
          {
            // TODO optimize our parser so fewer cases are forced to refrsh
            // the schema
            setSchemaIsDirty(true, request);
          }
        }
      }
      else if (request.isDrop())
      { // Delete the table(s) from the schema
        DatabaseSchema dbs = getDatabaseSchema();
        if (dbs != null)
        {
          // DatabaseTable t = dbs.getTable(request.getTableName());
          SortedSet<?> tablesToRemove = ((DropRequest) request).getTablesToDrop();
          if (tablesToRemove != null)
            for (Iterator<?> iter = tablesToRemove.iterator(); iter.hasNext();)
            {
              String tableToRemove = (String) iter.next();
              DatabaseTable t = dbs.getTable(tableToRemove);
              if (t != null)
              {
                // Remove table from schema
                dbs.removeTable(t);
                if (logger.isDebugEnabled())
                  logger.debug("Removed table '" + t.getName()
                      + "' from backend database schema");

                // Remove table from depending tables
                if (logger.isDebugEnabled())
                  logger
                      .debug("Removing table '"
                          + t.getName()
                          + "' from dependending tables in request manager database schema");
                getDatabaseSchema().removeTableFromDependingTables(t);
              }
            }
          return;
        }
      }
      else
      {
        // Unsupported force re-fetch from db
        setSchemaIsDirty(true, request);
      }
    }
    else
    {
      // Unsupported force re-fetch from db
      setSchemaIsDirty(true, request);
    }
  }

  //
  // Rewriting rules
  //

  /**
   * Add a <code>AbstractRewritingRule</code> at the end of the rule list.
   * 
   * @param rule a AbstractRewritingRule
   */
  public void addRewritingRule(AbstractRewritingRule rule)
  {
    if (rewritingRules == null)
      rewritingRules = new ArrayList<AbstractRewritingRule>();
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("backend.rewriting.rule.add", new String[]{
          rule.getQueryPattern(), rule.getRewrite()}));
    rewritingRules.add(rule);
  }

  /**
   * Rewrite the current query according to the rewriting rules.
   * 
   * @param sqlQuery request to rewrite
   * @return the rewritten SQL query according to rewriting rules.
   */
  protected String rewriteQuery1(String sqlQuery)
  {
    if (rewritingRules == null)
      return sqlQuery;
    int size = rewritingRules.size();
    for (int i = 0; i < size; i++)
    {
      AbstractRewritingRule rule = (AbstractRewritingRule) rewritingRules
          .get(i);
      sqlQuery = rule.rewrite(sqlQuery);
      if (rule.hasMatched())
      { // Rule matched, query rewriten
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("backend.rewriting.query", sqlQuery));
        if (rule.isStopOnMatch())
          break; // Ok, stop here.
      }
    }
    return sqlQuery;
  }

  /**
   * Perform transformations on SQL query prior to shipping to underlying
   * server.
   * 
   * @param request The request that is about to be submitted
   * @return SQL after transformations have been applied.
   */
  public String transformQuery(AbstractRequest request)
  {
    // Invoke backend interceptors to alter query if desired.
    InterceptorManagerAdapter adapter = InterceptorManagerAdapter.getInstance();
    adapter.invokeBackendRequestInterceptors(this, request);

    // Invoke rewrites.
    String sqlQuery = request.getSqlOrTemplate();
    return rewriteQuery1(sqlQuery);
  }

  /*
   * Debug/Monitoring
   */

  /**
   * Adds a pending request to this backend.<br />
   * Method is synchronized on <code>pendingRequests</code> field.
   * 
   * @param request the request to add
   * @see #pendingRequests
   */
  public void addPendingReadRequest(AbstractRequest request)
  {
    synchronized (pendingRequests)
    {
      totalRequest++;
      totalReadRequest++;
      pendingRequests.add(request);
    }
  }

  /**
   * Adds a pending request to this backend.<br />
   * Method is synchronized on <code>pendingTasks</code> field.
   * 
   * @param task the task to add
   * @see #pendingTasks
   */
  public void addPendingTask(AbstractTask task)
  {
    synchronized (pendingTasks)
    {
      pendingTasks.add(task);
    }
  }

  /**
   * Check if there is a task belonging to the given transaction in the backend
   * queue.
   * 
   * @param tid transaction identifier
   * @return true if there is a task belonging to the transaction in the queue
   */
  public boolean hasTaskForTransaction(long tid)
  {
    synchronized (pendingTasks)
    {
      for (Iterator<AbstractTask> iter = pendingTasks.iterator(); iter.hasNext();)
      {
        AbstractTask task = (AbstractTask) iter.next();

        // Check if the query is in the same transaction
        if (!task.isAutoCommit() && (task.getTransactionId() == tid))
        {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Adds a pending write request to this backend.<br />
   * Method is synchronized on <code>pendingRequests</code> field.
   * 
   * @param request the request to add
   * @see #pendingRequests
   */
  public void addPendingWriteRequest(AbstractRequest request)
  {
    synchronized (pendingRequests)
    {
      totalRequest++;
      totalWriteRequest++;
      pendingRequests.add(request);
    }
  }

  /**
   * @return Returns the activeTransactions.
   */
  public ArrayList<Long> getActiveTransactions()
  {
    return activeTransactions;
  }

  /**
   * Get data about this backend. Format is:
   * 
   * <pre>
   * data[0] = this.name;
   * data[1] = this.driverClassName;
   * data[2] = this.url;
   * data[3] = String.valueOf(this.activeTransactions.size());
   * data[4] = String.valueOf(this.pendingRequests.size());
   * data[5] = String.valueOf(this.isReadEnabled());
   * data[6] = String.valueOf(this.isWriteEnabled());
   * data[7] = String.valueOf(this.isInitialized());
   * data[8] = String.valueOf(this.schemaIsStatic);
   * data[9] = String.valueOf(this.connectionManagers.size());
   * data[10] = String.valueOf(getTotalActiveConnections());
   * data[11] = String.valueOf(totalRequest);
   * data[12] = String.valueOf(totalTransactions);
   * data[13] = lastKnownCheckpoint;
   *</pre>
   * 
   * @return an array of strings
   */
  public String[] getBackendData()
  {
    String[] data = new String[14];
    data[0] = this.name;
    data[1] = this.driverClassName;
    data[2] = this.url;
    data[3] = String.valueOf(this.activeTransactions.size());
    data[4] = String.valueOf(this.pendingRequests.size());
    data[5] = String.valueOf(this.isReadEnabled());
    data[6] = String.valueOf(this.isWriteEnabled());
    try
    {
      data[7] = String.valueOf(this.isInitialized());
    }
    catch (Exception e)
    {
      data[7] = "unknown";
    }
    data[8] = String.valueOf(this.schemaIsStatic);

    data[9] = String.valueOf(this.connectionManagers.size());
    data[10] = String.valueOf(getTotalActiveConnections());
    data[11] = String.valueOf(totalRequest);
    data[12] = String.valueOf(totalTransactions);
    if (lastKnownCheckpoint == null || lastKnownCheckpoint.equalsIgnoreCase(""))
      data[13] = "<unknown>";
    else
      data[13] = lastKnownCheckpoint;
    return data;
  }

  /**
   * Get the statistics of the backend.
   * 
   * @return a BackendStatistics
   */
  public BackendStatistics getBackendStats()
  {
    BackendStatistics stats = new BackendStatistics();
    stats.setBackendName(name);
    stats.setDriverClassName(driverClassName);
    stats.setUrl(url);
    stats.setNumberOfActiveTransactions(activeTransactions.size());
    stats.setNumberOfPendingRequests(pendingRequests.size());
    stats.setNumberOfPersistentConnections(persistentConnections.size());
    stats.setReadEnabled(isReadEnabled());
    stats.setWriteEnabled(isWriteEnabled());
    String initializationStatus = "<unknown>";
    try
    {
      initializationStatus = String.valueOf(this.isInitialized());
    }
    catch (Exception e)
    {
    }
    stats.setInitializationStatus(initializationStatus);
    stats.setSchemaStatic(schemaIsStatic);
    stats.setNumberOfConnectionManagers(connectionManagers.size());
    stats.setNumberOfTotalActiveConnections(getTotalActiveConnections());
    stats.setNumberOfTotalRequests(totalRequest);
    stats.setNumberOfTotalTransactions(totalTransactions);
    if (lastKnownCheckpoint == null || lastKnownCheckpoint.equalsIgnoreCase(""))
      stats.setLastKnownCheckpoint("<unknown>");
    else
      stats.setLastKnownCheckpoint(lastKnownCheckpoint);
    return stats;
  }

  /**
   * Returns the list of pending requests for this backend.
   * 
   * @return <code>Vector</code> of <code>AbstractRequests</code> or
   *         <code>AbstractTask</code> objects
   */
  public Vector<AbstractRequest> getPendingRequests()
  {
    return pendingRequests;
  }

  /**
   * Returns the list of pending requests for this backend.
   * 
   * @param count number of requests to retrieve, if 0, return all.
   * @param fromFirst count the request from first if true, or from last if
   *          false
   * @param clone should clone the pending request if true, block it if false
   * @return <code>ArrayList</code> of <code>String</code> description of
   *         each request.
   */
  public ArrayList<String> getPendingRequestsDescription(int count, boolean fromFirst,
      boolean clone)
  {
    int size = pendingRequests.size();
    int limit = (count == 0 || count > size) ? size : Math.min(size, count);
    ArrayList<String> list = new ArrayList<String>(limit);
    int start = (fromFirst) ? 0 : Math.min(limit - count, 0);
    if (!clone)
    {
      synchronized (pendingRequests)
      {
        for (int i = start; i < limit; i++)
          list.add(pendingRequests.get(i).toString());
      }
      return list;
    }
    else
    {
      @SuppressWarnings("unchecked")
	Vector<AbstractRequest> cloneVector = (Vector<AbstractRequest>) pendingRequests.clone();
      for (int i = start; i < limit; i++)
        list.add(cloneVector.get(i).toString());
      return list;
    }
  }

  /**
   * Get the total number of active connections for this backend
   * 
   * @return number of active connections for all
   *         <code>AbstractConnectionManager</code> connected to this backend
   */
  public long getTotalActiveConnections()
  {
    int activeConnections = 0;
    Iterator<String> iter = connectionManagers.keySet().iterator();
    while (iter.hasNext())
      activeConnections += ((AbstractConnectionManager) connectionManagers
          .get(iter.next())).getCurrentNumberOfConnections();
    return activeConnections;
  }

  /**
   * Returns the total number of transactions executed by this backend.
   * 
   * @return Total number of transactions.
   */
  public int getTotalTransactions()
  {
    return totalTransactions;
  }

  /**
   * Returns the total number of read requests executed by this backend.
   * 
   * @return Returns the totalReadRequest.
   */
  public int getTotalReadRequest()
  {
    return totalReadRequest;
  }

  /**
   * Returns the total number of write requests executed by this backend.
   * 
   * @return Returns the totalWriteRequest.
   */
  public int getTotalWriteRequest()
  {
    return totalWriteRequest;
  }

  /**
   * Returns the total number of requests executed by this backend.
   * 
   * @return Returns the totalRequest.
   */
  public int getTotalRequest()
  {
    return totalRequest;
  }

  /**
   * Removes a pending request from this backend. Note that the underlying
   * vector is synchronized.
   * 
   * @param request the request to remove
   * @return <code>true</code> if the request has been found and removed
   */
  public boolean removePendingRequest(AbstractRequest request)
  {
    return pendingRequests.remove(request);
  }

  /**
   * Removes a pending task from this backend. Note that the underlying vector
   * is synchronized.
   * 
   * @param task the task to remove
   * @return <code>true</code> if the task has been found and removed
   */
  public boolean removePendingTask(AbstractTask task)
  {
    return pendingTasks.remove(task);
  }

  //
  // Getters/Setters
  //

  /**
   * Returns the databaseProductName value.
   * 
   * @return Returns the databaseProductName.
   */
  public String getDatabaseProductName()
  {
    return driverCompliance.getDatabaseProductName();
  }

  /**
   * @return the driver compliance to Sequoia requirements.
   */
  public DriverCompliance getDriverCompliance()
  {
    return driverCompliance;
  }

  /**
   * Returns the driver path.
   * 
   * @return the driver path
   */
  public String getDriverPath()
  {
    return driverPath;
  }

  /**
   * Returns the database native JDBC driver class name.
   * 
   * @return the driver class name
   */
  public String getDriverClassName()
  {
    return driverClassName;
  }

  /**
   * Returns the backend logical name.
   * 
   * @return the backend logical name
   */
  public String getName()
  {
    return name;
  }

  /**
   * Returns the full qualified name of this backend.
   * 
   * @return a String representing the full qualified name of this backend.
   */
  private String getQualifiedName()
  {
    return virtualDatabaseName + "." + name;
  }

  /**
   * Return the sql short form length to use when reporting an error.
   * 
   * @return sql short form length
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getSqlShortForm(int)
   */
  public int getSqlShortFormLength()
  {
    return sqlShortFormLength;
  }

  /**
   * Returns the JDBC URL used to access the database.
   * 
   * @return a JDBC URL
   */
  public String getURL()
  {
    return url;
  }

  /**
   * Returns the taskQueues value.
   * 
   * @return Returns the taskQueues.
   */
  public BackendTaskQueues getTaskQueues()
  {
    return taskQueues;
  }

  /**
   * Sets the taskQueues value.
   * 
   * @param taskQueues The taskQueues to set.
   */
  public void setTaskQueues(BackendTaskQueues taskQueues)
  {
    this.taskQueues = taskQueues;
  }

  /**
   * Returns the virtual database name this backend belongs to.
   * 
   * @return Returns the virtual database name.
   */
  public String getVirtualDatabaseName()
  {
    return virtualDatabaseName;
  }

  //
  // XML mapping
  //

  /**
   * Get xml information about this backend.
   * 
   * @return xml formatted information on this database backend.
   */
  public synchronized String getXml()
  {
    // escape & XML entity and replace it by &amp; so that the
    // url attribute is XML compliant
    String escapedUrl = url.replaceAll("&", "&amp;");

    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_DatabaseBackend + " "
        + DatabasesXmlTags.ATT_name + "=\"" + name + "\" "
        + DatabasesXmlTags.ATT_driver + "=\"" + driverClassName + "\" "
        + DatabasesXmlTags.ATT_url + "=\"" + escapedUrl + "\" "
        + DatabasesXmlTags.ATT_connectionTestStatement + "=\""
        + connectionTestStatement + "\">");

    boolean expandSchema = this.schema != null
        && dynamicPrecision == DatabaseBackendSchemaConstants.DynamicPrecisionStatic;

    info.append(getSchemaXml(expandSchema));

    if (rewritingRules != null)
    {
      int size = rewritingRules.size();
      for (int i = 0; i < size; i++)
        info.append(((AbstractRewritingRule) rewritingRules.get(i)).getXml());
    }
    if (connectionManagers != null)
    {
      if (connectionManagers.isEmpty() == false)
      {
        AbstractConnectionManager connectionManager;
        Iterator<AbstractConnectionManager> iter = connectionManagers.values().iterator();
        while (iter.hasNext())
        {
          connectionManager = (AbstractConnectionManager) iter.next();
          info.append(connectionManager.getXml());
        }
      }
    }
    info.append("</" + DatabasesXmlTags.ELT_DatabaseBackend + ">");
    return info.toString();
  }

  /**
   * The getXml() method does not return the schema if it is not static anymore,
   * to avoid confusion between static and dynamic schema. This method returns a
   * static view of the schema, whatever the dynamic precision is.
   * 
   * @param expandSchema if we should force the schema to be expanded. This is
   *          needed as the default getXml should call this method.
   * @return an xml formatted string
   */
  public String getSchemaXml(boolean expandSchema)
  {
    StringBuffer info = new StringBuffer();
    info.append("<"
        + DatabasesXmlTags.ELT_DatabaseSchema
        + " "
        + DatabasesXmlTags.ATT_dynamicPrecision
        + "=\""
        + DatabaseBackendSchemaConstants
            .getDynamicSchemaLevel(dynamicPrecision) + "\" "
        + DatabasesXmlTags.ATT_gatherSystemTables + "=\""
        + (gatherSystemTables ? "true" : "false") + "\">");
    synchronized (this)
    {
      if (expandSchema && (schema != null))
        info.append(schema.getXml());
    }
    info.append("</" + DatabasesXmlTags.ELT_DatabaseSchema + ">");
    return info.toString();
  }

  /**
   * Sets the sqlShortFormLength value.
   * 
   * @param sqlShortFormLength The sqlShortFormLength to set.
   */
  public void setSqlShortFormLength(int sqlShortFormLength)
  {
    this.sqlShortFormLength = sqlShortFormLength;
  }

  /**
   * String description of the backend and its current state
   * 
   * @return a string description of the backend.
   */
  public String toString()
  {
    return "Backend: Name[" + this.name + "] State["
        + BackendState.description(state) + "] JDBCConnected["
        + isJDBCConnected() + "] ActiveTransactions["
        + activeTransactions.size() + ": " + activeTransactions.toString()
        + "] PersistentConnections[" + persistentConnections
        + "] PendingRequests[" + pendingRequests.size() + "]";
  }

  //
  // Worker threads management
  //

  /**
   * Return the first available BackendWorkerThread. This should only be used
   * for notification of request abort.
   * 
   * @return a BackendWorkerThread or null if none exists
   */
  public BackendWorkerThread getBackendWorkerThreadForNotification()
  {
    if ((workerThreads == null) || workerThreads.isEmpty())
      return null;
    return (BackendWorkerThread) workerThreads.get(0);
  }

  /**
   * Returns the nbOfWorkerThreads value.
   * 
   * @return Returns the nbOfWorkerThreads.
   */
  public int getNbOfWorkerThreads()
  {
    return nbOfWorkerThreads;
  }

  /**
   * Sets the nbOfWorkerThreads value.
   * 
   * @param nbOfWorkerThreads The nbOfWorkerThreads to set.
   */
  // TODO nobody uses this method. Should be removed
  public void setNbOfWorkerThreads(int nbOfWorkerThreads)
  {
    this.nbOfWorkerThreads = nbOfWorkerThreads;
  }

  /**
   * Start a new Deadlock Detection Thread (throws a RuntimeException if called
   * twice without stopping the thread before the second call).
   * 
   * @param vdb the virtual database the backend is attached to
   */
  public void startDeadlockDetectionThread(VirtualDatabase vdb)
  {
    taskQueues.startDeadlockDetectionThread(vdb);

  }

  /**
   * Start the BackendWorkerThreads for this backend.
   * 
   * @param loadBalancer load balancer requesting the activation
   */
  public void startWorkerThreads(AbstractLoadBalancer loadBalancer)
  {
    taskQueues.setAllowTasksToBePosted(true);
    synchronized (workerThreadSync)
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "loadbalancer.backend.workerthread.starting", new String[]{
                String.valueOf(nbOfWorkerThreads), name}));

      if (workerThreads == null)
        workerThreads = new ArrayList<BackendWorkerThread>();
      // Create worker threads
      for (int i = 0; i < nbOfWorkerThreads; i++)
      {
        BackendWorkerThread thread = new BackendWorkerThread(this, loadBalancer);
        workerThreads.add(thread);
        // Dedicate the first thread to commit/rollback operations
        thread.setPlayCommitRollbackOnly(i == 0);
        thread.start();
      }
    }
  }

  /**
   * Terminate all worker threads.
   */
  public void terminateWorkerThreads()
  {
    terminateWorkerThreads(true);
  }

  /**
   * Terminate all worker threads.
   * 
   * @param wait if true waits for worker threads to terminate before returning
   */
  public void terminateWorkerThreads(boolean wait)
  {
    synchronized (workerThreadSync)
    {
      if (workerThreads == null)
        return;

      // Terminate worker threads by posting a kill task for each of them
      int size = workerThreads.size();

      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "loadbalancer.backend.workerthread.stopping", new String[]{
                String.valueOf(size), name}));

      for (int i = 0; i < size; i++)
      {
        KillThreadTask killBlockingThreadTask = new KillThreadTask(1, 1);
        taskQueues.addTaskToBackendTotalOrderQueue(killBlockingThreadTask);
      }

      if (wait)
        // Wait for thread termination
        for (Iterator<BackendWorkerThread> iter = workerThreads.iterator(); iter.hasNext();)
        {
          BackendWorkerThread thread = (BackendWorkerThread) iter.next();
          if (thread != Thread.currentThread())
          { // Do not try to wait for self if we are the one who started the
            // disabling.
            try
            {
              thread.join();
            }
            catch (InterruptedException ignore)
            {
            }
          }
        }

      // Remove the threads from the list
      workerThreads.clear();

      // Cleanup what could remain in the queues
      taskQueues.abortRemainingRequests();
    }
  }

  /**
   * Terminate the Deadlock Detection Thread. Throws a RuntimeException is the
   * thread was already stopped (or not started).
   */
  public void terminateDeadlockDetectionThread()
  {
    taskQueues.terminateDeadlockDetectionThread();
  }

  /**
   * Convert a <code>&lt;DatabaseBackend&gt;List</code> to a
   * <code>&lt;BackendInfo&gt;List</code>.
   * <p>
   * The DatabaseBackend objects cannot be serialized because they are used as
   * MBean and notification emitters, so we want to extract the BackendInfo
   * (which are <code>Serializable</code>) out of them.
   * </p>
   * <p>
   * <strong>This method does not keep the XML configuration of the BackendInfo
   * objects. Subsequent calls to getXml() on BackendInfos returned by this
   * method will always return <code>null</code>
   * </p>
   * 
   * @param backends a <code>List</code> of <code>DatabaseBackends</code>
   * @return a <code>List</code> of <code>BackendInfo</code> (possibly empty
   *         if the list of backends was <code>null</code>
   * @see BackendInfo#toDatabaseBackends(List)
   */
  public static/* <BackendInfo> */List<BackendInfo> toBackendInfos(
  /* <DatabaseBackend> */List<?> backends)
  {
    if (backends == null)
    {
      return new ArrayList<BackendInfo>();
    }
    List<BackendInfo> backendInfos = new ArrayList<BackendInfo>(backends.size());
    for (Iterator<?> iter = backends.iterator(); iter.hasNext();)
    {
      DatabaseBackend backend = (DatabaseBackend) iter.next();
      BackendInfo backendInfo = new BackendInfo(backend);
      // we do not keep the XML configuration in the BackendInfo
      // FIXME I did that to mimic current behavior but I don't see the
      // reason why (maybe the size of XML to transfer on the wire?)
      backendInfo.setXml(null);
      backendInfos.add(backendInfo);
    }
    return backendInfos;
  }

  /**
   * Check if the given vdb user is a valid user for this backend.
   * 
   * @param vdbUser to be checked.
   * @return true if the vdb user is valid, false otherwise.
   */
  public boolean isValidBackendUser(VirtualDatabaseUser vdbUser)
  {
    Connection conn = null;
    try
    {
      conn = DriverManager.getConnection(url, vdbUser.getLogin(), vdbUser
          .getPassword(), driverPath, driverClassName);
      return true;
    }
    catch (SQLException ignore)
    {
      if (logger.isDebugEnabled())
      {
        logger.debug("Failed to get connection using vdb user "
            + vdbUser.getLogin() + " as real user", ignore);
      }
      return false;
    }
    finally
    {
      if (conn != null)
      {
        try
        {
          conn.close();
        }
        catch (SQLException ignore)
        {
          // Silently ignore
        }
      }
    }
  }

  /**
   * Get the log4j logger for this backend
   * 
   * @return the logger for this backend
   */
  public Trace getLogger()
  {
    return logger;
  }
}
