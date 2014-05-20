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
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.controller.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.LinkedList;

import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * A <code>ConnectionManager</code> object is responsible to talk directly
 * with a database backend.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Stephane.Giron@continuent.com">Stephane Giron </a>
 * @version 1.0
 */
public abstract class AbstractConnectionManager
    implements
      XmlComponent,
      Cloneable
{
  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructors/Destructors
  // 3. Connection handling
  // 4. Getter/Setter (possibly in alphabetical order)
  //

  /** Logger instance. */
  static Trace                                    logger                              = Trace
                                                                                          .getLogger("org.continuent.sequoia.controller.connection");

  /** URL of the <code>DatabaseBackend</code> owning this connection manager. */
  protected String                                backendUrl;

  /**
   * Name of the <code>DatabaseBackend</code> owning this connection manager.
   */
  protected String                                backendName;

  /** Backend connection login to be used by this connection manager. */
  protected String                                rLogin;

  /** Backend connection password to be used by this connection manager. */
  protected String                                rPassword;

  /** The class name of the driver */
  protected String                                driverClassName;

  /**
   * The path to the driver if null the default directory is used
   */
  protected String                                driverPath;

  /** <code>true</code> if the connection pool has been initialized. */
  protected boolean                               initialized;

  /**
   * <code>true</code> if the connection manager has been shutdown and should
   * no longer hand out Connections.
   */
  protected boolean                               isShutdown;

  /** Hastable of connections associated to a transaction (tid->Connection) */
  private transient Hashtable<Long, PooledConnection>                     connectionForTransaction;

  /**
   * Hashtable&lt;Long, PooledConnection&gt; which associates a persistent
   * connection id (encapsulated in a Long) with a PooledConnection.
   */
  protected transient Hashtable<Long, PooledConnection>                   persistentConnections;

  /** Virtual Login used this connection manager */
  private String                                  vLogin;

  protected String                                connectionTestStatement;

  /**
   * Stores the time on which persistent connections have been last used.
   */
  protected LinkedList<Long>                            persistentConnectionsIdleTime;

  /**
   * Stack of idle persistent connections
   */
  protected LinkedList<PooledConnection>                            idlePersistentConnections;

  /**
   * Allow to check idle persistent connections against the backend.
   */
  protected IdlePersistentConnectionsPingerThread persistentConnectionPingerThread;

  /**
   * Time after which an idle persistent connection will be checked.
   */
  protected int                                   idlePersistentConnectionPingInterval;

  /**
   * Indicates if the idle persistent connections checker thread is running.
   */
  protected boolean                               idlePersistentConnectionPingRunning = false;

  /*
   * Constructor(s)
   */

  /**
   * Creates a new <code>AbstractConnectionManager</code> instance: assigns
   * login/password and instanciates transaction id/connection mapping.
   * 
   * @param backendUrl URL of the <code>DatabaseBackend</code> owning this
   *          connection manager
   * @param backendName name of the <code>DatabaseBackend</code> owning this
   *          connection manager
   * @param rLogin backend connection login to be used by this connection
   *          manager
   * @param rPassword backend connection password to be used by this connection
   *          manager
   * @param driverPath path for driver
   * @param driverClassName class name for driver
   */
  protected AbstractConnectionManager(String backendUrl, String backendName,
      String rLogin, String rPassword, String driverPath, String driverClassName)
  {
    if (backendUrl == null)
      throw new IllegalArgumentException(
          "Illegal null database backend URL in AbstractConnectionManager constructor");

    if (backendName == null)
      throw new IllegalArgumentException(
          "Illegal null database backend name in AbstractConnectionManager constructor");

    if (rLogin == null)
      throw new IllegalArgumentException(
          "Illegal null database backend login in AbstractConnectionManager constructor");

    if (rPassword == null)
      throw new IllegalArgumentException(
          "Illegal null database backend password in AbstractConnectionManager constructor");

    if (driverPath != null)
    {
      if (driverClassName == null)
      {
        throw new IllegalArgumentException(
            "Illegal null database backend driverClassName in AbstractConnectionManager constructor");
      }
    }
    this.backendUrl = backendUrl;
    this.backendName = backendName;
    this.rLogin = rLogin;
    this.rPassword = rPassword;
    this.driverPath = driverPath;
    this.driverClassName = driverClassName;
    connectionForTransaction = new Hashtable<Long, PooledConnection>();
    persistentConnections = new Hashtable<Long, PooledConnection>();

    // Get the IDLE_PERSISTENT_CONNECTION_PING_INTERVAL and convert it in ms
    idlePersistentConnectionPingInterval = ControllerConstants.IDLE_PERSISTENT_CONNECTION_PING_INTERVAL * 1000;
    if (idlePersistentConnectionPingInterval > 0)
    {
      this.idlePersistentConnections = new LinkedList<PooledConnection>();
      this.persistentConnectionsIdleTime = new LinkedList<Long>();
    }

  }

  /**
   * Ensures that the connections are closed when the object is garbage
   * collected.
   * 
   * @exception Throwable if an error occurs.
   */
  protected void finalize() throws Throwable
  {
    if (isInitialized())
      finalizeConnections();

    super.finalize();
  }

  /**
   * @see java.lang.Object#clone()
   */
  protected abstract Object clone() throws CloneNotSupportedException;

  /**
   * Creates a new connection manager with the same parameters but a different
   * backend user and password.
   * 
   * @param rLogin real login
   * @param rPassword real password
   * @return an AbstractConnectionManager for the new user
   */
  public abstract AbstractConnectionManager clone(String rLogin,
      String rPassword);

  /**
   * Copy this connection manager and replace the name of the backend and its
   * url Every other parameter is the same
   * 
   * @param url the url to the backend associated to this ConnectionManager
   * @param name the name of the backend
   * @return <code>AbstractConnectionManager</code>
   * @throws Exception if clone fails
   */
  public AbstractConnectionManager copy(String url, String name)
      throws Exception
  {
    AbstractConnectionManager connectionManager = (AbstractConnectionManager) this
        .clone();
    connectionManager.backendName = name;
    connectionManager.backendUrl = url;
    return connectionManager;
  }

  /*
   * Connection handling
   */

  /**
   * Delete a connection that is no more valid.
   * 
   * @param connection the connection to delete.
   */
  public abstract void deleteConnection(PooledConnection connection);

  /**
   * Close an unwanted connection. The connection may be in a bad state so we
   * ignore all errors.
   * 
   * @param connection to be closed
   */
  protected void closeConnection(PooledConnection connection)
  {
    try
    {
      connection.close();
    }
    catch (SQLException e)
    {
      logger.error(e.getMessage());
    }
  }

  /**
   * Delete a bad connection that was used for a transaction. The corresponding
   * connection is deleted by calling
   * {@link #deleteConnection(PooledConnection)}.
   * 
   * @param transactionId the transaction id.
   * @see #releaseConnection(PooledConnection)
   */
  public void deleteConnection(long transactionId)
  {
    // Underlying Hashtable is synchronized
    PooledConnection c = (PooledConnection) connectionForTransaction
        .remove(new Long(transactionId));

    if (c == null)
      logger.error(Translate.get("connection.transaction.unknown",
          transactionId));
    else
      deleteConnection(c);
  }

  /**
   * Deletes a connection associated to the persistentConnectionId.<br />
   * Do nothing if there is no connection associated to the
   * persistentConnectionId
   * 
   * @param persistentConnectionId the id of the persistent connection to delete
   */
  public void deletePersistentConnection(long persistentConnectionId)
  {
    Long connectionId = new Long(persistentConnectionId);
    if (idlePersistentConnectionPingRunning)
    {
      PooledConnection c = (PooledConnection) persistentConnections
          .get(connectionId);
      synchronized (this)
      {
        if (idlePersistentConnections.contains(c))
        {
          persistentConnectionsIdleTime.remove(idlePersistentConnections
              .indexOf(c));
          idlePersistentConnections.remove(c);
        }
      }
    }
    persistentConnections.remove(connectionId);
  }

  /**
   * Releases all the connections to the database.
   * 
   * @exception SQLException if an error occurs.
   */
  public final void finalizeConnections() throws SQLException
  {
    connectionForTransaction.clear();
    doConnectionFinalization();
    if (idlePersistentConnectionPingRunning)
    {
      stopPersistentConnectionPingerThread();
      synchronized (this)
      {
        idlePersistentConnections.clear();
        persistentConnectionsIdleTime.clear();
      }
    }
  }

  /**
   * Must be implemented by class extending AbstractConnectionManager to
   * finalize their connections.
   * 
   * @throws SQLException if an error occurs
   */
  protected abstract void doConnectionFinalization() throws SQLException;

  /**
   * Force all connections to be renewed when they are used next. This just sets
   * a flag and connections will be lazily replaced as needed.
   */
  public abstract void flagAllConnectionsForRenewal();

  /**
   * Get a connection from DriverManager.
   * 
   * @return a new connection or null if Driver.getConnection() failed.
   * @see DriverManager#getConnection(String, String, String, String, String)
   */
  public Connection getConnectionFromDriver()
  {
    try
    {
      return DriverManager.getConnection(backendUrl, rLogin, rPassword,
          driverPath, driverClassName);
    }
    catch (SQLException ignore)
    {
      if (logger.isWarnEnabled())
      {
        logger.warn("Failed to get connection for driver ", ignore);
      }
      return null;
    }
  }

  /**
   * Gets a connection from the pool (implementation specific).
   * 
   * @return a <code>Connection</code> or <code>null</code> if no connection
   *         is available or if the connection has not been initialized.
   * @throws UnreachableBackendException if the backend must be disabled
   */
  protected abstract PooledConnection getConnection()
      throws UnreachableBackendException;

  /**
   * Releases a connection.
   * 
   * @param connection the connection to release.
   */
  protected abstract void releaseConnection(PooledConnection connection);

  /**
   * Releases a persistent connection. This will close this connection
   * immediatly.
   * 
   * @param connection the connection to release.
   */
  protected abstract void releasePersistentConnection(
      PooledConnection connection);

  /**
   * Gets a connection from the pool in autocommit mode. If connections needed
   * to be renewed, they are renewed here.
   * 
   * @return a <code>PooledConnection</code> or <code>null</code> if no
   *         connection is available or if the connection has not been
   *         initialized.
   * @throws UnreachableBackendException if the backend must be disabled
   */
  private PooledConnection getRenewedConnectionInAutoCommit()
      throws UnreachableBackendException
  {
    PooledConnection c;
    while (true)
    { // Loop until all connections that must be renewed have been renewed
      c = getConnection();
      if (c != null)
      {
        if (c.mustBeRenewed())
          deleteConnection(c);
        else
          break;
      }
      else
        break;
    }

    return c;
  }

  /**
   * Gets a new connection for a transaction. This function calls
   * {@link #getConnection()}to get the connection and store the mapping
   * between the connection and the transaction id.
   * <p>
   * Note that this function does not start the transaction, it is up to the
   * caller to call setAutoCommit(false) on the returned connection.
   * 
   * @param transactionId the transaction id.
   * @return a <code>Connection</code> or <code>null</code> if no connection
   *         is available .
   * @throws UnreachableBackendException if the backend must be disabled
   * @see #getConnection()
   */
  public PooledConnection getConnectionForTransaction(long transactionId)
      throws UnreachableBackendException
  {
    PooledConnection c = getRenewedConnectionInAutoCommit();
    registerConnectionForTransaction(c, transactionId);
    return c;
  }

  /**
   * Initializes the connection(s) to the database. The caller must ensure that
   * the driver has already been loaded else an exception will be thrown.
   * 
   * @exception SQLException if an error occurs.
   */
  public final void initializeConnections() throws SQLException
  {
    isShutdown = false;
    connectionForTransaction.clear();
    doConnectionInitialization();
  }

  /**
   * Mark the ConnectionManager as shutdown. getConnection() should return null
   * from now on and any threads waiting in getConnection() should wake up. This
   * is used when a database is shutting down due to an error. It is not needed
   * in the case of a clean shutdown, because everybody will stop requesting
   * connections at the correct time.
   */
  public synchronized void shutdown()
  {
    isShutdown = true;
    notifyAll();
  }

  /**
   * Must be implemented by class extending AbstractConnectionManager to
   * initalize their connections.
   * 
   * @throws SQLException if an error occurs
   */
  protected abstract void doConnectionInitialization() throws SQLException;

  /**
   * Enlist a connection for a transaction (maintains the transaction id <->
   * connection mapping).
   * 
   * @param c a pooled connection
   * @param transactionId the transaction id to register this connection for
   */
  public void registerConnectionForTransaction(PooledConnection c,
      long transactionId)
  {
    if (c != null)
    {
      // Underlying Hashtable is synchronized
      Long lTid = new Long(transactionId);
      if (connectionForTransaction.put(lTid, c) != null)
      {
        logger
            .error("A new connection for transaction "
                + lTid
                + " has been opened but there was a remaining connection for this transaction that has not been closed.");
      }
    }
  }

  /**
   * Retrieves a connection used for a transaction. This connection must have
   * been allocated by calling {@link #getConnectionForTransaction(long)}.
   * 
   * @param transactionId the transaction id.
   * @return a <code>Connection</code> or <code>null</code> if no connection
   *         has been found for this transaction id.
   * @see #getConnectionForTransaction(long)
   */
  public PooledConnection retrieveConnectionForTransaction(long transactionId)
  {
    Long id = new Long(transactionId);
    // Underlying Hashtable is synchronized
    return (PooledConnection) connectionForTransaction.get(id);
  }

  /**
   * Releases a connection used for a transaction. The corresponding connection
   * is released by calling {@link #releaseConnection(PooledConnection)}.
   * 
   * @param transactionId the transaction id.
   * @see #releaseConnection(PooledConnection)
   */
  public void releaseConnectionForTransaction(long transactionId)
  {
    // Underlying Hashtable is synchronized
    PooledConnection c = (PooledConnection) connectionForTransaction
        .remove(new Long(transactionId));

    if (c == null)
      logger.error(Translate.get("connection.transaction.unknown",
          transactionId));
    else
    {
      if (!c.isDefaultTransactionIsolation())
      { // Reset transaction isolation (SEQUOIA-532)
        try
        {
          c.restoreDefaultTransactionIsolation();
        }
        catch (Throwable e)
        {
          logger.error("Error while resetting transaction ", e);
        }
      }

      // Only release the connection if it is not persistent
      if (!persistentConnections.containsValue(c))
        releaseConnection(c);
    }
  }

  /**
   * Gets a connection from the pool in autocommit mode. If a persistent id is
   * defined for the request, the appropriate connection is retrieved.
   * 
   * @param request the request asking for a connection (we will check its
   *          persistent connection id)
   * @return a <code>PooledConnection</code> or <code>null</code> if no
   *         connection is available or if the connection has not been
   *         initialized.
   * @throws UnreachableBackendException if the backend must be disabled
   */
  public PooledConnection retrieveConnectionInAutoCommit(AbstractRequest request)
      throws UnreachableBackendException
  {
    if ((request != null) && request.isPersistentConnection())
    {
      Long id = new Long(request.getPersistentConnectionId());
      // Underlying Hashtable is synchronized
      PooledConnection c = (PooledConnection) persistentConnections.get(id);
      if (c == null)
      {
        c = getRenewedConnectionInAutoCommit();
        if (c != null)
          persistentConnections.put(id, c);
      }
      if (idlePersistentConnectionPingRunning)
        synchronized (this)
        {
          if (idlePersistentConnections.contains(c))
          {
            persistentConnectionsIdleTime.remove(idlePersistentConnections
                .indexOf(c));
            idlePersistentConnections.remove(c);
          }
        }

      return c;
    }
    else
      return getRenewedConnectionInAutoCommit();
  }

  /**
   * Releases a connection if it is not persistent. The corresponding connection
   * is released by calling {@link #releaseConnection(PooledConnection)}.
   * 
   * @param request the request asking for a connection (we will check its
   *          persistent connection id)
   * @param c the connection to release.
   * @see #releaseConnection(PooledConnection)
   */
  public void releaseConnectionInAutoCommit(AbstractRequest request,
      PooledConnection c)
  {
    if ((request != null) && request.isPersistentConnection())
    {
      if (idlePersistentConnectionPingRunning)
      {
        boolean notifyPersistentConnectionPingerThread = false;
        synchronized (this)
        {
          persistentConnectionsIdleTime.addLast(new Long(System
              .currentTimeMillis()));
          idlePersistentConnections.addLast(c);
          notifyPersistentConnectionPingerThread = persistentConnectionsIdleTime
              .size() > 0;
        }

        if (notifyPersistentConnectionPingerThread)
          notifyPersistentConnectionPingerThread();
      }
      return;
    }

    try
    {
      if (!c.getConnection().getAutoCommit())
      {
        c.getConnection().setAutoCommit(true);
        if (logger.isDebugEnabled())
        {
          Exception e = new Exception("Connection returned with autocommit off");
          logger.debug(e);
        }
      }
    }
    catch (SQLException e)
    {
      // ignore this for now somebody will deal with it later
    }
    releaseConnection(c);
  }

  protected void notifyPersistentConnectionPingerThread()
  {
    synchronized (persistentConnectionPingerThread)
    {
      persistentConnectionPingerThread.notify();
    }
  }

  /**
   * Releases a connection used for a persistent connection. The corresponding
   * connection is released by calling
   * {@link #releaseConnection(PooledConnection)}.
   * 
   * @param persistentConnectionId the persistent connection id.
   * @see #releaseConnection(PooledConnection)
   */
  public void releasePersistentConnectionInAutoCommit(
      long persistentConnectionId)
  {
    // Underlying Hashtable is synchronized
    PooledConnection c = (PooledConnection) persistentConnections
        .remove(new Long(persistentConnectionId));

    if (c == null)
      logger.error(Translate.get("connection.persistent.id.unknown",
          persistentConnectionId));
    else
    {
      if (idlePersistentConnectionPingRunning)
        synchronized (this)
        {
          if (idlePersistentConnections.contains(c))
          {
            persistentConnectionsIdleTime.remove(idlePersistentConnections
                .indexOf(c));
            idlePersistentConnections.remove(c);
          }
        }
      releasePersistentConnection(c);
    }
  }

  /**
   * Tests if the connections have been initialized.
   * 
   * @return <code>true</code> if the connections have been initialized.
   */
  public boolean isInitialized()
  {
    return initialized;
  }

  /*
   * Getter/setter methods
   */

  /**
   * Get the current number of connections open for this connection manager.
   * 
   * @return the current number of open connections
   */
  public abstract int getCurrentNumberOfConnections();

  /**
   * Returns the driverClassName value.
   * 
   * @return Returns the driverClassName.
   */
  public String getDriverClassName()
  {
    return driverClassName;
  }

  /**
   * Returns the driverPath value.
   * 
   * @return Returns the driverPath.
   */
  public String getDriverPath()
  {
    return driverPath;
  }

  /**
   * Returns the login used by this connection manager.
   * 
   * @return a <code>String</code> value.
   */
  public String getLogin()
  {
    return rLogin;
  }

  /**
   * Returns the password used by this connection manager.
   * 
   * @return a <code>String</code> value.
   */
  public String getPassword()
  {
    return rPassword;
  }

  /*
   * Debug/monitoring information
   */

  /**
   * @return Returns the vLogin.
   */
  public String getVLogin()
  {
    return vLogin;
  }

  /**
   * @param login The vLogin to set.
   */
  public void setVLogin(String login)
  {
    vLogin = login;
  }

  /**
   * Set the SQL string used to test whether or not the server is available.
   * 
   * @param connectionTestStatement connection test statement to use for this
   *          pool
   */
  public void setConnectionTestStatement(String connectionTestStatement)
  {
    this.connectionTestStatement = connectionTestStatement;
  }

  /**
   * Gets xml formatted information on this connection manager
   * 
   * @return xml formatted string that conforms to sequoia.dtd
   */
  protected abstract String getXmlImpl();

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_ConnectionManager + " "
        + DatabasesXmlTags.ATT_vLogin + "=\"" + vLogin + "\"  " + ""
        + DatabasesXmlTags.ATT_rLogin + "=\"" + rLogin + "\"  " + ""
        + DatabasesXmlTags.ATT_rPassword + "=\"" + rPassword + "\"  " + ">");
    info.append(this.getXmlImpl());
    info.append("</" + DatabasesXmlTags.ELT_ConnectionManager + ">");
    return info.toString();
  }

  protected void stopPersistentConnectionPingerThread()
  {
    synchronized (persistentConnectionPingerThread)
    {
      persistentConnectionPingerThread.isKilled = true;
      idlePersistentConnectionPingRunning = false;
      persistentConnectionPingerThread.notify();
    }
    try
    {
      persistentConnectionPingerThread.join();
    }
    catch (InterruptedException e)
    {
    }
  }

  /*
   * this will test that persistent connections that are idled are still
   * available
   */
  class IdlePersistentConnectionsPingerThread extends Thread
  {
    private boolean                   isKilled = false;
    private AbstractConnectionManager thisPool;

    protected IdlePersistentConnectionsPingerThread(String pBackendName,
        AbstractConnectionManager thisPool)
    {
      super("IdlePersistentConnectionsPingerThread for backend:" + pBackendName);
      this.thisPool = thisPool;
    }

    /**
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
      long idleTime, releaseTime;
      synchronized (this)
      {
        try
        {
          while (!isKilled)
          {
            if (idlePersistentConnections.isEmpty())
            {
              this.wait();
            }

            if (isKilled)
              continue; // Just exit

            PooledConnection c = null;

            synchronized (thisPool)
            {
              if (persistentConnectionsIdleTime.isEmpty())
              {
                continue; // Sanity check
              }

              releaseTime = ((Long) persistentConnectionsIdleTime.getFirst())
                  .longValue();
              idleTime = System.currentTimeMillis() - releaseTime;

              if (idleTime >= idlePersistentConnectionPingInterval)
              {

                c = (PooledConnection) idlePersistentConnections.removeFirst();
                persistentConnectionsIdleTime.removeFirst();
              }
            }

            if (c == null)
            { // Nothing to free, wait for next deadline
              wait(idlePersistentConnectionPingInterval - idleTime);
            }
            else
            {
              Statement s = null;
              try
              {
                s = c.getConnection().createStatement();
                // check the connection is still valid
                s.execute(connectionTestStatement);
                s.close();
                // and put it again in the pool
                synchronized (thisPool)
                {
                  persistentConnectionsIdleTime.addLast(new Long(System
                      .currentTimeMillis()));
                  idlePersistentConnections.addLast(c);
                }
              }
              catch (SQLException e)
              {
                // Connection lost... we release it... and open a new connection
                if (s != null)
                { // Clean statement
                  try
                  {
                    s.close();
                  }
                  catch (SQLException ignore)
                  {
                  }
                }
                if (c != null)
                { // Clean connection
                  try
                  {
                    c.close();
                  }
                  catch (SQLException ignore)
                  {
                  }
                }
                try
                {
                  c.close();
                }
                catch (SQLException e1)
                {
                  String msg = "An error occured while closing idle connection after the timeout: "
                      + e;
                  logger.error(msg);
                }
              }
            }
          }
        }
        catch (InterruptedException e)
        {
          logger
              .error("Wait on IdlePersistentConnectionsPingerThread interrupted in ConnectionManager: "
                  + e);
        }
      }
    }
  }
}