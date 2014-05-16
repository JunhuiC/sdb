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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.connection;

import java.sql.Connection;
import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This connection manager creates a new <code>Connection</code> every time
 * the {@link #getConnection}method is called.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class SimpleConnectionManager extends AbstractConnectionManager
{
  private int nbOfConnections = 0;

  /**
   * Creates a new <code>SimpleConnectionManager</code> instance.
   * 
   * @param backendUrl URL of the <code>DatabaseBackend</code> owning this
   *          connection manager.
   * @param backendName name of the <code>DatabaseBackend</code> owning this
   *          connection manager.
   * @param login backend connection login to be used by this connection
   *          manager.
   * @param password backend connection password to be used by this connection
   *          manager.
   * @param driverPath path for driver
   * @param driverClassName class name for driver
   */
  public SimpleConnectionManager(String backendUrl, String backendName,
      String login, String password, String driverPath, String driverClassName)
  {
    super(backendUrl, backendName, login, password, driverPath, driverClassName);
  }

  /**
   * @see java.lang.Object#clone()
   */
  protected Object clone() throws CloneNotSupportedException
  {
    return new SimpleConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName);
  }
   
  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#clone(String,
   *      String)
   */
  public AbstractConnectionManager clone(String rLogin, String rPassword)
  {
    return new SimpleConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName);
  }

  /**
   * Does nothing.
   * 
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#doConnectionInitialization()
   */
  protected void doConnectionInitialization() throws SQLException
  {
    initialized = true;
    if (idlePersistentConnectionPingInterval > 0)
    {
      persistentConnectionPingerThread = new IdlePersistentConnectionsPingerThread(
          backendName, this);
      persistentConnectionPingerThread.start();
      idlePersistentConnectionPingRunning = true;
    }
  }

  /**
   * Does nothing.
   * 
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#doConnectionFinalization()
   */
  protected void doConnectionFinalization() throws SQLException
  {
    initialized = false;
  }

  /**
   * Gets a new connection from the underlying driver.
   * 
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getConnection()
   */
  public PooledConnection getConnection() throws UnreachableBackendException
  {
    if (!initialized)
    {
      logger
          .error("Requesting a connection from a non-initialized connection manager");
      return null;
    }
    if (isShutdown)
    {
      return null;
    }
    addConnection();
    Connection c = getConnectionFromDriver();
    if (c == null)
    {
      removeConnection();
      logger.error("Unable to get connection from " + backendUrl);
      if (nbOfConnections == 0)
      {
        logger.error("Backend '" + backendUrl + "' is considered unreachable. "
            + "(No active connection and none can be opened)");
        throw new UnreachableBackendException();
      }
    }
    return new PooledConnection(c);
  }

  /**
   * Closes the connection.
   * 
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#releaseConnection(PooledConnection)
   */
  public void releaseConnection(PooledConnection connection)
  {
    removeConnection();
    try
    {
      connection.close();
    }
    catch (SQLException e)
    {
      logger.error("Failed to close connection for '" + backendUrl + "'", e);
    }
  }

  /**
   * Closes the persistent connection.
   * 
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#releasePersistentConnection(PooledConnection)
   */
  protected void releasePersistentConnection(PooledConnection c)
  {
    releaseConnection(c);
  }
  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#deleteConnection(PooledConnection)
   */
  public void deleteConnection(PooledConnection c)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#flagAllConnectionsForRenewal()
   */
  public void flagAllConnectionsForRenewal()
  {
    // Nothing to do here, all connections are always renewed
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getCurrentNumberOfConnections()
   */
  public int getCurrentNumberOfConnections()
  {
    return nbOfConnections;
  }

  private synchronized void addConnection()
  {
    nbOfConnections++;
  }

  private synchronized void removeConnection()
  {
    nbOfConnections--;
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getXmlImpl()
   */
  public String getXmlImpl()
  {
    return "<" + DatabasesXmlTags.ELT_SimpleConnectionManager + "/>";
  }

}