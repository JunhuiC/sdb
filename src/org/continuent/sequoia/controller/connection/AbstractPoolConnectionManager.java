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
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.continuent.sequoia.common.i18n.Translate;

/**
 * This connection manager uses a pool of persistent connections with the
 * database. The allocation/release policy is implemented by the subclasses
 * (abstract
 * {@link org.continuent.sequoia.controller.connection.AbstractConnectionManager#getConnection()}/
 * {@link org.continuent.sequoia.controller.connection.AbstractConnectionManager#releaseConnection(PooledConnection)}
 * from
 * {@link org.continuent.sequoia.controller.connection.AbstractConnectionManager}).
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public abstract class AbstractPoolConnectionManager
    extends AbstractConnectionManager
{
  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Connection handling
  // 4. Getter/Setter (possibly in alphabetical order)
  //

  /** Stack of available <code>PooledConnection</code> */
  protected transient LinkedList<PooledConnection> freeConnections;

  /**
   * Pool of currently used connections (<code>Vector</code> type because
   * synchronisation is needed). Uses <code>PooledConnection</code> objects.
   */
  protected transient ArrayList<PooledConnection>  activeConnections;

  /** Size of the connection pool with the real database. */
  protected int                  poolSize;

  /*
   * Constructor(s)
   */

  /**
   * Creates a new <code>AbstractPoolConnectionManager</code> instance.
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
   * @param poolSize size of the connection pool.
   */
  public AbstractPoolConnectionManager(String backendUrl, String backendName,
      String login, String password, String driverPath, String driverClassName,
      int poolSize)
  {
    super(backendUrl, backendName, login, password, driverPath, driverClassName);

    // VariableConnectionPool can be initialized with no connections
    if (!(this instanceof VariablePoolConnectionManager) && poolSize < 1)
      throw new IllegalArgumentException(
          "Illegal value for size of the pool connection manager: " + poolSize);

    this.poolSize = poolSize;
    this.freeConnections = new LinkedList<PooledConnection>();
    this.activeConnections = new ArrayList<PooledConnection>(poolSize);
    this.initialized = false;

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("connection.backend.pool.created",
          new String[]{backendName, String.valueOf(poolSize)}));
  }

  /*
   * Connection handling
   */

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#doConnectionInitialization()
   */
  protected synchronized void doConnectionInitialization() throws SQLException
  {
    doConnectionInitialization(poolSize);
    if (idlePersistentConnectionPingInterval > 0)
    {
      persistentConnectionPingerThread = new IdlePersistentConnectionsPingerThread(
          backendName, this);
      persistentConnectionPingerThread.start();
      idlePersistentConnectionPingRunning = true;
    }
  }

  /**
   * Initialize initPoolSize connections in the pool.
   * 
   * @param initPoolSize number of connections to initialize
   * @throws SQLException if an error occurs
   */
  protected synchronized void doConnectionInitialization(int initPoolSize)
      throws SQLException
  {
    if (initialized)
      throw new SQLException("Connection pool for backend '" + backendUrl
          + "' already initialized");

    if (initPoolSize > poolSize)
    {
      logger.warn(Translate.get("connection.max.poolsize.reached",
          new String[]{String.valueOf(initPoolSize), String.valueOf(poolSize),
              String.valueOf(poolSize)}));
      initPoolSize = poolSize;
    }

    poolSize = initConnections(initPoolSize);
    initialized = true;

    if (poolSize == 0) // Should never happen
      logger.error(Translate.get("connection.empty.pool"));
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("connection.pool.initialized", new String[]{
          String.valueOf(initPoolSize), backendUrl}));

  }

  /**
   * initConnections initializes the requested number of connections on the
   * backend
   * 
   * @param initPoolSize the requested number of connections to create
   * @return the number of connections that were created
   */
  protected int initConnections(int initPoolSize)
  {
    Connection c = null;

    boolean connectionsAvailable = true;
    int i = 0;
    while ((i < initPoolSize) && connectionsAvailable)
    {
      c = getConnectionFromDriver();

      if (c == null)
        connectionsAvailable = false;

      if (!connectionsAvailable)
      {
        if (i > 0)
        {
          logger.warn(Translate.get("connection.limit.poolsize", i));
        }
        else
        {
          logger.warn(Translate.get("connection.initialize.pool.failed"));
          poolSize = 0;
        }
      }
      else
      {
        PooledConnection pc = new PooledConnection(c);
        freeConnections.addLast(pc);
        i++;
      }
    }
    return i;
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#doConnectionFinalization()
   */
  protected synchronized void doConnectionFinalization() throws SQLException
  {
    if (!initialized)
    {
      String msg = Translate.get("connection.pool.not.initialized");
      logger.error(msg);
      throw new SQLException(msg);
    }

    PooledConnection c;
    boolean error = false;

    // Close free connections
    initialized = false;
    int freed = 0;
    while (!freeConnections.isEmpty())
    {
      c = (PooledConnection) freeConnections.removeLast();
      try
      {
        c.close();
      }
      catch (SQLException e)
      {
        error = true;
      }
      freed++;
    }
    if (logger.isInfoEnabled())
      logger.info(Translate.get("connection.freed.connection", new String[]{
          String.valueOf(freed), backendUrl}));

    // Close active connections
    int size = activeConnections.size();
    if (size > 0)
    {
      logger.warn(Translate.get("connection.connections.still.active", size));
      for (int i = 0; i < size; i++)
      {
        c = (PooledConnection) activeConnections.get(i);

        // Try to remove connection from persistent connection map.
        // This calls for a more evolved data structure such as a "two-way" map.
        // If somebody closes a persistent connection while we are in the 
        // just retry
        boolean retry;
        do
        {
          retry = false;
          try
          {
            for (Iterator<?> iter = persistentConnections.entrySet().iterator(); iter
                .hasNext();)
            {
              Map.Entry<?,?> element = (Map.Entry<?,?>) iter.next();
              if (element.getValue() == c)
              {
                iter.remove();
              }
            }
          }
          catch (ConcurrentModificationException e)
          {
            retry = true;
          }
        }
        while (retry);
        /*
         * Closing a connection while it is use can lead to problems.
         * MySQL can deadlock and Sybase produces NPEs. Either the request 
         * will complete and the Connection will be closed or the 
         */
        
//        try
//        {
//          c.close();
//        }
//        catch (SQLException e)
//        {
//          error = true;
//        }
      }
    }

    // Clear connections to ensure that the eventually not closed connections
    // will be closed when the objects will be garbage collected
    freeConnections.clear();
    activeConnections.clear();
    this.notifyAll();
    System.gc();

    if (error)
    {
      String msg = Translate.get("connection.free.connections.failed");
      logger.error(msg);
      throw new SQLException(msg);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#flagAllConnectionsForRenewal()
   */
  public synchronized void flagAllConnectionsForRenewal()
  {
    if (!initialized)
    {
      String msg = Translate.get("connection.pool.not.initialized");
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    for (Iterator<PooledConnection> iter = freeConnections.iterator(); iter.hasNext();)
    {
      PooledConnection c = (PooledConnection) iter.next();
      c.setMustBeRenewed(true);
    }

    for (Iterator<?> iter = activeConnections.iterator(); iter.hasNext();)
    {
      PooledConnection c = (PooledConnection) iter.next();
      c.setMustBeRenewed(true);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getCurrentNumberOfConnections()
   */
  public int getCurrentNumberOfConnections()
  {
    return poolSize;
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#releasePersistentConnection(org.continuent.sequoia.controller.connection.PooledConnection)
   */
  protected void releasePersistentConnection(PooledConnection c)
  {
    deleteConnection(c);
  }
}