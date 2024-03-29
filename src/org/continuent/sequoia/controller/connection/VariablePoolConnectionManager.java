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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This connection manager provides connection pooling with a dynamically
 * adjustable pool size.
 * <p>
 * If the maximum number of active connections is not reached, the
 * {@link #getConnection()}method creates a connection. Else, the execution is
 * blocked until a connection is freed or the timeout expires. blocked until a
 * connection is freed or the timeout expires.
 * <p>
 * Idle connections in the pool are removed after the timeout idleTimeout if the
 * minimum pool size has not been reached.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class VariablePoolConnectionManager
    extends AbstractPoolConnectionManager
{
  /** Initial pool size to be initialized at startup. */
  private int                         initPoolSize;

  /** Minimum pool size. */
  private int                         minPoolSize;

  /** Maximum pool size. */
  private int                         maxPoolSize;

  /**
   * Time a connection can stay idle before begin released (removed from the
   * pool) in milliseconds (0 means forever)
   */
  private int                         idleTimeout;

  /** Maximum time to wait for a connection in milliseconds. */
  private int                         waitTimeout;

  /** Stores the time on which connections have been released. */
  private LinkedList<Long>                  releaseTimes;

  /** Allow to remove idle connections in the pool. */
  private RemoveIdleConnectionsThread removeIdleConnectionsThread;

  private Connection                  ping;

  /**
   * Creates a new <code>VariablePoolConnectionManager</code> instance with
   * the default minPoolSize(initial pool size to be initialized at startup).
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
   * @param minPoolSize minimum pool size.
   * @param maxPoolSize maximum pool size. 0 means no limit.
   * @param idleTimeout time a connection can stay idle before begin released
   *          (removed from the pool) in seconds. 0 means no timeout: once
   *          allocated, connections are never released.
   * @param waitTimeout maximum time to wait for a connection in seconds. 0
   *          means no timeout: waits until one connection is freed.
   */
  public VariablePoolConnectionManager(String backendUrl, String backendName,
      String rLogin, String rPassword, String driverPath,
      String driverClassName, int minPoolSize, int maxPoolSize,
      int idleTimeout, int waitTimeout)
  {
    this(backendUrl, backendName, rLogin, rPassword, driverPath,
        driverClassName, minPoolSize, minPoolSize, maxPoolSize, idleTimeout,
        waitTimeout);
  }

  /**
   * @see java.lang.Object#clone()
   */
  protected Object clone() throws CloneNotSupportedException
  {
    return new VariablePoolConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName, minPoolSize, maxPoolSize,
        idleTimeout, waitTimeout);
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#clone(String,
   *      String)
   */
  public AbstractConnectionManager clone(String rLogin, String rPassword)
  {
    return new VariablePoolConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName, initPoolSize, minPoolSize,
        maxPoolSize, idleTimeout, waitTimeout);
  }

  /**
   * Creates a new <code>VariablePoolConnectionManager</code> instance.
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
   * @param initPoolSize initial pool size to be intialized at startup
   * @param minPoolSize minimum pool size.
   * @param maxPoolSize maximum pool size. 0 means no limit.
   * @param idleTimeout time a connection can stay idle before begin released
   *          (removed from the pool) in seconds. 0 means no timeout: once
   *          allocated, connections are never released.
   * @param waitTimeout maximum time to wait for a connection in seconds. 0
   *          means no timeout: waits until one connection is freed.
   */
  public VariablePoolConnectionManager(String backendUrl, String backendName,
      String rLogin, String rPassword, String driverPath,
      String driverClassName, int initPoolSize, int minPoolSize,
      int maxPoolSize, int idleTimeout, int waitTimeout)
  {
    super(backendUrl, backendName, rLogin, rPassword, driverPath,
        driverClassName, maxPoolSize == 0 ? (initPoolSize > minPoolSize
            ? initPoolSize
            : minPoolSize) : maxPoolSize);
    this.initPoolSize = initPoolSize;
    this.minPoolSize = minPoolSize;
    this.maxPoolSize = maxPoolSize;
    this.idleTimeout = idleTimeout * 1000;
    this.waitTimeout = waitTimeout * 1000;
  }

  /**
   * Gets the max pool size.
   * 
   * @return a <code>int</code> value.
   */
  public int getMaxPoolSize()
  {
    return maxPoolSize;
  }

  /**
   * Gets the min pool size.
   * 
   * @return a <code>int</code> value.
   */
  public int getMinPoolSize()
  {
    return minPoolSize;
  }

  /**
   * Gets the idle timeout.
   * 
   * @return a <code>int</code> value.
   */
  public int getIdleTimeout()
  {
    return idleTimeout;
  }

  /**
   * Gets the wait timeout.
   * 
   * @return a <code>int</code> value.
   */
  public int getWaitTimeout()
  {
    return waitTimeout;
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#doConnectionInitialization()
   */
  protected void doConnectionInitialization() throws SQLException
  {
    poolSize = maxPoolSize == 0 ? (initPoolSize > minPoolSize
        ? initPoolSize
        : minPoolSize) : maxPoolSize;
    this.ping = getConnectionFromDriver();
    synchronized (this)
    {
      doConnectionInitialization(initPoolSize);

      if (idleTimeout != 0)
      {
        // Create the thread which manages the free connections
        removeIdleConnectionsThread = new RemoveIdleConnectionsThread(
            this.backendName, this);

        // Intialize release time for the initial connections if an idleTimeout
        // is set
        releaseTimes = new LinkedList<Long>();
        Iterator<PooledConnection> it = freeConnections.iterator();
        Long currentTime = new Long(System.currentTimeMillis());
        while (it.hasNext())
        {
          it.next();
          releaseTimes.addLast(currentTime);
        }
      }
    }

    // Start the thread outside synchronized(this)
    if (removeIdleConnectionsThread != null)
    {
      removeIdleConnectionsThread.start();

      synchronized (removeIdleConnectionsThread)
      {
        if (releaseTimes.size() > 0)
        {
          removeIdleConnectionsThread.notify();
        }
      }
    }
    if (idlePersistentConnectionPingInterval > 0)
    {
      persistentConnectionPingerThread = new IdlePersistentConnectionsPingerThread(
          backendName, this);
      persistentConnectionPingerThread.start();
      idlePersistentConnectionPingRunning = true;
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.controller.connection.AbstractPoolConnectionManager#doConnectionInitialization(int)
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

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("connection.pool.initialized", new String[]{
          String.valueOf(initPoolSize), backendUrl}));
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#doConnectionFinalization()
   */
  protected void doConnectionFinalization() throws SQLException
  {
    try
    {
      if (ping != null)
        ping.close();
      ping = null;
    }
    catch (SQLException e)
    {
      // ignore the errors we are shutting down
    }
    synchronized (this)
    {
      super.doConnectionFinalization();
    }

    if (removeIdleConnectionsThread != null)
    {
      synchronized (removeIdleConnectionsThread)
      {
        removeIdleConnectionsThread.isKilled = true;
        removeIdleConnectionsThread.notify();
      }
      try
      {
        removeIdleConnectionsThread.join();
      }
      catch (InterruptedException e)
      {
      }
    }
  }

  /**
   * Gets a connection from the pool.
   * <p>
   * If the current number of active connections is lower than the maximum pool
   * size, a new connection is created. If the creation fails, this method waits
   * for a connection to be freed.
   * <p>
   * If the maximum number of active connections is reached, this methods blocks
   * until a connection is freed or the timeout expires.
   * 
   * @return a connection from the pool or <code>null</code> if the timeout
   *         has expired.
   * @throws UnreachableBackendException if the backend must be disabled
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getConnection()
   */
  public synchronized PooledConnection getConnection()
      throws UnreachableBackendException
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
    long lTimeout = waitTimeout;
    if (freeConnections.isEmpty())
    {
      if ((maxPoolSize == 0) || (activeConnections.size() < maxPoolSize))
      {
        Connection c = getConnectionFromDriver();
        if (c == null)
        {
          if (activeConnections.size() == 0)
          { // No connection active and backend unreachable, the backend
            // is probably dead
            logger.error("Backend " + backendName + " is no more accessible.");
            throw new UnreachableBackendException();
          }
          /*
           * Ping the server with an open connection in order to determine
           * whether the server has failed or is has run out of connections. If
           * the ping fails, the server is down.
           */
          try
          {
            Statement pingStatement = ping.createStatement();
            pingStatement.execute(connectionTestStatement);
            pingStatement.close();
          }
          catch (SQLException e)
          {
            isShutdown = true;
            logger.error("Backend " + backendName + " is no more accessible.");
            throw new UnreachableBackendException();
          }
          /*
           * There are currently no connections available from the server, just
           * wait for a connection to be freed
           */

          if (logger.isWarnEnabled())
            logger.warn("Failed to create new connection on backend '"
                + backendName + "', waiting for a connection to be freed.");
        }
        else
        {
          freeConnections.addLast(new PooledConnection(c));
          if (idleTimeout != 0)
          {
            releaseTimes.add(new Long(System.currentTimeMillis()));
          }
          poolSize++;
        }
      }

      /*
       * We have to do a while loop() because there is a potential race here.
       * When freeConnections is notified in releaseConnection, a new thread can
       * take the lock on freeConnections before we wake up/reacquire the lock
       * on freeConnections. Therefore, we could wake up and have no connection
       * to take! We ensure that everything is correct with a while statement
       * and recomputing the timeout between 2 wakeup.
       */
      while (freeConnections.isEmpty())
      {
        if (activeConnections.size() == 0 || isShutdown)
        { // No connection active and backend unreachable, the backend
          // is probably dead
          logger.error("Backend " + backendName + " is no more accessible.");
          throw new UnreachableBackendException();
        }
        // Wait
        try
        {
          if (lTimeout > 0)
          {
            long start = System.currentTimeMillis();
            // Convert seconds to milliseconds for wait call
            this.wait(waitTimeout);
            long end = System.currentTimeMillis();
            lTimeout -= end - start;
            if (lTimeout <= 0)
            {
              if (logger.isWarnEnabled())
                logger.warn("Timeout expired for connection on backend '"
                    + backendName
                    + "', consider increasing pool size (current size is "
                    + poolSize + ") or timeout (current timeout is "
                    + (waitTimeout / 1000) + " seconds)");
              return null;
            }
          }
          else
          {
            this.wait();
          }
        }
        catch (InterruptedException e)
        {
          logger
              .error("Wait on freeConnections interrupted in VariablePoolConnectionManager");
          return null;
        }
      }
    }

    // Get the connection
    try
    {
      PooledConnection c = (PooledConnection) freeConnections.removeLast();
      if (idleTimeout != 0)
        releaseTimes.removeLast();
      activeConnections.add(c);
      return c;
    }
    catch (NoSuchElementException e)
    {
      if (logger.isErrorEnabled())
        logger.error("Failed to get a connection on backend '" + backendName
            + "' but an idle connection was expected");
      return null;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#releaseConnection(org.continuent.sequoia.controller.connection.PooledConnection)
   */
  public void releaseConnection(PooledConnection c)
  {
    boolean notifyThread = false;
    synchronized (this)
    {
      if (!initialized)
      {
        closeConnection(c);
        return; // We probably have been disabled
      }

      if (activeConnections.remove(c))
      {
        if (idleTimeout != 0)
        {
          notifyThread = freeConnections.isEmpty()
              || (freeConnections.size() == minPoolSize);
          releaseTimes.addLast(new Long(System.currentTimeMillis()));
        }
        c.removeAllTemporaryTables();
        freeConnections.addLast(c);
        this.notify();
      }
      else
        logger.error("Failed to release connection " + c
            + " (not found in active pool)");
    }

    if (notifyThread)
      synchronized (removeIdleConnectionsThread)
      {
        removeIdleConnectionsThread.notify();
      }
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#deleteConnection(org.continuent.sequoia.controller.connection.PooledConnection)
   */
  public synchronized void deleteConnection(PooledConnection c)
  {
    closeConnection(c);

    if (!initialized)
      return; // We probably have been disabled

    if (activeConnections.remove(c))
    {
      poolSize--;
      if (poolSize < minPoolSize)
      {
        Connection newConnection = getConnectionFromDriver();
        if (newConnection == null)
        {
          if (logger.isWarnEnabled())
            logger.warn("Bad connection : " + c
                + " has been removed but cannot be replaced.");
        }
        else
        {
          poolSize++;
          freeConnections.addLast(new PooledConnection(newConnection));
          if (idleTimeout != 0)
            releaseTimes.addLast(new Long(System.currentTimeMillis()));
          this.notify();
          if (logger.isDebugEnabled())
            logger.debug("Bad connection " + c
                + " has been replaced by a new connection.");
        }
      }
      else if (logger.isDebugEnabled())
        logger.debug("Bad connection " + c + " has been removed.");
    }
    else
      logger.error("Failed to release connection " + c
          + " (not found in active pool)");
    notifyAll();
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getXmlImpl()
   */
  public String getXmlImpl()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_VariablePoolConnectionManager + " "
        + DatabasesXmlTags.ATT_initPoolSize + "=\"" + initPoolSize + "\" "
        + DatabasesXmlTags.ATT_minPoolSize + "=\"" + minPoolSize + "\" "
        + DatabasesXmlTags.ATT_maxPoolSize + "=\"" + maxPoolSize + "\" "
        + DatabasesXmlTags.ATT_idleTimeout + "=\"" + idleTimeout / 1000 + "\" "
        + DatabasesXmlTags.ATT_waitTimeout + "=\"" + waitTimeout / 1000
        + "\"/>");
    return info.toString();
  }

  /**
   * Allows to remove idle free connections after the idleTimeout timeout.
   * 
   * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
   */
  protected class RemoveIdleConnectionsThread extends Thread
  {
    private boolean                       isKilled = false;
    private VariablePoolConnectionManager thisPool;

    protected RemoveIdleConnectionsThread(String pBackendName,
        VariablePoolConnectionManager thisPool)
    {
      super("RemoveIdleConnectionsThread for backend:" + pBackendName);
      this.thisPool = thisPool;
    }

    /**
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
      long idleTime, releaseTime;
      boolean isMinPoolSizeReached = false;
      synchronized (this)
      {
        try
        {
          while (!isKilled)
          {
            isMinPoolSizeReached = false;
            // the thread is not launched if idleTimeout equals to 0 (the
            // connections are never released in this case)
            if (freeConnections.isEmpty())
            {
              wait(); // wait on the thread RemoveIdleConnectionsThread
            }
            else if (freeConnections.size() <= minPoolSize)
            {
              // Ping these remaining connections when their time out occurs
              isMinPoolSizeReached = true;
            }

            if (isKilled)
              continue; // Just exit

            PooledConnection c = null;
            synchronized (thisPool)
            {
              if (releaseTimes.isEmpty())
                continue; // Sanity check

              releaseTime = ((Long) releaseTimes.getFirst()).longValue();
              idleTime = System.currentTimeMillis() - releaseTime;

              if (idleTime >= idleTimeout)
              {
                c = (PooledConnection) freeConnections.removeFirst();
                releaseTimes.removeFirst();
              }
            }

            if (c == null)
            { // Nothing to free, wait for next deadline
              wait(idleTimeout - idleTime);
            }
            else if (isMinPoolSizeReached)
            {
              Statement s = null;
              try
              {
                s = c.getConnection().createStatement();
                // check the connection is still valid
                s.execute(connectionTestStatement);
                s.close();
                // and put it again in the pool
                releaseTimes.addLast(new Long(System.currentTimeMillis()));
                freeConnections.addLast(c);
              }
              catch (SQLException e)
              {
                // Connection lost... we release it...
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
                finally
                {
                  poolSize--;
                }
              }
            }
            else
            { // Free the connection out of the synchronized block
              try
              {
                c.close();
              }
              catch (SQLException e)
              {
                String msg = "An error occured while closing idle connection after the timeout: "
                    + e;
                logger.error(msg);
              }
              finally
              {
                poolSize--;
              }
              logger.debug("Released idle connection (idle timeout reached)");
              continue;

            }
          }
        }
        catch (InterruptedException e)
        {
          logger
              .error("Wait on removeIdleConnectionsThread interrupted in VariablePoolConnectionManager: "
                  + e);
        }
      }
    }
  }

}