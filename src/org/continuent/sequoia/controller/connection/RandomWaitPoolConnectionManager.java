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
import java.util.NoSuchElementException;

import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This connection manager waits when the pool is empty. Requests are stacked
 * using the Java wait/notify mechanism. Therefore the FIFO order is not
 * guaranteed and the first request to get the freed connection is the thread
 * that gets elected by the scheduler.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class RandomWaitPoolConnectionManager
    extends AbstractPoolConnectionManager
{
  /** Time to wait for a connection in milliseconds (0 means wait forever). */
  private int timeout;

  /**
   * Creates a new <code>RandomWaitPoolConnectionManager</code> instance.
   * 
   * @param backendUrl URL of the <code>DatabaseBackend</code> owning this
   *          connection manager
   * @param backendName name of the <code>DatabaseBackend</code> owning this
   *          connection manager
   * @param login backend connection login to be used by this connection manager
   * @param password backend connection password to be used by this connection
   *          manager
   * @param driverPath path for driver
   * @param driverClassName class name for driver
   * @param poolSize size of the connection pool
   * @param timeout time to wait for a connection in seconds (0 means wait
   *          forever)
   */
  public RandomWaitPoolConnectionManager(String backendUrl, String backendName,
      String login, String password, String driverPath, String driverClassName,
      int poolSize, int timeout)
  {
    super(backendUrl, backendName, login, password, driverPath,
        driverClassName, poolSize);
    this.timeout = timeout * 1000;
  }

  /**
   * @see java.lang.Object#clone()
   */
  protected Object clone() throws CloneNotSupportedException
  {
    return new RandomWaitPoolConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName, poolSize, timeout);
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#clone(String,
   *      String)
   */
  public AbstractConnectionManager clone(String rLogin, String rPassword)
  {
    return new RandomWaitPoolConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName, poolSize, timeout);
  }

  /**
   * Gets the timeout.
   * 
   * @return a <code>int</code> value.
   */
  public int getTimeout()
  {
    return timeout;
  }

  /**
   * Gets a connection from the pool.
   * <p>
   * If the pool is empty, this methods blocks until a connection is freed or
   * the timeout expires.
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
    long lTimeout = timeout;

    // We have to do a while loop() because there is a potential race here.
    // When freeConnections is notified in releaseConnection, a new thread
    // can
    // take the lock on freeConnections before we wake up/reacquire the lock
    // on freeConnections. Therefore, we could wake up and have no connection
    // to take! We ensure that everything is correct with a while statement
    // and recomputing the timeout between 2 wakeup.
    while (freeConnections.isEmpty())
    {
      // Wait
      try
      {
        if (lTimeout > 0)
        {
          long start = System.currentTimeMillis();
          // Convert seconds to milliseconds for wait call
          this.wait(timeout);
          long end = System.currentTimeMillis();
          lTimeout -= end - start;
          if (lTimeout <= 0)
          {
            if (activeConnections.size() == 0)
            { // No connection active and backend unreachable, the backend
              // is probably dead
              logger
                  .error("Backend " + backendName + " is no more accessible.");
              throw new UnreachableBackendException();
            }
            if (logger.isWarnEnabled())
              logger.warn("Timeout expired for connection on backend '"
                  + backendName
                  + "', consider increasing pool size (current size is "
                  + poolSize + ") or timeout (current timeout is "
                  + (timeout / 1000) + " seconds)");
            return null;
          }
        }
        else
          this.wait();
      }
      catch (InterruptedException e)
      {
        logger
            .error("Wait on freeConnections interrupted in RandomWaitPoolConnectionManager: "
                + e);
        return null;
      }
    }
    if (isShutdown)
    {
      return null;
    }
    // Get the connection
    try
    {
      PooledConnection c = (PooledConnection) freeConnections.removeLast();
      activeConnections.add(c);
      return c;
    }
    catch (NoSuchElementException e)
    {
      int missing = poolSize
          - (activeConnections.size() + freeConnections.size());
      if (missing > 0)
      { // Re-allocate missing connections
        logger
            .info("Trying to reallocate " + missing + " missing connections.");
        PooledConnection connectionToBeReturned = null;
        while (missing > 0)
        {
          Connection c = getConnectionFromDriver();
          if (c == null)
          {
            if (missing == poolSize)
            {
              String msg = Translate.get("loadbalancer.backend.unreacheable",
                  backendName);
              logger.error(msg);
              throw new UnreachableBackendException(msg);
            }
            logger.warn("Unable to re-allocate " + missing
                + " missing connections.");
            break;
          }
          else
          {
            if (connectionToBeReturned == null)
              connectionToBeReturned = new PooledConnection(c);
            else
              freeConnections.addLast(new PooledConnection(c));
          }
          missing--;
        }
        return connectionToBeReturned;
      }
      if (logger.isErrorEnabled())
        logger.error("Failed to get a connection on backend '" + backendName
            + "' whereas an idle connection was expected");
      return null;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#releaseConnection(org.continuent.sequoia.controller.connection.PooledConnection)
   */
  public synchronized void releaseConnection(PooledConnection c)
  {
    if (!initialized)
    {
      closeConnection(c);
      return; // We probably have been disabled
    }

    if (activeConnections.remove(c))
    {
      c.removeAllTemporaryTables();
      freeConnections.addLast(c);
      this.notify();
    }
    else
      logger.error("Failed to release connection " + c
          + " (not found in active pool)");
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
      Connection newConnection = getConnectionFromDriver();
      if (newConnection == null)
      {
        if (logger.isDebugEnabled())
          logger.error("Bad connection " + c
              + " has been removed but cannot be replaced.");
      }
      else
      {
        freeConnections.addLast(newConnection);
        this.notify();
        if (logger.isDebugEnabled())
          logger.debug("Bad connection " + c
              + " has been replaced by a new connection.");
      }
    }
    else
      logger.error("Failed to release connection " + c
          + " (not found in active pool)");
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getXmlImpl()
   */
  public String getXmlImpl()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RandomWaitPoolConnectionManager
        + " " + DatabasesXmlTags.ATT_poolSize + "=\"" + poolSize + "\" "
        + DatabasesXmlTags.ATT_timeout + "=\"" + timeout / 1000 + "\"/>");
    return info.toString();
  }

}
