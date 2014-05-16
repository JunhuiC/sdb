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
 * This connection manager returns <code>null</code> when the pool is empty.
 * Therefore all requests fail fast until connections are freed.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class FailFastPoolConnectionManager
    extends AbstractPoolConnectionManager
{

  /**
   * Creates a new <code>FailFastPoolConnectionManager</code> instance.
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
   */
  public FailFastPoolConnectionManager(String backendUrl, String backendName,
      String login, String password, String driverPath, String driverClassName,
      int poolSize)
  {
    super(backendUrl, backendName, login, password, driverPath,
        driverClassName, poolSize);
  }

  /**
   * Gets a connection from the pool. Returns <code>null</code> if the pool is
   * empty.
   * 
   * @return a connection from the pool or <code>null</code> if the pool is
   *         exhausted
   * @throws UnreachableBackendException if the backend must be disabled
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getConnection()
   */
  public synchronized PooledConnection getConnection()
      throws UnreachableBackendException
  {
    if (!initialized)
    {
      logger.error(Translate.get("connection.request.not.initialized"));
      return null;
    }
    if (isShutdown)
    {
      return null;
    }
    try
    { // Both freeConnections and activeConnections are synchronized
      PooledConnection c = (PooledConnection) freeConnections.removeLast();
      activeConnections.add(c);
      return c;
    }
    catch (NoSuchElementException e)
    { // No free connection
      int missing = poolSize
          - (activeConnections.size() + freeConnections.size());
      if (missing > 0)
      { // Re-allocate missing connections
        logger.info(Translate.get("connection.reallocate.missing", missing));
        PooledConnection connectionToBeReturned = null;
        while (missing > 0)
        {
          Connection c = getConnectionFromDriver();
          if (c == null)
          {
            if (missing == poolSize)
            {
              logger.error(Translate.get("connection.backend.unreachable",
                  backendName));
              throw new UnreachableBackendException();
            }
            logger.warn(Translate.get("connection.reallocate.failed", missing));
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
      if (logger.isWarnEnabled())
        logger.warn(Translate.get("connection.backend.out.of.connections",
            new String[]{backendName, String.valueOf(poolSize)}));
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
    }
    else
      logger.error(Translate.get("connection.release.failed", c.toString()));
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
          logger.error(Translate
              .get("connection.replaced.failed", c.toString()));
      }
      else
      {
        freeConnections.addLast(newConnection);
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("connection.replaced.success", c
              .toString()));
      }
    }
    else
      logger.error(Translate.get("connection.replaced.failed.exception", c
          .toString()));
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#getXmlImpl()
   */
  public String getXmlImpl()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_FailFastPoolConnectionManager + " "
        + DatabasesXmlTags.ATT_poolSize + "=\"" + poolSize / 1000 + "\"/>");
    return info.toString();
  }

  /**
   * @see java.lang.Object#clone()
   */
  protected Object clone() throws CloneNotSupportedException
  {
    return new FailFastPoolConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName, poolSize);
  }

  /**
   * @see org.continuent.sequoia.controller.connection.AbstractConnectionManager#clone(String,
   *      String)
   */
  public AbstractConnectionManager clone(String rLogin, String rPassword)
  {
    return new FailFastPoolConnectionManager(backendUrl, backendName, rLogin,
        rPassword, driverPath, driverClassName, poolSize);
  }

}
