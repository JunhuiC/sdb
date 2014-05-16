/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2007 Continuent, Inc.
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
 * Initial developer(s): Robert Hodges.
 * Contributor(s): Stephane Giron.
 */

package org.continuent.sequoia.controller.recoverylog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.connection.DriverManager;

/**
 * Encapsulates connection information for recovery log updates.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a> *
 * @author <a href="stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class RecoveryLogConnectionManager
{
  static Trace              logger                      = Trace
                                                            .getLogger("org.continuent.sequoia.controller.recoverylog");

  // Connection information.
  private String            url;
  private String            login;
  private String            password;
  private String            driverName;
  private String            driverClassName;
  private String            logTableName;
  private int               autoCloseTimeoutMillis;

  private boolean           connected                   = false;
  private long              lastAccessMillis            = System
                                                            .currentTimeMillis();

  private Connection        connection;
  private PreparedStatement internalRecoveryTableInsert = null;
  private PreparedStatement internalRecoveryTableUpdate = null;

  /** Create a new connection holder. */
  public RecoveryLogConnectionManager(String url, String login,
      String password, String driverName, String driverClassName,
      String logTableName, int autoCloseTimeout)
  {
    this.url = url;
    this.login = login;
    this.password = password;
    this.driverName = driverName;
    this.driverClassName = driverClassName;
    this.logTableName = logTableName;
    this.autoCloseTimeoutMillis = autoCloseTimeout * 1000;
  }

  /** Renew the connection. */
  private void connect() throws SQLException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Logging in recovery log connection");
    }
    connected = false;
    connection = DriverManager.getConnection(url, login, password, driverName,
        driverClassName);
    internalRecoveryTableInsert = connection.prepareStatement("INSERT INTO "
        + logTableName + " VALUES(?,?,?,?,?,?,?,?,?,?)");
    internalRecoveryTableUpdate = connection.prepareStatement("UPDATE "
        + logTableName
        + " SET exec_status=?,update_count=?,exec_time=? WHERE log_id=?");
    connected = true;
  }

  /** Release the connection. */
  private void release()
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Releasing recovery log connection: " + connection);
    }
    connected = false;
    if (internalRecoveryTableInsert != null)
    {
      try
      {
        internalRecoveryTableInsert.close();
      }
      catch (Exception ignore)
      {
      }
    }
    if (internalRecoveryTableUpdate != null)
    {
      try
      {
        internalRecoveryTableUpdate.close();
      }
      catch (Exception ignore)
      {
      }
    }
    if (connection != null)
    {
      try
      {
        connection.close();
      }
      catch (Exception ignore)
      {
      }
    }
    internalRecoveryTableInsert = null;
    internalRecoveryTableUpdate = null;
    connection = null;
    lastAccessMillis = 0;
  }

  /** Invalidate the current connection. Should be called after an error, etc. */
  public void invalidate()
  {
    release();
  }

  /** Validates the connection in case it needs to be connected or renewed. */
  private void validateConnection() throws SQLException
  {
    if (!connected)
      connect();
    else if (lastAccessMillis > 0)
    {
      if ((lastAccessMillis + this.autoCloseTimeoutMillis) < System
          .currentTimeMillis())
      {
        release();
        connect();
      }
    }
    lastAccessMillis = System.currentTimeMillis();
  }

  /** Returns the recovery log connection. */
  public synchronized Connection getConnection() throws SQLException
  {
    validateConnection();
    return connection;
  }

  /**
   * Return a PreparedStatement to log an entry as follows:
   * <p>
   * INSERT INTO LogTableName VALUES(?,?,?,?,?,?,?)
   * 
   * @return a PreparedStatement
   * @throws SQLException if an error occurs
   */
  public synchronized PreparedStatement getLogPreparedStatement()
      throws SQLException
  {
    validateConnection();
    return internalRecoveryTableInsert;
  }

  /**
   * Return a PreparedStatement to update an entry as follows:
   * <p>
   * UPDATE LogTableName SET exec_status=?,exec_time=? WHERE log_id=?
   * 
   * @return a PreparedStatement
   * @throws SQLException if an error occurs
   */
  public synchronized PreparedStatement getUpdatePreparedStatement()
      throws SQLException
  {
    validateConnection();
    return internalRecoveryTableUpdate;
  }
}