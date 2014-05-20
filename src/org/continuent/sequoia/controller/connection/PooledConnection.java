/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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
import java.util.LinkedList;
import java.util.List;

/**
 * This class defines a PooledConnection that is a connection in a pool with
 * associated metadata.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class PooledConnection
{
  private Connection connection;
  private boolean    mustBeRenewed               = false;
  private int        currentTransactionIsolation = org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL;
  private int        oldTransactionIsolation;

  /*
   * List of temporary tables that were defined in this connection.
   */
  private List<String>       temporaryTables;

  /**
   * Creates a new <code>PooledConnection</code> object
   * 
   * @param c the real connection to the database
   */
  public PooledConnection(Connection c)
  {
    this.connection = c;
    temporaryTables = new LinkedList<String>();
  }

  /**
   * Returns the real connection to the database
   * 
   * @return Returns the connection.
   */
  public Connection getConnection()
  {
    return connection;
  }

  /**
   * Returns true if the connection must be renewed.
   * 
   * @return Returns true if the connection must be renewed.
   */
  boolean mustBeRenewed()
  {
    return mustBeRenewed;
  }

  /**
   * Sets the mustBeRenewed value.
   * 
   * @param mustBeRenewed true if the connection to the database must be
   *          renewed.
   */
  public void setMustBeRenewed(boolean mustBeRenewed)
  {
    this.mustBeRenewed = mustBeRenewed;
  }

  //
  // Transaction isolation related functions
  //

  /**
   * Returns true if this connection is set to the default transaction
   * isolation.
   * 
   * @return true if default isolation is used
   */
  public boolean isDefaultTransactionIsolation()
  {
    return currentTransactionIsolation == org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL;
  }

  /**
   * Restore the default transaction isolation that was recorded when
   * setTransactionIsolation was called. This is a no-op if the transaction
   * isolation of the connection is already set to the default.
   * 
   * @throws SQLException if we failed to restore the transaction isolation on
   *           the connection
   */
  public final void restoreDefaultTransactionIsolation() throws SQLException
  {
    if (isDefaultTransactionIsolation())
      return;
    connection.setTransactionIsolation(oldTransactionIsolation);
    currentTransactionIsolation = org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL;
  }

  /**
   * Sets the transaction isolation on the connection and record its previous
   * isolation level.
   * 
   * @param transactionIsolationLevel The transactionIsolation to set.
   * @throws SQLException if we failed to get the current transaction isolation
   *           of the connection
   */
  public final void setTransactionIsolation(int transactionIsolationLevel)
      throws SQLException
  {
    oldTransactionIsolation = connection.getTransactionIsolation();
    this.currentTransactionIsolation = transactionIsolationLevel;
    connection.setTransactionIsolation(transactionIsolationLevel);
  }

  /**
   * Closes the underlying connection
   * 
   * @throws SQLException in case of error
   */
  public void close() throws SQLException
  {
    if (connection != null && !connection.isClosed())
    {
      connection.close();
      connection = null;
    }
  }

  /*
   * Temporary tables management
   */
  /**
   * Adds a temporary table inside this connection's context
   * 
   * @param temporaryTable the name of the table to add for this connection
   */
  public void addTemporaryTables(String temporaryTable)
  {
    if (!existsTemporaryTable(temporaryTable))
      temporaryTables.add(temporaryTable);
  }

  /**
   * Removes the list of temporary tables from this connection's context. This
   * is used when releasing the connection to the pool.
   */
  public void removeAllTemporaryTables()
  {
    temporaryTables.clear();
  }

  /**
   * Check if a temporary table exists inside this connection's context
   * 
   * @param tableName name of the table that is looked for
   * @return true if the table can be find inside this connection
   */
  public boolean existsTemporaryTable(String tableName)
  {
    return temporaryTables.contains(tableName);
  }

}
