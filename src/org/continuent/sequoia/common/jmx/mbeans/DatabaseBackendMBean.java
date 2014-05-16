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
 * Contributor(s): 
 */

package org.continuent.sequoia.common.jmx.mbeans;

import java.util.List;

/**
 * MBeanInterface to the DatabaseBackend
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public interface DatabaseBackendMBean
{
  /**
   * Tests if this backend is enabled (active and synchronized).
   * 
   * @return <code>true</code> if this backend is enabled
   * @throws Exception if an error occurs
   */
  boolean isInitialized() throws Exception;

  /**
   * Tests if this backend is read enabled (active and synchronized).
   * 
   * @return <code>true</code> if this backend is enabled.
   */
  boolean isReadEnabled();

  /**
   * Tests if this backend is write enabled (active and synchronized).
   * 
   * @return <code>true</code> if this backend is enabled.
   */
  boolean isWriteEnabled();

  /**
   * Is the backend completely disabled ? This usually means it has a known
   * state with a checkpoint associated to it.
   * 
   * @return <code>true</code> if the backend is disabled
   */
  boolean isDisabled();

  /**
   * Enables the database backend for reads. This method should only be called
   * when the backend is synchronized with the others.
   */
  void enableRead();

  /**
   * Enables the database backend for writes. This method should only be called
   * when the backend is synchronized with the others.
   */
  void enableWrite();

  /**
   * Disables the database backend for reads. This does not affect write ability
   */
  void disableRead();

  /**
   * Disables the database backend for writes. This does not affect read ability
   * although the backend will not be coherent anymore as soon as a write as
   * occured. This should be used in conjunction with a checkpoint to recover
   * missing writes.
   */
  void disableWrite();

  /**
   * Sets the database backend state to disable. This state is just an
   * indication and it has no semantic effect. It is up to the request manager
   * (especially the load balancer) to ensure that no more requests are sent to
   * this backend.
   */
  void disable();

  /**
   * Returns the SQL statement to use to check the connection validity.
   * 
   * @return a <code>String</code> containing a SQL statement
   */
  String getConnectionTestStatement();

  /**
   * Returns the database native JDBC driver class name.
   * 
   * @return the driver class name
   */
  String getDriverClassName();

  /**
   * Returns the backend logical name.
   * 
   * @return the backend logical name
   */
  String getName();

  /**
   * Returns a description of the state of the backend
   * 
   * @see org.continuent.sequoia.common.jmx.notifications.SequoiaNotificationList
   * @return a string description of the state. Can be enabled, disabled,
   *         recovering, backuping ...
   */
  String getState();

  /**
   * Return the integer value corresponding to the state of the backend. The
   * values are defined in <code>BackendState</code>
   * 
   * @return <tt>int</tt> value
   * @see org.continuent.sequoia.common.jmx.management.BackendState
   */
  int getStateValue();

  /**
   * Returns the list of pending requests for this backend.
   * 
   * @param count number of requests to retrieve, if 0, return all.
   * @param fromFirst count the request from first if true, or from last if
   *          false
   * @param clone should clone the pending request if true, block it if false
   * @return <code>List</code> of <code>String</code> description of
   *         each request.
   */
  List getPendingRequestsDescription(int count, boolean fromFirst,
      boolean clone);

  /**
   * Returns the list of active transactions for this backend.
   * 
   * @return <code>List</code> of <code>Long</code>, corresponding to
   *         active transaction identifier.
   */
  List getActiveTransactions();

  /**
   * Gets the names of the tables. <b>NOTE</b>: The returned array will contain
   * only one entry per actual table prefixed by the schema name + "."
   * 
   * @return the names as an array of <code>Strings</code>, or an empty array
   *         if no tables were found
   */
  String[] getTablesNames();

  /**
   * Gets the names of the columns.
   * 
   * @param tableName fully qualified name of the table (with
   *          <code><schema>.</code> prefix)
   * @return an array containing the columns names, or null if tableName is not
   *         a valid table name.
   */
  String[] getColumnsNames(String tableName);

  /**
   * Gets a description of the given table locks.
   * 
   * @param tableName fully qualified name of the table (with
   *          <code><schema>.</code> prefix)
   * @return a (localized) string containing either "No locks" or "Locked by
   *         <tid>"
   */
  String getLockInfo(String tableName);

  /**
   * Lists all stored procedures in this schema. <b>Note:</b> There is an issue
   * with stored procedures with same name (but different parameters): one will
   * appear with 0 parameters, the 2nd with the total parameters from the 2
   * stored proc. See SEQUOIA-296 for more info
   * 
   * @return An array of Strings containing all stored procedure names (one
   *         array entry per procedure)
   */
  String[] getStoredProceduresNames();

  /**
   * Returns the JDBC URL used to access the database.
   * 
   * @return a JDBC URL
   */
  String getURL();

  /**
   * @return Returns the schemaIsStatic.
   */
  boolean isSchemaStatic();

  /**
   * Returns the driver path.
   * 
   * @return the driver path
   */
  String getDriverPath();

  /**
   * Returns the lastKnownCheckpoint value.
   * 
   * @return Returns the lastKnownCheckpoint.
   */
  String getLastKnownCheckpoint();

  /**
   * Is the backend accessible ?
   * 
   * @return <tt>true</tt> if a jdbc connection is still possible from the
   *         controller, <tt>false</tt> if connectionTestStatement failed
   */
  boolean isJDBCConnected();

  /**
   * The getXml() method does not return the schema if it is not static anymore,
   * to avoid confusion between static and dynamic schema. This method returns a
   * static view of the schema, whatever the dynamic precision is.
   * 
   * @param expandSchema if we should force the schema to be expanded. This is
   *          needed as the default getXml should call this method.
   * @return an xml formatted string
   */
  String getSchemaXml(boolean expandSchema);

  /**
   * Return a string description of the backend in xml format. This does not
   * include the schema description if the dynamic precision is not set to
   * static.
   * 
   * @return an xml formatted string
   */
  String getXml();
}