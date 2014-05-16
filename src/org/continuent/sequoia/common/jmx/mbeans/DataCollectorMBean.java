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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): 
 */

package org.continuent.sequoia.common.jmx.mbeans;

import org.continuent.sequoia.common.exceptions.DataCollectorException;
import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;

/**
 * DataCollector interface to used via JMX. This interface defines the entry
 * point to collect dynamic data for all Sequoia components.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public interface DataCollectorMBean
{

  // ****************************************//
  // *************** Controller Data ********//
  // ****************************************//

  /**
   * Get general information on the load of the controller. Get the number of
   * threads and total memory used up
   * 
   * @throws DataCollectorException if collection of information fails
   * @return array of strings
   */
  String[][] retrieveControllerLoadData() throws DataCollectorException;

  /**
   * Get dynamic data of the different virtual databases, like pending
   * connections size, currentNb of threads and number of active threads.
   * 
   * @throws DataCollectorException if collection of information fails
   * @return array of strings
   */
  String[][] retrieveVirtualDatabasesData() throws DataCollectorException;

  /**
   * Try to see if a virtual database exists from its name
   * 
   * @param name of the virtual database
   * @return true if exists, false otherwise
   */
  boolean hasVirtualDatabase(String name);

  // ****************************************//
  // *************** Database Data **********//
  // ****************************************//

  /**
   * Get the current SQL statistics for all databases
   * 
   * @throws DataCollectorException if collection of information fails
   * @return the statistics
   */
  String[][] retrieveSQLStats() throws DataCollectorException;

  /**
   * Get the current cache content for all databases
   * 
   * @throws DataCollectorException if collection of information fails
   * @return the cache content
   */
  String[][] retrieveCacheData() throws DataCollectorException;

  /**
   * Get the current cache stats content for all databases
   * 
   * @throws DataCollectorException if collection of information fails
   * @return the cache stats content
   */
  String[][] retrieveCacheStatsData() throws DataCollectorException;

  /**
   * Get the current list of backends data for all databases
   * 
   * @throws DataCollectorException if collection of information fails
   * @return the backend list content
   */
  String[][] retrieveBackendsData() throws DataCollectorException;

  /**
   * Get the current list of current users and associated data for all databases
   * 
   * @throws DataCollectorException if collection of information fails
   * @return data on users
   */
  String[][] retrieveClientsData() throws DataCollectorException;

  /**
   * Get the current SQL statistics
   * 
   * @param virtualDatabasename of the database to get the data from
   * @return the statistics
   * @throws DataCollectorException if collection of information fails
   */
  String[][] retrieveSQLStats(String virtualDatabasename)
      throws DataCollectorException;

  /**
   * Get the current cache content
   * 
   * @param virtualDatabasename of the database to get the data from
   * @return the cache content
   * @throws DataCollectorException if collection of information fails
   */
  String[][] retrieveCacheData(String virtualDatabasename)
      throws DataCollectorException;

  /**
   * Get the current cache stats content
   * 
   * @param virtualDatabasename of the database to get the data from
   * @return the cache stats content
   * @throws DataCollectorException if collection of information fails
   */
  String[][] retrieveCacheStatsData(String virtualDatabasename)
      throws DataCollectorException;

  /**
   * Get the current list of backends data
   * 
   * @param virtualDatabasename of the database to get the data from
   * @return the backend list content
   * @throws DataCollectorException if collection of information fails
   */
  String[][] retrieveBackendsData(String virtualDatabasename)
      throws DataCollectorException;

  /**
   * Retrive information about the scheduler, like number of pending requests,
   * number of writes executed and number of read executed
   * 
   * @param virtualDatabasename of the database to get the data from
   * @return data on the associated scheduler
   * @throws DataCollectorException if collection of data fails
   */
  String[][] retrieveSchedulerData(String virtualDatabasename)
      throws DataCollectorException;

  /**
   * Get the current list of current users and associated data
   * 
   * @param virtualDatabasename of the database to get the data from
   * @return data on users
   * @throws DataCollectorException if collection of information fails
   */
  String[][] retrieveClientsData(String virtualDatabasename)
      throws DataCollectorException;

  // ****************************************//
  // *************** Fine grain Data ********//
  // ****************************************//
  /**
   * Get some data information on a fine grain approach
   * 
   * @param collector for the data to be accessed
   * @return <code>long</code> value of the data
   * @throws DataCollectorException if collection of information fails
   */
  long retrieveData(AbstractDataCollector collector)
      throws DataCollectorException;

  /**
   * Get starting point for exchanging data on a particular target
   * 
   * @param dataType as given in the DataCollection interface
   * @param targetName if needed (like backendname,clientName ...)
   * @param virtualDbName if needed
   * @return collector instance
   * @throws DataCollectorException if fails to get proper collector instance
   * @see org.continuent.sequoia.common.jmx.monitoring.DataCollection
   */
  AbstractDataCollector retrieveDataCollectorInstance(int dataType,
      String targetName, String virtualDbName) throws DataCollectorException;

  /**
   * Gets content data of the recovery log
   * 
   * @param databaseName the virtual database name
   * @return data on the recovery log
   * @throws DataCollectorException if collection of information fails
   */
  String[][] retrieveRecoveryLogData(String databaseName) throws DataCollectorException;

}