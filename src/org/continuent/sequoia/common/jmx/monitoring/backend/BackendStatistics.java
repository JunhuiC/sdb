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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.jmx.monitoring.backend;

import java.io.Serializable;

/**
 * This class defines a BackendStatistics
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 */
public class BackendStatistics implements Serializable
{
  private static final long serialVersionUID = 586357005726124555L;

  /**
   * Name of the backend
   */
  private String            backendName;

  /**
   * Class name of the driver for the backend
   */
  private String            driverClassName;
  /**
   * URL of the backend
   */
  private String            url;
  /**
   * Is the backend enable for read?
   */
  private boolean           readEnabled;
  /**
   * Is the backend enable for write?
   */
  private boolean           writeEnabled;
  /**
   * the status of the initialization
   */
  private String            initializationStatus;
  /**
   * Is the schema of the backend static?
   */
  private boolean           schemaStatic;
  /**
   * number of total active connections
   */
  private long              numberOfTotalActiveConnections;
  /**
   * Last known checkpoint of the backend
   */
  private String            lastKnownCheckpoint;
  /**
   * Number of active transactions
   */
  private int               numberOfActiveTransactions;
  /**
   * Number of pending requests
   */
  private int               numberOfPendingRequests;
  /**
   * Number of persistent connections
   */
  private int               numberOfPersistentConnections;
  /**
   * Number of connection managers
   */
  private int               numberOfConnectionManagers;
  /**
   * Number of total requests
   */
  private int               numberOfTotalRequests;
  /**
   * Number of total transactions
   */
  private int               numberOfTotalTransactions;

  /**
   * Returns the backendName value.
   * 
   * @return Returns the backendName.
   */
  public String getBackendName()
  {
    return backendName;
  }

  /**
   * Sets the backendName value.
   * 
   * @param backendName The backendName to set.
   */
  public void setBackendName(String backendName)
  {
    this.backendName = backendName;
  }

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
   * Sets the driverClassName value.
   * 
   * @param driverClassName The driverClassName to set.
   */
  public void setDriverClassName(String driverClassName)
  {
    this.driverClassName = driverClassName;
  }

  /**
   * Returns the initializationStatus value.
   * 
   * @return Returns the initializationStatus.
   */
  public String getInitializationStatus()
  {
    return initializationStatus;
  }

  /**
   * Sets the initializationStatus value.
   * 
   * @param initializationStatus The initializationStatus to set.
   */
  public void setInitializationStatus(String initializationStatus)
  {
    this.initializationStatus = initializationStatus;
  }

  /**
   * Returns the lastKnownCheckpoint value.
   * 
   * @return Returns the lastKnownCheckpoint.
   */
  public String getLastKnownCheckpoint()
  {
    return lastKnownCheckpoint;
  }

  /**
   * Sets the lastKnownCheckpoint value.
   * 
   * @param lastKnownCheckpoint The lastKnownCheckpoint to set.
   */
  public void setLastKnownCheckpoint(String lastKnownCheckpoint)
  {
    this.lastKnownCheckpoint = lastKnownCheckpoint;
  }

  /**
   * Returns the numberOfActiveTransactions value.
   * 
   * @return Returns the numberOfActiveTransactions.
   */
  public int getNumberOfActiveTransactions()
  {
    return numberOfActiveTransactions;
  }

  /**
   * Sets the numberOfActiveTransactions value.
   * 
   * @param numberOfActiveTransactions The numberOfActiveTransactions to set.
   */
  public void setNumberOfActiveTransactions(int numberOfActiveTransactions)
  {
    this.numberOfActiveTransactions = numberOfActiveTransactions;
  }

  /**
   * Returns the numberOfConnectionManagers value.
   * 
   * @return Returns the numberOfConnectionManagers.
   */
  public int getNumberOfConnectionManagers()
  {
    return numberOfConnectionManagers;
  }

  /**
   * Sets the numberOfConnectionManagers value.
   * 
   * @param numberOfConnectionManagers The numberOfConnectionManagers to set.
   */
  public void setNumberOfConnectionManagers(int numberOfConnectionManagers)
  {
    this.numberOfConnectionManagers = numberOfConnectionManagers;
  }

  /**
   * Returns the numberOfPendingRequests value.
   * 
   * @return Returns the numberOfPendingRequests.
   */
  public int getNumberOfPendingRequests()
  {
    return numberOfPendingRequests;
  }

  /**
   * Sets the numberOfPendingRequests value.
   * 
   * @param numberOfPendingRequests The numberOfPendingRequests to set.
   */
  public void setNumberOfPendingRequests(int numberOfPendingRequests)
  {
    this.numberOfPendingRequests = numberOfPendingRequests;
  }

  /**
   * Returns the numberOfPersistentConnections value.
   * 
   * @return Returns the numberOfPersistentConnections.
   */
  public final int getNumberOfPersistentConnections()
  {
    return numberOfPersistentConnections;
  }

  /**
   * Sets the numberOfPersistentConnections value.
   * 
   * @param numberOfPersistentConnections The numberOfPersistentConnections to
   *          set.
   */
  public final void setNumberOfPersistentConnections(
      int numberOfPersistentConnections)
  {
    this.numberOfPersistentConnections = numberOfPersistentConnections;
  }

  /**
   * Returns the numberOfTotalActiveConnections value.
   * 
   * @return Returns the numberOfTotalActiveConnections.
   */
  public long getNumberOfTotalActiveConnections()
  {
    return numberOfTotalActiveConnections;
  }

  /**
   * Sets the numberOfTotalActiveConnections value.
   * 
   * @param numberOfTotalActiveConnections The numberOfTotalActiveConnections to
   *          set.
   */
  public void setNumberOfTotalActiveConnections(
      long numberOfTotalActiveConnections)
  {
    this.numberOfTotalActiveConnections = numberOfTotalActiveConnections;
  }

  /**
   * Returns the numberOfTotalRequests value.
   * 
   * @return Returns the numberOfTotalRequests.
   */
  public int getNumberOfTotalRequests()
  {
    return numberOfTotalRequests;
  }

  /**
   * Sets the numberOfTotalRequests value.
   * 
   * @param numberOfTotalRequests The numberOfTotalRequests to set.
   */
  public void setNumberOfTotalRequests(int numberOfTotalRequests)
  {
    this.numberOfTotalRequests = numberOfTotalRequests;
  }

  /**
   * Returns the numberOfTotalTransactions value.
   * 
   * @return Returns the numberOfTotalTransactions.
   */
  public int getNumberOfTotalTransactions()
  {
    return numberOfTotalTransactions;
  }

  /**
   * Sets the numberOfTotalTransactions value.
   * 
   * @param numberOfTotalTransactions The numberOfTotalTransactions to set.
   */
  public void setNumberOfTotalTransactions(int numberOfTotalTransactions)
  {
    this.numberOfTotalTransactions = numberOfTotalTransactions;
  }

  /**
   * Returns the readEnabled value.
   * 
   * @return Returns the readEnabled.
   */
  public boolean isReadEnabled()
  {
    return readEnabled;
  }

  /**
   * Sets the readEnabled value.
   * 
   * @param readEnabled The readEnabled to set.
   */
  public void setReadEnabled(boolean readEnabled)
  {
    this.readEnabled = readEnabled;
  }

  /**
   * Returns the schemaStatic value.
   * 
   * @return Returns the schemaStatic.
   */
  public boolean isSchemaStatic()
  {
    return schemaStatic;
  }

  /**
   * Sets the schemaStatic value.
   * 
   * @param schemaStatic The schemaStatic to set.
   */
  public void setSchemaStatic(boolean schemaStatic)
  {
    this.schemaStatic = schemaStatic;
  }

  /**
   * Returns the url value.
   * 
   * @return Returns the url.
   */
  public String getUrl()
  {
    return url;
  }

  /**
   * Sets the url value.
   * 
   * @param url The url to set.
   */
  public void setUrl(String url)
  {
    this.url = url;
  }

  /**
   * Returns the writeEnabled value.
   * 
   * @return Returns the writeEnabled.
   */
  public boolean isWriteEnabled()
  {
    return writeEnabled;
  }

  /**
   * Sets the writeEnabled value.
   * 
   * @param writeEnabled The writeEnabled to set.
   */
  public void setWriteEnabled(boolean writeEnabled)
  {
    this.writeEnabled = writeEnabled;
  }

}
