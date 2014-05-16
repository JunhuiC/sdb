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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.monitoring.datacollector;

import java.util.ArrayList;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.continuent.sequoia.common.exceptions.DataCollectorException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.JmxException;
import org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean;
import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;
import org.continuent.sequoia.common.jmx.monitoring.DataCollection;
import org.continuent.sequoia.common.jmx.monitoring.backend.ActiveConnectionsCollector;
import org.continuent.sequoia.common.jmx.monitoring.backend.ActiveTransactionCollector;
import org.continuent.sequoia.common.jmx.monitoring.backend.PendingRequestsCollector;
import org.continuent.sequoia.common.jmx.monitoring.backend.ReadRequestsCollector;
import org.continuent.sequoia.common.jmx.monitoring.backend.RequestsCollector;
import org.continuent.sequoia.common.jmx.monitoring.backend.TransactionsCollector;
import org.continuent.sequoia.common.jmx.monitoring.backend.WriteRequestsCollector;
import org.continuent.sequoia.common.jmx.monitoring.cache.CacheEntriesCollector;
import org.continuent.sequoia.common.jmx.monitoring.cache.CountHitsCollector;
import org.continuent.sequoia.common.jmx.monitoring.cache.CountInsertCollector;
import org.continuent.sequoia.common.jmx.monitoring.cache.CountSelectCollector;
import org.continuent.sequoia.common.jmx.monitoring.cache.HitsRatioCollector;
import org.continuent.sequoia.common.jmx.monitoring.client.ClientActiveTimeCollector;
import org.continuent.sequoia.common.jmx.monitoring.controller.ControllerIdleThreadsCollector;
import org.continuent.sequoia.common.jmx.monitoring.controller.ControllerWorkerPendingQueueCollector;
import org.continuent.sequoia.common.jmx.monitoring.controller.ThreadsCountCollector;
import org.continuent.sequoia.common.jmx.monitoring.controller.TotalMemoryCollector;
import org.continuent.sequoia.common.jmx.monitoring.controller.UsedMemoryCollector;
import org.continuent.sequoia.common.jmx.monitoring.scheduler.NumberReadCollector;
import org.continuent.sequoia.common.jmx.monitoring.scheduler.NumberRequestsCollector;
import org.continuent.sequoia.common.jmx.monitoring.scheduler.NumberWriteCollector;
import org.continuent.sequoia.common.jmx.monitoring.scheduler.PendingTransactionsCollector;
import org.continuent.sequoia.common.jmx.monitoring.scheduler.PendingWritesCollector;
import org.continuent.sequoia.common.jmx.monitoring.virtualdatabase.ActiveDatabaseThreadCollector;
import org.continuent.sequoia.common.jmx.monitoring.virtualdatabase.DatabaseThreadsCollector;
import org.continuent.sequoia.common.jmx.monitoring.virtualdatabase.PendingDatabaseConnectionCollector;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;

/**
 * This class implements retrieval of data to all possible objects in the
 * controller or other Sequoia components. It gets its interface from the
 * corresponding MBean.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 */
public class DataCollector extends AbstractStandardMBean
    implements
      DataCollectorMBean
{
  /** Logger instance */
  static Trace               logger                     = Trace
                                                            .getLogger("org.continuent.sequoia.controller.jmx");
  Controller                 controller;

  /**
   * No cache enabled exception
   */
  public static final String NO_CACHE_ENABLED           = "No cache enabled";
  /**
   * Not Implemented exception
   */
  public static final String NOT_IMPLEMENTED            = "Not Implemented";
  /**
   * Cannot get access to backend exception
   */
  public static final String BACKEND_NOT_ACCESSIBLE     = "Cannot reach backend";

  /**
   * Cannot disable backend exception
   */
  public static final String BACKEND_CANNOT_BE_DISABLED = "Backend cannot be disabled";

  /**
   * Client not found exception
   */
  public static final String CLIENT_NOT_FOUND           = "Client not found";
  /**
   * Invalid Data type for collector
   */
  public static final String INVALID_COLLECTOR_TYPE     = "Invalid Collector Type";
  /**
   * Authentication failed
   */
  public static final String AUTHENTICATION_FAILED      = "Authentication failed";
  /**
   * Database not found
   */
  public static final String DATABASE_NOT_FOUND         = "Database does not exists";

  /**
   * Create a new DataCollector associated to this controller.
   * 
   * @param controller to collect data from
   * @throws NotCompliantMBeanException - if the mbeanInterface does not follow
   *           JMX design patterns for Management Interfaces, or if this does
   *           not implement the specified interface.
   * @exception JmxException the bean could not be registered
   */
  public DataCollector(Controller controller)
      throws NotCompliantMBeanException, JmxException
  {
    super(DataCollectorMBean.class);
    this.controller = controller;
    ObjectName objectName = JmxConstants.getDataCollectorObjectName();
    MBeanServerManager.registerMBean(this, objectName);

  }

  /**
   * @see AbstractStandardMBean#getAssociatedString
   */
  public String getAssociatedString()
  {
    return "datacollector";
  }

  /**
   * Try to get a virtual database from its name
   * 
   * @param name of the virtual database
   * @return virtual database object
   * @throws DataCollectorException if does not exist
   */
  private VirtualDatabase getVirtualDatabase(String name)
      throws DataCollectorException
  {
    VirtualDatabase vd = controller.getVirtualDatabase(name);
    if (vd == null)
      throw new DataCollectorException("Unknown Database");
    return vd;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveBackendsData()
   */
  public String[][] retrieveBackendsData() throws DataCollectorException
  {
    throw new DataCollectorException("Not Implemented");
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveBackendsData(java.lang.String)
   */
  public String[][] retrieveBackendsData(String virtualDatabasename)
      throws DataCollectorException
  {
    VirtualDatabase vdb = getVirtualDatabase(virtualDatabasename);
    try
    {
      vdb.acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get("virtualdatabase.fail.read.lock", e);
      throw new DataCollectorException(msg);
    }
    ArrayList backends = vdb.getBackends();
    int backendListSize = backends.size();
    String[][] data = new String[backendListSize][];
    for (int i = 0; i < backendListSize; i++)
    {
      data[i] = ((DatabaseBackend) backends.get(i)).getBackendData();
    }
    vdb.releaseReadLockBackendLists();
    return data;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveCacheData()
   */
  public String[][] retrieveCacheData() throws DataCollectorException
  {
    throw new DataCollectorException(NOT_IMPLEMENTED);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveRecoveryLogData(java.lang.String)
   */
  public String[][] retrieveRecoveryLogData(String databaseName)
      throws DataCollectorException
  {
    VirtualDatabase vdb = getVirtualDatabase(databaseName);
    RecoveryLog log = vdb.getRequestManager().getRecoveryLog();
    if (log == null)
      throw new DataCollectorException("Recovery log is not defined");
    return log.getData();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveCacheData(java.lang.String)
   */
  public String[][] retrieveCacheData(String virtualDatabasename)
      throws DataCollectorException
  {
    VirtualDatabase vdb = getVirtualDatabase(virtualDatabasename);
    AbstractResultCache cache = vdb.getRequestManager().getResultCache();
    if (cache == null)
      throw new DataCollectorException(NO_CACHE_ENABLED);
    else
    {
      try
      {
        return cache.getCacheData();
      }
      catch (Exception e)
      {
        throw new DataCollectorException(e.getMessage());
      }
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveCacheStatsData()
   */
  public String[][] retrieveCacheStatsData() throws DataCollectorException
  {
    throw new DataCollectorException(NOT_IMPLEMENTED);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveCacheStatsData(java.lang.String)
   */
  public String[][] retrieveCacheStatsData(String virtualDatabasename)
      throws DataCollectorException
  {
    VirtualDatabase vdb = getVirtualDatabase(virtualDatabasename);
    AbstractResultCache cache = vdb.getRequestManager().getResultCache();
    if (cache == null)
      throw new DataCollectorException("No Cache enabled.");
    else
    {
      try
      {
        return cache.getCacheStatsData();
      }
      catch (Exception e)
      {
        throw new DataCollectorException(e.getMessage());
      }
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveClientsData()
   */
  public String[][] retrieveClientsData() throws DataCollectorException
  {
    throw new DataCollectorException("Not Implemented");
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveClientsData(java.lang.String)
   */
  public String[][] retrieveClientsData(String virtualDatabasename)
      throws DataCollectorException
  {
    VirtualDatabase vdb = getVirtualDatabase(virtualDatabasename);
    ArrayList activeThreads = vdb.getActiveThreads();
    int size = activeThreads.size();
    String[][] data = new String[size][];
    for (int i = 0; i < size; i++)
    {
      data[i] = ((VirtualDatabaseWorkerThread) activeThreads.get(i))
          .retrieveClientData();
    }
    return data;
  }

  /**
   * The returned string array will be:
   * <ul>
   * <li>data[0][0] = Controller IP address
   * <li>data[0][1] = Memory total (in megabytes)
   * <li>data[0][2] = Memory used (in megabytes)
   * <li>data[0][3] = Number of active threads
   * <li>data[0][4] = Number of pending client connections
   * <li>data[0][5] = Number of idle worker threads (<code>ControllerWorkerThread</code>)
   * </ul>
   * 
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveControllerLoadData()
   */
  public String[][] retrieveControllerLoadData()
  {
    long total = Runtime.getRuntime().totalMemory();
    long free = Runtime.getRuntime().freeMemory();
    String[][] data = new String[1][6];
    data[0][0] = controller.getIPAddress();
    data[0][1] = String.valueOf(total / 1024 / 1024);
    data[0][2] = String.valueOf((total - free) / 1024 / 1024);
    data[0][3] = String.valueOf(Thread.activeCount());
    data[0][4] = String.valueOf(controller.getConnectionThread()
        .getControllerServerThreadPendingQueueSize());
    data[0][5] = String.valueOf(controller.getConnectionThread()
        .getIdleWorkerThreads());
    return data;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveSQLStats()
   */
  public String[][] retrieveSQLStats() throws DataCollectorException
  {
    throw new DataCollectorException("Not Implemented");
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveSQLStats()
   */
  public String[][] retrieveSQLStats(String virtualDatabasename)
      throws DataCollectorException
  {
    VirtualDatabase vdb = getVirtualDatabase(virtualDatabasename);
    if (vdb.getSQLMonitor() == null)
      throw new DataCollectorException("No SQL monitoring enabled.");
    else
      return vdb.getSQLMonitor().getAllStatsInformation();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveVirtualDatabasesData()
   */
  public String[][] retrieveVirtualDatabasesData()
  {
    ArrayList dbs = controller.getVirtualDatabases();
    int size = dbs.size();
    String[][] data = new String[size][4];
    VirtualDatabase db;
    for (int i = 0; i < size; i++)
    {
      db = (VirtualDatabase) dbs.get(i);
      data[i][0] = db.getVirtualDatabaseName();
      data[i][1] = String.valueOf(db.getActiveThreads().size());
      data[i][2] = String.valueOf(db.getPendingConnections().size());
      data[i][3] = String.valueOf(db.getCurrentNbOfThreads());
    }
    return data;
  }

  /**
   * @return Returns the controller.
   */
  public Controller getController()
  {
    return controller;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveSchedulerData(java.lang.String)
   */
  public String[][] retrieveSchedulerData(String virtualDatabasename)
      throws DataCollectorException
  {
    VirtualDatabase vdb = getVirtualDatabase(virtualDatabasename);
    AbstractScheduler scheduler = vdb.getRequestManager().getScheduler();
    String[][] data = new String[1][];
    data[0] = scheduler.getSchedulerData();
    return data;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveDataCollectorInstance
   */
  public AbstractDataCollector retrieveDataCollectorInstance(int dataType,
      String targetName, String virtualDbName) throws DataCollectorException
  {
    switch (dataType)
    {
      /*
       * Controller Collectors
       */
      case DataCollection.CONTROLLER_TOTAL_MEMORY :
        return new TotalMemoryCollector(controller);
      case DataCollection.CONTROLLER_USED_MEMORY :
        return new UsedMemoryCollector(controller);
      case DataCollection.CONTROLLER_WORKER_PENDING_QUEUE :
        return new ControllerWorkerPendingQueueCollector(controller);
      case DataCollection.CONTROLLER_THREADS_NUMBER :
        return new ThreadsCountCollector(controller);
      case DataCollection.CONTROLLER_IDLE_WORKER_THREADS :
        return new ControllerIdleThreadsCollector(controller);
        /*
         * Backend collectors
         */
      case DataCollection.BACKEND_ACTIVE_TRANSACTION :
        return new ActiveTransactionCollector(targetName, virtualDbName);
      case DataCollection.BACKEND_PENDING_REQUESTS :
        return new PendingRequestsCollector(targetName, virtualDbName);
      case DataCollection.BACKEND_TOTAL_ACTIVE_CONNECTIONS :
        return new ActiveConnectionsCollector(targetName, virtualDbName);
      case DataCollection.BACKEND_TOTAL_REQUEST :
        return new RequestsCollector(targetName, virtualDbName);
      case DataCollection.BACKEND_TOTAL_READ_REQUEST :
        return new ReadRequestsCollector(targetName, virtualDbName);
      case DataCollection.BACKEND_TOTAL_WRITE_REQUEST :
        return new WriteRequestsCollector(targetName, virtualDbName);
      case DataCollection.BACKEND_TOTAL_TRANSACTIONS :
        return new TransactionsCollector(targetName, virtualDbName);
        /*
         * VirtualDatabase collectors
         */
      case DataCollection.DATABASES_ACTIVE_THREADS :
        return new ActiveDatabaseThreadCollector(virtualDbName);
      case DataCollection.DATABASES_PENDING_CONNECTIONS :
        return new PendingDatabaseConnectionCollector(virtualDbName);
      case DataCollection.DATABASES_NUMBER_OF_THREADS :
        return new DatabaseThreadsCollector(virtualDbName);
        /*
         * Cache stats collectors
         */
      case DataCollection.CACHE_STATS_COUNT_HITS :
        return new CountHitsCollector(virtualDbName);
      case DataCollection.CACHE_STATS_COUNT_INSERT :
        return new CountInsertCollector(virtualDbName);
      case DataCollection.CACHE_STATS_COUNT_SELECT :
        return new CountSelectCollector(virtualDbName);
      case DataCollection.CACHE_STATS_HITS_PERCENTAGE :
        return new HitsRatioCollector(virtualDbName);
      case DataCollection.CACHE_STATS_NUMBER_ENTRIES :
        return new CacheEntriesCollector(virtualDbName);
        /*
         * Scheduler collectors
         */
      case DataCollection.SCHEDULER_NUMBER_READ :
        return new NumberReadCollector(virtualDbName);
      case DataCollection.SCHEDULER_NUMBER_REQUESTS :
        return new NumberRequestsCollector(virtualDbName);
      case DataCollection.SCHEDULER_NUMBER_WRITES :
        return new NumberWriteCollector(virtualDbName);
      case DataCollection.SCHEDULER_PENDING_TRANSACTIONS :
        return new PendingTransactionsCollector(virtualDbName);
      case DataCollection.SCHEDULER_PENDING_WRITES :
        return new PendingWritesCollector(virtualDbName);
        /*
         * Client collectors
         */
      case DataCollection.CLIENT_TIME_ACTIVE :
        return new ClientActiveTimeCollector(virtualDbName, targetName);

        /*
         * Unknown collector
         */
      default :
        throw new DataCollectorException(INVALID_COLLECTOR_TYPE);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#retrieveData
   */
  public long retrieveData(AbstractDataCollector collector)
      throws DataCollectorException
  {
    // Get the new value and return it
    // recall reference to controller (is transient in collector)
    collector.setController(controller);
    return collector.collectValue();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DataCollectorMBean#hasVirtualDatabase(java.lang.String)
   */
  public boolean hasVirtualDatabase(String name)
  {
    return controller.hasVirtualDatabase(name);
  }
}