/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2007 Continuent, Inc.
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
 * Contributor(s): Vadim Kassin, Jaco Swart, Jean-Bernard van Zuylen
 */

package org.continuent.sequoia.controller.loadbalancer;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.continuent.sequoia.common.exceptions.BadConnectionException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.locks.ReadPrioritaryFIFOWriteLock;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.protocol.PreparedStatementSerialization;
import org.continuent.sequoia.common.sql.filters.MacrosHandler;
import org.continuent.sequoia.common.sql.metadata.SequoiaParameterMetaData;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.DriverCompliance;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.CreateRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.protocol.SuspendWritesMessage;

/**
 * The Request Load Balancer should implement the load balancing of the requests
 * among the backend nodes.
 * <p>
 * The requests comes from the Request Controller and are sent to the Connection
 * Managers.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:vadim@kase.kz">Vadim Kassin </a>
 * @author <a href="mailto:jaco.swart@iblocks.co.uk">Jaco Swart </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat </a>
 * @version 1.0
 */
public abstract class AbstractLoadBalancer implements XmlComponent
{

  //
  // How the code is organized ?
  //
  // 1. Member variables/Constructor
  // 2. Getter/Setter (possibly in alphabetical order)
  // 3. Request handling
  // 4. Transaction management
  // 5. Backend management
  // 6. Debug/Monitoring
  //

  // Virtual Database this load balancer is attached to.
  protected VirtualDatabase             vdb;
  protected RecoveryLog                 recoveryLog;
  protected int                         raidbLevel;
  protected int                         parsingGranularity;
  /** Reference to distributed virtual database total order queue */
  protected LinkedList<?>                  totalOrderQueue;

  protected MacrosHandler               macroHandler;

  /**
   * List of enabled backends (includes backends in either ENABLED or DISABLING
   * state).
   * 
   * @see org.continuent.sequoia.common.jmx.management.BackendState
   */
  protected ArrayList<Object>                  enabledBackends;
  protected ReadPrioritaryFIFOWriteLock backendListLock = new ReadPrioritaryFIFOWriteLock();

  /** Should we wait for all backends to commit before returning ? */
  public WaitForCompletionPolicy        waitForCompletionPolicy;

  private static int                    defaultTransactionIsolationLevel;

  protected static Trace                logger          = Trace
                                                            .getLogger("org.continuent.sequoia.controller.loadbalancer");

  /**
   * Generic constructor that sets some member variables and checks that
   * backends are in the disabled state
   * 
   * @param vdb The virtual database this load balancer belongs to
   * @param raidbLevel The RAIDb level of this load balancer
   * @param parsingGranularity The parsing granularity needed by this load
   *          balancer
   */
  protected AbstractLoadBalancer(VirtualDatabase vdb, int raidbLevel,
      int parsingGranularity) throws SQLException
  {
    this.raidbLevel = raidbLevel;
    this.parsingGranularity = parsingGranularity;
    this.vdb = vdb;
    this.totalOrderQueue = vdb.getTotalOrderQueue();
    this.enabledBackends = new ArrayList<Object>();
    try
    {
      vdb.acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
      throw new SQLException(msg);
    }
    int size = vdb.getBackends().size();
    ArrayList<?> backends = vdb.getBackends();
    for (int i = 0; i < size; i++)
    {
      DatabaseBackend backend = (DatabaseBackend) backends.get(i);
      if (backend.isReadEnabled() || backend.isWriteEnabled())
      {
        if (logger.isWarnEnabled())
          logger.warn(Translate.get(
              "loadbalancer.constructor.backends.not.disabled", backend
                  .getName()));
        try
        {
          disableBackend(backend, true);
        }
        catch (Exception e)
        { // Set the disabled state anyway
          backend.disable();
        }
      }
    }
    vdb.releaseReadLockBackendLists();
  }

  //
  // Getter/Setter methods
  //

  /**
   * Returns the defaultTransactionIsolationLevel value.
   * 
   * @return Returns the defaultTransactionIsolationLevel.
   */
  public static final int getDefaultTransactionIsolationLevel()
  {
    return defaultTransactionIsolationLevel;
  }

  /**
   * Sets the defaultTransactionIsolationLevel value.
   * 
   * @param defaultTransactionIsolationLevel The
   *          defaultTransactionIsolationLevel to set.
   */
  public final void setDefaultTransactionIsolationLevel(
      int defaultTransactionIsolationLevel)
  {
    AbstractLoadBalancer.defaultTransactionIsolationLevel = defaultTransactionIsolationLevel;
  }

  /**
   * This sets the macro handler for this load balancer. Handling macros
   * prevents different backends to generate different values when interpreting
   * the macros which could result in data inconsitencies.
   * 
   * @param handler <code>MacrosHandler</code> instance
   */
  public void setMacroHandler(MacrosHandler handler)
  {
    this.macroHandler = handler;
  }

  /**
   * Get the needed query parsing granularity.
   * 
   * @return needed query parsing granularity
   */
  public int getParsingGranularity()
  {
    return parsingGranularity;
  }

  /**
   * Returns the RAIDbLevel.
   * 
   * @return int the RAIDb level
   */
  public int getRAIDbLevel()
  {
    return raidbLevel;
  }

  /**
   * Returns the recoveryLog value.
   * 
   * @return Returns the recoveryLog.
   */
  public final RecoveryLog getRecoveryLog()
  {
    return recoveryLog;
  }

  /**
   * Sets the recoveryLog value.
   * 
   * @param recoveryLog The recoveryLog to set.
   */
  public final void setRecoveryLog(RecoveryLog recoveryLog)
  {
    this.recoveryLog = recoveryLog;
  }

  /**
   * Associate a weight to a backend identified by its logical name.
   * 
   * @param name the backend name
   * @param w the weight
   * @throws SQLException if an error occurs
   */
  public void setWeight(String name, int w) throws SQLException
  {
    throw new SQLException("Weight is not supported by this load balancer");
  }

  //
  // Utility functions
  //

  /**
   * Acquire the given lock and check the number of threads. Throw a
   * NoMoreBackendException if no thread is available else returns the number of
   * threads.
   * 
   * @param request object to remove from the total order queue in case no
   *          backend is available
   * @param requestDescription description of the request to put in the error
   *          message in case of an error
   * @return the number of threads in the acquired list
   * @throws SQLException if there was a problem to acquire the lock on the
   *           enabled backend list
   * @throws NoMoreBackendException if no backends are available anymore
   */
  protected int acquireLockAndCheckNbOfThreads(Object request,
      String requestDescription) throws SQLException, NoMoreBackendException
  {
    try
    {
      backendListLock.acquireRead();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
      throw new SQLException(msg);
    }

    int nbOfThreads = enabledBackends.size();
    if (nbOfThreads == 0)
    {
      releaseLockAndUnlockNextQuery(request);
      throw new NoMoreBackendException(Translate
          .get("loadbalancer.backendlist.empty"));
    }
    else
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("loadbalancer.execute.on.several",
            new String[]{requestDescription, String.valueOf(nbOfThreads)}));
    }
    return nbOfThreads;
  }

  /**
   * Returns the number of nodes to wait for according to the defined
   * <code>waitForCompletion</code> policy.
   * 
   * @param nbOfThreads total number of threads
   * @return int number of threads to wait for
   */
  protected int getNbToWait(int nbOfThreads)
  {
    int nbToWait;
    switch (waitForCompletionPolicy.getPolicy())
    {
      case WaitForCompletionPolicy.FIRST :
        nbToWait = 1;
        break;
      case WaitForCompletionPolicy.MAJORITY :
        nbToWait = nbOfThreads / 2 + 1;
        break;
      case WaitForCompletionPolicy.ALL :
        nbToWait = nbOfThreads;
        break;
      default :
        logger
            .warn(Translate.get("loadbalancer.waitforcompletion.unsupported"));
        nbToWait = nbOfThreads;
        break;
    }
    return nbToWait;
  }

  /**
   * Interprets the macros in the request (depending on the
   * <code>MacroHandler</code> set for this class) and modify either the
   * skeleton or the query itself. Note that the given object is directly
   * modified.
   * 
   * @param request the request to process
   */
  public void handleMacros(AbstractRequest request)
  {
    if (macroHandler == null)
      return;

    // Do not handle macros for requests that don't need it.
    if (!request.needsMacroProcessing())
      return;

    macroHandler.processMacros(request);
  }

  /**
   * Release the backend list lock and remove the current query from the head of
   * the total order queue to unlock the next query.
   * 
   * @param currentQuery the current query to remove from the total order queue
   */
  protected void releaseLockAndUnlockNextQuery(Object currentQuery)
  {
    backendListLock.releaseRead();

    // Unblock next query from total order queue
    removeObjectFromAndNotifyTotalOrderQueue(currentQuery);
  }

  /**
   * Remove an entry of the total order queue (usually the head) and notify the
   * queue so that the next queries can be scheduled.
   * 
   * @param request Object that should be removed from the total order queue
   */
  public void removeObjectFromAndNotifyTotalOrderQueue(Object request)
  {
    if ((totalOrderQueue != null) && (request != null))
    {
      synchronized (totalOrderQueue)
      {
        try
        {
          if (totalOrderQueue.remove(request))
          {
            if (logger.isDebugEnabled())
              logger.debug("Removed " + request + " from total order queue");
            totalOrderQueue.notifyAll();
          }
          else if (logger.isDebugEnabled())
          {
            logger.debug(request + " was not in the total order queue");
          }
        }
        catch (RuntimeException e)
        {
          logger.warn("Unable to remove request " + request
              + " from total order queue", e);
        }
      }
    }
  }

  /**
   * Wait for the completion of the given task. Note that this method must be
   * called within a synchronized block on the task.
   * 
   * @param timeout timeout in ms for this task
   * @param requestDescription description of the request to put in the error
   *          message in case of a timeout
   * @param task the task to wait for completion
   * @throws SQLException if the timeout has expired
   */
  public static void waitForTaskCompletion(long timeout,
      String requestDescription, AbstractTask task) throws SQLException
  {
    // Wait for completion (notified by the task)
    try
    {
      // Wait on task
      if (timeout > 0)
      {
        long start = System.currentTimeMillis();
        task.wait(timeout);
        long end = System.currentTimeMillis();
        long remaining = timeout - (end - start);
        if (remaining <= 0)
        {
          if (logger.isErrorEnabled())
            logger.error("Timed out while waiting for task " + task);
          if (task.setExpiredTimeout())
          { // Task will be ignored by all backends
            String msg = Translate.get("loadbalancer.request.timeout",
                new String[]{requestDescription,
                    String.valueOf(task.getSuccess()),
                    String.valueOf(task.getFailed())});

            logger.warn(msg);
            throw new SQLException(msg);
          }
          // else task execution already started, to late to cancel
        }
        // No need to update request timeout since the execution is finished
      }
      else
        task.wait();
    }
    catch (InterruptedException e)
    {
      if (task.setExpiredTimeout())
      { // Task will be ignored by all backends
        String msg = Translate.get("loadbalancer.request.timeout",
            new String[]{requestDescription, String.valueOf(task.getSuccess()),
                String.valueOf(task.getFailed())});

        logger.warn(msg);
        throw new SQLException(msg);
      }
      // else task execution already started, to late to cancel
    }
  }

  /**
   * If we are executing in a distributed virtual database, we have to make sure
   * that we post the query in the queue following the total order. This method
   * does not remove the request from the total order queue. You have to call
   * removeHeadFromAndNotifyTotalOrderQueue() to do so.
   * 
   * @param request the request to wait for (can be any object but usually a
   *          DistributedRequest, Commit or Rollback)
   * @param errorIfNotFound true if an error message should be logged if the
   *          request is not found in the total order queue
   * @return true if the element was found and wait has succeeded, false
   *         otherwise
   * @see #removeHeadFromAndNotifyTotalOrderQueue(Object)
   */
  public boolean waitForTotalOrder(Object request, boolean errorIfNotFound)
  {
    if (totalOrderQueue != null)
    {
      synchronized (totalOrderQueue)
      {
        int index = totalOrderQueue.indexOf(request);
        while (index > 0)
        {
          if (logger.isDebugEnabled())
            logger.debug("Waiting for " + index
                + " queries to execute (current is " + totalOrderQueue.get(0)
                + ")");

          // All suspended requests can be bypassed
          boolean foundNonSuspendedRequest = false;
          for (int i = 0; i < index; i++)
          {
            if (!vdb.getRequestManager().getScheduler().isSuspendedRequest(
                totalOrderQueue.get(i)))
            {
              foundNonSuspendedRequest = true;
              break;
            }
          }
          if (!foundNonSuspendedRequest)
          {
            index = 0;
            break;
          }

          try
          {
            totalOrderQueue.wait();
          }
          catch (InterruptedException ignore)
          {
          }
          index = totalOrderQueue.indexOf(request);
        }
        if (index == -1)
        {
          if (errorIfNotFound)
            logger
                .error("Request was not found in total order queue, posting out of order ("
                    + request + ")");
          return false;
        }
        else
          return true;
      }
    }
    return false;
  }

  /**
   * This will block the given request if there are any suspending task in
   * progress (before the request in the total order queue).
   * 
   * @param request the request that we are processing
   */
  public void waitForSuspendWritesToComplete(AbstractRequest request)
  {
    if (totalOrderQueue != null)
    {
      synchronized (totalOrderQueue)
      {
        boolean hasToWait = true;
        while (hasToWait)
        {
          hasToWait = false;
          // Checking total order queue to see if there is an
          // SuspendWritesMessage before this request.
          // If this is the case, this request will have to wait.
          for (Iterator<?> iter = totalOrderQueue.iterator(); iter.hasNext();)
          {
            Object elem = iter.next();
            if (elem instanceof SuspendWritesMessage)
            {
              // Found a SuspendWritesMessage, so wait...
              hasToWait = true;
              break;
            }
            else if (elem instanceof AbstractRequest)
            {
              // Found the request itself, let'go then...
              AbstractRequest req = (AbstractRequest) elem;
              if (req == request)
                break;
            }
          }
          if (hasToWait)
            try
            {
              totalOrderQueue.wait();
            }
            catch (InterruptedException ignore)
            {
            }
        }
      }
    }
  }

  //
  // Request Handling
  //

  /**
   * Chooses the first available backend for read operations
   * 
   * @param request request to execute
   * @return the chosen backend
   * @throws SQLException if an error occurs or if no backend is available
   */
  protected DatabaseBackend chooseFirstBackendForReadRequest(
      AbstractRequest request) throws SQLException
  {
    // Choose a backend
    try
    {
      vdb.acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = Translate.get(
          "loadbalancer.backendlist.acquire.readlock.failed", e);
      logger.error(msg);
      throw new SQLException(msg);
    }

    /*
     * The backend that will execute the query
     */
    DatabaseBackend backend = null;

    // Note that vdb lock is released in the finally clause of this try/catch
    // block
    try
    {
      ArrayList<?> backends = vdb.getBackends();
      int size = backends.size();

      if (size == 0)
        throw new SQLException(Translate.get(
            "loadbalancer.execute.no.backend.available", request.getId()));

      // Choose the first available backend
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend b = (DatabaseBackend) backends.get(i);
        if (b.isReadEnabled())
        {
          backend = b;
          break;
        }
      }
    }
    catch (Throwable e)
    {
      String msg = Translate.get("loadbalancer.execute.find.backend.failed",
          new String[]{request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.error(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      vdb.releaseReadLockBackendLists();
    }

    if (backend == null)
      throw new NoMoreBackendException(Translate.get(
          "loadbalancer.execute.no.backend.enabled", request.getId()));

    return backend;
  }

  /**
   * Perform a read request. It is up to the implementation to choose to which
   * backend node(s) this request should be sent.
   * 
   * @param request an <code>SelectRequest</code>
   * @param metadataCache MetadataCache (null if none)
   * @return the corresponding <code>ControllerResultSet</code>
   * @exception SQLException if an error occurs
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   */
  public abstract ControllerResultSet statementExecuteQuery(
      SelectRequest request, MetadataCache metadataCache) throws SQLException,
      AllBackendsFailedException;

  /**
   * Perform a write request. This request should usually be broadcasted to all
   * nodes.
   * 
   * @param request an <code>AbstractWriteRequest</code>
   * @return number of rows affected by the request
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @exception NoMoreBackendException if no backends are left to execute the
   *              request
   * @exception SQLException if an error occurs
   */
  public abstract ExecuteUpdateResult statementExecuteUpdate(
      AbstractWriteRequest request) throws AllBackendsFailedException,
      NoMoreBackendException, SQLException;

  /**
   * Perform a write request and return a ResultSet containing the auto
   * generated keys.
   * 
   * @param request an <code>AbstractWriteRequest</code>
   * @param metadataCache MetadataCache (null if none)
   * @return auto generated keys
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @exception NoMoreBackendException if no backends are left to execute the
   *              request
   * @exception SQLException if an error occurs
   */
  public abstract GeneratedKeysResult statementExecuteUpdateWithKeys(
      AbstractWriteRequest request, MetadataCache metadataCache)
      throws AllBackendsFailedException, NoMoreBackendException, SQLException;

  /**
   * Call a request that returns multiple results.
   * 
   * @param request the request to execute
   * @param metadataCache MetadataCache (null if none)
   * @return an <code>ExecuteResult</code> object
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract ExecuteResult statementExecute(AbstractRequest request,
      MetadataCache metadataCache) throws AllBackendsFailedException,
      SQLException;

  /**
   * Call a read-only stored procedure that returns a ResultSet. The stored
   * procedure will be executed by one node only.
   * 
   * @param proc the stored procedure call
   * @param metadataCache MetadataCache (null if none)
   * @return a <code>ControllerResultSet</code> value
   * @exception SQLException if an error occurs
   */
  public abstract ControllerResultSet readOnlyCallableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException;

  /**
   * Call a read-only stored procedure that returns multiple results. The stored
   * procedure will be executed by one node only.
   * 
   * @param proc the stored procedure call
   * @param metadataCache MetadataCache (null if none)
   * @return a <code>ExecuteResult</code> object containing all results
   * @exception SQLException if an error occurs
   */
  public abstract ExecuteResult readOnlyCallableStatementExecute(
      StoredProcedure proc, MetadataCache metadataCache) throws SQLException;

  /**
   * Call a stored procedure that returns a ResultSet. This stored procedure can
   * possibly perform writes and will therefore be executed by all nodes.
   * 
   * @param proc the stored procedure call
   * @param metadataCache MetadataCache (null if none)
   * @return a <code>ControllerResultSet</code> value
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @exception SQLException if an error occurs
   */
  public abstract ControllerResultSet callableStatementExecuteQuery(
      StoredProcedure proc, MetadataCache metadataCache)
      throws AllBackendsFailedException, SQLException;

  /**
   * Call a stored procedure that performs an update.
   * 
   * @param proc the stored procedure call
   * @return number of rows affected
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract ExecuteUpdateResult callableStatementExecuteUpdate(
      StoredProcedure proc) throws AllBackendsFailedException, SQLException;

  /**
   * Call a stored procedure that returns multiple results.
   * 
   * @param proc the stored procedure call
   * @param metadataCache MetadataCache (null if none)
   * @return an <code>ExecuteResult</code> object
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract ExecuteResult callableStatementExecute(StoredProcedure proc,
      MetadataCache metadataCache) throws AllBackendsFailedException,
      SQLException;

  /**
   * Return a ControllerResultSet containing the PreparedStatement metaData of
   * the given sql template
   * 
   * @param request the request containing the sql template
   * @return an empty ControllerResultSet with the metadata
   * @throws SQLException if a database error occurs
   */
  public abstract ControllerResultSet getPreparedStatementGetMetaData(
      AbstractRequest request) throws SQLException;

  /**
   * Returns a <code>ParameterMetaData</code> containing the metadata of the
   * prepared statement parameters for the given request
   * 
   * @param request the request containing the sql template
   * @return the metadata of the given prepared statement parameters
   * @throws SQLException if a database error occurs
   */
  public abstract ParameterMetaData getPreparedStatementGetParameterMetaData(
      AbstractRequest request) throws SQLException;

  /**
   * Setup a Statement or a PreparedStatement (decoded if parameters of the
   * request are not null).
   */
  private static Statement setupStatementOrPreparedStatement(
      AbstractRequest request, DatabaseBackend backend,
      BackendWorkerThread workerThread, Connection c,
      boolean setupResultSetParameters, boolean needGeneratedKeys)
      throws SQLException
  {
    Statement s; // Can also be used as a PreparedStatement
    if (request.getPreparedStatementParameters() == null)
      s = c.createStatement();
    else
    {
      String rewrittenTemplate = backend.transformQuery(request);
      if (needGeneratedKeys)
        s = c.prepareStatement(rewrittenTemplate,
            Statement.RETURN_GENERATED_KEYS);
      else
        s = c.prepareStatement(rewrittenTemplate);
      PreparedStatementSerialization.setPreparedStatement(request
          .getPreparedStatementParameters(), (PreparedStatement) s);
    }

    // Let the worker thread know which statement we are using in case there is
    // a need to cancel that statement during its execution
    if (workerThread != null)
      workerThread.setCurrentStatement(s);

    DriverCompliance driverCompliance = backend.getDriverCompliance();
    if (driverCompliance.supportSetQueryTimeout())
      s.setQueryTimeout(request.getTimeout());

    if (setupResultSetParameters)
    {
      if ((request.getCursorName() != null)
          && (driverCompliance.supportSetCursorName()))
        s.setCursorName(request.getCursorName());
      if ((request.getFetchSize() != 0)
          && driverCompliance.supportSetFetchSize())
      {
        // We need to let the DriverCompliance instance "approve" our value, as
        // implementations like MySQL access only very specific fetch size
        // values.
        s.setFetchSize(driverCompliance
            .convertFetchSize(request.getFetchSize()));
      }
      if ((request.getMaxRows() > 0) && driverCompliance.supportSetMaxRows())
        s.setMaxRows(request.getMaxRows());
    }
    s.setEscapeProcessing(request.getEscapeProcessing());
    return s;
  }

  /**
   * Execute a statement on a backend. If the execution fails, the connection is
   * checked for validity. If the connection was not valid, the query is
   * automatically retried on a new connection.<br>
   * 
   * @param request the request to execute
   * @param backend the backend on which the request is executed
   * @param workerThread the backend worker thread executing this query (or null
   *          if none)
   * @param c connection used to create the statement
   * @param metadataCache MetadataCache (null if none)
   * @return the ControllerResultSet
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the connection was bad
   * @throws UnreachableBackendException if the backend is unreachable
   */
  public static final ControllerResultSet executeStatementExecuteQueryOnBackend(
      SelectRequest request, DatabaseBackend backend,
      BackendWorkerThread workerThread, Connection c,
      MetadataCache metadataCache) throws SQLException, BadConnectionException,
      UnreachableBackendException
  {
    ControllerResultSet rs = null;
    ResultSet backendRS = null;
    try
    {
      backend.addPendingReadRequest(request);

      Statement s = setupStatementOrPreparedStatement(request, backend,
          workerThread, c, true, false);

      // Execute the query
      if (request.getPreparedStatementParameters() == null)
        backendRS = s.executeQuery(backend.transformQuery(request));
      else
        backendRS = ((PreparedStatement) s).executeQuery();

      SQLWarning stWarns = null;
      if (request.getRetrieveSQLWarnings())
      {
        stWarns = s.getWarnings();
      }
      rs = new ControllerResultSet(request, backendRS, metadataCache, s, false);
      rs.setStatementWarnings(stWarns);
    }
    catch (SQLException e)
    { // Something bad happened
      /*
       * A persistent connection is a direct tie between the client program and
       * the backend database. If the connection is broken, it cannot be retried
       * on another connection on the same backend, because the client may be
       * depending on state that was associated with the broken connection.
       */
      if (backend.isValidConnection(c))
        throw e; // Connection is valid, throw the exception
      else if (request.isPersistentConnection())
        throw new UnreachableBackendException("Bad persistent connection", e);
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      // we can close this resultset if fetch size was 0
      if (backendRS != null && request.getFetchSize() == 0)
        try
        {
          backendRS.close();
        }
        catch (SQLException ignore)
        {
        }
      backend.removePendingRequest(request);
    }
    return rs;
  }

  /**
   * Execute an update prepared statement on a backend. If the execution fails,
   * the connection is checked for validity. If the connection was not valid,
   * the query is automatically retried on a new connection.
   * 
   * @param request the request to execute
   * @param backend the backend on which the request is executed
   * @param workerThread the backend worker thread executing this query (or null
   *          if none)
   * @param pc pooled connection used to create the statement
   * @return int Number of rows effected
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the connection was bad
   */
  public static final ExecuteUpdateResult executeStatementExecuteUpdateOnBackend(
      AbstractWriteRequest request, DatabaseBackend backend,
      BackendWorkerThread workerThread, PooledConnection pc)
      throws SQLException, BadConnectionException
  {
    Statement s = null;
    Connection c = pc.getConnection();
    try
    {
      backend.addPendingWriteRequest(request);

      s = setupStatementOrPreparedStatement(request, backend, workerThread, c,
          false, false);

      if (request.requiresConnectionFlush())
      {
        pc.setMustBeRenewed(true);
      }

      // Execute the query
      ExecuteUpdateResult eur;
      if (request.getPreparedStatementParameters() == null)
        eur = new ExecuteUpdateResult(s.executeUpdate(backend
            .transformQuery(request)));
      else
        eur = new ExecuteUpdateResult(((PreparedStatement) s).executeUpdate());
      // get warnings, if required
      if (request.getRetrieveSQLWarnings())
        eur.setStatementWarnings(s.getWarnings());

      if (request.requiresConnectionPoolFlush())
        backend.flagAllConnectionsForRenewal();

      if (request instanceof CreateRequest
          && ((CreateRequest) request).createsTemporaryTable())
        pc.addTemporaryTables(request.getTableName());
      return eur;
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
      {
        checkForEarlyRollback(c, e);
        throw e; // Connection is valid, throw the exception
      }
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      backend.removePendingRequest(request);
      try
      {
        if (s != null)
          s.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * checkForEarlyRollback checks after a failure occurred during the execution
   * of a statement if the statement was inside a transaction and if the cluster
   * should abort the transaction at this time (early rollback) in order to free
   * locks at the backend level.
   * 
   * @param c connection on which the failing statement was executed
   * @param e exception that was received from the backend
   * @throws SQLException in the case that the rollback would fail.
   */
  private static void checkForEarlyRollback(Connection c, SQLException e)
      throws SQLException
  {
    // If controller is set so, force a transaction to rollback as soon as a
    // failure occurs on a statement execution
    if (ControllerConstants.FORCE_BACKEND_EARLY_ROLLBACK_ON_FAILURE
        && !c.getAutoCommit())
      try
      {
        c.rollback();
      }
      catch (SQLException rollbackError)
      {
        SQLException sqlE = new SQLException("Exception during early rollback");
        rollbackError.initCause(e);
        sqlE.initCause(rollbackError);
        throw sqlE;
      }
  }

  /**
   * Execute an update prepared statement on a backend. If the execution fails,
   * the connection is checked for validity. If the connection was not valid,
   * the query is automatically retried on a new connection.
   * 
   * @param request the request to execute
   * @param backend the backend on which the request is executed
   * @param workerThread the backend worker thread executing this query (or null
   *          if none)
   * @param pc connection used to create the statement
   * @param metadataCache MetadataCache (null if none)
   * @return ControllerResultSet containing the auto-generated keys
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the connection was bad
   */
  public static final GeneratedKeysResult executeStatementExecuteUpdateWithKeysOnBackend(
      AbstractWriteRequest request, DatabaseBackend backend,
      BackendWorkerThread workerThread, PooledConnection pc,
      MetadataCache metadataCache) throws SQLException, BadConnectionException
  {
    if (!backend.getDriverCompliance().supportGetGeneratedKeys())
      throw new SQLException("Backend " + backend.getName()
          + " does not support RETURN_GENERATED_KEYS");

    Statement s = null;
    Connection c = pc.getConnection();
    try
    {
      backend.addPendingWriteRequest(request);

      s = setupStatementOrPreparedStatement(request, backend, workerThread, c,
          false, true);

      if (request.requiresConnectionFlush())
      {
        pc.setMustBeRenewed(true);
      }
      // Execute the query
      int updateCount;
      if (request.getPreparedStatementParameters() == null)
        updateCount = s.executeUpdate(backend.transformQuery(request),
            Statement.RETURN_GENERATED_KEYS);
      else
        updateCount = ((PreparedStatement) s).executeUpdate();
      // get warnings, if required
      SQLWarning stWarns = null;
      if (request.getRetrieveSQLWarnings())
      {
        stWarns = s.getWarnings();
      }
      ControllerResultSet rs = new ControllerResultSet(request, s
          .getGeneratedKeys(), metadataCache, s, false);
      GeneratedKeysResult gkr = new GeneratedKeysResult(rs, updateCount);
      gkr.setStatementWarnings(stWarns);

      if (request.requiresConnectionPoolFlush())
        backend.flagAllConnectionsForRenewal();

      return gkr;
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
      {
        checkForEarlyRollback(c, e);
        throw e; // Connection is valid, throw the exception
      }
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      backend.removePendingRequest(request);
      try
      {
        if (s != null)
          s.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Execute a request that returns multiple results on the given backend. The
   * statement is setXXX if the driver has not processed the statement.
   * 
   * @param request the request to execute
   * @param backend the backend on which to execute the stored procedure
   * @param workerThread the backend worker thread executing this query (or null
   *          if none)
   * @param pc the connection on which to execute the stored procedure
   * @param metadataCache the matedatacache to build the ControllerResultSet
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the connection was bad
   */
  public static final ExecuteResult executeStatementExecuteOnBackend(
      AbstractRequest request, DatabaseBackend backend,
      BackendWorkerThread workerThread, PooledConnection pc,
      MetadataCache metadataCache) throws SQLException, BadConnectionException
  {
    Statement s = null;
    Connection c = pc.getConnection();
    try
    {
      backend.addPendingWriteRequest(request);

      // Disable fetch size when using execute()
      request.setFetchSize(0);

      s = setupStatementOrPreparedStatement(request, backend, workerThread, c,
          true, false);

      if (request.requiresConnectionFlush())
      {
        pc.setMustBeRenewed(true);
      }
      // Execute the query
      boolean hasResult;
      if (request.getPreparedStatementParameters() == null)
        hasResult = s.execute(backend.transformQuery(request));
      else
        hasResult = ((PreparedStatement) s).execute();

      int updatedRows = 0;
      // Process the result and get all ResultSets or udpate counts
      ExecuteResult result = new ExecuteResult();
      // get warnings, if required
      if (request.getRetrieveSQLWarnings())
        result.setStatementWarnings(s.getWarnings());
      int niter = 0;
      do
      {
        if (hasResult)
        {
          ControllerResultSet crs = new ControllerResultSet(request, s
              .getResultSet(), metadataCache, null, true);
          result.addResult(crs);
        }
        else
        {
          updatedRows = s.getUpdateCount();
          result.addResult(updatedRows);
        }
        hasResult = s.getMoreResults();
        niter++;
        logUnreasonableNumberOfIterations(niter);
      }
      while (hasResult || (updatedRows != -1));

      if (request.requiresConnectionPoolFlush())
        backend.flagAllConnectionsForRenewal();

      return result;
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
      {
        checkForEarlyRollback(c, e);
        throw e; // Connection is valid, throw the exception
      }
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      backend.removePendingRequest(request);
      try
      {
        if (s != null)
          s.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Fetch Out and Named parameters if any. The information about the parameters
   * is found in the StoredProcedure object and the results are stored in the
   * same structures.
   * <p>
   * After calling this method, the stored procedure object does not contain
   * anymore the types of the parameters but their returned values.
   * 
   * @param cs callable statement to fetch from
   * @param proc stored procedure object with parameters information
   * @throws SQLException if an error occurs during fetching
   */
  private static void fetchOutAndNamedParameters(CallableStatement cs,
      StoredProcedure proc) throws SQLException
  {
    // First fetch the out parameters
    List<?> outParamIndexes = proc.getOutParameterIndexes();
    if (outParamIndexes != null)
    {
      for (Iterator<?> iter = outParamIndexes.iterator(); iter.hasNext();)
      {
        Object index = iter.next();
        if (index instanceof Integer)
          proc.setOutParameterValue(index, cs.getObject(((Integer) index)
              .intValue()));
        else
          // Named OUT parameter
          proc.setOutParameterValue(index, cs.getObject((String) index));
      }
    }

    // Fetch the named parameters
    List<?> namedParamNames = proc.getNamedParameterNames();
    if (namedParamNames != null)
    {
      for (Iterator<?> iter = namedParamNames.iterator(); iter.hasNext();)
      {
        // Overwrite the type with the result (re-use the same map)
        String paramName = (String) iter.next();
        proc.setNamedParameterValue(paramName, cs.getObject(paramName));
      }
    }
  }

  /**
   * Setup a Statement or a PreparedStatement (decoded if a SQL template is
   * found in the request).
   */
  private static CallableStatement setupCallableStatement(StoredProcedure proc,
      DatabaseBackend backend, BackendWorkerThread workerThread, Connection c,
      boolean setupResultSetParameters) throws SQLException
  {
    // TODO: Query parameters are no longer rewritten. Query rewriting
    // needs to be integrated with interceptors.
    CallableStatement cs; // Can also be used as a PreparedStatement
    cs = c.prepareCall(backend.transformQuery(proc));
    if (proc.getPreparedStatementParameters() != null)
      PreparedStatementSerialization.setCallableStatement(proc
          .getPreparedStatementParameters(), cs, proc);

    // Let the worker thread know which statement we are using in case there is
    // a need to cancel that statement during its execution
    if (workerThread != null)
      workerThread.setCurrentStatement(cs);

    DriverCompliance driverCompliance = backend.getDriverCompliance();
    if (driverCompliance.supportSetQueryTimeout())
      cs.setQueryTimeout(proc.getTimeout());

    if (setupResultSetParameters)
    {
      if ((proc.getCursorName() != null)
          && (driverCompliance.supportSetCursorName()))
        cs.setCursorName(proc.getCursorName());
      if ((proc.getFetchSize() != 0) && driverCompliance.supportSetFetchSize())
        cs.setFetchSize(proc.getFetchSize());
      if ((proc.getMaxRows() > 0) && driverCompliance.supportSetMaxRows())
        cs.setMaxRows(proc.getMaxRows());
    }
    cs.setEscapeProcessing(proc.getEscapeProcessing());
    return cs;
  }

  /**
   * Execute a read stored procedure on the given backend. The callable
   * statement is setXXX if the driver has not processed the statement.<br>
   * 
   * @param proc the stored procedure to execute
   * @param backend the backend on which to execute the stored procedure
   * @param workerThread the backend worker thread executing this query (or null
   *          if none)
   * @param c the connection on which to execute the stored procedure
   * @param metadataCache the matedatacache to build the ControllerResultSet
   * @return the controllerResultSet
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the connection was bad
   */
  public static final ControllerResultSet executeCallableStatementExecuteQueryOnBackend(
      StoredProcedure proc, DatabaseBackend backend,
      BackendWorkerThread workerThread, Connection c,
      MetadataCache metadataCache) throws SQLException, BadConnectionException
  {
    CallableStatement cs = null;
    ResultSet backendRS = null;
    try
    {
      backend.addPendingReadRequest(proc);

      cs = setupCallableStatement(proc, backend, workerThread, c, true);

      // Execute the query
      backendRS = cs.executeQuery();

      SQLWarning stWarns = null;
      if (proc.getRetrieveSQLWarnings())
      {
        stWarns = cs.getWarnings();
      }
      ControllerResultSet rs = new ControllerResultSet(proc, backendRS,
          metadataCache, cs, false);
      rs.setStatementWarnings(stWarns);
      fetchOutAndNamedParameters(cs, proc);

      if (proc.requiresConnectionPoolFlush())
        backend.flagAllConnectionsForRenewal();

      return rs;
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
        throw e; // Connection is valid, throw the exception
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      // we can close this resultset if fetch size was 0
      if (backendRS != null && proc.getFetchSize() == 0)
      {
        try
        {
          backendRS.close();
        }
        catch (SQLException ignore)
        {
        }
      }
      backend.removePendingRequest(proc);
    }
  }

  /**
   * Execute a write stored procedure on the given backend. The callable
   * statement is setXXX if the driver has not processed the statement.
   * 
   * @param proc the stored procedure to execute
   * @param backend the backend on which to execute the stored procedure
   * @param workerThread the backend worker thread executing this query (or null
   *          if none)
   * @param c the connection on which to execute the stored procedure
   * @return the number of updated rows
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the connection was bad
   */
  public static final ExecuteUpdateResult executeCallableStatementExecuteUpdateOnBackend(
      StoredProcedure proc, DatabaseBackend backend,
      BackendWorkerThread workerThread, PooledConnection pc)
      throws SQLException, BadConnectionException
  {
    CallableStatement cs = null;
    Connection c = pc.getConnection();
    try
    {
      backend.addPendingWriteRequest(proc);

      cs = setupCallableStatement(proc, backend, workerThread, c, false);

      if (proc.requiresConnectionFlush())
      {
        pc.setMustBeRenewed(true);
      }

      // Execute the query
      ExecuteUpdateResult eur = new ExecuteUpdateResult(cs.executeUpdate());
      // get warnings, if required
      if (proc.getRetrieveSQLWarnings())
        eur.setStatementWarnings(cs.getWarnings());

      fetchOutAndNamedParameters(cs, proc);

      if (proc.requiresConnectionPoolFlush())
        backend.flagAllConnectionsForRenewal();

      return eur;
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
      {
        checkForEarlyRollback(c, e);
        throw e; // Connection is valid, throw the exception
      }
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      backend.removePendingRequest(proc);
      try
      {
        if (cs != null)
          cs.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Execute a stored procedure that returns multiple results on the given
   * backend. The callable statement is setXXX if the driver has not processed
   * the statement.
   * 
   * @param proc the stored procedure to execute
   * @param backend the backend on which to execute the stored procedure
   * @param workerThread the backend worker thread executing this query (or null
   *          if none)
   * @param c the connection on which to execute the stored procedure
   * @param metadataCache the matedatacache to build the ControllerResultSet
   * @return an <code>ExecuteResult</code> object
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the connection was bad
   */
  public static final ExecuteResult executeCallableStatementExecuteOnBackend(
      StoredProcedure proc, DatabaseBackend backend,
      BackendWorkerThread workerThread, PooledConnection pc,
      MetadataCache metadataCache) throws SQLException, BadConnectionException
  {
    CallableStatement cs = null;
    Connection c = pc.getConnection();
    try
    {
      backend.addPendingWriteRequest(proc);

      // Disable fetch size when using execute()
      proc.setFetchSize(0);

      cs = setupCallableStatement(proc, backend, workerThread, c, true);

      if (proc.requiresConnectionFlush())
      {
        pc.setMustBeRenewed(true);
      }

      // Execute the query
      boolean hasResult = cs.execute();
      int updatedRows = 0;
      // Process the result and get all ResultSets or udpate counts
      ExecuteResult result = new ExecuteResult();
      // get warnings, if required
      if (proc.getRetrieveSQLWarnings())
        result.setStatementWarnings(cs.getWarnings());
      int niter = 0;
      do
      {
        if (hasResult)
        {
          ControllerResultSet crs = new ControllerResultSet(proc, cs
              .getResultSet(), metadataCache, null, true);
          result.addResult(crs);
        }
        else
        {
          updatedRows = cs.getUpdateCount();
          result.addResult(updatedRows);
        }
        if (updatedRows != -1)
          hasResult = cs.getMoreResults();

        niter++;
        logUnreasonableNumberOfIterations(niter);
      }
      while (hasResult || (updatedRows != -1));

      fetchOutAndNamedParameters(cs, proc);

      if (proc.requiresConnectionPoolFlush())
        backend.flagAllConnectionsForRenewal();

      return result;
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
      {
        checkForEarlyRollback(c, e);
        throw e; // Connection is valid, throw the exception
      }
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      backend.removePendingRequest(proc);
      try
      {
        if (cs != null)
          cs.close();
      }
      catch (SQLException ignore)
      {
      }
    }
  }

  /**
   * Sanity check to log buggy JDBC drivers triggering infinite loops. Logs at
   * regular intervals. Warning: does log on zero, so please start at 1!
   * 
   * @param niter number to check
   */
  private static void logUnreasonableNumberOfIterations(int niter)
  {
    if (niter % 1024 != 0)
      return;

    // The time has come...
    Throwable t = new Throwable(); // get a stacktrace
    logger.warn(niter + " getMoreResults() iterations", t);
  }

  /**
   * Get PreparedStatement metadata before the statement is executed.
   * 
   * @param sqlTemplate the PreparedStatement sql template
   * @param backend the backend on which we execute the request
   * @param c the connection to create the statement from
   * @return an empty ResultSet with the associated metadata
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the database connection was bad
   */
  public static final ControllerResultSet preparedStatementGetMetaDataOnBackend(
      String sqlTemplate, DatabaseBackend backend, Connection c)
      throws SQLException, BadConnectionException
  {
    PreparedStatement ps = null;
    try
    {
      ps = c.prepareStatement(sqlTemplate);
      return new ControllerResultSet(ControllerConstants.CONTROLLER_FACTORY
          .getResultSetMetaDataFactory().copyResultSetMetaData(
              ps.getMetaData(), null), new ArrayList<Object[]>());
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
        throw e; // Connection is valid, throw the exception
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      if (ps != null)
      {
        try
        {
          ps.close();
        }
        catch (SQLException ignored)
        {
        }
      }
    }
  }

  /**
   * Gets PreparedStatement paramaters metadata for the given SQL template<br>
   * We need to convert them to Sequoia ParameterMetaData here in case the
   * metadata gathering needs the connection to remain opened.
   * 
   * @param sqlTemplate the PreparedStatement sql template
   * @param backend the backend on which we execute the request
   * @param c the connection to create the statement from
   * @return the metadata of the given prepared statement parameters
   * @throws SQLException if an error occurs
   * @throws BadConnectionException if the database connection was bad
   */
  public static final ParameterMetaData preparedStatementGetParameterMetaDataOnBackend(
      String sqlTemplate, DatabaseBackend backend, Connection c)
      throws SQLException, BadConnectionException
  {
    PreparedStatement ps = null;
    try
    {
      ps = c.prepareStatement(sqlTemplate);
      ParameterMetaData pmd = ps.getParameterMetaData();
      if (pmd == null)
        return null;
      return new SequoiaParameterMetaData(pmd);
    }
    catch (SQLException e)
    { // Something bad happened
      if (backend.isValidConnection(c))
        throw e; // Connection is valid, throw the exception
      else
        throw new BadConnectionException(e);
    }
    finally
    {
      if (ps != null)
      {
        try
        {
          ps.close();
        }
        catch (SQLException ignored)
        {
        }
      }
    }
  }

  //
  // Transaction management
  //

  /**
   * Abort a transaction and all its currently pending or executing queries.
   * 
   * @param tm The transaction marker metadata
   * @throws SQLException if an error occurs
   */
  public abstract void abort(TransactionMetaData tm) throws SQLException;

  /**
   * Begin a new transaction.
   * 
   * @param tm The transaction marker metadata
   * @throws SQLException if an error occurs
   */
  public abstract void begin(TransactionMetaData tm) throws SQLException;

  /**
   * Commit a transaction.
   * 
   * @param tm The transaction marker metadata
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract void commit(TransactionMetaData tm)
      throws AllBackendsFailedException, SQLException;

  /**
   * Rollback a transaction.
   * 
   * @param tm The transaction marker metadata
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract void rollback(TransactionMetaData tm)
      throws AllBackendsFailedException, SQLException;

  /**
   * Rollback a transaction to a savepoint
   * 
   * @param tm The transaction marker metadata
   * @param savepointName The name of the savepoint
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract void rollbackToSavepoint(TransactionMetaData tm,
      String savepointName) throws AllBackendsFailedException, SQLException;

  /**
   * Set a savepoint to a transaction.
   * 
   * @param tm The transaction marker metadata
   * @param name The name of the new savepoint
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract void setSavepoint(TransactionMetaData tm, String name)
      throws AllBackendsFailedException, SQLException;

  /**
   * Release a savepoint from a transaction
   * 
   * @param tm The transaction marker metadata
   * @param name The name of the savepoint ro release
   * @throws AllBackendsFailedException if all backends failed to execute the
   *           request
   * @throws SQLException if an error occurs
   */
  public abstract void releaseSavepoint(TransactionMetaData tm, String name)
      throws AllBackendsFailedException, SQLException;

  /**
   * Close a persistent connection.
   * 
   * @param login login requesting the connection closing
   * @param persistentConnectionId id of the persistent connection to close
   * @throws SQLException if an error occurs
   */
  public abstract void closePersistentConnection(String login,
      long persistentConnectionId) throws SQLException;

  /**
   * Open a persistent connection.
   * 
   * @param login login requesting the connection closing
   * @param persistentConnectionId id of the persistent connection to open
   * @throws SQLException if an error occurs
   */
  public abstract void openPersistentConnection(String login,
      long persistentConnectionId) throws SQLException;

  /**
   * Factorized code to start a transaction on a backend and to retrieve a
   * connection on this backend
   * 
   * @param backend the backend needed to check valid connection against this
   *          backend test statement
   * @param cm the connection manager to use to retrieve connections
   * @param request request that will execute (must carry transaction id and
   *          transaction isolation level (does nothing if equals to
   *          Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL))
   * @return a valid connection with a started transaction
   * @throws SQLException if the backend is valid but set autocommit cannot be
   *           set to false
   * @throws UnreachableBackendException if the backend is not reachable, ie not
   *           valid connection can be retrieved
   * @see org.continuent.sequoia.driver.Connection#DEFAULT_TRANSACTION_ISOLATION_LEVEL
   */
  public static final Connection getConnectionAndBeginTransaction(
      DatabaseBackend backend, AbstractConnectionManager cm,
      AbstractRequest request) throws SQLException, UnreachableBackendException
  {
    PooledConnection pc = null;
    boolean isConnectionValid = false;
    Connection c;

    do
    {
      if (request.isPersistentConnection())
      { // Retrieve the persistent connection and register it for the
        // transaction
        pc = cm.retrieveConnectionInAutoCommit(request);
        cm.registerConnectionForTransaction(pc, request.getTransactionId());
      }
      else
      { // Get a new connection for the transaction
        pc = cm.getConnectionForTransaction(request.getTransactionId());
      }

      // Sanity check
      if (pc == null)
      {
        throw new SQLException(Translate.get(
            "loadbalancer.unable.get.connection", new String[]{
                String.valueOf(request.getTransactionId()), backend.getName()}));
      }
      c = pc.getConnection();
      try
      {
        if (request.getTransactionIsolation() != org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL)
        {
          /*
           * A user specified transaction isolation will prevail on any other
           * settings
           */
          pc.setTransactionIsolation(request.getTransactionIsolation());
        }
        else if (defaultTransactionIsolationLevel != org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL)
        {
          /*
           * The defaultTransactionIsolationLevel can be enforced in the
           * configuration file to force all transactions to use this level of
           * isolation
           */
          pc.setTransactionIsolation(defaultTransactionIsolationLevel);
        }

        c.setAutoCommit(false);
        isConnectionValid = true;
      }
      catch (SQLException e)
      {
        if (backend.isValidConnection(c))
          throw e; // Connection is valid, throw the exception
        else
        {
          cm.deleteConnection(request.getTransactionId());
          if (request.isPersistentConnection())
          {
            cm.deletePersistentConnection(request.getPersistentConnectionId());
          }
        }
      }
    }
    while (!isConnectionValid);
    return c;
  }

  //
  // Backends management
  //

  /**
   * Enable a backend without further check. The backend is at least read
   * enabled but could also be enabled for writes. Ask the corresponding
   * connection manager to initialize the connections if needed.
   * 
   * @param db The database backend to enable
   * @param writeEnabled True if the backend must be enabled for writes
   * @throws SQLException if an error occurs
   */
  public abstract void enableBackend(DatabaseBackend db, boolean writeEnabled)
      throws SQLException;

  /**
   * Disable a backend without further check. Ask the corresponding connection
   * manager to finalize the connections if needed. This method should not be
   * called directly but instead should access the
   * <code>RequestManager.disableBackend(...)</code> method.
   * 
   * @param db The database backend to disable
   * @param forceDisable true if disable must be forced
   * @throws SQLException if an error occurs
   */
  public abstract void disableBackend(DatabaseBackend db, boolean forceDisable)
      throws SQLException;

  /**
   * Get the number of currently enabled backends. 0 means that no backend is
   * available.
   * 
   * @return number of currently enabled backends
   */
  public int getNumberOfEnabledBackends()
  {
    return enabledBackends.size();
  }

  /**
   * Get information about the Request Load Balancer
   * 
   * @return <code>String</code> containing information
   */
  public abstract String getInformation();

  /**
   * Get information about the Request Load Balancer in xml
   * 
   * @return <code>String</code> containing information, xml formatted
   */
  public abstract String getXmlImpl();

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_LoadBalancer + " "
        + DatabasesXmlTags.ATT_transactionIsolation + "=\"");
    switch (defaultTransactionIsolationLevel)
    {
      case Connection.TRANSACTION_READ_UNCOMMITTED :
        info.append(DatabasesXmlTags.VAL_readUncommitted);
        break;
      case Connection.TRANSACTION_READ_COMMITTED :
        info.append(DatabasesXmlTags.VAL_readCommitted);
        break;
      case Connection.TRANSACTION_REPEATABLE_READ :
        info.append(DatabasesXmlTags.VAL_repeatableRead);
        break;
      case Connection.TRANSACTION_SERIALIZABLE :
        info.append(DatabasesXmlTags.VAL_serializable);
        break;
      default :
        info.append(DatabasesXmlTags.VAL_databaseDefault);
        break;
    }
    info.append("\">");
    info.append(getXmlImpl());
    info.append("</" + DatabasesXmlTags.ELT_LoadBalancer + ">");
    return info.toString();
  }
}
