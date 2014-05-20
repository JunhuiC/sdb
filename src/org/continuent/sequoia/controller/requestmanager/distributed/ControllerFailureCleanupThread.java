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

package org.continuent.sequoia.controller.requestmanager.distributed;

import java.sql.SQLException;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a ControllerFailureCleanupThread
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ControllerFailureCleanupThread extends Thread
{
  private Hashtable<?, ?>                  cleanupThreadsList;
  private DistributedVirtualDatabase dvdb;
  private DistributedRequestManager  drm;
  private long                       failedControllerId;
  private long                       failoverTimeoutInMs;
  private List<Long>                       persistentConnectionsToRecover;
  private List<Long>                       transactionsToRecover;
  private Trace                      logger = Trace
                                                .getLogger("org.continuent.sequoia.controller.requestmanager.cleanup");

  /**
   * Creates a new <code>ControllerFailureCleanupThread</code> object
   * 
   * @param failoverTimeoutInMs time to wait before proceeding to the cleanup
   * @param failedControllerId id of the controller that failed
   * @param distributedVirtualDatabase the distributed virtual database on which
   *          we should proceed with the cleanup
   * @param cleanupThreads list of cleanup threads from which we should remove
   *          ourselves when we terminate
   */
  public ControllerFailureCleanupThread(
      DistributedVirtualDatabase distributedVirtualDatabase,
      long failedControllerId, long failoverTimeoutInMs,
      Hashtable<?, ?> cleanupThreads, HashMap<?, ?> writesFlushed)
  {
    super("ControllerFailureCleanupThread for controller " + failedControllerId);
    this.dvdb = distributedVirtualDatabase;
    drm = (DistributedRequestManager) dvdb.getRequestManager();
    this.failedControllerId = failedControllerId;
    this.failoverTimeoutInMs = failoverTimeoutInMs;
    this.cleanupThreadsList = cleanupThreads;
  }

  /**
   * {@inheritDoc}
   * 
   * @see java.lang.Runnable#run()
   */
  public void run()
  {
    try
    {
      doRun();
    }
    catch (Throwable t)
    {
      logger.fatal("Cleanup failed", t);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @see java.lang.Thread#run()
   */
  public void doRun()
  {
    Long controllerIdKey = new Long(failedControllerId);
    try
    {
      synchronized (this)
      {
        /*
         * Ensure that all group communication messages get flushed locally
         * (especially commit/rollback before building the open transaction
         * list)
         */
        dvdb.flushGroupCommunicationMessagesLocally(failedControllerId);

        notifyAll();
      }

      // Wait for clients to failover
      try
      {
        synchronized (this)
        {
          if (logger.isInfoEnabled())
            logger.info("Waiting " + failoverTimeoutInMs
                + "ms for client of controller " + controllerIdKey
                + " to failover");
          wait(failoverTimeoutInMs);
        }
      }
      catch (InterruptedException ignore)
      {
      }

      /*
       * Notify queries that were expecting an answer from the failed controller
       * that this will never happen.
       */
      drm.cleanupAllFailedQueriesFromController(controllerIdKey.longValue());

      /*
       * Build list of transactions and persistent connections for which
       * failover did not happen. Start with pending requests in the scheduler
       * then look for active resources in the distributed request manager.
       */
      transactionsToRecover = parseTransactionMetadataListForControllerId(drm
          .getScheduler().getActiveTransactions());
      rollbackInactiveTransactions(controllerIdKey);
      persistentConnectionsToRecover = parsePersistentConnections(drm
          .getScheduler().getOpenPersistentConnections());
      // getTransactionAndPersistentConnectionsFromRequests(drm.getScheduler()
      // .getActiveReadRequests());
      getTransactionAndPersistentConnectionsFromRequests(drm.getScheduler()
          .getActiveWriteRequests());

      // If both lists are empty there is nothing to cleanup
      if ((transactionsToRecover.isEmpty())
          && (persistentConnectionsToRecover.isEmpty()))
        return;

      /*
       * Ok, now everything should have been recovered. Cleanup all remaining
       * objects. The following methods will take care of not cleaning up
       * resources for which failover was notified through
       * notifyTransactionFailover or notifyPersistentConnectionFailover.
       */
      // abortRemainingTransactions(controllerIdKey);
      closeRemainingPersistentConnections(controllerIdKey);
    }
    finally
    {
      logger.info("Cleanup for controller " + failedControllerId
          + " failure is completed.");

      // Remove ourselves from the thread list
      cleanupThreadsList.remove(this);
    }
  }

  /**
   * Rollback all the transactions for clients that did not reconnect. The
   * transaction can exist at this point for 3 reasons.
   * <ul>
   * <li>The client did not have an active request when the controller failed
   * <li>The client had an active request that completed after the failure
   * <li>The active request is blocked by a transaction in the first state.
   * <ul>
   * In the first two cases the, it is safe to immediately rollback the
   * transaction, because there is no current activity. In the third case, we
   * need to wait until the request complete before doing the rollback. So we go
   * through the list of transactions needing recovery repeatly rollbacking the
   * inactive transactions. Rollingback transactions should let the other
   * pending transactions complete.
   * 
   * @param controllerIdKey the controller id
   */
  private void rollbackInactiveTransactions(Long controllerIdKey)
  {
    List<?> transactionsRecovered = dvdb.getTransactionsRecovered(controllerIdKey);
    Map<?, ?> readRequests = drm.getScheduler().getActiveReadRequests();
    Map<?, ?> writeRequests = drm.getScheduler().getActiveWriteRequests();
    while (!transactionsToRecover.isEmpty())
    {
      int waitingForCompletion = 0;
      // Iterate on the list of active transactions (based on scheduler
      // knowledge) that were started by the failed controller.
      for (Iterator<Long> iter = transactionsToRecover.iterator(); iter.hasNext();)
      {
        Long lTid = (Long) iter.next();

        if ((transactionsRecovered == null)
            || !transactionsRecovered.contains(lTid))
        {
          if (!hasRequestForTransaction(lTid.longValue(), readRequests)
              && !hasRequestForTransaction(lTid.longValue(), writeRequests))
          {
            if (logger.isInfoEnabled())
              logger.info("Rollingback transaction " + lTid
                  + " started by dead controller " + failedControllerId
                  + " since client did not ask for failover");

            try
            {
              boolean logRollback = dvdb.getRecoveryLog()
                  .hasLoggedBeginForTransaction(lTid);
              dvdb.rollback(lTid.longValue(), logRollback);
            }
            catch (SQLException e)
            {
              logger.error("Failed to rollback transaction " + lTid
                  + " started by dead controller " + failedControllerId, e);
            }
            catch (VirtualDatabaseException e)
            {
              logger.error("Failed to rollback transaction " + lTid
                  + " started by dead controller " + failedControllerId, e);
            }

            iter.remove();
          }
          else
          {
            waitingForCompletion++;
            if (logger.isDebugEnabled())
              logger.debug("Waiting for activity to complete for " + lTid
                  + " started by dead controller " + failedControllerId
                  + " since client did not ask for failover");
          }
        }
      }

      if (waitingForCompletion == 0)
        break;
      try
      {
        synchronized (writeRequests)
        {
          if (!writeRequests.isEmpty())
          {
            writeRequests.wait(500);
            continue;
          }
        }
        synchronized (readRequests)
        {
          if (!readRequests.isEmpty())
          {
            readRequests.wait(500);
            continue;
          }
        }
      }
      catch (InterruptedException e)
      {
      }
    } // while (!transactionsToRecover.isEmpty())
  }

  /**
   * Returns true if the given map contains an AbstractRequest value that
   * belongs to the given transaction.
   * 
   * @param transactionId transaction id to look for
   * @param map map of Long(transaction id) -> AbstractRequest
   * @return true if a request in the map matches the transaction id
   */
  private boolean hasRequestForTransaction(long transactionId, Map<?, ?> map)
  {
    synchronized (map)
    {
      for (Iterator<?> iter = map.values().iterator(); iter.hasNext();)
      {
        AbstractRequest request = (AbstractRequest) iter.next();
        if (transactionId == request.getTransactionId())
          return true;
      }
    }

    return false;
  }

  /**
   * Shutdown this thread and wait for its completion (the thread will try to
   * end asap and skip work if possible).
   */
  public synchronized void shutdown()
  {
    notifyAll();
    try
    {
      this.join();
    }
    catch (InterruptedException e)
    {
      logger
          .warn("Controller cleanup thread may not have completed before it was terminated");
    }
  }

  private void closeRemainingPersistentConnections(Long controllerId)
  {
    List<?> persistentConnectionsRecovered = dvdb
        .getControllerPersistentConnectionsRecovered(controllerId);
    for (Iterator<Long> iter = persistentConnectionsToRecover.iterator(); iter
        .hasNext();)
    {
      Long lConnectionId = (Long) iter.next();

      if ((persistentConnectionsRecovered == null)
          || !persistentConnectionsRecovered.contains(lConnectionId))
      {
        if (logger.isInfoEnabled())
          logger.info("Closing persistent connection " + lConnectionId
              + " started by dead controller " + failedControllerId
              + " since client did not ask for failover");

        drm.closePersistentConnection(lConnectionId);
      }
    }
  }

  /**
   * Update the transactionsToRecover and persistentConnectionsToRecover lists
   * with the requests found in the HashMap that are matching our failed
   * controller id.
   * 
   * @param map the map to parse
   */
  private void getTransactionAndPersistentConnectionsFromRequests(Map<?, ?> map)
  {
    synchronized (map)
    {
      for (Iterator<?> iter = map.keySet().iterator(); iter.hasNext();)
      {
        Long lTid = (Long) iter.next();
        if ((lTid.longValue() & DistributedRequestManager.CONTROLLER_ID_BIT_MASK) == failedControllerId)
        { // Request id matches the failed controller
          AbstractRequest request = (AbstractRequest) map.get(lTid);
          if (!request.isAutoCommit())
          {
            /*
             * Re-check transaction id in case this was already a failover from
             * another controller
             */
            if ((request.getTransactionId() & DistributedRequestManager.CONTROLLER_ID_BIT_MASK) == failedControllerId)
            {
              Long tidLong = new Long(request.getTransactionId());
              if (!transactionsToRecover.contains(tidLong))
              {
                transactionsToRecover.add(tidLong);
              }
            }
          }
          if (request.isPersistentConnection())
          {
            /*
             * Re-check persistent connection id in case this was a failover
             * from another controller
             */
            Long connIdLong = new Long(request.getPersistentConnectionId());
            if ((request.getPersistentConnectionId() & DistributedRequestManager.CONTROLLER_ID_BIT_MASK) == failedControllerId)
              if (!persistentConnectionsToRecover.contains(connIdLong))
              {
                persistentConnectionsToRecover.add(connIdLong);
              }
          }
        }
      }
    }
  }

  private List<Long> parsePersistentConnections(Map<?, ?> map)
  {
    LinkedList<Long> result = new LinkedList<Long>();
    synchronized (map)
    {
      for (Iterator<?> iter = map.keySet().iterator(); iter.hasNext();)
      {
        Long persistentConnectionId = (Long) iter.next();
        if ((persistentConnectionId.longValue() & DistributedRequestManager.CONTROLLER_ID_BIT_MASK) == failedControllerId)
        { // Request id matches the failed controller
          result.add(persistentConnectionId);
        }
      }
      return result;
    }
  }

  /**
   * Parse the list containing transaction metadata and return a list containing
   * all transaction ids matching the controllerId this thread is taking care
   * of.
   * 
   * @param list transaction metadata list to parse
   * @return sublist containing ids matching failedControllerId
   */
  private List<Long> parseTransactionMetadataListForControllerId(Collection<?> list)
  {
    LinkedList<Long> result = new LinkedList<Long>();
    synchronized (list)
    {
      boolean retry = true;
      while (retry)
      {
        try
        {
          for (Iterator<?> iter = list.iterator(); iter.hasNext();)
          {
            TransactionMetaData tm = (TransactionMetaData) iter.next();
            if ((tm.getTransactionId() & DistributedRequestManager.CONTROLLER_ID_BIT_MASK) == failedControllerId)
              result.addLast(new Long(tm.getTransactionId()));
          }

          retry = false;
        }
        catch (ConcurrentModificationException e)
        {
          // TODO: handle exception
        }
      }
    }
    return result;
  }

}
