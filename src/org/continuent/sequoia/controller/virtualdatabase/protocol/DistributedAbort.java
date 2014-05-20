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

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;

/**
 * Execute a distributed abort.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class DistributedAbort extends DistributedTransactionMarker
{
  private static final long   serialVersionUID = -8954391235872189513L;

  private transient Long      tid;
  // Login that rollbacks the transaction. This is used in case the remote
  // controller has to log the commit but didn't see the begin in which case it
  // will not be able to retrieve the transaction marker metadata
  private String              login;

  private DistributedRollback totalOrderAbort;

  /**
   * Creates a new <code>Abort</code> message.
   * 
   * @param login login that rollback the transaction
   * @param transactionId id of the transaction to commit
   */
  public DistributedAbort(String login, long transactionId)
  {
    super(transactionId);
    this.login = login;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedTransactionMarker#scheduleCommand(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Object scheduleCommand(DistributedRequestManager drm)
      throws SQLException
  {
    LinkedList<Object> totalOrderQueue = drm.getVirtualDatabase().getTotalOrderQueue();
    if (totalOrderQueue != null)
    {
      synchronized (totalOrderQueue)
      {
        totalOrderAbort = new DistributedRollback(login, transactionId);
        totalOrderQueue.addLast(totalOrderAbort);
      }
    }
    return totalOrderAbort;
  }

  /**
   * Execution of a distributed rollback command on the specified
   * <code>DistributedRequestManager</code>
   * 
   * @param drm the DistributedRequestManager that will execute the rollback
   * @return Boolean.TRUE if everything went fine or a SQLException if an error
   *         occured
   * @throws SQLException if an error occurs
   */
  public Serializable executeCommand(DistributedRequestManager drm)
      throws SQLException
  {
    Trace logger = drm.getLogger();

    boolean transactionStartedOnThisController = true;
    tid = new Long(transactionId);
    TransactionMetaData tm;
    try
    {
      tm = drm.getTransactionMetaData(tid);
    }
    catch (SQLException ignore)
    {
      // The transaction was started before the controller joined the
      // cluster, build a fake tm so that we will be able to log it.
      transactionStartedOnThisController = false;
      tm = new TransactionMetaData(transactionId, 0, login, false, 0);
    }

    VirtualDatabaseWorkerThread vdbwt = drm.getVirtualDatabase()
        .getVirtualDatabaseWorkerThreadForTransaction(transactionId);
    if (vdbwt != null)
      vdbwt.notifyAbort(transactionId);

    boolean hasBeenScheduled = false;
    try
    {
      if (transactionStartedOnThisController)
      { // Rollback would fail on a fake tm so skip it
        drm.getScheduler().rollback(tm, totalOrderAbort);
        hasBeenScheduled = true;
      }

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("transaction.aborting", tid));

      // Send to load balancer
      drm.getLoadBalancer().abort(tm);

      // Update recovery log
      drm.getRecoveryLog().logRequestCompletion(tm.getLogId(), true, 0);

      // Invalidate the query result cache if this transaction has updated the
      // cache or altered the schema
      if ((drm.getResultCache() != null)
          && (tm.altersQueryResultCache() || tm.altersDatabaseSchema()))
        drm.getResultCache().rollback(transactionId);

      // Check for schema modifications that need to be rollbacked
      if (tm.altersDatabaseSchema())
      {
        if (drm.getMetadataCache() != null)
          drm.getMetadataCache().flushCache();
        drm.setSchemaIsDirty(true);
      }

      if (hasBeenScheduled) // skipped if tm was fake
      {
        // Notify scheduler for completion
        drm.getScheduler().rollbackCompleted(tm, true);
      }
    }
    catch (NoMoreBackendException e)
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.distributed.abort.logging.only", transactionId));

      addAbortFailureOnAllBackends(drm, hasBeenScheduled, tm);
      throw e;
    }
    catch (SQLException e)
    {
      logger.warn(Translate
          .get("virtualdatabase.distributed.abort.sqlexception"), e);
      addAbortFailureOnAllBackends(drm, hasBeenScheduled, tm);
      return e;
    }
    catch (RuntimeException re)
    {
      logger.warn(Translate
          .get("virtualdatabase.distributed.rollback.exception"), re);
      addAbortFailureOnAllBackends(drm, hasBeenScheduled, tm);
      throw new SQLException(re.getMessage());
    }
    finally
    {
      if (!hasBeenScheduled)
      {
        // Query has not been scheduled, we have to remove the entry from the
        // total order queue (wait to be sure that we do it in the proper order)
        if (drm.getLoadBalancer().waitForTotalOrder(totalOrderAbort, false))
          drm.getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(
              totalOrderAbort);
      }

      if (transactionStartedOnThisController)
      {
        drm.completeTransaction(tid);
      }

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("transaction.aborted", tid));
    }
    return Boolean.TRUE;
  }

  private void addAbortFailureOnAllBackends(DistributedRequestManager drm,
      boolean hasBeenScheduled, TransactionMetaData tm)
  {
    AbstractRequest abortRequest = new UnknownWriteRequest("abort", false, 0,
        "\n");
    abortRequest.setLogin(login);
    abortRequest.setTransactionId(transactionId);
    abortRequest.setIsAutoCommit(false);
    abortRequest.setLogId(tm.getLogId());
    drm.addFailedOnAllBackends(abortRequest, hasBeenScheduled);
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Abort transaction " + transactionId;
  }
}