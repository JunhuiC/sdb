/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;

/**
 * Execute a distributed commit.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class DistributedCommit extends DistributedTransactionMarker
{
  private static final long serialVersionUID = 1222810057093662283L;

  // Login that commits the transaction. This is used in case the remote
  // controller has to log the commit but didn't see the begin in which case it
  // will not be able to retrieve the transaction marker metadata
  private String            login;

  /**
   * Creates a new <code>Commit</code> message.
   * 
   * @param login login that commit the transaction
   * @param transactionId id of the transaction to commit
   */
  public DistributedCommit(String login, long transactionId)
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
        totalOrderQueue.addLast(this);
      }
    }
    return this;
  }

  /**
   * Execution of a distributed commit command on the specified
   * <code>DistributedRequestManager</code>
   * 
   * @param drm the DistributedRequestManager that will execute the commit
   * @return Boolean.TRUE if everything went fine or a SQLException if an error
   *         occured
   * @throws SQLException if an error occurs
   */
  public Serializable executeCommand(DistributedRequestManager drm)
      throws SQLException
  {
    boolean transactionStartedOnThisController = true;
    Long tid = new Long(transactionId);
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

    Trace logger = drm.getLogger();
    boolean hasBeenScheduled = false;
    try
    {
      if (transactionStartedOnThisController)
      {
        drm.getScheduler().commit(tm, false, this);
        hasBeenScheduled = true;
      }

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("transaction.commit", String.valueOf(tid)));

      // Send to load balancer
      drm.getLoadBalancer().commit(tm);

      // Notify the cache
      if (drm.getResultCache() != null)
        drm.getResultCache().commit(tm.getTransactionId());

      // Update recovery log
      drm.getRecoveryLog().logRequestCompletion(tm.getLogId(), true, 0);

      if (transactionStartedOnThisController)
      {
        // Notify scheduler for completion
        drm.getScheduler().commitCompleted(tm, true);
        drm.completeTransaction(tid);
      }
    }
    catch (NoMoreBackendException e)
    {
      addFailedCommitOnAllBackends(drm, tm, hasBeenScheduled);
      throw e;
    }
    catch (SQLException e)
    {

      if (tm.isReadOnly())
      {
        boolean beginLogged = false;
        if (drm.getRecoveryLog() != null)
        {
          beginLogged = drm.getRecoveryLog().hasLoggedBeginForTransaction(tid);
        }
        if (!beginLogged)
        {
          if (logger.isWarnEnabled())

          {
            logger
                .warn("Ignoring failure of commit for read-only transaction, exception was: "
                    + e);
          }

          if (hasBeenScheduled)
            drm.getScheduler().commitCompleted(tm, true);

          if (transactionStartedOnThisController)
          {
            drm.completeTransaction(tid);
          }
          return Boolean.TRUE;
        }
      }
      addFailedCommitOnAllBackends(drm, tm, hasBeenScheduled);
      logger.warn(Translate
          .get("virtualdatabase.distributed.commit.sqlexception"), e);
      return e;
    }
    catch (RuntimeException re)
    {
      addFailedCommitOnAllBackends(drm, tm, hasBeenScheduled);
      logger.warn(
          Translate.get("virtualdatabase.distributed.commit.exception"), re);
      throw new SQLException(re.getMessage());
    }
    catch (AllBackendsFailedException e)
    {
      addFailedCommitOnAllBackends(drm, tm, hasBeenScheduled);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.distributed.commit.all.backends.locally.failed",
            transactionId));
      return e;
    }

    return Boolean.TRUE;
  }

  private void addFailedCommitOnAllBackends(DistributedRequestManager drm,
      TransactionMetaData tm, boolean hasBeenScheduled)
  {
    AbstractRequest request = new UnknownWriteRequest("commit", false, 0, "\n");
    request.setTransactionId(transactionId);
    request.setLogId(tm.getLogId());
    drm.addFailedOnAllBackends(request, hasBeenScheduled);
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Commit transaction " + transactionId;
  }
}