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
 * Initial developer(s): Jean-Bernard van Zuylen.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;

/**
 * Execute a distributed rollback to savepoint
 * 
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class DistributedRollbackToSavepoint
    extends DistributedTransactionMarker
{
  private static final long serialVersionUID = -8670132997537808225L;

  private String            savepointName;

  /**
   * Creates a new <code>RollbackToSavepoint</code> message
   * 
   * @param transactionId the transaction identifier
   * @param savepointName the savepoint name
   */
  public DistributedRollbackToSavepoint(long transactionId, String savepointName)
  {
    super(transactionId);
    this.savepointName = savepointName;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedTransactionMarker#scheduleCommand(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Object scheduleCommand(DistributedRequestManager drm)
      throws SQLException
  {
    LinkedList totalOrderQueue = drm.getVirtualDatabase().getTotalOrderQueue();
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
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedTransactionMarker#executeCommand(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Serializable executeCommand(DistributedRequestManager drm)
      throws SQLException
  {
    boolean hasBeenScheduled = false;

    // Let's find the transaction marker since it will be used even for
    // logging purposes
    Long tid = new Long(transactionId);
    TransactionMetaData tm;
    try
    {
      tm = drm.getTransactionMetaData(tid);
    }
    catch (SQLException e)
    {
      // Remove the rollback from the total order queue
      drm.getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(this);
      throw e;
    }

    // Check that a savepoint with given name has been set
    if (!drm.hasSavepoint(tid, savepointName))
    {
      // Remove the rollback from the total order queue
      drm.getLoadBalancer().removeObjectFromAndNotifyTotalOrderQueue(this);
      addRollbackFailureOnAllBackends(drm, hasBeenScheduled, tm);
      throw new SQLException(Translate.get("transaction.savepoint.not.found",
          new String[]{savepointName, String.valueOf(transactionId)}));
    }

    try
    {
      // Wait for the scheduler to give us the authorization to execute
      drm.getScheduler().rollback(tm, savepointName, this);
      hasBeenScheduled = true;

      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get("transaction.rollbacksavepoint", new String[]{
                savepointName, String.valueOf(transactionId)}));

      // Send to load balancer
      drm.getLoadBalancer().rollbackToSavepoint(tm, savepointName);

      // Update recovery log
      drm.getRecoveryLog().logRequestCompletion(tm.getLogId(), true, 0);

      // Notify scheduler for completion
      drm.getScheduler().savepointCompleted(transactionId);
    }
    catch (NoMoreBackendException e)
    {
      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get(
                "virtualdatabase.distributed.rollbacksavepoint.logging.only",
                new String[]{savepointName, String.valueOf(transactionId)}));

      addRollbackFailureOnAllBackends(drm, hasBeenScheduled, tm);
      throw e;
    }
    catch (SQLException e)
    {
      addRollbackFailureOnAllBackends(drm, hasBeenScheduled, tm);
      drm
          .getLogger()
          .warn(
              Translate
                  .get("virtualdatabase.distributed.rollbacksavepoint.sqlexception"),
              e);
      return e;
    }
    catch (RuntimeException re)
    {
      addRollbackFailureOnAllBackends(drm, hasBeenScheduled, tm);
      drm.getLogger().warn(
          Translate
              .get("virtualdatabase.distributed.rollbacksavepoint.exception"),
          re);
      throw new SQLException(re.getMessage());
    }
    catch (AllBackendsFailedException e)
    {
      addRollbackFailureOnAllBackends(drm, hasBeenScheduled, tm);
      if (drm.getLogger().isDebugEnabled())
        drm
            .getLogger()
            .debug(
                Translate
                    .get(
                        "virtualdatabase.distributed.rollbacksavepoint.all.backends.locally.failed",
                        new String[]{savepointName,
                            String.valueOf(transactionId)}));
      return e;
    }
    finally
    {
      // Remove all the savepoints set after the savepoint we rollback to
      drm.removeSavepoints(tid, savepointName);
    }
    return Boolean.TRUE;
  }

  private void addRollbackFailureOnAllBackends(DistributedRequestManager drm,
      boolean hasBeenScheduled, TransactionMetaData tm)
  {
    AbstractRequest request = new UnknownWriteRequest("rollback "
        + savepointName, false, 0, "\n");
    request.setTransactionId(transactionId);
    request.setLogId(tm.getLogId());
    drm.addFailedOnAllBackends(request, hasBeenScheduled);
  }

  /**
   * Returns the savepointName value.
   * 
   * @return Returns the savepointName.
   */
  public String getSavepointName()
  {
    return savepointName;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj)
  {
    if (super.equals(obj))
      return savepointName.equals(((DistributedRollbackToSavepoint) obj)
          .getSavepointName());
    else
      return false;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Rollback transaction " + transactionId + " to savepoint "
        + savepointName;
  }
}
