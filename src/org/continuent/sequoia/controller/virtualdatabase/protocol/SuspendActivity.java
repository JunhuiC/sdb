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
 * Contributor(s): Damian Arregui, Stephane Giron.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * This class a SuspendActivity message which does the following:
 * <ol>
 * <li>Suspend new persistent connections, new transactions and new writes (in
 * this order)
 * <li>Wait for completion of current transactions and persistent connections.
 * </ol>
 * <p>
 * 
 * @author <a href="mailto:ralph.hannus@continuent.com">Ralph Hannus</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class SuspendActivity extends DistributedVirtualDatabaseMessage
{
  private static final long    serialVersionUID = -8451114082404567986L;

  private transient LinkedList totalOrderQueue;

  /**
   * Creates a new <code>DisableBackendsAndSetCheckpoint</code> object
   */
  public SuspendActivity()
  {
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    totalOrderQueue = dvdb.getTotalOrderQueue();
    if (totalOrderQueue == null)
      return new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.total.order.queue", dvdb
              .getVirtualDatabaseName()));

    synchronized (totalOrderQueue)
    {
      SuspendWritesMessage request = new SuspendWritesMessage(
          "Suspend activity");
      totalOrderQueue.addLast(request);
      return request;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    dvdb.setActivityStatus(VirtualDatabase.SUSPENDING);
    Trace logger = dvdb.getLogger();
    dvdb.addOngoingActivitySuspension(sender);

    // Wait for our turn to execute
    boolean found = dvdb.waitForTotalOrder(handleMessageSingleThreadedResult,
        false);

    AbstractScheduler scheduler = dvdb.getRequestManager().getScheduler();

    // Suspend new persistent connections
    scheduler.suspendNewPersistentConnections();

    // Suspend new transactions and writes
    scheduler.suspendNewTransactionsAndWrites();

    // Remove ourselves from the queue to allow others to complete if needed
    if (!found)
      logger
          .error("Suspend activity was not found in total order queue, posting out of order");
    else
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.removeFirst();
        totalOrderQueue.notifyAll();
      }

    // Wait for writes to complete
    try
    {
      scheduler.waitForSuspendedWritesToComplete();
    }
    catch (SQLException e)
    {
      dvdb.getLogger().error("Failed to wait for writes to complete");
      return e;
    }

    // Wait for transactions to complete
    try
    {
      scheduler.waitForSuspendedTransactionsToComplete();
    }
    catch (SQLException e)
    {
      dvdb.getLogger().error(
          "Failed to wait for suspended transactions to complete");
      return e;
    }

    // Wait for opened persistent connections to be closed
    try
    {
      scheduler.waitForPersistentConnectionsToComplete();
    }
    catch (SQLException e)
    {
      dvdb.getLogger().error(
          "Failed to wait for persistent connections to close");
      return e;
    }

    if (!dvdb.hasRecoveryLog())
    {
      // Resume transactions, writes and persistent connections
      scheduler.resumeWritesTransactionsAndPersistentConnections();
      return new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));
    }

    dvdb.setActivityStatus(VirtualDatabase.SUSPENDED);

    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#cancel(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase)
   */
  public void cancel(DistributedVirtualDatabase dvdb)
  {
    if (dvdb.getLogger().isWarnEnabled())
      dvdb
          .getLogger()
          .warn(
              "Canceling SuspendActivity message: resuming writes, transactions and persistent connections");
    AbstractScheduler scheduler = dvdb.getRequestManager().getScheduler();
    scheduler.resumeWritesTransactionsAndPersistentConnections();
    dvdb.setActivityStatus(VirtualDatabase.RUNNING);
  }
}
