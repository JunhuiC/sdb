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
import java.util.LinkedList;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * This class defines a ResumeActivity message. It undoes the affects of
 * SuspendActivity. When executed it does the following things:
 * <ol>
 * <li>Resume writes, transactions and persistent connections.
 * <li>Only waits for BlockActivity and SuspendActivity distributed messages in
 * the total order queue before executing.
 * </ol>
 * <p>
 * 
 * @see BlockActivity
 * @see SuspendActivity
 * @author <a href="mailto:ralph.hannus@continuent.com">Ralph Hannus</a>
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @author <a href="mailto:Stephane.Giron@continuent.com>Stephane Giron </a>
 * @version 1.0
 */
public class ResumeActivity extends DistributedVirtualDatabaseMessage
{
  private static final long    serialVersionUID = 7285174496382359441L;

  private transient LinkedList totalOrderQueue;

  private boolean              interactiveCommand;

  /**
   * Creates a new <code>ResumeActivity</code> object
   */
  public ResumeActivity()
  {
    this(false);
  }

  public ResumeActivity(boolean isInteractive)
  {
    interactiveCommand = isInteractive;
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
      ResumeActivityMessage request = new ResumeActivityMessage();
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
    dvdb.setActivityStatus(VirtualDatabase.RESUMING);
    Trace logger = dvdb.getLogger();

    // Wait for our turn to execute
    boolean found = dvdb.waitForBlockAndSuspendInTotalOrder(
        handleMessageSingleThreadedResult, false);

    if (interactiveCommand && dvdb.isSuspendedFromLocalController())
      dvdb.interactiveResumeActivity();

    AbstractScheduler scheduler = dvdb.getRequestManager().getScheduler();
    scheduler.resumeWritesTransactionsAndPersistentConnections();
    dvdb.removeOngoingActivitySuspension(sender);

    // Remove ourselves from the queue to allow others to complete if needed
    if (!found)
      logger
          .error("Resume activity was not found in total order queue, posting out of order");
    else
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.remove(handleMessageSingleThreadedResult);
        totalOrderQueue.notifyAll();
      }
    dvdb.setActivityStatus(VirtualDatabase.RUNNING);
    return null;
  }
}
