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
 * Contributor(s): Damian Arregui.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.jmx.management.BackendState;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * This class defines a DisableBackendsAndSetCheckpoint message. Writes,
 * Transactions and Persistent connections must be suspended before this request
 * is issued. When executed it does the following things:
 * <ol>
 * <li>Set checkpoint
 * <li>Disable backends if needed
 * <li>Resume writes, transactions and persistent connections.
 * </ol>
 * <p>
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @author <a href="mailto:ralph.hannus@continuent.com">Ralph hannus</a>
 * @version 1.0
 */
public class DisableBackendsAndSetCheckpoint
    extends DistributedVirtualDatabaseMessage
{
  private static final long    serialVersionUID = 5296233035014770268L;

  private ArrayList<?>            backendInfos;
  private String               checkpointName;
  private transient LinkedList<Object> totalOrderQueue;

  /**
   * Creates a new <code>DisableBackendsAndSetCheckpoint</code> object
   * 
   * @param backendInfos list of Backend Info to be disabled, if the list is
   *          empty no backends are disabled
   * @param checkpointName name of the checkpoint to be set
   */
  public DisableBackendsAndSetCheckpoint(
      ArrayList/* <BackendInfo> */<?> backendInfos, String checkpointName)
  {
    this.backendInfos = backendInfos;
    this.checkpointName = checkpointName;
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
      SuspendWritesMessage request = new SuspendWritesMessage("Disable "
          + checkpointName);
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
    Trace logger = dvdb.getLogger();

    // Wait for our turn to execute
    boolean found = dvdb.getRequestManager().getLoadBalancer()
        .waitForTotalOrder(handleMessageSingleThreadedResult, false);

    AbstractScheduler scheduler = dvdb.getRequestManager().getScheduler();
    // Remove ourselves from the queue to allow others to complete if needed
    if (!found)
      logger.error("Disable backend " + backendInfos.toString()
          + " was not found in total order queue, posting out of order ("
          + checkpointName + ")");
    else
      dvdb.getRequestManager().getLoadBalancer()
          .removeObjectFromAndNotifyTotalOrderQueue(
              handleMessageSingleThreadedResult);

    if (!dvdb.hasRecoveryLog())
    {
      // Resume transactions, writes and persistent connections
      scheduler.resumeWritesTransactionsAndPersistentConnections();
      return new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));
    }

    try
    {
      // Insert the checkpoint
      logger.info(Translate.get("recovery.checkpoint.storing", checkpointName));
      dvdb.getRequestManager().getRecoveryLog().storeCheckpoint(checkpointName);

      ControllerException disableStatus = null;
      // Start disabling the backend locally
      if (dvdb.isLocalSender(sender))
      {
        Iterator<?> iter = backendInfos.iterator();
        while (iter.hasNext())
        {
          BackendInfo backendInfo = (BackendInfo) iter.next();

          // Get the real backend instance
          DatabaseBackend db = dvdb.getAndCheckBackend(backendInfo.getName(),
              VirtualDatabase.NO_CHECK_BACKEND);

          boolean cleanState = false;
          if (!(db.isReadEnabled() || db.isWriteEnabled()))
          {
            if (db.isDisabled() || (db.getStateValue() == BackendState.UNKNOWN))
              continue; // Backend already disabled, ignore

            // Else the backend is in a transition state (backuping, ...)
            String message = "Backend " + db.getName()
                + " in transition state ("
                + BackendState.description(db.getStateValue())
                + "), switching backend to unknown state";
            disableStatus = new ControllerException(message);
            logger.error(message);
            db.setState(BackendState.UNKNOWN);
          }
          else
          {
            // Signal the backend should not begin any new transaction
            db.setState(BackendState.DISABLING);
            logger.info(Translate.get("backend.state.disabling", db.getName()));

            // Sanity checks
            cleanState = (db.getActiveTransactions().size() == 0)
                && (!db.hasPersistentConnections());

            if (!cleanState)
            {
              String message = "Open transactions or persistent connections detected when disabling backend "
                  + db.getName() + ", switching backend to unknown state";
              // let the user known that the disable was not clean
              disableStatus = new ControllerException(message);
              logger.error(message);
              logger.error(db.getName() + " open transactions: "
                  + db.getActiveTransactions());
              logger.error(db.getName() + " open persistent connections: "
                  + db.hasPersistentConnections());
              db.setState(BackendState.UNKNOWN);
            }
          }

          // Now we can safely disable the backend since all transactions have
          // completed
          dvdb.getRequestManager().getLoadBalancer().disableBackend(db, false);

          if (cleanState)
          {
            // Update the last known checkpoint
            db.setLastKnownCheckpoint(checkpointName);
            logger.info(Translate.get("backend.state.disabled", db.getName()));
          }
        }
      }

      // update remote backend tables
      dvdb.handleRemoteDisableBackendsNotification(backendInfos, sender);
      return disableStatus;
    }
    catch (Exception e)
    {
      return new ControllerException(e);
    }
  }
}
