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

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.util.LinkedList;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a CompleteRecoveryLogResync message used to check that the
 * recovery log resync happened properly.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class CompleteRecoveryLogResync
    extends DistributedVirtualDatabaseMessage
{
  private static final long    serialVersionUID = 5811295250708240645L;

  private long                 commonCheckpointId;
  private String               commonCheckpointName;
  private String               nowCheckpointName;
  private long                 nbOfEntriesToResync;

  private transient LinkedList<Object> totalOrderQueue;

  /**
   * Creates a new <code>CompleteRecoveryLogResync</code> object. <br>
   * This object will be used to check that the restore log operation did not
   * generate any inconcistency (by checking that the number of copied log
   * entries from commonCheckpointId to the log id corresponding to
   * nowCheckpointName is equal to nbOfEntriesToResync)
   * 
   * @param commonCheckpointId common checkpoint id where resync will start
   * @param nowCheckpointName newly inserted checkpoint where resync will end
   * @param nbOfEntriesToResync number of entries to be resynchronized between
   *          these 2 checkpoints
   */
  public CompleteRecoveryLogResync(long commonCheckpointId,
      String nowCheckpointName, long nbOfEntriesToResync)
  {
    this.commonCheckpointId = commonCheckpointId;
    this.nowCheckpointName = nowCheckpointName;
    this.nbOfEntriesToResync = nbOfEntriesToResync;
  }

  /**
   * Creates a new <code>CompleteRecoveryLogResync</code> object<br>
   * It will be used there to check that the automatic resync of recovery logs
   * worked well, by verifying that the number of log entries that has been
   * transfered is the expected one (nbOfEntriesToResync) between the two log
   * entries identified by commonCheckpointName and nowCheckpointName.
   * 
   * @param commonCheckpointName common checkpoint name where resync will start
   * @param nowCheckpointName newly inserted checkpoint where resync will end
   * @param nbOfEntriesToResync number of entries to be resynchronized between
   *          these 2 checkpoints
   */
  public CompleteRecoveryLogResync(String commonCheckpointName,
      String nowCheckpointName, long nbOfEntriesToResync)
  {
    this.commonCheckpointName = commonCheckpointName;
    this.nowCheckpointName = nowCheckpointName;
    this.nbOfEntriesToResync = nbOfEntriesToResync;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    if (!dvdb.hasRecoveryLog())
      return new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));

    totalOrderQueue = dvdb.getTotalOrderQueue();
    if (totalOrderQueue == null)
      return new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.total.order.queue", dvdb.getVirtualDatabaseName()));

    synchronized (totalOrderQueue)
    {
      totalOrderQueue.addLast(this);
      return this;
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
    if (!dvdb.hasRecoveryLog())
      return new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));

    Trace logger = dvdb.getLogger();
    try
    {
      // Wait for our turn to execute
      if (!dvdb.waitForTotalOrder(handleMessageSingleThreadedResult, false))
        logger
            .error("CompleteRecoveryLogResync was not found in total order queue, posting out of order ("
                + commonCheckpointName + ")");
      else
        synchronized (totalOrderQueue)
        {
          totalOrderQueue.removeFirst();
          totalOrderQueue.notifyAll();
        }

      RecoveryLog recoveryLog = dvdb.getRequestManager().getRecoveryLog();
      long nowCheckpointId = recoveryLog.getCheckpointLogId(nowCheckpointName);
      if (commonCheckpointName != null)
      {
        commonCheckpointId = recoveryLog
            .getCheckpointLogId(commonCheckpointName);
      }
      long localNbOfLogEntries = recoveryLog.getNumberOfLogEntries(
          commonCheckpointId, nowCheckpointId);
      long diff = localNbOfLogEntries - nbOfEntriesToResync;

      if (logger.isDebugEnabled())
      {
        logger.debug("Recovery log is being resynchronized between "
            + commonCheckpointName + "and " + nowCheckpointId + " ("
            + nowCheckpointName + ")");
        logger.debug("Recovery log has resynchronized " + localNbOfLogEntries
            + "entries (diff with remote controller is " + diff + ")");
      }

      if (diff != 0)
        logger
            .error("Detected inconsistency in restore log operation, logs differ by "
                + diff
                + " entries (original was "
                + nbOfEntriesToResync
                + ", local is " + localNbOfLogEntries + ")");
      else
        logger.info("Recovery log re-synchronized " + nbOfEntriesToResync
            + " entries successfully");

      // Send the difference
      return new Long(diff);
    }
    catch (Exception e)
    {
      logger.error("Unable to complete recovery log resynchronization", e);
      return new VirtualDatabaseException(
          "Unable to complete recovery log resynchronization", e);
    }
  }
}
