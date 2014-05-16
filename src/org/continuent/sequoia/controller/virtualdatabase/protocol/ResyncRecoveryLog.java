/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
 * Contact: sequoia@continuent.org
 * 
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
 * Initial developer(s): Olivier Fambon.
 * Contributor(s): Stephane Giron.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;

import org.continuent.hedera.adapters.MulticastRequestAdapter;
import org.continuent.hedera.adapters.MulticastResponse;
import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.events.LogEntry;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This message is used attempt an automatic recovery log resynchronization when
 * a vdb is loaded on a controller that is not the first in it's group. Upon
 * reception of this message, the 'mother' vdb checks to see if the specified
 * checkpoint exists. If not, the call fails, and the sender should try another
 * chekpoint name. If the specified checkpoint exists, a 'now' checkpoint is set
 * (cluster-wide) and the call returns with this object containing the name of
 * this checkpoint and the size of the chunck or LogEntries that it will send.
 * 
 * @see org.continuent.sequoia.controller.virtualdatabase.protocol.ReplicateLogEntries
 * @see org.continuent.sequoia.controller.virtualdatabase.protocol.CopyLogEntry
 * @author <a href="mailto:Olivier.Fambon@continuent.com>Olivier Fambon </a>
 * @author <a href="mailto:Stephane.Giron@continuent.com>Stephane Giron </a>
 * @version 1.0
 */
public class ResyncRecoveryLog extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = 3246850782028970719L;

  private String            checkpointName;

  /**
   * Creates a new <code>ResyncRecoveryLog</code> message
   * 
   * @param checkpointName The checkpoint from which to resync the log.
   */
  public ResyncRecoveryLog(String checkpointName)
  {
    this.checkpointName = checkpointName;
  }

  /**
   * Returns the checkpointName value.
   * 
   * @return Returns the 'now' checkpointName.
   */
  public String getCheckpointName()
  {
    return checkpointName;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    return null;
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

    RecoveryLog recoveryLog = dvdb.getRequestManager().getRecoveryLog();

    long commonCheckpointId;
    try
    {
      commonCheckpointId = recoveryLog.getCheckpointLogId(checkpointName);
    }
    catch (SQLException e)
    {
      return new VirtualDatabaseException("Unable to retrieve checkpoint "
          + checkpointName, e);
    }

    /* set a global 'now' checkpoint (temporary)
     * When this request completes all activity on the system is 
     * suspended and must be resumed by calling RequestManager.resumeActivity()
     */
    String nowCheckpointName;
    try
    {
      nowCheckpointName = dvdb.setLogReplicationCheckpoint(sender);
    }
    catch (VirtualDatabaseException e)
    {
      return e;
    }

    // get it's id (ewerk) so that we can replicate it on the other side
    long nowCheckpointId;
    Trace logger = dvdb.getLogger();
    try
    {
      nowCheckpointId = recoveryLog.getCheckpointLogId(nowCheckpointName);
    }
    catch (SQLException e)
    {
      dvdb.getRequestManager().resumeActivity(false);
      String errorMessage = "Cannot find 'now checkpoint' log entry";
      logger.error(errorMessage);
      return new VirtualDatabaseException(errorMessage);
    }

    // Compute the number of entries to be replayed and send it to the remote
    // controller so that it can allocate the proper number of entries in its
    // recovery log
    long nbOfEntriesToResync = nowCheckpointId - commonCheckpointId;
    long diff;
    try
    {
      Serializable replyValue = dvdb.sendMessageToController(sender,
          new InitiateRecoveryLogResync(checkpointName, commonCheckpointId,
              nowCheckpointName, nbOfEntriesToResync), dvdb
              .getMessageTimeouts().getReplicateLogEntriesTimeout());
      if (replyValue instanceof Long)
        diff = ((Long) replyValue).longValue();
      else
        throw new RuntimeException(
            "Invalid answer from remote controller on InitiateRecoveryLogResync ("
                + replyValue + ")");
    }
    catch (Exception e)
    {
      String errorMessage = "Failed to initialize recovery log resynchronization";
      logger.error(errorMessage, e);
      return new VirtualDatabaseException(errorMessage, e);
    }
    finally
    {
      dvdb.getRequestManager().resumeActivity(false);
    }

    logger.info("Resynchronizing from checkpoint " + checkpointName + " ("
        + commonCheckpointId + ") to checkpoint " + nowCheckpointName + " ("
        + nowCheckpointId + ")");

    // protect from concurrent log updates: fake a recovery (increments
    // semaphore)
    recoveryLog.beginRecovery();

    // copy the entries over to the remote controller.
    // Send them one by one over to the remote controller, coz each LogEntry can
    // potentially be huge (e.g. if it contains a blob)
    try
    {
      ArrayList dest = new ArrayList();
      dest.add(sender);
      long copyLogEntryTimeout = dvdb.getMessageTimeouts()
          .getCopyLogEntryTimeout();
      for (long id = commonCheckpointId; id < nowCheckpointId; id++)
      {
        LogEntry entry = recoveryLog.getNextLogEntry(id);
        if (entry == null)
        {
          String errorMessage = "Cannot find expected log entry: " + id;
          logger.error(errorMessage);
          return new VirtualDatabaseException(errorMessage);
        }

        // Because 'getNextLogEntry()' will hunt for the next valid log entry,
        // we need to update the iterator with the new id value - 1
        id = entry.getLogId() - 1;
        entry.setLogId(entry.getLogId() + diff);

        MulticastResponse resp = dvdb.getMulticastRequestAdapter()
            .multicastMessage(dest, new CopyLogEntry(entry),
                MulticastRequestAdapter.WAIT_NONE, copyLogEntryTimeout);
        if (resp.getFailedMembers() != null)
          throw new IOException("Failed to deliver log entry " + id
              + " to remote controller " + sender);
      }
    }
    catch (Exception e)
    {
      String errorMessage = "Failed to complete recovery log resynchronization";
      logger.error(errorMessage, e);
      return new VirtualDatabaseException(errorMessage, e);
    }
    finally
    {
      recoveryLog.endRecovery(); // release semaphore
    }

    // Now check that no entry was missed by the other controller since we
    // shipped all entries asynchronously without getting any individual ack
    // (much faster to address SEQUOIA-504)
    try
    {
      long localNbOfLogEntries = recoveryLog.getNumberOfLogEntries(
          commonCheckpointId, nowCheckpointId);

      if (logger.isDebugEnabled())
        logger.debug("Checking that " + localNbOfLogEntries
            + " entries were replicated on remote controller");

      Serializable replyValue = dvdb.sendMessageToController(sender,
          new CompleteRecoveryLogResync(checkpointName, nowCheckpointName,
              localNbOfLogEntries), dvdb.getMessageTimeouts()
              .getReplicateLogEntriesTimeout());
      if (replyValue instanceof Long)
      {
        diff = ((Long) replyValue).longValue();
        if (diff != 0)
          return new VirtualDatabaseException(
              "Recovery log resynchronization reports a difference of " + diff
                  + " entries");
      }
      else
        throw new RuntimeException(
            "Invalid answer from remote controller on CompleteRecoveryLogResync ("
                + replyValue + ")");
    }
    catch (Exception e)
    {
      String errorMessage = "Failed to initialize recovery log resynchronization";
      logger.error(errorMessage, e);
      return new VirtualDatabaseException(errorMessage, e);
    }

    return null;
  }

}