/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Olivier Fambon.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This message is used both to prepare the sending of a set of log entries to a
 * remote controller's vdb recovery log (intialization) and to terminate it
 * (termination). Initialization is the second step of the process to rebuild
 * the remote recovery log from a live one, termination is the final step of the
 * process. The process is as such so that eventually the remote vdb backends
 * can be restored from dumps. Upon reception of this message for the
 * initialisation phase (identified by a null dumpCheckpointName), the remote
 * recovery log is cleared from the beginning upto the 'now' checkpoint.
 * Subsequently, CopyLogEntry messages are sent and log entries are inserted 'as
 * are' into the remote recovery log. Then, upon reception of this message for
 * the termination phase (non-null dumpCheckpointName), the remote recovery log
 * makes the dumpCheckpointName available for restore operations.
 * 
 * @see CopyLogEntry
 * @author <a href="mailto:Olivier.Fambon@emicnetworks.com>Olivier Fambon </a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ReplicateLogEntries extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = -2813776509468770586L;

  private String            checkpointName;
  private String            dumpCheckpointName;
  private long              checkpointId;
  private String            dumpName;

  /**
   * Creates a new <code>ReplicateLogEntries</code> message
   * 
   * @param checkpointName The checkpoint (aka now checkpoint) before wich
   *          entries are to be replaced.
   * @param dumpCheckpointName The dump checkoint from which entries are to be
   *          replaced. If this one is null, the recovery log is reset.
   * @param dumpName the name of the dump associated to dumpCheckpointName.
   * @param checkpointId The id associated to checkpoint THIS ID SHOULD BE
   *          HIDDEN
   */
  public ReplicateLogEntries(String checkpointName, String dumpCheckpointName,
      String dumpName, long checkpointId)
  {
    this.dumpCheckpointName = dumpCheckpointName;
    this.checkpointName = checkpointName;
    this.checkpointId = checkpointId;
    this.dumpName = dumpName;
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
   * Returns the dump checkpoint name .
   * 
   * @return Returns the dump CheckpointName.
   */
  public String getDumpCheckpointName()
  {
    return dumpCheckpointName;
  }

  /**
   * Returns the Checkpoint id.
   * 
   * @return the Checkpoint id
   */
  public long getCheckpointId()
  {
    return checkpointId;
  }

  void performSanityChecks(DistributedVirtualDatabase dvdb)
      throws VirtualDatabaseException, SQLException
  {
    if (!dvdb.hasRecoveryLog())
    {
      String errorMessage = "Tentative handleReplicateLogEntries on vdb with no recovery log";
      throw new VirtualDatabaseException(errorMessage);
    }

    if (dvdb.getRecoveryLog().getDumpInfo(dumpName) == null)
      throw new VirtualDatabaseException("Invalid dump name: " + dumpName);
  }

  void performInitializationPhase(DistributedVirtualDatabase dvdb)
      throws SQLException
  {
    dvdb.getRequestManager().getRecoveryLog()
        .resetLogTableIdAndDeleteRecoveryLog(checkpointName, checkpointId);
  }

  void performTerminationPhase(DistributedVirtualDatabase dvdb)
      throws SQLException, VirtualDatabaseException
  {
    // 1: store dump checkpoint name in the checkpoints table
    dvdb.getRequestManager().getRecoveryLog().storeDumpCheckpointName(
        dumpCheckpointName, checkpointId);
    // (should this one be synchronous ?)

    // 2: update the dump table with it, and make the dump valid for restore
    dvdb.getRecoveryLog().updateDumpCheckpoint(dumpName, dumpCheckpointName);

    // 3: update global counters based on new log
    dvdb.initGlobalCounters(dvdb.getControllerId());
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    try
    {
      // Note: with this test placed here, the check is performed both at
      // initialization and at completion.
      performSanityChecks(dvdb);

      if (dumpCheckpointName == null)
        performInitializationPhase(dvdb);
      else
        performTerminationPhase(dvdb);

    }
    catch (Exception e)
    {
      dvdb.getLogger().warn(e);
      return new ControllerException(e);
    }

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
    return (Serializable) handleMessageSingleThreadedResult;
  }
}