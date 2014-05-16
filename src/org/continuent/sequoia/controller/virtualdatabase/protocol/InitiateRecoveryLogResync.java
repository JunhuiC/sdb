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

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a InitiateRecoveryLogResync message used to send
 * parameters needed to initiate an automated recovery log synchronization. The
 * message sends back the delta to apply to entries id to be inserted in the
 * local recovery log.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class InitiateRecoveryLogResync
    extends DistributedVirtualDatabaseMessage
{
  private static final long    serialVersionUID = -6799114439172152L;

  private long                 originCommonCheckpointId;
  private String               commonCheckpointName;
  private String               nowCheckpointName;
  private long                 nbOfEntriesToResync;

  /**
   * Creates a new <code>InitiateRecoveryLogResync</code> object
   * 
   * @param commonCheckpointName common checkpoint name where resync will start
   * @param commonCheckpointId common checkpoint id on the original node (the
   *          one that is up-to-date)
   * @param nowCheckpointName newly inserted checkpoint where resync will end
   * @param nbOfEntriesToResync number of entries to be resynchronized between
   *          these 2 checkpoints
   */
  public InitiateRecoveryLogResync(String commonCheckpointName,
      long commonCheckpointId, String nowCheckpointName,
      long nbOfEntriesToResync)
  {
    this.commonCheckpointName = commonCheckpointName;
    this.originCommonCheckpointId = commonCheckpointId;
    this.nowCheckpointName = nowCheckpointName;
    this.nbOfEntriesToResync = nbOfEntriesToResync;
  }

  /**
   * Returns the commonCheckpointId value.
   * 
   * @return Returns the commonCheckpointId.
   */
  public final long getOriginCommonCheckpointId()
  {
    return originCommonCheckpointId;
  }

  /**
   * Returns the commonCheckpointName value.
   * 
   * @return Returns the commonCheckpointName.
   */
  public final String getCommonCheckpointName()
  {
    return commonCheckpointName;
  }

  /**
   * Returns the nbOfEntriesToResync value.
   * 
   * @return Returns the nbOfEntriesToResync.
   */
  public final long getNbOfEntriesToResync()
  {
    return nbOfEntriesToResync;
  }

  /**
   * Returns the nowCheckpointName value.
   * 
   * @return Returns the nowCheckpointName.
   */
  public final String getNowCheckpointName()
  {
    return nowCheckpointName;
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
    
    return this;
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
      RecoveryLog recoveryLog = dvdb.getRequestManager().getRecoveryLog();
      long commonCheckpointId = recoveryLog
          .getCheckpointLogId(getCommonCheckpointName());
      long nowCheckpointId = recoveryLog
          .getCheckpointLogId(getNowCheckpointName());

      // Compute how much additional space is needed to resync the log
      long shift = getNbOfEntriesToResync()
          - (nowCheckpointId - commonCheckpointId);

      // Shift entries only if we do not have enough space
      if (shift > 0)
        recoveryLog.moveEntries(nowCheckpointId, shift);

      // Cleanup entries in the log hole that will be resynced
      recoveryLog.deleteLogEntriesAndCheckpointBetween(commonCheckpointId,
          nowCheckpointId + shift);

      // Update now checkpoint
      recoveryLog.removeCheckpoint(nowCheckpointName);
      recoveryLog.storeCheckpoint(nowCheckpointName, nowCheckpointId + shift);

      return new Long(commonCheckpointId - getOriginCommonCheckpointId());
    }
    catch (Exception e)
    {
      logger.error("Unable to initialize recovery log resynchronization", e);
      return new VirtualDatabaseException(
          "Unable to initialize recovery log resynchronization", e);
    }
  }

}
