/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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

package org.continuent.sequoia.controller.recoverylog.events;

import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a ResetLogEvent to reset the log
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ResetLogEvent implements LogEvent
{
  private long   oldId;
  private long   newId;
  private String checkpointName;

  /**
   * Creates a new <code>ResetLogEvent</code> object
   * 
   * @param oldCheckpointId the old id of the checkpoint
   * @param newCheckpointId the new checkpoint identifier
   * @param checkpointName the checkpoint name to delete from.
   */
  public ResetLogEvent(long oldCheckpointId, long newCheckpointId,
      String checkpointName)
  {
    this.oldId = oldCheckpointId;
    this.newId = newCheckpointId;
    this.checkpointName = checkpointName;
  }

  /**
   * @see org.continuent.sequoia.controller.recoverylog.events.LogEvent#belongToTransaction(long)
   */
  public boolean belongToTransaction(long tid)
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.recoverylog.events.LogEvent#execute(org.continuent.sequoia.controller.recoverylog.LoggerThread)
   */
  public void execute(LoggerThread loggerThread, RecoveryLogConnectionManager manager)
  {
    try
    {
      loggerThread.deleteCheckpointTable(manager);
      loggerThread.storeCheckpointWithLogId(manager, checkpointName, newId);
      loggerThread.deleteLogEntriesBeforeId(manager, oldId);
    }
    catch (SQLException e)
    {
      manager.invalidate();
      loggerThread.getLogger().error(
          Translate.get("recovery.jdbc.loggerthread.log.reset.failed",
              checkpointName), e);
      // Push object back in the queue, it needs to be logged again
      loggerThread.putBackAtHeadOfQueue(this, e);
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "ResetLogEvent for checkpoint " + checkpointName;
  }

}
