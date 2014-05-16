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
 * Initial developer(s): Stephane Giron.
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
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DeleteLogEntriesAndCheckpointBetweenEvent implements LogEvent
{
  private long fromId;
  private long toId;

  /**
   * Creates a new <code>DeleteLogEntriesAndCheckpointBetweenEvent</code>
   * object. Used to delete a range of log entries in the recovery log.
   * 
   * @param fromId the first id of the range
   * @param toId the last id of the range
   */
  public DeleteLogEntriesAndCheckpointBetweenEvent(long fromId, long toId)
  {
    this.fromId = fromId;
    this.toId = toId;
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
      loggerThread.deleteLogEntriesAndCheckpointBetween(manager, fromId, toId);
    }
    catch (SQLException e)
    {
      manager.invalidate();
      loggerThread.getLogger().error(
          Translate.get("recovery.jdbc.loggerthread.shift.failed", e
              .getMessage()), e);
      // Push object back in the queue, it needs to be logged again
      loggerThread.putBackAtHeadOfQueue(this, e);
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Delete log entries from " + fromId + " to " + toId;
  }

}
