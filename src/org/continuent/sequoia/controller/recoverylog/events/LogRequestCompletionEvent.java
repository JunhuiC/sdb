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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a LogRequestEvent to log a request log entry.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LogRequestCompletionEvent implements LogEvent
{
  private long    logId;
  private boolean success;
  private long    execTime;
  private int     updateCount;

  /**
   * Creates a new <code>LogRequestEvent</code> object
   * 
   * @param logId the recovery log id entry to update
   * @param success true if the execution was successful
   * @param updateCount the number of updated rows returned by executeUpdate()
   *          if applicable
   * @param execTime request execution time in ms
   */
  public LogRequestCompletionEvent(long logId, boolean success,
      int updateCount, long execTime)
  {
    this.logId = logId;
    this.success = success;
    this.execTime = execTime;
    this.updateCount = updateCount;
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
    Trace logger = loggerThread.getLogger();
    try
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("recovery.jdbc.loggerthread.log.info", logId));

      PreparedStatement pstmt = manager.getUpdatePreparedStatement();
      if (success)
        pstmt.setString(1, LogEntry.SUCCESS);
      else
        pstmt.setString(1, LogEntry.FAILED);
      pstmt.setInt(2, updateCount);
      pstmt.setLong(3, execTime);
      pstmt.setLong(4, logId);
      if (logger.isDebugEnabled())
        logger.debug(pstmt.toString());
      int updatedRows = pstmt.executeUpdate();
      if (updatedRows != 1)
        logger
            .error("Recovery log was unable to update request completion status: "
                + pstmt.toString());
    }
    catch (SQLException e)
    {
      manager.invalidate();
      logger.error(Translate.get(
          "recovery.jdbc.loggerthread.log.update.failed", new String[]{
              Long.toString(logId), Boolean.toString(success)}), e);
      // Push object back in the queue, it needs to be logged again
      loggerThread.putBackAtHeadOfQueue(this, e);
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "LogRequestCompletionEvent id " + logId + ", success=" + success;
  }
}
