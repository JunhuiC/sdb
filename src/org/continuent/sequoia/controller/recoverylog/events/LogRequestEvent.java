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
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a LogRequestEvent to log a request log entry.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LogRequestEvent implements LogEvent
{
  protected LogEntry logEntry;

  /**
   * Creates a new <code>LogRequestEvent</code> object
   * 
   * @param entry the log entry to use (not null)
   */
  public LogRequestEvent(LogEntry entry)
  {
    if (entry == null)
      throw new RuntimeException(
          "Invalid null entry in LogRequestEvent constructor");
    this.logEntry = entry;
  }

  /**
   * @see org.continuent.sequoia.controller.recoverylog.events.LogEvent#belongToTransaction(long)
   */
  public boolean belongToTransaction(long tid)
  {
    return logEntry.getTid() == tid;
  }

  /**
   * @see org.continuent.sequoia.controller.recoverylog.events.LogEvent#execute(org.continuent.sequoia.controller.recoverylog.LoggerThread)
   */
  public void execute(LoggerThread loggerThread, 
      RecoveryLogConnectionManager manager)
  {
    Trace logger = loggerThread.getLogger();
    try
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("recovery.jdbc.loggerthread.log.info",
            logEntry.getLogId()));

      PreparedStatement pstmt = manager.getLogPreparedStatement();
      pstmt.setLong(RecoveryLog.COLUMN_INDEX_LOG_ID, logEntry.getLogId());
      pstmt.setString(RecoveryLog.COLUMN_INDEX_VLOGIN, logEntry.getLogin());
      pstmt.setString(RecoveryLog.COLUMN_INDEX_SQL, logEntry.getQuery());
      pstmt.setString(RecoveryLog.COLUMN_INDEX_SQL_PARAMS, logEntry
          .getQueryParams());
      pstmt.setString(RecoveryLog.COLUMN_INDEX_AUTO_CONN_TRAN, logEntry
          .getAutoConnTrans());
      pstmt.setLong(RecoveryLog.COLUMN_INDEX_TRANSACTION_ID, logEntry.getTid());
      pstmt.setLong(RecoveryLog.COLUMN_INDEX_REQUEST_ID, logEntry
          .getRequestId());
      pstmt.setString(RecoveryLog.COLUMN_INDEX_EXEC_STATUS, logEntry
          .getExecutionStatus());
      pstmt.setLong(RecoveryLog.COLUMN_INDEX_EXEC_TIME, logEntry
          .getExecutionTimeInMs());
      pstmt.setLong(RecoveryLog.COLUMN_INDEX_UPDATE_COUNT, logEntry
          .getUpdateCountResult());

      if (logger.isDebugEnabled())
        logger.debug(pstmt.toString());
      try
      {
        pstmt.setEscapeProcessing(logEntry.getEscapeProcessing());
      }
      catch (Exception ignore)
      {
      }
      int updatedRows = pstmt.executeUpdate();
      if ((updatedRows != 1) && logger.isWarnEnabled())
        logger
            .warn("Recovery log did not update a single entry while executing: "
                + pstmt.toString());
    }
    catch (SQLException e)
    {
      manager.invalidate();
      if ("T".equals(logEntry.getAutoConnTrans()))
        logger.error(Translate.get(
            "recovery.jdbc.loggerthread.log.failed.transaction", new String[]{
                logEntry.getQuery(), String.valueOf(logEntry.getTid())}), e);
      else
        logger.error(Translate.get("recovery.jdbc.loggerthread.log.failed",
            logEntry.getQuery()), e);
      // Push object back in the queue, it needs to be logged again
      loggerThread.putBackAtHeadOfQueue(this, e);
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "LogRequestEvent: " + logEntry;
  }

}
