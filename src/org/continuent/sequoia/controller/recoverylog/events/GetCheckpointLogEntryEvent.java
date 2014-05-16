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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.recoverylog.events;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.recoverylog.CheckpointLogEntry;
import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a GetCheckpointLogEntryEvent.
 * <p>
 * This event is used to retrieve a log entry associated to a checkpoint.
 * <br/>Once this event has been execute, the log entry is available through the
 * <code>getCheckpointLogEntry()</code> method
 */
public class GetCheckpointLogEntryEvent implements LogEvent
{
  private RecoveryLog        recoveryLog;
  private String             checkpointName;
  private CheckpointLogEntry logEntry         = null;
  private SQLException       catchedException = null;

  /**
   * Creates a new <code>GetCheckpointLogEntryEvent</code> object
   * 
   * @param recoveryLog the recovery log we are attached to
   * @param checkpointName name of the checkpoint to look for
   */
  public GetCheckpointLogEntryEvent(RecoveryLog recoveryLog,
      String checkpointName)
  {
    this.recoveryLog = recoveryLog;
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
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = manager.getConnection().prepareStatement("SELECT "
          + recoveryLog.getLogTableSqlColumnName()
          + ",vlogin,auto_conn_tran,transaction_id,request_id FROM "
          + recoveryLog.getLogTableName() + ","
          + recoveryLog.getCheckpointTableName() + " WHERE name LIKE ? and "
          + recoveryLog.getLogTableName() + ".log_id="
          + recoveryLog.getCheckpointTableName() + ".log_id");
      stmt.setString(1, checkpointName);
      rs = stmt.executeQuery();

      if (rs.next())
      {
        logEntry = new CheckpointLogEntry(rs.getString(1), rs.getString(2), rs
            .getString(3), rs.getLong(4), rs.getLong(5));
        return;
      }

      String msg = Translate.get("recovery.jdbc.checkpoint.not.found",
          checkpointName);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      catchedException = new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.not.found.error", new String[]{
              checkpointName, e.getMessage()}));
    }
    finally
    {
      try
      {
        if (rs != null)
          rs.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
      synchronized (this)
      {
        notify();
      }
    }
  }

  /**
   * Returns the catched exception if an error occured during the execution.
   * Returns null if no error occured or the query did not execute yet.
   * 
   * @return Returns the catchedException.
   */
  public final SQLException getCatchedException()
  {
    return catchedException;
  }

  /**
   * Gets the CheckpointLogEntry corresponding to the checkpoint
   * 
   * @return the CheckpointLogEntry that has been found
   * @throws SQLException if an error occurs
   */
  public CheckpointLogEntry getCheckpointLogEntry() throws SQLException
  {
    if (logEntry == null)
    {
      throw catchedException;
    }
    return logEntry;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "GetCheckpointLogEntryEvent for checkpoint " + checkpointName;
  }
}
