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
import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a GetCheckpointLogIdEvent.
 * <p>
 * This event is used to retrieve a log ID associated to a checkpoint. <br>
 * Once this event has been execute, the log ID is available through the
 * <code>getCheckpointRequestId()</code> method
 */
public class GetCheckpointLogIdEvent implements LogEvent
{

  private String       checkpointTableName;
  private String       checkpointName;
  private long         logId            = -1;
  private SQLException catchedException = null;
  private boolean      retrying         = false;
  private boolean      mustNotify       = true;

  /**
   * Creates a new <code>GetCheckpointLogIdEvent</code> object
   * 
   * @param checkpointTableName the name of the checkpoint table
   * @param checkpointName the name of the checkpoint
   */
  public GetCheckpointLogIdEvent(String checkpointTableName,
      String checkpointName)
  {
    this.checkpointTableName = checkpointTableName;
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
  public void execute(LoggerThread loggerThread,
      RecoveryLogConnectionManager manager)
  {
    PreparedStatement stmt = null;
    ResultSet rs = null;
    mustNotify = true;
    try
    {
      stmt = manager.getConnection().prepareStatement(
          "SELECT log_id FROM " + checkpointTableName + " WHERE name LIKE ?");
      stmt.setString(1, checkpointName);
      rs = stmt.executeQuery();

      if (rs.next())
        logId = rs.getLong(1);
      else
      {
        String msg = Translate.get("recovery.jdbc.checkpoint.not.found",
            checkpointName);
        catchedException = new SQLException(msg);
      }
    }
    catch (SQLException e)
    {
      if (!retrying)
      {
        retrying = true;
        mustNotify = false;
        manager.invalidate();
        loggerThread.getLogger().error(
            Translate.get("recovery.jdbc.checkpoint.not.found.error", e
                .getMessage()), e);
        loggerThread.getLogger().warn(
            "Retrying previous failing operation once...");
        // Push object back in the queue, it needs to be logged again
        loggerThread.putBackAtHeadOfQueue(this, e);
      }
      else
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
      // Notify only if needed : when it has to be retried, we do not notify. It
      // will be notified only after retry (whether it is successful or not)
      if (mustNotify)
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
   * Gets the log identifier corresponding to the checkpoint
   * 
   * @return long the request identifier corresponding to the checkpoint.
   * @throws SQLException if an error occurs
   */
  public long getCheckpointLogId() throws SQLException
  {
    if (logId == -1)
    {
      throw catchedException;
    }
    return logId;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "GetCheckpointLogIdEvent for checkpoint " + checkpointName;
  }
}
