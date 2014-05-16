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
 * Initial developer(s): Damian Arregui.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.recoverylog.events;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a FindClosePersistentConnectionEvent.
 * <p>
 * This event is used to retrieve a close associated to a persistent connection ID.
 * <br/>Once this event has been executed, the fact that the close was found
 * or not can be checked through the <code>wasFound()</code> method.
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public class FindClosePersistentConnectionEvent implements LogEvent
{
  private RecoveryLog  recoveryLog;
  private long         persistentConnectionId;
  private boolean      found            = false;
  private String       status           = LogEntry.MISSING;
  private SQLException catchedException = null;

  /**
   * Creates a new <code>FindRollbackEvent</code> object
   * 
   * @param connection the connection used to access the recovery log table
   * @param recoveryLog the recovery log object
   * @param persistentConnectionId the ID of the request to be found
   */
  public FindClosePersistentConnectionEvent(RecoveryLog recoveryLog,
      long persistentConnectionId)
  {
    this.recoveryLog = recoveryLog;
    this.persistentConnectionId = persistentConnectionId;
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
      stmt = manager.getConnection().prepareStatement("SELECT log_id, exec_status FROM "
          + recoveryLog.getLogTableName() + " WHERE ((transaction_id=?) AND ("
          + recoveryLog.getLogTableSqlColumnName() + "='close'))");
      stmt.setLong(1, persistentConnectionId);
      rs = stmt.executeQuery();

      // Found?
      found = rs.next();
      if (found)
        status = rs.getString("exec_status");
    }
    catch (SQLException e)
    {
      catchedException = new SQLException(Translate.get(
          "recovery.jdbc.rollback.not.found.error", new Object[]{
              new Long(persistentConnectionId), e.getMessage()}));
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
   * Returns the status of the rollback statement if found in the recovery log
   * 
   * @return the status of the rollback statement found in the recovery log, or
   *         LogEntry.MISSING if not found
   */
  public String getStatus()
  {
    return status;
  }

  /**
   * Checks if the rollback was found.
   * 
   * @return true if the rollback has been found, false otherwise
   */
  public boolean wasFound()
  {
    return found;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "FindClosePersistentConnectionEvent for request ID " + persistentConnectionId;
  }
}
