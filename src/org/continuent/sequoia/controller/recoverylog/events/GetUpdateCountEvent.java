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

import org.continuent.sequoia.common.exceptions.NoResultAvailableException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a GetUpdateCountEvent.
 * <p>
 * This event is used to retrieve an update count associated to a request ID.
 * <br/>Once this event has been executed, the update count is available through
 * the <code>getUpdateCount)</code> method.
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public class GetUpdateCountEvent implements LogEvent
{
  private RecoveryLog recoveryLog;
  private long        requestId;
  private int         updateCount      = -1;
  private Exception   catchedException = null;

  /**
   * Creates a new <code>GetUpdateCountEvent</code> object
   * 
   * @param connection the connection used to access the recovery log table
   * @param recoveryLog the recovery log object
   * @param requestId the ID of the request to be found
   */
  public GetUpdateCountEvent(RecoveryLog recoveryLog, long requestId)
  {
    this.recoveryLog = recoveryLog;
    this.requestId = requestId;
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
      stmt = manager.getConnection()
          .prepareStatement("SELECT update_count, exec_status FROM "
              + recoveryLog.getLogTableName() + " WHERE request_id=?");
      stmt.setLong(1, requestId);
      rs = stmt.executeQuery();
      if (rs.next())
      {
        // Found!
        updateCount = rs.getInt(1);
        if (!LogEntry.SUCCESS.equals(rs.getString(2)))
          throw new SQLException(Translate.get("recovery.jdbc.update.failed"));
      }
      else
      {
        // Not found
        String msg = Translate.get("recovery.jdbc.updatecount.not.found",
            requestId);
        catchedException = new NoResultAvailableException(msg);
      }
    }
    catch (SQLException e)
    {
      catchedException = new SQLException(Translate.get(
          "recovery.jdbc.updatecount.not.found.error", new Object[]{
              new Long(requestId), e.getMessage()}));
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
   * Returns the catched exception if an error occured during the execution (can
   * be SQLException or NoResultAvailableException). Returns null if no error
   * occured or the query did not execute yet.
   * 
   * @return Returns the catchedException.
   */
  public final Exception getCatchedException()
  {
    return catchedException;
  }

  /**
   * Gets the update count corresponding to the request ID
   * 
   * @return the update count that has been found
   */
  public int getUpdateCount()
  {
    return updateCount;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "GetUpdateCountEvent for request ID " + requestId;
  }
}
