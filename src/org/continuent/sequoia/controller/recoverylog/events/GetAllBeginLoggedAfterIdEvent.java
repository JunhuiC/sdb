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

package org.continuent.sequoia.controller.recoverylog.events;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a GetAllBeginLoggedAfterIdEvent.
 * <p>
 * This event is used to retrieve a log ID associated to a checkpoint. <br>
 * Once this event has been execute, the log ID is available through the
 * <code>getCheckpointRequestId()</code> method
 */
public class GetAllBeginLoggedAfterIdEvent implements LogEvent
{

  private String       logTableName;
  private long         checkpointId;
  private List         beginList        = null;
  private SQLException catchedException = null;
  private String       logTableSqlColumnName;

  /**
   * Creates a new <code>GetAllBeginLoggedAfterIdEvent</code> object
   * 
   * @param logTableName the name of the checkpoint table
   * @param logTableSqlColumnName name of the recovery log table sql column
   * @param checkpointId the name of the checkpoint
   */
  public GetAllBeginLoggedAfterIdEvent(String logTableName,
      String logTableSqlColumnName, long checkpointId)
  {
    this.logTableName = logTableName;
    this.checkpointId = checkpointId;
    this.logTableSqlColumnName = logTableSqlColumnName;
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
      stmt = manager.getConnection().prepareStatement("SELECT transaction_id FROM "
          + logTableName + " WHERE log_id>? AND " + logTableSqlColumnName
          + " LIKE ?");
      stmt.setLong(1, checkpointId);
      stmt.setString(2, "begin");
      rs = stmt.executeQuery();
      beginList = new ArrayList();
      while (rs.next())
        beginList.add(new Long(rs.getLong(1)));
    }
    catch (SQLException e)
    {
      catchedException = e;
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
   * Return the list of transactions started after the given id
   * 
   * @return begin list
   * @throws SQLException if an error occured
   */
  public List getBeginList() throws SQLException
  {
    if (beginList == null)
      throw catchedException;
    return beginList;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "GetCheckpointLogIdEvent for checkpoint " + checkpointId;
  }

}
