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

import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;

/**
 * This class defines a FindLastIdEvent.
 * <p>
 * This event is used to retrieve the last ID for requests, transactions or
 * persistent connections in the log which was allocated for (by) a given
 * controller ID.
 * <p>
 * <br/>Once this event has been executed, the returned value can be accessed
 * through the <code>getValue()</code> method. If no previous ID was used in
 * this space, returned value will be controller ID.
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public class FindLastIdEvent implements LogEvent
{
  private String       request;
  private long         controllerId;
  private String       sql;
  private long         value;
  private SQLException catchedException = null;

  /**
   * Creates a new <code>FindLastIdEvent</code> object
   * 
   * @param requestId the ID of the request to be found
   */
  public FindLastIdEvent(String request, long controllerId, String sql)
  {
    this.request = request;
    this.controllerId = controllerId;
    this.sql = sql;
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
      stmt = manager.getConnection().prepareStatement(request);
      long minIdForThisController = controllerId;
      long maxIdForThisController = minIdForThisController
          | ~DistributedRequestManager.CONTROLLER_ID_BIT_MASK;
      stmt.setLong(1, minIdForThisController);
      stmt.setLong(2, maxIdForThisController);
      if (sql != null)
        stmt.setString(3, sql);
      rs = stmt.executeQuery();
      value = 0;
      if (rs.next())
        value = rs.getLong(1);
      if (value == 0)
        // Table is empty: return controllerId
        value = controllerId;
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
   * @return last ID delivered by controller or controller ID if none was
   *         delivered.
   */
  public long getValue()
  {
    return value;
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
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "FindLastIdEvent for controller ID " + controllerId;
  }
}
