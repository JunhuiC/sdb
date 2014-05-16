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

import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a GetNumberOfLogEntriesEvent that finds a number of log
 * entries available between 2 log id boundaries.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class GetNumberOfLogEntriesEvent implements LogEvent
{
  private long lowerId;
  private long upperId;
  private long nbOfLogEntries;

  /**
   * Creates a new <code>GetNumberOfLogEntriesEvent</code> object
   * 
   * @param lowerLogId the lower log id
   * @param upperLogId the upper log id
   */
  public GetNumberOfLogEntriesEvent(long lowerLogId, long upperLogId)
  {
    this.lowerId = lowerLogId;
    this.upperId = upperLogId;
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
      nbOfLogEntries = loggerThread.getNumberOfLogEntries(manager, lowerId, upperId);
    }
    catch (SQLException e)
    {
      manager.invalidate();
      loggerThread.getLogger().error(
          "Failed to retrieve number of log entries", e);
      // Push object back in the queue, it needs to be logged again
      loggerThread.putBackAtHeadOfQueue(this, e);
    }
    finally
    {
      synchronized (this)
      {
        notify();
      }
    }
  }

  /**
   * Returns the nbOfLogEntries value. This value can only be retrieved after
   * complete execution of the event.
   * 
   * @return Returns the nbOfLogEntries.
   */
  public long getNbOfLogEntries()
  {
    return nbOfLogEntries;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Get number of log entries between " + lowerId + " and " + upperId;
  }

}
