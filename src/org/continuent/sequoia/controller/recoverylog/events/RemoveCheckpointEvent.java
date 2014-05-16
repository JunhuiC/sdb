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

import java.sql.SQLException;

import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class is used to remove a Checkpoint from the recovery log checkpoint
 * tables.
 */
public class RemoveCheckpointEvent implements LogEvent
{

  private String  checkpointName;
  private boolean mustNotify;
  private boolean retrying = false;

  /**
   * Create a new <code>RemoveCheckpointEvent</code> object.
   * 
   * @param checkpointName the checkpoint name to remove
   */
  public RemoveCheckpointEvent(String checkpointName)
  {
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
    mustNotify = true;
    try
    {
      loggerThread.removeCheckpoint(manager, checkpointName);
    }
    catch (SQLException e)
    {
      manager.invalidate();
      if (!retrying)
      {
        retrying = true;
        mustNotify = false;
        // Push object back in the queue, it needs to be logged again
        loggerThread.putBackAtHeadOfQueue(this, e);
      }
    }
    finally
    {
      if (mustNotify)
        // Notify task completion
        synchronized (this)
        {
          notify();
        }
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "RemoveCheckpointEvent for checkpoint " + checkpointName;
  }

}
