/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Initial developer(s): Olivier Fambon.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.recoverylog.events;

import java.sql.SQLException;

import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class is used to Store a Checkpoint in the recovery log checkpoint
 * tables so that it is available e.g. for restore operations.
 * 
 * @author <a href="mailto:olivier.fambon@emicnetworks.com">Olivier Fambon</a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class StoreCheckpointWithLogIdEvent implements LogEvent
{
  private long   checkpointId;
  private String checkpointName;

  /**
   * Creates a new <code>StoreCheckpointWithLogIdEvent</code> object
   * 
   * @param checkpointName the checkpoint name to create.
   * @param checkpointId the id of the checkpoint
   */
  public StoreCheckpointWithLogIdEvent(String checkpointName, long checkpointId)
  {
    this.checkpointId = checkpointId;
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
    try
    {
      loggerThread.storeCheckpointWithLogId(manager, checkpointName, checkpointId);
    }
    catch (SQLException e)
    {
      // Push object back in the queue, it needs to be logged again => potential
      // infinite loop
      loggerThread.putBackAtHeadOfQueue(this, e);
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Storing checkpoint " + checkpointName + " with id " + checkpointId;
  }

}
