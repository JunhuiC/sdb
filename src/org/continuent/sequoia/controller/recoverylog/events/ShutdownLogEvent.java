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

import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a ShutdownLogEvent.  Post an instance 
 * of this class to the recoverly log to force it to shut down.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShutdownLogEvent implements LogEvent
{
  /**
   * Creates a new <code>ShutdownLogEvent</code> instance.  
   */
  public ShutdownLogEvent()
  {
    super();
  }

  /**
   * @see org.continuent.sequoia.controller.recoverylog.events.LogEvent#belongToTransaction(long)
   */
  public boolean belongToTransaction(long tid)
  {
    return false;
  }

  /**
   * Close recovery log and invoke shutdown on the logger thread which will 
   * cause it to terminate as soon as processing of this event is completed. 
   * 
   * @see org.continuent.sequoia.controller.recoverylog.events.LogEvent#execute(org.continuent.sequoia.controller.recoverylog.LoggerThread)
   */
  public void execute(LoggerThread loggerThread, RecoveryLogConnectionManager manager)
  {
    // This closes the database.  For local Hypersonic recovery logs we 
    // may in future want to issue a SHUTDOWN COMPACT to ensure the 
    // database is stored efficiently.
    loggerThread.shutdown();
    synchronized(this)
    {
      notify();
    }
    loggerThread.getLogger().info("Executed log shutdown event");
  }
}
