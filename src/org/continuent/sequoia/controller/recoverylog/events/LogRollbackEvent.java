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

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This class defines a LogRollbackEvent to log a rollback or potentially clean
 * the recovery log for that transaction that rollbacks.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LogRollbackEvent extends LogRequestEvent
{
  /**
   * Creates a new <code>LogRollbackEvent</code> object
   * 
   * @param entry a log entry that describes a rollback (the SQL query is
   *          ignored)
   */
  public LogRollbackEvent(LogEntry entry)
  {
    super(entry);
  }

  /**
   * @see org.continuent.sequoia.controller.recoverylog.events.LogEvent#execute(org.continuent.sequoia.controller.recoverylog.LoggerThread)
   */
  public void execute(LoggerThread loggerThread, RecoveryLogConnectionManager manager)
  {
    Trace logger = loggerThread.getLogger();

    // First let's try to remove the transaction from the log if no-one is
    // currently processing the log
    try
    {
      if (!loggerThread.getRecoveryLog().isRecovering())
      {
        if (loggerThread.removeEmptyTransaction(manager, 
            logEntry.getTid()))
          return;
      }
    }
    catch (SQLException e)
    {
      manager.invalidate();
      logger.error(Translate.get("recovery.jdbc.loggerthread.log.failed",
          new String[]{"rollback", String.valueOf(logEntry.getTid())}), e);
      // Fallback to default logging
    }

    // Ok, someone has a lock on the log, just log the rollback.
    super.execute(loggerThread, manager);
  }

}
