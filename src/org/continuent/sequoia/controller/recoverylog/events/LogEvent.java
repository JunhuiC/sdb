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

import org.continuent.sequoia.controller.recoverylog.LoggerThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogConnectionManager;

/**
 * This interface defines a LogEvent that defines the profile of actions that
 * manipulate the recovery log like logging a request.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface LogEvent
{
  /**
   * Returns true if this logEvent belongs to the given transaction.
   * 
   * @param tid the transaction identifier
   * @return true if this logEvent belongs to this transaction
   */
  boolean belongToTransaction(long tid);

  /**
   * Called by the LoggerThread to perform the needed operation on the log for
   * this entry.  This method may *only* use the passed in manager instance
   * to get access to the database. 
   * 
   * @param loggerThread the logger thread calling this method
   * @param manager a recovery log connection manager that should be used for 
   * database access
   */
  void execute(LoggerThread loggerThread, RecoveryLogConnectionManager manager);

}
