/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend;

import java.sql.SQLException;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.recoverylog.BackendRecoveryInfo;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;

/**
 * This class defines a BackendStateListener
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class BackendStateListener
{

  Trace               logger = Trace.getLogger(BackendStateListener.class
                                 .getName());
  private String      virtualDatabaseName;
  private RecoveryLog recoveryLog;

  /**
   * Creates a new <code>BackendStateListener</code> object
   * 
   * @param vdbName virtual database name
   * @param recoveryLog recovery log
   */
  public BackendStateListener(String vdbName, RecoveryLog recoveryLog)
  {
    this.virtualDatabaseName = vdbName;
    this.recoveryLog = recoveryLog;
  }

  /**
   * Update the persistent state of the backend in the recovery log
   * when the state of the backend instance has been changed
   * 
   * @param backend the backend to update information from
   */
  public synchronized void stateChanged(DatabaseBackend backend)
  {
    try
    {
      recoveryLog.storeBackendRecoveryInfo(virtualDatabaseName,
          new BackendRecoveryInfo(backend.getName(), backend
              .getLastKnownCheckpoint(), backend.getStateValue(),
              virtualDatabaseName));
    }
    catch (SQLException e)
    {
      logger.error("Could not store information for backend: "
          + backend.getName(), e);
    }
  }
}