/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.recoverylog;

import org.continuent.sequoia.common.jmx.management.BackendState;

/**
 * A instance of this class gives information on a specific backend state from
 * the recovery log. For a backend, we have its name, the virtual database that
 * owns it, the lastKnownCheckpoint,and the state of the backend ().
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public final class BackendRecoveryInfo
{
  private String backendName;
  private String checkpoint;
  /** State is defined in <code>BackendState</code> */
  private int    backendState;
  private String virtualDatabase;

  /**
   * Creates a new <code>BackendRecoveryInfo</code> object
   * 
   * @param backendName backend name
   * @param lastCheckpoint last known checkpoint name
   * @param backendState backend state as defined in <code>BackendState</code>
   * @param virtualDatabase virtual database name
   */
  public BackendRecoveryInfo(String backendName, String lastCheckpoint,
      int backendState, String virtualDatabase)
  {
    this.backendName = backendName;
    this.checkpoint = lastCheckpoint;
    this.backendState = backendState;
    this.virtualDatabase = virtualDatabase;
  }

  /**
   * Returns the backendName value.
   * 
   * @return Returns the backendName.
   */
  public String getBackendName()
  {
    return backendName;
  }

  /**
   * Sets the backendName value.
   * 
   * @param backendName The backendName to set.
   */
  public void setBackendName(String backendName)
  {
    this.backendName = backendName;
  }

  /**
   * Returns the backend state as defined in <code>BackendState</code>.
   * 
   * @return Returns the backend state.
   */
  public int getBackendState()
  {
    return backendState;
  }

  /**
   * Sets the backend state value. The value must be defined in
   * <code>BackendState</code>
   * 
   * @param backendState The backend state to set.
   */
  public void setBackendState(int backendState)
  {
    this.backendState = backendState;
  }

  /**
   * Returns the lastCheckpoint value.
   * 
   * @return Returns the lastCheckpoint.
   */
  public String getCheckpoint()
  {
    return checkpoint;
  }

  /**
   * Sets the lastCheckpoint value.
   * 
   * @param lastCheckpoint The lastCheckpoint to set.
   */
  public void setCheckpoint(String lastCheckpoint)
  {
    this.checkpoint = lastCheckpoint;
  }

  /**
   * Returns the virtualDatabase value.
   * 
   * @return Returns the virtualDatabase.
   */
  public String getVirtualDatabase()
  {
    return virtualDatabase;
  }

  /**
   * Sets the virtualDatabase value.
   * 
   * @param virtualDatabase The virtualDatabase to set.
   */
  public void setVirtualDatabase(String virtualDatabase)
  {
    this.virtualDatabase = virtualDatabase;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Backend:" + this.backendName + ", VirtualDatabase:"
        + this.virtualDatabase + ", State:"
        + BackendState.description(this.backendState) + ", Checkpoint:"
        + this.checkpoint;
  }
}