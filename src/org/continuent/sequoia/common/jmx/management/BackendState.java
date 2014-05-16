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

package org.continuent.sequoia.common.jmx.management;

import org.continuent.sequoia.common.i18n.Translate;

/**
 * This describes the different backend states
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public final class BackendState
{
  /**
   * The backend can execute read requests, but not write requests
   */
  public static final int READ_ENABLED_WRITE_DISABLED = 0;

  /**
   * The backend can execute read and write requests
   */
  public static final int READ_ENABLED_WRITE_ENABLED  = 1;

  /**
   * The backend can execute write requests but not read requests
   */
  public static final int READ_DISABLED_WRITE_ENABLED = 2;

  /**
   * The backend cannot execute any requests
   */
  public static final int DISABLED                    = 3;

  /**
   * The backend is loading data from a dump file
   */
  public static final int RESTORING                   = 4;

  /**
   * The backend is set for disabled. It is finishing to execute open
   * transactions and waits for persistent connections to close
   */
  public static final int DISABLING                   = 5;

  /**
   * The backend is in a restore process. The content of a backup file is being
   * copied onto the backen
   */
  public static final int BACKUPING                   = 6;

  /**
   * Unknown backend state. This is used when the state of the backend cannot be
   * determined properly. This is the state the backend is set to after a
   * backup, restore or recovery failure.
   */
  public static final int UNKNOWN                     = -1;

  /**
   * Replaying request from the recovery log
   */
  public static final int REPLAYING                   = 7;

  /**
   * Return a description which can be displayed to the user corresponding to
   * one of the backend states.
   * 
   * @param state a int representing a backend state
   * @return a String representing the state of the backend which can be
   *         displayed to the user
   */
  public static String description(int state)
  {
    switch (state)
    {
      case READ_ENABLED_WRITE_DISABLED :
        return Translate.get("BackendState.readEnabledWriteDisabled"); //$NON-NLS-1$
      case READ_ENABLED_WRITE_ENABLED :
        return Translate.get("BackendState.readEnabledWriteEnabled"); //$NON-NLS-1$
      case READ_DISABLED_WRITE_ENABLED :
        return Translate.get("BackendState.readDisabledWriteEnabled"); //$NON-NLS-1$
      case DISABLED :
        return Translate.get("BackendState.disabled"); //$NON-NLS-1$
      case RESTORING :
        return Translate.get("BackendState.restoring"); //$NON-NLS-1$
      case DISABLING :
        return Translate.get("BackendState.disabling"); //$NON-NLS-1$
      case BACKUPING :
        return Translate.get("BackendState.backingUp"); //$NON-NLS-1$
      case REPLAYING :
        return Translate.get("BackendState.replaying"); //$NON-NLS-1$
      case UNKNOWN :
      default :
        return Translate.get("BackendState.unknown"); //$NON-NLS-1$
    }
  }
}