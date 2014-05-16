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

package org.continuent.sequoia.common.exceptions;

import java.sql.SQLException;

/**
 * This class defines a NoMoreBackendException. This means that a controller
 * does not have any backend left to execute the query. The exception might
 * carry an optional identifier of the request that failed. This is useful to
 * unlog a remote request that has failed since each controller has its own
 * local id for each distributed request.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class NoMoreBackendException extends SQLException
{
  private static final long serialVersionUID = 8265839783849395122L;

  private long   recoveryLogId = 0;
  private String login         = null;

  /**
   * Creates a new <code>NoMoreBackendException</code> object
   */
  public NoMoreBackendException()
  {
    super();
  }

  /**
   * Creates a new <code>NoMoreBackendException</code> object
   * 
   * @param reason the error message
   */
  public NoMoreBackendException(String reason)
  {
    super(reason);
  }

  /**
   * Creates a new <code>NoMoreBackendException</code> object
   * 
   * @param reason the error message
   * @param sqlState the SQL state
   */
  public NoMoreBackendException(String reason, String sqlState)
  {
    super(reason, sqlState);
  }

  /**
   * Creates a new <code>NoMoreBackendException</code> object
   * 
   * @param reason the error message
   * @param sqlState the SQL state
   * @param vendorCode vendor specific code
   */
  public NoMoreBackendException(String reason, String sqlState, int vendorCode)
  {
    super(reason, sqlState, vendorCode);
  }

  /**
   * Returns the recovery log id of the request that failed.
   * 
   * @return Returns the recoveryLogId.
   */
  public long getRecoveryLogId()
  {
    return recoveryLogId;
  }

  /**
   * Sets the recovery log id of the request that failed.
   * 
   * @param recoveryLogId The recoveryLogId to set.
   */
  public void setRecoveryLogId(long recoveryLogId)
  {
    this.recoveryLogId = recoveryLogId;
  }

  /**
   * Returns the login of the request that failed.
   * 
   * @return Returns the login.
   */
  public String getLogin()
  {
    return login;
  }

  /**
   * Sets the login of the request that failed.
   * 
   * @param login The login to set.
   */
  public void setLogin(String login)
  {
    this.login = login;
  }
}