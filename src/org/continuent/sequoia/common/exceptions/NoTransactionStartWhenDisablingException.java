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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions;

import java.sql.SQLException;

/**
 * This class defines a NoTransactionStartWhenDisablingException. It is thrown
 * when someone tries to start a new transaction on a backend that is disabling.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class NoTransactionStartWhenDisablingException extends SQLException
{
  private static final long serialVersionUID = -9008333075362715240L;

  /**
   * Creates a new <code>NoTransactionStartWhenDisablingException</code>
   * object
   */
  public NoTransactionStartWhenDisablingException()
  {
    super();
  }

  /**
   * Creates a new <code>NoTransactionStartWhenDisablingException</code>
   * object
   * 
   * @param reason the error message
   */
  public NoTransactionStartWhenDisablingException(String reason)
  {
    super(reason);
  }

  /**
   * Creates a new <code>NoTransactionStartWhenDisablingException</code>
   * object
   * 
   * @param reason the error message
   * @param sqlState the SQL state
   */
  public NoTransactionStartWhenDisablingException(String reason, String sqlState)
  {
    super(reason, sqlState);
  }

  /**
   * Creates a new <code>NoTransactionStartWhenDisablingException</code>
   * object
   * 
   * @param reason the error message
   * @param sqlState the SQL state
   * @param vendorCode vendor specific code
   */
  public NoTransactionStartWhenDisablingException(String reason,
      String sqlState, int vendorCode)
  {
    super(reason, sqlState, vendorCode);
  }

}