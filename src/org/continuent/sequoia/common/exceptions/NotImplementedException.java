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
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.common.exceptions;

import java.sql.SQLException;

/**
 * This exception is thrown for all non implemented features in the Sequoia
 * driver.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class NotImplementedException extends SQLException
{
  private static final long serialVersionUID = 6615147787748938642L;

  /**
   * Creates a new <code>NotImplementedException</code> instance.
   * 
   * @param reason the exception cause
   * @param sqlState the SQL state
   * @param vendorCode the vendor code
   */
  public NotImplementedException(
    String reason,
    String sqlState,
    int vendorCode)
  {
    super(reason, sqlState, vendorCode);
  }

  /**
   * Creates a new <code>NotImplementedException</code> instance.
   * 
   * @param reason the exception cause
   * @param sqlState the SQL state
   */
  public NotImplementedException(String reason, String sqlState)
  {
    super(reason, sqlState);
  }

  /**
   * Creates a new <code>NotImplementedException</code> instance.
   * 
   * @param callingMethod the calling method that failed
   */
  public NotImplementedException(String callingMethod)
  {
    super(callingMethod + " not implemented");
  }

  /**
   * Creates a new <code>NotImplementedException</code> instance.
   */
  public NotImplementedException()
  {
    super("Feature not implemented");
  }
}
