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

package org.continuent.sequoia.common.exceptions;

import java.sql.SQLException;

/**
 * This class defines a NoMoreControllerException. This exception is thrown when
 * all controllers in a Sequoia URL are unavailable.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class NoMoreControllerException extends SQLException
{
  private static final long serialVersionUID = -1970216751572913552L;

  /**
   * Creates a new <code>NoMoreControllerException</code> object
   */
  public NoMoreControllerException()
  {
    super();
  }

  /**
   * Creates a new <code>NoMoreControllerException</code> object
   * 
   * @param reason the error message
   */
  public NoMoreControllerException(String reason)
  {
    super(reason);
  }

  /**
   * Creates a new <code>NoMoreControllerException</code> object
   * 
   * @param reason the error message
   * @param sqlState the SQL state
   */
  public NoMoreControllerException(String reason, String sqlState)
  {
    super(reason, sqlState);
  }

  /**
   * Creates a new <code>NoMoreControllerException</code> object
   * 
   * @param reason the error message
   * @param sqlState the SQL state
   * @param vendorCode vendor specific code
   */
  public NoMoreControllerException(String reason, String sqlState,
      int vendorCode)
  {
    super(reason, sqlState, vendorCode);
  }

}
