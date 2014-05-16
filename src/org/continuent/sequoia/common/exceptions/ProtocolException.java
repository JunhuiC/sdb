/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks
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
 * Initial developer(s): Marc Herbert
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions;


import org.continuent.sequoia.common.exceptions.driver.DriverSQLException;

/**
 * This class defines a ProtocolException
 * 
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @version 1.0
 */
public class ProtocolException extends DriverSQLException
{
  private static final long serialVersionUID = 5211274551332052100L;

  /**
   * Creates a new <code>ProtocolException</code> object
   */
  public ProtocolException()
  {
    super();
  }

  /**
   * Creates a new <code>ProtocolException</code> object
   * 
   * @param message the error message
   */
  public ProtocolException(String message)
  {
    super(message);
  }

}
