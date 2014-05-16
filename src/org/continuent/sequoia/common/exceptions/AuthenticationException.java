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

/**
 * This class defines an AuthenticationException in case the authentication with
 * the controller fails.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class AuthenticationException extends SequoiaException
{
  private static final long serialVersionUID = 933382937208867189L;

  /**
   * Creates a new <code>AuthenticationException</code> object
   */
  public AuthenticationException()
  {
    super();
  }

  /**
   * Creates a new <code>AuthenticationException</code> object
   * 
   * @param message the error message
   */
  public AuthenticationException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>AuthenticationException</code> object
   * 
   * @param cause the root cause
   */
  public AuthenticationException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Creates a new <code>AuthenticationException</code> object
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public AuthenticationException(String message, Throwable cause)
  {
    super(message, cause);
  }

}
