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
 * Initial developer(s): Mathieu Peltier.
 * Contributor(s): _______________________
 */

package org.continuent.sequoia.common.authentication;

import org.continuent.sequoia.common.exceptions.SequoiaException;

/**
 * Authentication manager exception.
 * 
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier</a>
 * @version 1.0
 */
public class AuthenticationManagerException extends SequoiaException
{
  private static final long serialVersionUID = 4789982508554668552L;

  /**
   * Creates a new <code>AuthenticationManagerException</code> instance.
   */
  public AuthenticationManagerException()
  {
  }

  /**
   * Creates a new <code>AuthenticationManagerException</code> instance.
   * 
   * @param message the error message
   */
  public AuthenticationManagerException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>AuthenticationManagerException</code> instance.
   * 
   * @param cause the root cause
   */
  public AuthenticationManagerException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Creates a new <code>AuthenticationManagerException</code> instance.
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public AuthenticationManagerException(String message, Throwable cause)
  {
    super(message, cause);
  }
}