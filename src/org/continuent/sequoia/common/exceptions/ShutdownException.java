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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): _______________________
 */

package org.continuent.sequoia.common.exceptions;


/**
 * Controller exception.
 * 
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier</a>
 * @version 1.0
 */
public class ShutdownException extends SequoiaException
{
  private static final long serialVersionUID = 1421907016443553749L;

  /**
   * Creates a new <code>ShutdownException</code> instance.
   */
  public ShutdownException()
  {
  }

  /**
   * Creates a new <code>ShutdownException</code> instance.
   * 
   * @param message the error message
   */
  public ShutdownException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>ShutdownException</code> instance.
   * 
   * @param cause the root cause
   */
  public ShutdownException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Creates a new <code>ShutdownException</code> instance.
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public ShutdownException(String message, Throwable cause)
  {
    super(message, cause);
  }
}
