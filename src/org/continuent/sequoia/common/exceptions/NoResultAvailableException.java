/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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
 * Initial developer(s): Stephane Giron.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions;

/**
 * This class defines a NoResultAvailableException thrown when a request
 * succeeded on one or several remote controllers, but all of these controllers
 * failed during result retrieval
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class NoResultAvailableException extends Exception
{

  private static final long serialVersionUID = -5209237539990966516L;

  /**
   * Creates a new <code>NoResultAvailableException</code> instance.
   */
  public NoResultAvailableException()
  {
    super();
  }

  /**
   * Creates a new <code>NoResultAvailableException</code> instance.
   * 
   * @param message the error message
   */
  public NoResultAvailableException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>NoResultAvailableException</code> instance.
   * 
   * @param cause the root cause
   */
  public NoResultAvailableException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Creates a new <code>NoResultAvailableException</code> instance.
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public NoResultAvailableException(String message, Throwable cause)
  {
    super(message, cause);
  }

}
