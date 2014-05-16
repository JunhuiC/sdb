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
 * Contributor(s): 
 */

package org.continuent.sequoia.common.exceptions;

/**
 * Backup Exception class in case errors happen while backup or recovery is
 * executed, but octopus is not responsible for it.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public class BackupException extends SequoiaException
{
  private static final long serialVersionUID = -8984575523599197283L;

  /**
   * Creates a new <code>BackupException</code> instance.
   */
  public BackupException()
  {
  }

  /**
   * Creates a new <code>BackupException</code> instance.
   * 
   * @param message the error message
   */
  public BackupException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>BackupException</code> instance.
   * 
   * @param cause the root cause
   */
  public BackupException(Throwable cause)
  {
    this.cause = cause;
  }

  /**
   * Creates a new <code>BackupException</code> instance.
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public BackupException(String message, Throwable cause)
  {
    super(message);
    this.cause = cause;
  }

}