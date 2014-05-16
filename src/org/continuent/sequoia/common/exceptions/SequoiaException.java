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
 * Initial developer(s): Mathieu Peltier.
 * Contributor(s): _______________________.
 */

package org.continuent.sequoia.common.exceptions;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;

/**
 * Sequoia base exception.
 * 
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public class SequoiaException extends Exception implements Serializable
{
  private static final long serialVersionUID = -1899348090329064503L;

  /** Optional exception cause */
  protected Throwable       cause;

  /**
   * Creates a new <code>SequoiaException</code> instance.
   */
  public SequoiaException()
  {
  }

  /**
   * Creates a new <code>SequoiaException</code> instance.
   * 
   * @param message the error message
   */
  public SequoiaException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>SequoiaException</code> instance.
   * 
   * @param cause the root cause
   */
  public SequoiaException(Throwable cause)
  {
    this.cause = cause;
  }

  /**
   * Creates a new <code>SequoiaException</code> instance.
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public SequoiaException(String message, Throwable cause)
  {
    super(message);
    this.cause = cause;
  }

  /**
   * Gets the root cause of this exception.
   * 
   * @return a <code>Throwable</code> object
   */
  public Throwable getCause()
  {
    return cause;
  }

  /**
   * @see java.lang.Throwable#fillInStackTrace()
   */
  public synchronized Throwable fillInStackTrace()
  {
    if (cause != null)
    {
      return cause.fillInStackTrace();
    }
    else
    {
      return super.fillInStackTrace();
    }
  }

  /**
   * @see java.lang.Throwable#getStackTrace()
   */
  public StackTraceElement[] getStackTrace()
  {
    if (cause != null)
    {
      return cause.getStackTrace();
    }
    else
    {
      return super.getStackTrace();
    }
  }

  /**
   * @see java.lang.Throwable#getMessage()
   */
  public String getMessage()
  {
    if (cause != null)
    {
      return cause.getMessage();
    }
    else
    {
      return super.getMessage();
    }
  }

  /**
   * @see java.lang.Throwable#printStackTrace()
   */
  public void printStackTrace()
  {
    if (cause != null)
    {
      cause.printStackTrace();
    }
    else
    {
      super.printStackTrace();
    }
  }

  /**
   * @see java.lang.Throwable#printStackTrace(java.io.PrintStream)
   */
  public void printStackTrace(PrintStream arg0)
  {
    if (cause != null)
    {
      cause.printStackTrace(arg0);
    }
    else
    {
      super.printStackTrace(arg0);
    }
  }

  /**
   * @see java.lang.Throwable#printStackTrace(java.io.PrintWriter)
   */
  public void printStackTrace(PrintWriter arg0)
  {
    if (cause != null)
    {
      cause.printStackTrace(arg0);
    }
    else
    {
      super.printStackTrace(arg0);
    }
  }

  /**
   * @see java.lang.Throwable#setStackTrace(java.lang.StackTraceElement[])
   */
  public void setStackTrace(StackTraceElement[] arg0)
  {
    if (cause != null)
    {
      cause.setStackTrace(arg0);
    }
    else
    {
      super.setStackTrace(arg0);
    }
  }

}
