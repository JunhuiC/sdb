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
 * Initial developer(s): Emmanuel Cecchet
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions.driver;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.driver.protocol.SerializableException;

/**
 * This class customizes SQLException and is used for reporting internal IO
 * exception catched by the driver.
 */
public class DriverIOException extends SQLException
{
  private static final long serialVersionUID = 5066115114622251828L;

  /**
   * @see SQLException#SQLException()
   */
  public DriverIOException()
  {
    super();
  }

  /**
   * @see SQLException#SQLException(java.lang.String)
   */
  public DriverIOException(String reason)
  {
    super(reason);
  }

  /**
   * @see SQLException#SQLException(java.lang.String, java.lang.String)
   */
  public DriverIOException(String reason, String sQLState)
  {
    super(reason, sQLState);
  }

  /**
   * @see SQLException#SQLException(java.lang.String, java.lang.String, int)
   */
  public DriverIOException(String reason, String sQLState, int vendorCode)
  {
    super(reason, sQLState, vendorCode);
  }

  /**
   * Creates a new <code>DriverIOException</code> around a
   * SerializableException received from controller, itself converted from an
   * SQLException in most cases. So we set SQLState and vendorCode.
   * 
   * @param message message
   * @param cause exception from controller to wrap
   */
  public DriverIOException(String message, SerializableException cause)
  {
    super(message, cause.getSQLState(), cause.getErrorCode());
    initCause(cause);
  }

  /**
   * Missing message constructor: let's borrow message from cause.
   * 
   * @param cause exception to wrap
   */
  public DriverIOException(SerializableException cause)
  {
    this("Message of cause: " + cause.getLocalizedMessage(), cause);
  }

  /**
   * Missing message constructor: let's borrow message from cause.
   * 
   * @param cause exception to wrap
   */
  public DriverIOException(Exception cause)
  {
    /**
     * @see #DriverIOException(String, SerializableException)
     * @see #DriverIOException(String, Exception)
     */
    this("Message of cause: " + cause.getLocalizedMessage(), cause);
  }

  /**
   * Creates a new <code>DriverIOException</code> around an exception of a
   * type not specifically handled elsewhere. Typically used for exceptions
   * internal to the driver.
   * 
   * @param message message
   * @param cause generic exception to wrap
   */
  public DriverIOException(String message, Exception cause)
  {
    super(message);
    initCause(cause);
  }

  /**
   * @see #DriverIOException(String, SQLException)
   * @deprecated
   */
  public DriverIOException(SQLException cause)
  {
    this("", cause);
  }

  /**
   * An SQLException should not be wrapped inside a DriverSQLException: this is
   * a symptom of mixing different layers.
   * 
   * @param message message
   * @param cause cause
   * @deprecated
   * @throws IllegalArgumentException always
   */
  public DriverIOException(String message, SQLException cause)
      throws IllegalArgumentException
  {
    // ok let's be tolerant for the moment
    super(message);
    initCause(cause);

    // TODO: ... but this is the future:
    // A (Driver-)SQLException should be created here and nowhere below

    // IllegalArgumentException iae = new IllegalArgumentException(
    // "Bug: cause of a DriverSQLException should not itself be an SQLException
    // "
    // + message);
    // iae.initCause(cause);
    // throw iae;
  }

  /**
   * Overrides super method so we print the serializable stack trace of next
   * exceptions in the chain (if they use our serializable stack trace)
   * 
   * @see java.lang.Throwable#printStackTrace(java.io.PrintStream)
   */
  public void printStackTrace(PrintStream s)
  {
    /*
     * super does unfortunately not call printStackTrace() recursively on the
     * chain: instead it breaks object encapsulation by calling instead printing
     * methods and private fields on nexts. And since our chain uses its own
     * private stack trace implementation (because of JDK 1.4 woes) this does
     * print nothing in the end.
     */
    super.printStackTrace(s);

    // So we have to call printStackStrace() ourselves.
    Throwable cause = getCause();
    if (null != cause && cause instanceof SerializableException)
    {
      s.println("SerializableStackTrace of each cause:");
      ((SerializableException) cause).printStackTrace(s);
    }
  }

  /**
   * Overrides super method so we print the serializable stack trace of next
   * exceptions in the chain (if they use our serializable stack trace)
   * 
   * @see java.lang.Throwable#printStackTrace()
   */
  public void printStackTrace()
  {
    /**
     * This comes back to
     * 
     * @see DriverSQLException#printStackTrace(PrintStream)
     */
    super.printStackTrace();

  }

  /**
   * Overrides super method so we print the serializable stack trace of next
   * exceptions in the chain (if they use our serializable stack trace)
   * 
   * @see java.lang.Throwable#printStackTrace(java.io.PrintWriter)
   */
  public void printStackTrace(PrintWriter s)
  {
    /** @see #printStackTrace(PrintStream) */
    super.printStackTrace(s);

    Throwable cause = getCause();
    if (null != cause && cause instanceof SerializableException)
    {
      s.println("SerializableStackTrace of each cause:");
      ((SerializableException) cause).printStackTrace(s);
    }
  }

}
