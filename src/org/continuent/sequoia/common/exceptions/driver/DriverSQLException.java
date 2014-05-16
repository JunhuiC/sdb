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

package org.continuent.sequoia.common.exceptions.driver;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.driver.protocol.SerializableException;

/**
 * This class customizes SQLException. Since JDBC allows only SQLExceptions, it
 * is used to systematically wrap underlying exceptions (typically coming from
 * the controller). The main feature added to SQLException is to override
 * printStackTrace() methods so they also print the non-standard, serializable
 * stack traces of SerializableException coming from the controller. Another
 * feature is to provide constructors with a "cause" (chaining), avoiding the
 * use of initCause()
 */
/**
 * FIXME: this class relies on "multiple dispatch", which does not exist in Java
 * (doh!). The current workaround it to cast properly at each call site. The
 * definitive fix is to use instanceof.
 */
public class DriverSQLException extends SQLException
{
  private static final long serialVersionUID = 5414032528107747411L;

  /**
   * @see SQLException#SQLException()
   */
  public DriverSQLException()
  {
    super();
  }

  /**
   * @see SQLException#SQLException(java.lang.String)
   */
  public DriverSQLException(String reason)
  {
    super(reason);
  }

  /**
   * @see SQLException#SQLException(java.lang.String, java.lang.String)
   */
  public DriverSQLException(String reason, String sQLState)
  {
    super(reason, sQLState);
  }

  /**
   * @see SQLException#SQLException(java.lang.String, java.lang.String, int)
   */
  public DriverSQLException(String reason, String sQLState, int vendorCode)
  {
    super(reason, sQLState, vendorCode);
  }

  /**
   * Creates a new <code>DriverSQLException</code> around a
   * SerializableException received from controller, itself converted from an
   * SQLException in most cases. So we set SQLState and vendorCode.
   * 
   * @param message message
   * @param cause exception from controller to wrap
   */
  public DriverSQLException(String message, SerializableException cause)
  {
    super(message, cause.getSQLState(), cause.getErrorCode());
    initCause(cause);
  }

  /**
   * Missing message constructor: let's borrow message from cause.
   * 
   * @param cause exception to wrap
   */
  public DriverSQLException(SerializableException cause)
  {
    this("Message of cause: " + cause.getLocalizedMessage(), cause);
  }

  /**
   * Missing message constructor: let's borrow message from cause.
   * 
   * @param cause exception to wrap
   */
  public DriverSQLException(Exception cause)
  {
    /**
     * @see #DriverSQLException(String, SerializableException)
     * @see #DriverSQLException(String, Exception)
     */
    this("Message of cause: " + cause.getLocalizedMessage(), cause);
  }

  /**
   * Creates a new <code>DriverSQLException</code> around an exception of a
   * type not specifically handled elsewhere. Typically used for exceptions
   * internal to the driver.
   * 
   * @param message message
   * @param cause generic exception to wrap
   */
  public DriverSQLException(String message, Exception cause)
  {
    super(message);
    initCause(cause);
  }

  /**
   * @see #DriverSQLException(String, SQLException)
   * @deprecated
   */
  public DriverSQLException(SQLException cause)
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
  public DriverSQLException(String message, SQLException cause)
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
