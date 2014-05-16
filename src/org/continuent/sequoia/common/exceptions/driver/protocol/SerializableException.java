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

package org.continuent.sequoia.common.exceptions.driver.protocol;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;

/**
 * This class implements our own Exception chain, overriding Throwable as much
 * as possible, relying on our own SerializableStackTraceElement instead of
 * java.lang.StackStraceElement.
 * <p>
 * The main reason for this class is that java.lang.StackStraceElement is not
 * serializable in JDK 1.4 (the constructor is missing, whereas 1.5 has it).
 * Unfortunately we cannot override everything we would like to because
 * StackTraceElement is also final and so SerializableStackTraceElement cannot
 * subtype+override it.
 * 
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @version 1.0
 */
public class SerializableException extends Exception
{
  private static final long               serialVersionUID = -7676999309139924676L;

  private String                          sqlState;
  private int                             vendorCode;

  private SerializableStackTraceElement[] stackTrace;

  /**
   * @see Exception#Exception(java.lang.Throwable)
   */
  SerializableException(String message, SerializableException cause)
  {
    super(message, cause);
  }

  /**
   * Converts a chain of Throwables to a chain of
   * <code>SerializableException</code>. The resulting chain has the same
   * length.
   * 
   * @param start head of chain to convert
   */

  SerializableException(Throwable start)
  {
    this(start.getMessage(), null == start.getCause()
        ? null
        : new SerializableException(start.getCause())); // recursion

    convertStackTrace(start);
  }

  void convertStackTrace(Throwable regularEx)
  {
    StackTraceElement[] regularST = regularEx.getStackTrace();
    stackTrace = new SerializableStackTraceElement[regularST.length];
    for (int i = 0; i < regularST.length; i++)
      stackTrace[i] = new SerializableStackTraceElement(regularST[i]);

    // nullifies super's, non-serializable stack trace since we don't want to
    // use it anymore
    setStackTrace(null /* ignored arg */);

  }

  /* ***** Serialization / Networking ********* */

  /**
   * Constructs/reads a new SerializableException chain from the stream
   */
  SerializableException(DriverBufferedInputStream in) throws IOException
  {
    // receive message and next exception (recursive)
    // (left to right evaluation is guaranteed by JLS bible)
    super(in.readLongUTF(), in.readBoolean() ? new SerializableException(in) : null);

    // receive stackTrace
    stackTrace = new SerializableStackTraceElement[in.readInt()];
    for (int i = 0; i < stackTrace.length; i++)
      stackTrace[i] = new SerializableStackTraceElement(in);

    // receive SQL fields
    setSQLState(in.readLongUTF());
    setErrorCode(in.readInt());

  }

  /**
   * Send the Serializable chain to the given stream.
   * 
   * @param out destination stream
   * @throws IOException stream error
   */
  public void sendToStream(DriverBufferedOutputStream out) throws IOException
  {
    // send message
    out.writeLongUTF(getMessage());

    // send next exception if any (chaining)
    if (null != getCause())
    {
      out.writeBoolean(true);
      ((SerializableException) getCause()).sendToStream(out); // recursion
    }
    else
      out.writeBoolean(false); // stop condition

    // send stack trace
    out.writeInt(stackTrace.length);
    for (int i = 0; i < stackTrace.length; i++)
      stackTrace[i].sendToStream(out);

    // send SQL fields
    out.writeLongUTF(getSQLState());
    out.writeInt(getErrorCode());

    out.flush();
  }

  /**
   * @see java.lang.Throwable#printStackTrace()
   */
  public void printStackTrace()
  {
    printStackTrace(System.err);
  }

  /**
   * Prints this throwable and its backtrace to the specified print stream.
   * 
   * @param s <code>PrintStream</code> to use for output
   */
  public void printStackTrace(PrintStream s)
  {
    synchronized (s)
    {
      s.println(this);
      for (int i = 0; i < stackTrace.length; i++)
        s.println("\tAt: " + stackTrace[i]);

      SerializableException ourCause = (SerializableException) getCause();
      if (ourCause != null)
      {
        s.println("Caused   by");
        ourCause.printStackTrace(s);
      }
    }
  }

  /**
   * Prints this throwable and its backtrace to the specified print writer.
   * 
   * @param s <code>PrintWriter</code> to use for output
   */
  public void printStackTrace(PrintWriter s)
  {
    synchronized (s)
    {
      s.println(this);
      for (int i = 0; i < stackTrace.length; i++)
        s.println("\tat " + stackTrace[i]);

      SerializableException ourCause = (SerializableException) getCause();
      if (ourCause != null)
      {
        s.println("Caused-by");
        ourCause.printStackTrace(s);
      }
    }
  }

  /**
   * @deprecated
   * @see java.lang.Throwable#fillInStackTrace()
   */
  public synchronized Throwable fillInStackTrace()
  {
    setStackTrace(null);
    return this;
  }

  /**
   * Please use getSerializedStackTrace() instead. Unfortunately
   * StackTraceElement has no constructor in 1.4 and cannot be overriden
   * (final).
   * 
   * @deprecated
   * @see java.lang.Throwable#getStackTrace()
   */
  public StackTraceElement[] getStackTrace()
  {
    return new StackTraceElement[0];
  }

  /**
   * Returns our private stack trace, the one which is serializable.
   * 
   * @return our private stack trace
   */
  public SerializableStackTraceElement[] getSerializableStackTrace()
  {
    return (SerializableStackTraceElement[]) stackTrace.clone();
  }

  /**
   * This method is deprecated and erases the regular, non serializable stack
   * trace. Please use setSerializedStackTrace() instead. Unfortunately
   * StackTraceElement has no constructor in 1.4 and cannot be overriden
   * (final).
   * 
   * @deprecated
   * @see java.lang.Throwable#setStackTrace(java.lang.StackTraceElement[])
   */
  public void setStackTrace(StackTraceElement[] ignored)
  {
    super.setStackTrace(new StackTraceElement[0]);
  }

  /**
   * Sets the vendorCode value.
   * 
   * @param vendorCode The vendorCode to set.
   */
  void setErrorCode(int vendorCode)
  {
    this.vendorCode = vendorCode;
  }

  /**
   * Returns the vendorCode value.
   * 
   * @return Returns the vendorCode.
   */
  public int getErrorCode()
  {
    return vendorCode;
  }

  /**
   * Sets the sQLState value.
   * 
   * @param sQLState The sQLState to set.
   */
  public void setSQLState(String sQLState)
  {
    this.sqlState = sQLState;
  }

  /**
   * Returns the sQLState value.
   * 
   * @return Returns the sQLState.
   */
  public String getSQLState()
  {
    return sqlState;
  }

  /**
   * Override super, adding an extra check because we do not want a mixed chain.
   * 
   * @see java.lang.Throwable#initCause(java.lang.Throwable)
   */
  public Throwable initCause(Throwable cause)
  {
    throwsIfNotSerializable(cause);

    super.initCause(cause);
    return this;
  }

  private void throwsIfNotSerializable(Throwable cause)
      throws IllegalArgumentException
  {
    if (null == cause)
      return;

    if (!(cause instanceof SerializableException))
      throw new IllegalArgumentException(
          "The cause of SerializableException has to be a SerializableException");
  }

}
