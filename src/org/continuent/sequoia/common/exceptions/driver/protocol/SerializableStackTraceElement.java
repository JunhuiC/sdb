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
import java.io.Serializable;

import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;

/**
 * This class re-implements StackTraceElement of JDK 1.5, as a brute force
 * workaround for the forgotten constructor in JDK 1.4.
 * 
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @version 1.0
 */
public class SerializableStackTraceElement implements Serializable
{
  private String declaringClass;
  private String methodName;
  private String fileName;
  private int    lineNumber;

//  /**
//   * This is the constructor forgotten in 1.4 (and available in 1.5)
//   * the only reason why we exist.
//   */
//  private SerializableStackTraceElement(String declaringClass, String methodName,
//      String fileName, int lineNumber)
//  {
//    this.declaringClass = declaringClass;
//    this.methodName = methodName;
//    this.fileName = fileName;
//    this.lineNumber = lineNumber;
//
//  }
  
  /**
   * Constructs/converts a standard StackTraceElement (non-serializable in 1.4)
   * into a SerializableStackTraceElement.
   * 
   * @param st the element to convert.
   */
  public SerializableStackTraceElement(StackTraceElement st)
  {
    this.declaringClass = st.getClassName();
    this.methodName = st.getMethodName();
    this.fileName = st.getFileName();
    this.lineNumber = st.getLineNumber();
  }

  /**
   * Deserializes a new <code>SerializableStackTraceElement</code> from the
   * stream
   * 
   * @param in the stream to read from
   * @throws IOException stream error
   */
  public SerializableStackTraceElement(DriverBufferedInputStream in) throws IOException
  {
    declaringClass = in.readLongUTF();
    methodName = in.readLongUTF();
    fileName = in.readLongUTF();
    lineNumber = in.readInt();
  }

  /**
   * Serializes the object to the given stream.
   * 
   * @param out the stream to send the object to
   * @throws IOException stream error
   */
  public void sendToStream(DriverBufferedOutputStream out) throws IOException
  {
    out.writeLongUTF(declaringClass);
    out.writeLongUTF(methodName);
    out.writeLongUTF(fileName);
    out.writeInt(lineNumber);

  }

  /**
   * 
   * @see StackTraceElement#getLineNumber()
   */
  public int getLineNumber()
  {
    return lineNumber;
  }

  /**
   * 
   * @see StackTraceElement#getClassName()
   */
  public String getClassName()
  {
    return declaringClass;
  }

  /**
   * 
   * @see StackTraceElement#getMethodName()
   */
  public String getMethodName()
  {
    return methodName;
  }

  /**
   * 
   * @see StackTraceElement#isNativeMethod()
   */
  public boolean isNativeMethod()
  {
    return lineNumber == -2;
  }

  /**
   * 
   * @see StackTraceElement#toString()
   */
  public String toString()
  {
    return getClassName()
        + "."
        + methodName
        + (isNativeMethod() ? "(Native Method)" : (fileName != null
            && lineNumber >= 0
            ? "(" + fileName + ":" + lineNumber + ")"
            : (fileName != null ? "(" + fileName + ")" : "(Unknown Source)")));
  }

}
