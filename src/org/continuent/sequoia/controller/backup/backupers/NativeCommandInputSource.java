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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class defines a NativeCommandInputSource, which provides input
 * to processes.  Current implementation could be extended to use
 * interfaces but this works for now. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class NativeCommandInputSource
{
  String[] inputStringArray;
  InputStream inputStream;
  
  /**
   * Creates a new <code>NativeCommandInputSource</code> object to read
   * from a strings of arrays.  
   */
  public NativeCommandInputSource(String[] arrayInput)
  {
    this.inputStringArray = arrayInput;
  }

  /**
   * Creates a new <code>NativeCommandInputSource</code> object to read
   * from an InputStream. 
   * @param inputStream An open input stream
   */
  public NativeCommandInputSource(InputStream inputStream)
  {
    this.inputStream = inputStream;
  }

  /**
   * Factory method to create an input source that reads arrays. 
   * 
   * @param inputArray Array of string inputs.  
   * @return Configured input source
   */
  public static NativeCommandInputSource 
    createArrayInputSource(String[] inputArray)
  {
    if (inputArray == null)
      return null;
    else
      return new NativeCommandInputSource(inputArray);
  }
  
  /**
   * Factory method to create an input source that reads arrays. 
   * 
   * @param inputStream An open InputStream containing input
   * @return Configured input source
   */
  public static NativeCommandInputSource 
    createInputStreamSource(InputStream inputStream)
  {
    if (inputStream == null)
      return null;
    else
      return new NativeCommandInputSource(inputStream);
  }

  /**
   * Writes all available input to the native command process, returning
   * when the input source is exhausted. 
   *
   * @param outputStream Stream that feeds input to process
   * @throws IOException Thrown if there is an IO error
   */
  public void write(OutputStream outputStream) throws IOException
  {
    if (inputStringArray != null)
    {
      // Write array input. 
      BufferedOutputStream feed = new BufferedOutputStream(outputStream);
      for (int i = 0; i < inputStringArray.length; i++)
      {
        feed.write(inputStringArray[i].getBytes());
        feed.write("\n".getBytes());
        feed.flush();
      }
    }
    else 
    {
      // Write output from stream. 
      byte[] buff = new byte[1024];
      int len = 0;
      while ((len = inputStream.read(buff)) > 0)
      {
        outputStream.write(buff, 0, len);
      }
    }
  }
  
  /**
   * Closes the input source. 
   * 
   * @throws IOException Thrown if there is an error during the close 
   * operation
   */
  public void close() throws IOException
  {
    if (inputStringArray != null)
    {
      // Do nothing
    }
    else
    {
      inputStream.close();
    }    
  }
}