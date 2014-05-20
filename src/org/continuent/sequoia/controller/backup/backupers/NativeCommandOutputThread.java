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
 * Initial developer(s): Dylan Hansen.
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;

import org.continuent.sequoia.common.log.Trace;

/**
 * Thread to process output of native commands. This class captures certain
 * errors and buffers up to a hard-coded number of lines of output. To see all
 * output turn up log4j debugging to DEBUG level.
 * 
 * @author <a href="mailto:dhansen@h2st.com">Dylan Hansen</a>
 * @version 1.0
 */
public class NativeCommandOutputThread extends Thread
{
  // Buffer this many output lines.
  private static final int MAX_OUTPUT_LINES = 25;

  ArrayList<String>                errors           = new ArrayList<String>();
  ArrayList<String>                output           = new ArrayList<String>();
  InputStream              inputStream      = null;
  OutputStream             outputStream     = null;

  // Logger
  static Trace             logger           = Trace
                                                .getLogger(NativeCommandOutputThread.class
                                                    .getName());

  /**
   * Creates a new <code>NativeCommandOutputThread</code> object
   * 
   * @param inStream input stream for command which is closed by Thread when
   *          finished
   */
  public NativeCommandOutputThread(InputStream inStream)
  {
    inputStream = inStream;
  }

  /**
   * Creates a new <code>NativeCommandOutputThread</code> object that 
   * that transfers all output to the specified output stream. 
   * 
   * @param inStream input stream for command which is closed by thread when
   *          finished
   * @param outStream Output stream to which output is written; closed by 
   *          thread when finished
   */
  public NativeCommandOutputThread(InputStream inStream, 
      OutputStream outStream)
  {
    inputStream = inStream;
    outputStream = outStream;
  }

  /**
   * Reads from the input stream and outputs to the log. Adds any error message
   * to the "errors" ArrayList. Up to MAX_OUTPUT_LINES of output are stored.
   * IOExceptions are logged and result in thread termination.
   */
  public void run()
  {
    if (logger.isDebugEnabled())
      logger.debug("Starting NativeCommandOutputThread: " + this.getName());

    if (outputStream == null)
      doNormalOutput();
    else
      doRedirectOutput();    

    if (logger.isDebugEnabled())
      logger.debug("Terminating NativeCommandOutputThread: " + this.toString());
  }

  /**
   * Processes normal output. 
   */
  protected void doNormalOutput()
  {
    BufferedReader buffRead = new BufferedReader(new InputStreamReader(inputStream));

    String line = null;

    try
    { 
      while ((line = buffRead.readLine()) != null)
      {
        // Check to see if PostgreSQL output contains "FATAL: " or "ERROR: "
        if (line.indexOf("FATAL:  ") > -1 || line.indexOf("ERROR:  ") > -1)
        {
          errors.add(line);
        }
        if (output.size() < MAX_OUTPUT_LINES)
        {
          output.add(line);
        }

        if (logger.isDebugEnabled())
          logger.debug(this.getName() + ": " + line);
      }
    }
    catch (IOException ioe)
    {
      logger.warn(ioe.getMessage(), ioe);
    }
    finally
    {
      // Must close stream in this thread to avoid synchronization problems
      try
      {
        buffRead.close();
      }
      catch (IOException ioe)
      {
        logger.warn(ioe.getMessage(), ioe);
      }
    }
  }

  /**
   * Processes output redirection when output directly into another 
   * output stream. 
   */
  protected void doRedirectOutput()
  {
    try
    {
      byte[] buff = new byte[1024];
      int len = 0;
      while ((len = this.inputStream.read(buff)) > 0)
      {
        outputStream.write(buff, 0, len);
      }
    }
    catch (IOException ioe)
    {
      logger.warn(ioe.getMessage(), ioe);
    }
    finally
    {
      // Must close stream in this thread to avoid synchronization problems
      try
      {
        inputStream.close();
        outputStream.close();
      }
      catch (IOException ioe)
      {
        logger.warn(ioe.getMessage(), ioe);
      }
    }
  }

  /**
   * Return a list of errors
   * 
   * @return ArrayList of <code>String</code> objects
   */
  public ArrayList<String> getErrors()
  {
    return errors;
  }

  /**
   * Return buffered output lines.
   * 
   * @return Output line array.
   */
  public ArrayList<String> getOutput()
  {
    return output;
  }
}
