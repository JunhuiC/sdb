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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import org.continuent.sequoia.common.log.Trace;

/**
 * This class defines a NativeCommandExec, which abstracts out native command
 * execution logic into a small and easily verified class that does not have
 * dependencies on backuper implementations. Once instantiated this class can
 * execute a series of commands in sequence.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class NativeCommandExec
{
  /** Maximum milliseconds to wait for output threads to complete. */
  private static final int THREAD_WAIT_MAX = 30 * 1000;

  // Development logger
  private static Trace     logger          = Trace
                                               .getLogger(NativeCommandExec.class
                                                   .getName());

  // Output from most recent command execution.
  ArrayList<?>                stdout;
  ArrayList<?>                stderr;

  // Defines a class to hold native commands, be they array or a simple string.
  class NativeCommand
  {
    String   command;
    String[] commands;

    // Creates a native command based on a string.
    NativeCommand(String cmd)
    {
      this.command = cmd;
    }

    // Creates a native command based on an array.
    NativeCommand(String[] cmds)
    {
      this.commands = cmds;
    }

    // Return true if this command is a string rather than an array.
    boolean isString()
    {
      return (command != null);
    }

    // Return the command text in form suitable for error messages.
    String commandText()
    {
      if (isString())
        return command;
      else
      {
        // Concatenate commands for logging purposes.
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < commands.length; i++)
        {
          buf.append(commands[i] + " ");
        }
        return buf.toString();
      }
    }

    // Return the command string or null if this is an array command.
    String getCommand()
    {
      return command;
    }

    // Return the command array or null if this is a string command.
    String[] getCommands()
    {
      return commands;
    }
  }

  /**
   * Creates a new <code>NativeCommandExec</code> object
   */
  public NativeCommandExec()
  {
  }

  /**
   * Returns all stdout from the most recent command.
   * 
   * @return stdout contents (may be truncated)
   */
  public ArrayList<?> getStdout()
  {
    return stdout;
  }

  /**
   * Returns all stderr from the most recent command.
   * 
   * @return stderr contents (may be truncated)
   */
  public ArrayList<?> getStderr()
  {
    return stderr;
  }

  /**
   * Clears output arrays.
   */
  public void initOutput()
  {
    stdout = new ArrayList<Object>();
    stderr = new ArrayList<Object>();
  }

  /**
   * Utility method to execute a native command with careful logging and
   * coverage of all command outcomes.
   * 
   * @param cmd Command string
   * @param input Array containing lines of input
   * @param output Optional output stream to catch data written to standard
   *          output
   * @param timeout Timeout in seconds (0 = infinite)
   * @param workingDirectory working directory for the command (null to inherit
   *          from Sequoia working dir)
   * @param ignoreStdErrOutput true if output on std error should be ignored,
   *          else any output on stderr will be considered as a failure
   * @return true if command is successful
   */
  public boolean safelyExecNativeCommand(String cmd,
      NativeCommandInputSource input, OutputStream output, int timeout,
      File workingDirectory, boolean ignoreStdErrOutput)
  {
    NativeCommand nc = new NativeCommand(cmd);
    return safelyExecNativeCommand0(nc, input, output, timeout,
        workingDirectory, ignoreStdErrOutput);
  }

  /**
   * @see #safelyExecNativeCommand(String, NativeCommandInputSource,
   *      OutputStream, int, File)
   */
  public boolean safelyExecNativeCommand(String cmd,
      NativeCommandInputSource input, OutputStream output, int timeout,
      boolean ignoreStdErrOutput)
  {
    return safelyExecNativeCommand(cmd, input, output, timeout, null,
        ignoreStdErrOutput);
  }

  /**
   * Utility method to execute a native command with careful logging and
   * coverage of all command outcomes.
   * 
   * @param cmds Command array
   * @param input Input source for native command
   * @param output Optional output stream to catch data written to standard
   *          output
   * @param timeout Timeout in seconds (0 = infinite)
   * @param workingDirectory working directory for the command (null to inherit
   *          from Sequoia working dir)
   * @param ignoreStdErrOutput true if output on std error should be ignored,
   *          else any output on stderr will be considered as a failure
   * @return true if command is successful
   */
  public boolean safelyExecNativeCommand(String[] cmds,
      NativeCommandInputSource input, OutputStream output, int timeout,
      File workingDirectory, boolean ignoreStdErrOutput)
  {
    NativeCommand nc = new NativeCommand(cmds);
    return safelyExecNativeCommand0(nc, input, output, timeout,
        workingDirectory, ignoreStdErrOutput);
  }

  /**
   * @see #safelyExecNativeCommand(String[], NativeCommandInputSource,
   *      OutputStream, int, File)
   */
  public boolean safelyExecNativeCommand(String[] cmds,
      NativeCommandInputSource input, OutputStream output, int timeout,
      boolean ignoreStdErrOutput)
  {
    return safelyExecNativeCommand(cmds, input, output, timeout, null,
        ignoreStdErrOutput);
  }

  /**
   * Internal method to execute a native command with careful logging and
   * coverage of all command outcomes. This method among other things logs
   * output fully in the event of a failure.
   * 
   * @param cmd Command holder object
   * @param input Command input source
   * @param output Optional output stream to catch data written to standard
   *          output
   * @param timeout Timeout in seconds (0 = infinite)
   * @param workingDirectory working directory for the command (null to inherit
   *          from Sequoia working dir)
   * @param ignoreStdErrOutput true if output on std error should be ignored,
   *          else any output on stderr will be considered as a failure
   * @return true if command is successful
   */
  protected boolean safelyExecNativeCommand0(NativeCommand nc,
      NativeCommandInputSource input, OutputStream output, int timeout,
      File workingDirectory, boolean ignoreStdErrOutput)
  {
    boolean status;
    try
    {
      int errorCount;
      if (nc.isString())
        errorCount = executeNativeCommand(nc.getCommand(), input, output,
            timeout, workingDirectory, ignoreStdErrOutput);
      else
        errorCount = executeNativeCommand(nc.getCommands(), input, output,
            timeout, workingDirectory, ignoreStdErrOutput);
      status = (errorCount == 0);
      if (!status)
        logger.warn("Native command failed with error count=" + errorCount);
    }
    catch (InterruptedException e)
    {
      logger.warn("Native command timed out: command=" + nc.commandText()
          + " timeout=" + timeout);
      status = false;
    }
    catch (IOException e)
    {
      logger.warn("Native command I/O failed: command=" + nc.commandText(), e);
      status = false;
    }

    // If status is false, dump all output to the development log.
    if (status == false)
    {
      logOutput();
      logErrors();
    }

    return status;
  }

  /**
   * Manages execution of a command once it has been started as a process.
   * 
   * @param commandText The command text for logging messages
   * @param process The process object used to manage the command
   * @param inputSource Input source object or null if no input
   * @param output Optional output stream to catch standard out
   * @param timeout Time in seconds to await command (0 = forever)
   * @param ignoreStdErrOutput true if output on std error should be ignored,
   *          else any output on stderr will be considered as a failure
   * @return 0 if successful, any number otherwise
   * @throws InterruptedException If there is a timeout failure
   */
  protected int manageCommandExecution(String commandText, Process process,
      NativeCommandInputSource inputSource, OutputStream output, int timeout,
      boolean ignoreStdErrOutput) throws InterruptedException
  {
    if (logger.isInfoEnabled())
    {
      logger.info("Starting execution of \"" + commandText + "\"");
    }

    // Spawn 2 threads to capture the process output, prevents blocking
    NativeCommandOutputThread inStreamThread = new NativeCommandOutputThread(
        process.getInputStream(), output);
    NativeCommandOutputThread errStreamThread = new NativeCommandOutputThread(
        process.getErrorStream(), null);
    inStreamThread.start();
    errStreamThread.start();

    // Manage command execution.
    TimerTask task = null;
    try
    {
      // Schedule the timer if we need a timeout.
      if (timeout > 0)
      {
        final Thread t = Thread.currentThread();
        task = new TimerTask()
        {
          public void run()
          {
            t.interrupt();
          }
        };
        Timer timer = new Timer();
        timer.schedule(task, timeout * 1000);
      }

      // Provide standard input if present.
      if (inputSource != null)
      {
        OutputStream processOutput = process.getOutputStream();
        try
        {
          inputSource.write(processOutput);
        }
        catch (IOException e)
        {
          logger.warn("Writing of data to stdin halted by exception", e);
        }
        finally
        {
          try
          {
            inputSource.close();
          }
          catch (IOException e)
          {
            logger.warn("Input source close operation generated exception", e);
          }
          try
          {
            processOutput.close();
          }
          catch (IOException e)
          {
            logger.warn("Process stdin close operation generated exception", e);
          }
        }
      }

      // Wait for process to complete.
      process.waitFor();

      // Wait for threads to complete. Not strictly necessary
      // but makes it more likely we will read output properly.
      inStreamThread.join(THREAD_WAIT_MAX);
      errStreamThread.join(THREAD_WAIT_MAX);
    }
    catch (InterruptedException e)
    {
      logger.warn("Command exceeded timeout: " + commandText);
      process.destroy();
      throw e;
    }
    finally
    {
      // Cancel the timer and cleanup process stdin.
      if (task != null)
        task.cancel();

      // Collect output--this needs to happen no matter what.
      stderr = errStreamThread.getOutput();
      stdout = inStreamThread.getOutput();
    }

    if (logger.isInfoEnabled())
    {
      logger.info("Command \"" + commandText + "\" logged " + stderr.size()
          + " errors and terminated with exitcode " + process.exitValue());
    }

    if (ignoreStdErrOutput)
      return process.exitValue();
    else
    {
      if (stderr.size() > 0) // Return non-zero code if errors on stderr
        return -1;
      else
        return process.exitValue();
    }
  }

  /**
   * Executes a native operating system command expressed as a String.
   * 
   * @param command String of command to execute
   * @param input Native command input
   * @param output Optional output to catch standard out
   * @param timeout Time in seconds to await command (0 = forever)
   * @param workingDirectory working directory for the command (null to inherit
   *          from Sequoia working dir)
   * @param ignoreStdErrOutput true if output on std error should be ignored,
   *          else any output on stderr will be considered as a failure
   * @return 0 if successful, any number otherwise
   * @exception IOException if an I/O error occurs
   * @throws InterruptedException If there is a timeout failure
   */
  public int executeNativeCommand(String command,
      NativeCommandInputSource input, OutputStream output, int timeout,
      File workingDirectory, boolean ignoreStdErrOutput) throws IOException,
      InterruptedException
  {
    initOutput();
    Process process = Runtime.getRuntime()
        .exec(command, null, workingDirectory);
    return manageCommandExecution(command, process, input, output, timeout,
        ignoreStdErrOutput);
  }

  /**
   * @see #executeNativeCommand(String, NativeCommandInputSource, OutputStream,
   *      int, File)
   */
  public int executeNativeCommand(String command,
      NativeCommandInputSource input, OutputStream output, int timeout,
      boolean ignoreStdErrOutput) throws IOException, InterruptedException
  {
    return executeNativeCommand(command, input, output, timeout, null,
        ignoreStdErrOutput);
  }

  /**
   * Executes a native operating system command expressed as an array.
   * 
   * @param commands Array of strings (command + args) to execute
   * @param input Native command input
   * @param output Command output stream
   * @param timeout Time in seconds to await command (0 = forever)
   * @param workingDirectory working directory for the command (null to inherit
   *          from Sequoia working dir)
   * @param ignoreStdErrOutput true if output on std error should be ignored,
   *          else any output on stderr will be considered as a failure
   * @return 0 if successful, any number otherwise
   * @exception IOException if an I/O error occurs
   * @throws InterruptedException If there is a timeout failure
   */
  public int executeNativeCommand(String[] commands,
      NativeCommandInputSource input, OutputStream output, int timeout,
      File workingDirectory, boolean ignoreStdErrOutput) throws IOException,
      InterruptedException
  {
    initOutput();
    Process process = Runtime.getRuntime().exec(commands, null,
        workingDirectory);
    return manageCommandExecution(new NativeCommand(commands).commandText(),
        process, input, output, timeout, ignoreStdErrOutput);
  }

  /**
   * @see #executeNativeCommand(String[], NativeCommandInputSource,
   *      OutputStream, int, File)
   */
  public int executeNativeCommand(String[] commands,
      NativeCommandInputSource input, OutputStream output, int timeout,
      boolean ignoreStdErrOutput) throws IOException, InterruptedException
  {
    return executeNativeCommand(commands, input, output, timeout, null,
        ignoreStdErrOutput);
  }

  /**
   * Write process standard output to development log.
   */
  public void logOutput()
  {
    log("stdout", stdout);
  }

  /**
   * Write process error output to development log.
   */
  public void logErrors()
  {
    log("stderr", stderr);
  }

  /**
   * Utility routine to log output.
   * 
   * @param name Output type e.g., stdout or stderr
   * @param outArray List of lines of output
   */
  protected void log(String name, ArrayList<?> outArray)
  {
    StringWriter sw = new StringWriter();
    BufferedWriter writer = new BufferedWriter(sw);
    if (outArray != null)
    {
      int arraySize = outArray.size();
      for (int i = 0; i < arraySize; i++)
      {
        String line = (String) outArray.get(i);
        try
        {
          writer.newLine();
          writer.write(line);
        }
        catch (IOException e)
        {
          // This would be very unexpected when writing to a string.
          logger.error(
              "Unexpected exception while trying to log process output", e);
        }
      }
    }

    // Flush out collected output.
    try
    {
      writer.flush();
      writer.close();
    }
    catch (IOException e)
    {
    }

    String outText = sw.toString();
    logger.info("Process output (" + name + "):" + outText);
  }
}