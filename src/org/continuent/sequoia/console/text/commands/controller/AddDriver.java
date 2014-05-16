/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * This class defines a AddDriver
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class AddDriver extends ConsoleCommand
{

  /**
   * Creates a new <code>AddDriver.java</code> object
   * 
   * @param module the command is attached to
   */
  public AddDriver(AbstractConsoleModule module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws IOException, ConsoleException
  {
    checkJmxConnectionToController();
    String filename = null;
    // Get the file name if needed
    if (commandText == null || commandText.trim().equals("")) //$NON-NLS-1$
    {
      try
      {
        filename = console.readLine(ConsoleTranslate
            .get("controller.command.add.driver.input.filename")); //$NON-NLS-1$
      }
      catch (Exception che)
      {
      }
    }
    else
      filename = commandText.trim();

    if (filename == null || filename.equals("")) //$NON-NLS-1$
      throw new ConsoleException(ConsoleTranslate
          .get("controller.command.add.driver.null.filename")); //$NON-NLS-1$

    try
    {
      // Send the file contents to the controller
      jmxClient.getControllerProxy().addDriver(readDriver(filename));
      console.printInfo(ConsoleTranslate.get(
          "controller.command.add.driver.file.sent", filename)); //$NON-NLS-1$
    }
    catch (FileNotFoundException fnf)
    {
      throw new ConsoleException(ConsoleTranslate.get(
          "controller.command.add.driver.file.not.found", filename)); //$NON-NLS-1$
    }
    catch (Exception ioe)
    {
      throw new ConsoleException(ConsoleTranslate.get(
          "controller.command.add.driver.sent.failed", ioe)); //$NON-NLS-1$
    }
  }

  private byte[] readDriver(String filename) throws FileNotFoundException,
      IOException
  {
    File file;
    FileInputStream fileInput = null;
    file = new File(filename);
    fileInput = new FileInputStream(file);

    // Read the file into an array of bytes
    long size = file.length();
    if (size > Integer.MAX_VALUE)
      throw new IOException(ConsoleTranslate
          .get("controller.command.add.driver.file.too.big")); //$NON-NLS-1$
    byte[] bytes = new byte[(int) size];
    int nb = fileInput.read(bytes);
    fileInput.close();
    if (nb != size)
      throw new IOException(ConsoleTranslate
          .get("controller.command.add.driver.file.not.read")); //$NON-NLS-1$
    return bytes;
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "upload driver"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("controller.command.add.driver.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("controller.command.add.driver.params"); //$NON-NLS-1$
  }

}
