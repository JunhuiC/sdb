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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.ConsoleLauncher;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * This class defines a load driver command.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class LoadVirtualDatabase extends ConsoleCommand
{
  /**
   * Creates a new <code>LoadVirtualDatabase</code> command.
   * 
   * @param module the command is attached to
   */
  public LoadVirtualDatabase(AbstractConsoleModule module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    checkJmxConnectionToController();

    String filename = null;
    boolean initialize = false;
    boolean useforce = false;
    
    // Get the file name and options if not supplied on the command line
    while (commandText == null || commandText.trim().equals("")) //$NON-NLS-1$
      commandText = console.readLine(ConsoleTranslate
          .get("controller.command.load.vdb.input")); //$NON-NLS-1$

    // Handle command parameters and options
    StringTokenizer strtok = new StringTokenizer(commandText.trim());
    filename = strtok.nextToken();
    if (strtok.hasMoreTokens())
    {
      String option = strtok.nextToken();
      if (option.equalsIgnoreCase("init"))
        initialize = true;
      else if (option.equalsIgnoreCase("force"))
        useforce = true;
      else
        throw new ConsoleException("Unknown option: '" + option + "'");
    }

    // Read the file
    FileReader fileReader = new FileReader(filename);
    BufferedReader in = new BufferedReader(fileReader);
    StringBuffer xml = new StringBuffer();
    String line;
    do
    {
      line = in.readLine();
      if (line != null)
        xml.append(line);
    }
    while (line != null);

    // Send it to the controller
    if (initialize)
      jmxClient.getControllerProxy().initializeVirtualDatabases(xml.toString());
    else if (useforce)
      jmxClient.getControllerProxy().addVirtualDatabases(xml.toString(), true);
    else
      jmxClient.getControllerProxy().addVirtualDatabases(xml.toString());
      
    console.printInfo(ConsoleTranslate.get(
        "controller.command.load.vdb.success", //$NON-NLS-1$
        new String[] {filename, ConsoleLauncher.PRODUCT_NAME}));
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "load virtualdatabase configuration"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("controller.command.load.vdb.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("controller.command.load.vdb.params"); //$NON-NLS-1$
  }
}
