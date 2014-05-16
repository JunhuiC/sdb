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

package org.continuent.sequoia.console.text.commands.controller;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * This class defines a load driver command.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LoadDriver extends ConsoleCommand
{
  /**
   * Creates a new <code>LoadDriver</code> command.
   * 
   * @param module the command is attached to
   */
  public LoadDriver(AbstractConsoleModule module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    checkJmxConnectionToController();

    String className = null;
    // Get class name if needed
    if (commandText == null || commandText.trim().equals("")) //$NON-NLS-1$
      className = console.readLine(ConsoleTranslate
          .get("controller.command.load.driver.input")); //$NON-NLS-1$
    else
      className = commandText.trim();

    try
    {
      Class.forName(className);
    }
    catch (Exception e)
    {
      console
          .printError(ConsoleTranslate
              .get(
                  "controller.command.load.driver.failed", new String[]{className, e.toString()})); //$NON-NLS-1$
      return;
    }

    console.printInfo(ConsoleTranslate.get(
        "controller.command.load.driver.success", //$NON-NLS-1$
        className));
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "load driver"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("controller.command.load.driver.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("controller.command.load.driver.params"); //$NON-NLS-1$
  }
}
