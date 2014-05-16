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

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * Drop a virtual database from a controller command
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ShutdownVirtualDatabase extends ConsoleCommand
{

  /**
   * Creates a new <code>Load.java</code> object
   * 
   * @param module the command is attached to
   */
  public ShutdownVirtualDatabase(AbstractConsoleModule module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    checkJmxConnectionToController();
    StringTokenizer st = new StringTokenizer(commandText.trim());

    String database = null;
    int mode = Constants.SHUTDOWN_SAFE;
    try
    {
      database = st.nextToken();
      String modeStr = st.nextToken();
      mode = Integer.parseInt(modeStr);
    }
    catch (Exception e)
    {
    }

    if (database == null || database.length() == 0)
    {
      console.printError(ConsoleTranslate
          .get("controller.command.shutdown.virtualdatabase.null")); //$NON-NLS-1$
      console.printError(getUsage());
      return;
    }

    String user = console.readLine(ConsoleTranslate.get("admin.login.user")); //$NON-NLS-1$
    if (user == null)
      return;

    String password = console.readPassword(ConsoleTranslate
        .get("admin.login.password")); //$NON-NLS-1$
    if (password == null)
      return;

    console.printInfo(ConsoleTranslate.get(
        "controller.command.shutdown.virtualdatabase.echo", database)); //$NON-NLS-1$

    jmxClient.getVirtualDatabaseProxy(database, user, password).shutdown(mode);

    console.printInfo(ConsoleTranslate.get(
        "controller.command.shutdown.virtualdatabase.success", database)); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "shutdown virtualdatabase"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate
        .get("controller.command.shutdown.virtualdatabase.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate
        .get("controller.command.shutdown.virtualdatabase.description"); //$NON-NLS-1$
  }

}
