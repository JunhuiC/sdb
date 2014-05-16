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
 * Initial developer(s): Jeff Mesnil
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.sqlconsole;

import java.io.IOException;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.VirtualDatabaseConsole;

/**
 * This class defines a "fetchsize" sql command
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class SetFetchSize extends ConsoleCommand
{

  /**
   * Creates a new <code>SetFetchSize</code> object
   * 
   * @param module the command is attached to
   */
  public SetFetchSize(VirtualDatabaseConsole module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws IOException, ConsoleException
  {
    try
    {
      int fetchsize = new Integer(commandText.trim()).intValue();
      ((VirtualDatabaseConsole)module).setFetchsize(fetchsize);
      console.printInfo(ConsoleTranslate.get("sql.command.fetchsize.value", //$NON-NLS-1$
          fetchsize));
    }
    catch (NumberFormatException e)
    {
      console.printError(getUsage());
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "fetchsize"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("sql.command.fetchsize.params"); //$NON-NLS-1$
  }
  
  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("sql.command.fetchsize.description"); //$NON-NLS-1$
  }
}