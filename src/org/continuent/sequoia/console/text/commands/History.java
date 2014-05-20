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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands;

import java.util.List;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * This class defines a History
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 * @param <E>
 */
public class History extends ConsoleCommand
{

  /**
   * Creates a new <code>History</code> object
   * 
   * @param module module that owns this commands
   */
  public History(AbstractConsoleModule module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    List<?> list = module.getConsole().getHistory();
    StringTokenizer st = new StringTokenizer(commandText);
    if (st.countTokens() == 0)
    {
      for (int i = 0; i < list.size(); i++)
      {
        Object o = list.get(i);
        console.println("" + i + "\t" + o); //$NON-NLS-1$ //$NON-NLS-2$
      }
    }
    else
    {
      int line = Integer.parseInt(st.nextToken());
      console.printInfo((String) list.get(line));
      module.handleCommandLine((String) list.get(line),
          module.getHashCommands());
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "history"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("console.command.history"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return "[<command index>]"; //$NON-NLS-1$
  }
}