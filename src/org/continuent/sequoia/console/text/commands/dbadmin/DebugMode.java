/**
 * Sequoia: Database clustering technology.
 * Copyright 2005-2006 Continuent, Inc.
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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to switch to <em>debug mode</em> (i.e.
 * debug commands are available).
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 * @version 1.0
 */
public class DebugMode extends AbstractAdminCommand
{

  private static final String ON  = "on"; //$NON-NLS-1$
  private static final String OFF = "off"; //$NON-NLS-1$

  /**
   * Creates a new <code>DebugMode</code> object
   * 
   * @param module module that owns this commands
   */
  public DebugMode(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * mbean.hasVirtualDatabase(vdbName)
   * 
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    if (ON.equals(commandText.trim()))
    {
      ((VirtualDatabaseAdmin) module).addDebugCommands();
      console.printInfo(ConsoleTranslate.get("DebugMode.on")); //$NON-NLS-1$
    }
    else if (OFF.equals(commandText.trim()))
    {
      ((VirtualDatabaseAdmin) module).removeDebugCommands();
      console.printInfo(ConsoleTranslate.get("DebugMode.off")); //$NON-NLS-1$
    }
    else
    {
      console.printError(getUsage());
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "debug"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("DebugMode.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("DebugMode.params"); //$NON-NLS-1$
  }
}
