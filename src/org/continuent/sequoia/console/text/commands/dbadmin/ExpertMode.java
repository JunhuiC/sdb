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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to switch to <em>expert mode</em>
 * (i.e. advanced admin commands are available).
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 * @version 1.0
 */
public class ExpertMode extends AbstractAdminCommand
{

  private static final String ON = "on"; //$NON-NLS-1$
  private static final String OFF = "off"; //$NON-NLS-1$
  
  /**
   * Creates a new <code>ExpertMode</code> object
   * 
   * @param module module that owns this commands
   */
  public ExpertMode(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {     
    if (ON.equals(commandText.trim())) 
    {
      ((VirtualDatabaseAdmin)module).addExpertCommands(); 
      console.printInfo(ConsoleTranslate.get("admin.command.expert.mode.switched.on")); //$NON-NLS-1$
    } else if (OFF.equals(commandText.trim())) 
    {
      ((VirtualDatabaseAdmin)module).removeExpertCommands(); 
      console.printInfo(ConsoleTranslate.get("admin.command.expert.mode.switched.off")); //$NON-NLS-1$
    } else 
    {
      console.printError(getUsage());
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "expert"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.expert.mode.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.expert.mode.params"); //$NON-NLS-1$
  }
}
