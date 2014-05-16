/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.List;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the ForceEnable command that forces the enabling of a
 * backend without checkpoint checking.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class ForceEnable extends AbstractAdminCommand
{

  /**
   * Creates a new <code>ForceEnableEnable</code> object
   * 
   * @param module the command is attached to
   */
  public ForceEnable(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {

    if (commandText.trim().length() == 0)
    {
      console.printError(getUsage());
      return;
    }

    String[] backendNames;
    VirtualDatabaseMBean vjdc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);

    if (("*").equals(commandText.trim())) //$NON-NLS-1$
    {
      List backendNamesList = vjdc.getAllBackendNames();
      backendNames = (String[]) backendNamesList
          .toArray(new String[backendNamesList.size()]);
    }
    else
    {
      String backendName = commandText.trim();
      backendNames = new String[]{backendName};
    }
    for (int i = 0; i < backendNames.length; i++)
    {
      String backendName = backendNames[i];
      console.printInfo(ConsoleTranslate.get(
          "admin.command.force.enable.backend", backendName)); //$NON-NLS-1$
      vjdc.forceEnableBackend(backendName);
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "force enable"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.force.enable.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.force.enable.params"); //$NON-NLS-1$
  }
}
