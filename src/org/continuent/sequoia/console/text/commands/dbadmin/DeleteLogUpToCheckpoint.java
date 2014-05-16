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
 * Initial developer(s): Olivier Fambon.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to cleanup the recovery log upto a
 * specified checkpoint.
 * 
 * @author <a href="mailto:olivier.fambon@emicnetworks.com">Olivier Fambon </a>
 * @version 1.0
 */
public class DeleteLogUpToCheckpoint extends AbstractAdminCommand
{
  /**
   * Creates a new <code>RemoveCheckpoint.java</code> object
   * 
   * @param module the command is attached to
   */
  public DeleteLogUpToCheckpoint(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    String checkpointName = commandText.trim();

    if ("".equals(checkpointName)) //$NON-NLS-1$
    {
      console.printError(getUsage());
      return;
    }

    jmxClient.getVirtualDatabaseProxy(dbName, user, password)
        .deleteLogUpToCheckpoint(checkpointName);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "truncate log"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.deleteLogUpToCheckpoint.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate
        .get("admin.command.deleteLogUpToCheckpoint.description"); //$NON-NLS-1$
  }

}