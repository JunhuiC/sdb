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

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to copy the local virtual database
 * recovery log onto a remote controller's peer virtual database log. The copy
 * is performed from the specified checkpoint uptil 'now' (a new global
 * checkpoint). The copy is sent to the specified remote node.
 * 
 * @author <a href="mailto:Olivier.Fambon@emicnetworks.com">Olivier Fambon </a>
 * @version 1.0
 */
public class CopyLogFromCheckpoint extends AbstractAdminCommand
{
  /**
   * Creates a new <code>Disable.java</code> object
   * 
   * @param module the command is attached to
   */
  public CopyLogFromCheckpoint(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    StringTokenizer st = new StringTokenizer(commandText);
    String checkpointName = null, controllerName = null;
    try
    {
      checkpointName = st.nextToken();
      controllerName = st.nextToken();
    }
    catch (Exception e)
    {
      console.printError(getUsage());
      return;
    }

    jmxClient.getVirtualDatabaseProxy(dbName, user, password)
        .copyLogFromCheckpoint(checkpointName, controllerName);
    console.printInfo(ConsoleTranslate.get("admin.command.restore.log.done")); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.restore.log.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "restore log"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.restore.log.description"); //$NON-NLS-1$
  }

}
