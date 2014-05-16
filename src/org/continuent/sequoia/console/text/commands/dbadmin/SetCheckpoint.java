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

package org.continuent.sequoia.console.text.commands.dbadmin;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a SetCheckpoint
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class SetCheckpoint extends AbstractAdminCommand
{

  /**
   * Creates a new <code>SetCheckpoint</code> object
   * 
   * @param module module that owns this commands
   */
  public SetCheckpoint(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    commandText = commandText.trim();
    int firstWhiteSpace = commandText.indexOf(" "); //$NON-NLS-1$
    if (firstWhiteSpace < 0)
    {
      console.printError(getUsage());
      return;
    }
    String backendName = commandText.substring(0, firstWhiteSpace).trim();
    String checkpointName = commandText.substring(firstWhiteSpace,
        commandText.length()).trim();
    if ("".equals(checkpointName)) //$NON-NLS-1$
    {
      console.printError(getUsage());
      return;
    }
    console.printInfo(ConsoleTranslate.get("admin.command.set.checkpoint.echo", //$NON-NLS-1$
        new String[]{backendName, checkpointName}));
    VirtualDatabaseMBean vdjc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    vdjc.setBackendLastKnownCheckpoint(backendName, checkpointName);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "force checkpoint"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.set.checkpoint.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.set.checkpoint.params"); //$NON-NLS-1$
  }
}