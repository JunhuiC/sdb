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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to transfer a dump (identified by a dump
 * name ) from the current controller to another one.
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class TransferDump extends AbstractAdminCommand
{
  /**
   * Creates a new <code>TransferDump</code> object
   * 
   * @param module the command is attached to
   */
  public TransferDump(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    StringTokenizer st = new StringTokenizer(commandText);
    String dumpName = null;
    String controllerName = null;
    boolean noCopy = false;
    try
    {
      dumpName = st.nextToken();
      controllerName = st.nextToken();
      if (st.hasMoreTokens())
      {
        noCopy = st.nextToken().equalsIgnoreCase("nocopy"); //$NON-NLS-1$
      }
    }
    catch (Exception e)
    {
      console.printError(getUsage());
      return;
    }

    console.printInfo(ConsoleTranslate.get("admin.command.transfer.dump.echo", //$NON-NLS-1$
        new String[]{dumpName, controllerName}));
    jmxClient.getVirtualDatabaseProxy(dbName, user, password).transferDump(
        dumpName, controllerName, noCopy);
    console.printInfo(ConsoleTranslate.get("admin.command.transfer.dump.done")); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.transfer.dump.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "transfer dump"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.transfer.dump.description"); //$NON-NLS-1$
  }

}
