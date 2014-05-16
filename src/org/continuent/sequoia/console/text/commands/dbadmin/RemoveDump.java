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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.StringTokenizer;

import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a RemoveDump
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 * @version 1.0
 */
public class RemoveDump extends AbstractAdminCommand
{

  /**
   * Create a "remove dump" admin command.
   * 
   * @param module the command is attached to
   */
  public RemoveDump(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    StringTokenizer tokenizer = new StringTokenizer(commandText.trim());

    if (tokenizer.countTokens() < 1)
    {
      console.printError(getUsage());
      return;
    }

    String dumpName = tokenizer.nextToken();
    boolean keepsFile = false;

    if (tokenizer.countTokens() == 1)
    {
      String keepsFileStr = tokenizer.nextToken();
      keepsFile = "keepfile".equals(keepsFileStr); //$NON-NLS-1$
    }

    if ("".equals(dumpName)) //$NON-NLS-1$
    {
      console.printError(getUsage());
      return;
    }

    VirtualDatabaseMBean vdjc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    try
    {
      vdjc.deleteDump(dumpName, keepsFile);
      console.printInfo(ConsoleTranslate.get("admin.command.remove.dump.done")); //$NON-NLS-1$
    }
    catch (VirtualDatabaseException e)
    {
      console.printError(e.getMessage(), e); //$NON-NLS-1$
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "delete dump"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.remove.dump.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.remove.dump.parameters"); //$NON-NLS-1$
  }
}
