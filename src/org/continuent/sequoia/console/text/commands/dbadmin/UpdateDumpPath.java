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

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a "update dump path" admin command.
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class UpdateDumpPath extends AbstractAdminCommand
{
  
  /**
   * Creates an "update backup path" command.
   * 
   * @param module the command is attached to
   */
  public UpdateDumpPath(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    String dumpName = null;
    String newPath = null;
    StringTokenizer st = new StringTokenizer(commandText.trim());

    if (st.countTokens() != 2) 
    {
      console.printError(getUsage());
      return;
    }
    try
    {
      dumpName = st.nextToken();
      newPath = st.nextToken();
      if (dumpName == null || newPath == null)
      {
        console.printError(getUsage());
        return;
      }

      console.printInfo(ConsoleTranslate.get("admin.command.update.dump.path.echo", //$NON-NLS-1$
          new String[]{dumpName, newPath}));
      VirtualDatabaseMBean vdjc = jmxClient.getVirtualDatabaseProxy(dbName, user,
          password);
      vdjc.updateDumpPath(dumpName, newPath);
      console.printInfo(ConsoleTranslate.get("admin.command.update.dump.path.done", //$NON-NLS-1$
          new String[]{dumpName, newPath}));
    }
    catch (Exception e)
    {
      console.printError("problem while updating path", e);
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "force path"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.update.dump.path.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.update.dump.path.parameters"); //$NON-NLS-1$
  }
}
