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

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to purge the recovery log up to a
 * checkpoint associated with a specified dump
 */
public class PurgeLogUpToDump extends AbstractAdminCommand
{
  /**
   * Creates a new <code>PurgeLogUpToDump</code> object
   * 
   * @param module the command is attached to
   */
  public PurgeLogUpToDump(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    String dumpName = commandText.trim();

    if ("".equals(dumpName)) //$NON-NLS-1$
    {
      console.printError(getUsage());
      return;
    }

    VirtualDatabaseMBean vdb = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    DumpInfo[] dumpInfos = vdb.getAvailableDumps();
    DumpInfo foundDump = null;
    for (int i = 0; i < dumpInfos.length; i++)
    {
      DumpInfo dumpInfo = dumpInfos[i];
      if (dumpInfo.getDumpName().equals(dumpName))
      {
        foundDump = dumpInfo;
        break;
      }
    }
    if (foundDump == null)
    {
      console.printError(ConsoleTranslate.get(
          "admin.command.purgeLogUpToDump.nodump", dumpName)); //$NON-NLS-1$
      return;
    }
    vdb.deleteLogUpToCheckpoint(foundDump.getCheckpointName());
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "purge log"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.purgeLogUpToDump.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.purgeLogUpToDump.description"); //$NON-NLS-1$
  }

}