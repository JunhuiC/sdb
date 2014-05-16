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

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to display available backupers.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class ViewBackupers extends AbstractAdminCommand
{

  /**
   * Creates a "view backupers" command for the admin module.
   * 
   * @param module module that owns this commands
   */
  public ViewBackupers(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    VirtualDatabaseMBean vdjc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    String[] backuperNames = vdjc.getBackuperNames();
    if (backuperNames.length == 0)
    {
      console.printError(ConsoleTranslate.get("admin.command.view.backupers.nobackuper")); //$NON-NLS-1$
      return;
    }
    String[] dumpFormats = new String[backuperNames.length];
      for (int i = 0; i < backuperNames.length; i++)
      {
        String backuperName = backuperNames[i];
        String dumpFormat = vdjc.getDumpFormatForBackuper(backuperName);
        if (dumpFormat == null) 
        {
          dumpFormat = ""; //$NON-NLS-1$
        }
        dumpFormats[i] = dumpFormat;
      }
    String formattedBackupers = TableFormatter.format(getBackupersHeaders(),
        getBackupersAsCells(backuperNames, dumpFormats), true);
    console.println(formattedBackupers);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "show backupers"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.view.backupers.description"); //$NON-NLS-1$
  }

  private static String[][] getBackupersAsCells(String[] backuperNames, String[] dumpFormats)
  {
    String[][] backupersTable = new String[backuperNames.length][2];
    for (int i = 0; i < backupersTable.length; i++)
    {
      backupersTable[i][0] = backuperNames[i];
      backupersTable[i][1] = dumpFormats[i];
      }
    return backupersTable;
  }

  private static String[] getBackupersHeaders()
  {
    return new String[] {
        ConsoleTranslate.get("admin.command.view.backupers.prop.name"), //$NON-NLS-1$
        ConsoleTranslate.get("admin.command.view.backupers.prop.dump.format") //$NON-NLS-1$
    };
    }
}