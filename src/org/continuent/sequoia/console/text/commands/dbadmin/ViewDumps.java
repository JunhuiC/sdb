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

import java.text.SimpleDateFormat;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to display available dumps of a given
 * virtual database.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class ViewDumps extends AbstractAdminCommand
{

  /**
   * Creates a "view dumps" command for the admin module.
   * 
   * @param module module that owns this commands
   */
  public ViewDumps(VirtualDatabaseAdmin module)
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
    DumpInfo[] dumps = vdjc.getAvailableDumps();
    if (dumps.length == 0)
    {
      console.printError(ConsoleTranslate
          .get("admin.command.view.dumps.nodump")); //$NON-NLS-1$
    }
    else
    {
      String formattedDumps = TableFormatter.format(getDumpsDescriptions(),
          getDumpsAsStrings(dumps), true); // FIXME should display dumps
      // headers in colums
      console.println(formattedDumps);
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "show dumps"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.view.dumps.description"); //$NON-NLS-1$
  }

  private static String[][] getDumpsAsStrings(DumpInfo[] dumps)
  {
    SimpleDateFormat sdf = new SimpleDateFormat();
    String[][] dumpStr = new String[dumps.length][7];
    for (int i = 0; i < dumpStr.length; i++)
    {
      DumpInfo dump = dumps[i];
      dumpStr[i][0] = dump.getDumpName();
      dumpStr[i][1] = dump.getCheckpointName();
      dumpStr[i][2] = dump.getDumpFormat();
      dumpStr[i][3] = dump.getDumpPath();
      dumpStr[i][4] = sdf.format(dump.getDumpDate());
      dumpStr[i][5] = dump.getBackendName();
      dumpStr[i][6] = dump.getTables();
    }
    return dumpStr;
  }

  private static String[] getDumpsDescriptions()
  {
    String[] desc = new String[7];
    desc[0] = ConsoleTranslate.get("admin.command.view.dumps.prop.name"); //$NON-NLS-1$
    desc[1] = ConsoleTranslate.get("admin.command.view.dumps.prop.checkpoint"); //$NON-NLS-1$
    desc[2] = ConsoleTranslate.get("admin.command.view.dumps.prop.format"); //$NON-NLS-1$
    desc[3] = ConsoleTranslate.get("admin.command.view.dumps.prop.path"); //$NON-NLS-1$
    desc[4] = ConsoleTranslate.get("admin.command.view.dumps.prop.date"); //$NON-NLS-1$
    desc[5] = ConsoleTranslate.get("admin.command.view.dumps.prop.backend"); //$NON-NLS-1$
    desc[6] = ConsoleTranslate.get("admin.command.view.dumps.prop.tables"); //$NON-NLS-1$
    return desc;
  }
}