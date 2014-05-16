/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent.
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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to dump the backend schema.
 */
public class DumpBackendSchema extends AbstractAdminCommand
{

  /**
   * Creates a new <code>DumpBackendSchema</code> object
   * 
   * @param module the commands is attached to
   */
  public DumpBackendSchema(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    // args: <backend name> [table name] [/columns] [/locks]

    StringTokenizer st = new StringTokenizer(commandText.trim());
    String backendName;
    String tableName = null;
    boolean dumpColumns = false;
    boolean dumpLocks = false;
    if (st.countTokens() == 0 || st.countTokens() > 4)
    {
      console.printError(getUsage());
      return;
    }

    backendName = st.nextToken();
    while (st.hasMoreTokens())
    {
      String token = st.nextToken();

      if ("/columns".equals(token))
        dumpColumns = true;
      else if ("/locks".equals(token))
        dumpLocks = true;
      else
        tableName = token;
    }

    DatabaseBackendMBean dbMbean = jmxClient.getDatabaseBackendProxy(dbName,
        backendName, user, password);

    if (tableName == null)
    {
      String[] names = dbMbean.getTablesNames();
      for (int i = 0; i < names.length; i++)
      {
        printTable(names[i], dumpColumns, dumpLocks, dbMbean);
      }
    }
    else
    {
      printTable(tableName, dumpColumns, dumpLocks, dbMbean);
    }
  }

  private void printTable(String tableName, boolean dumpColumns,
      boolean dumpLocks, DatabaseBackendMBean dbMbean)
  {
    console.println(tableName);
    if (dumpColumns)
      printColumnNames(dbMbean, tableName);
    if (dumpLocks)
      printLocks(dbMbean, tableName);
  }

  private void printLocks(DatabaseBackendMBean dbMbean, String tableName)
  {
    console.println(dbMbean.getLockInfo(tableName));
  }

  private void printColumnNames(DatabaseBackendMBean dbMbean, String tableName)
  {
    String[] columns = dbMbean.getColumnsNames(tableName);

    if (columns == null)
    {
      console.printError(ConsoleTranslate.get("DumpBackendSchema.noSuchTable",
          tableName));
      return;
    }

    for (int j = 0; j < columns.length; j++)
      console.println("    " + columns[j]);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {

    return "dump backend schema"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("DumpBackendSchema.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("DumpBackendSchema.description"); //$NON-NLS-1$
  }
}
