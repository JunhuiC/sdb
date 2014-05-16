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
 * Initial developer(s): Jeff Mesnil
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.sqlconsole;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;
import org.continuent.sequoia.console.text.module.VirtualDatabaseConsole;

/**
 * This class defines a ShowTables
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class ShowTables extends ConsoleCommand
{

  /**
   * Creates a new <code>Quit.java</code> object
   * 
   * @param module the command is attached to
   */
  public ShowTables(AbstractConsoleModule module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws IOException, ConsoleException
  {
    try
    {
      DatabaseMetaData dbmd = ((VirtualDatabaseConsole) module).getConnection()
          .getMetaData();
      ResultSet tableSet = dbmd.getTables(null, null, null, new String[]{
          "TABLE", "VIEW"}); //$NON-NLS-1$ //$NON-NLS-2$
      if (tableSet == null)
      {
        console.printInfo(ConsoleTranslate
            .get("sql.command.show.tables.no.tables")); //$NON-NLS-1$
        return;
      }
      List tableNames = new ArrayList();
      while (tableSet.next())
      {
        try
        {
          tableNames.add(tableSet.getString(tableSet.findColumn("TABLE_NAME"))); //$NON-NLS-1$
        }
        catch (Exception e)
        {
          // do nothing
        }
      }
      if (tableNames.isEmpty())
      {
        console.printInfo(ConsoleTranslate
            .get("sql.command.show.tables.no.tables")); //$NON-NLS-1$
      }
      else
      {
        console.println(TableFormatter.format(new String[]{"tables"},
            getTableNamesAsCells(tableNames), true));
      }
    }
    catch (Exception e)
    {
      console.printError(ConsoleTranslate.get("sql.command.sqlquery.error", e), //$NON-NLS-1$
          e);
    }
  }

  private String[][] getTableNamesAsCells(List tableNames)
  {
    String[][] cells = new String[tableNames.size()][1];
    for (int i = 0; i < tableNames.size(); i++)
    {
      cells[i][0] = (String) tableNames.get(i);
    }
    return cells;
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "show tables"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("sql.command.show.tables.description"); //$NON-NLS-1$
  }
}