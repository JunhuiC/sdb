/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Continuent, Inc.
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
 * Initial developer(s): Emmanuel Cecchet
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class display statistics of the SQL monitoring module of the controller.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ShowSqlStats extends AbstractAdminCommand
{
  private String[] columnNames;

  /**
   * Creates a new <code>ShowSqlStats</code> object
   * 
   * @param module virtual database admin module
   */
  public ShowSqlStats(VirtualDatabaseAdmin module)
  {
    super(module);
    columnNames = new String[9];
    columnNames[0] = "SQL statement";
    columnNames[1] = "Count";
    columnNames[2] = "Error";
    columnNames[3] = "Cache hits";
    columnNames[4] = "% hits";
    columnNames[5] = "Min time (ms)";
    columnNames[6] = "Max time (ms)";
    columnNames[7] = "Avg time (ms)";
    columnNames[8] = "Total time (ms)";
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    String[][] stats = jmxClient.getDataCollectorProxy().retrieveSQLStats(
        dbName);
    if (stats == null)
      console.printError("No SQL statistics are available on this controller");
    else
      console.println(displayStats(stats));
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "show sql statistics"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.show.sql.stats.description");
  }

  /**
   * Format data for text consoles
   * 
   * @param data to display
   * @return a formatted string with tabs and end of line
   */
  public String displayStats(String[][] data)
  {
    if (data == null)
      return "";

    // constants used for formatting the output
    final String columnPadding = "    ";
    final String nameValueSeparator = ":  ";

    // solve the maximum length for column names
    // TODO: refactor this into its own method
    int maxNameLength = 0;
    for (int i = 0; i < columnNames.length; i++)
    {
      maxNameLength = Math.max(maxNameLength, columnNames[i].length());
    }

    // solve the maximum length for column values
    // TODO: refactor this into its own method
    int maxValueLength = 0;
    for (int i = 0; i < data.length; i++)
    {
      for (int j = 0; j < data[i].length; j++)
      {
        maxValueLength = Math.max(maxValueLength, data[i][j].length());
      }
    }

    // construct a separator line based on maximum column and value lengths
    // TODO: extract numbers into constants and this block into a new method
    char[] separator = new char[columnPadding.length() + maxNameLength
        + nameValueSeparator.length() + maxValueLength + 1]; /*
                                                               * the newline
                                                               * character
                                                               */
    for (int i = 0; i < separator.length; i++)
    {
      separator[i] = '-';
    }
    separator[separator.length - 1] = '\n';

    // loop through all the data and print padded lines into the StringBuffer
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < data.length; i++)
    {
      sb.append(separator);
      for (int j = 0; j < data[i].length; j++)
      {
        // create the padding needed for this particular column
        // TODO: extract this into its own method
        char[] namePadding = new char[maxNameLength - columnNames[j].length()];
        for (int x = 0; x < namePadding.length; x++)
        {
          namePadding[x] = ' ';
        }

        sb.append(columnPadding);
        sb.append(columnNames[j]);
        sb.append(nameValueSeparator);
        sb.append(namePadding);
        sb.append(data[i][j]);
        sb.append("\n");
      }
      if (i + 1 == data.length)
      {
        sb.append(separator);
      }
    }
    return sb.toString();
  }

}