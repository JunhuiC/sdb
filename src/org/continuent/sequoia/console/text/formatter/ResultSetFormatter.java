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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.formatter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.Console;
import org.continuent.sequoia.console.text.ConsoleException;

/**
 * Utility class to format a <code>ResultSet</code> to display it prettily in
 * the text console
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class ResultSetFormatter
{
  
  /**
   * by default, display result set's rows horizontally (everything on the same line)
   */
  private static boolean displayRowsHorizontally = true;

  /** Max column width when displaying a <code>ResultSet</code>. */
  private static final int MAX_COLUMN_DISPLAY_WIDTH = 25;


  /**
   * Toggles the display of rows either horizontally (by default) or vertically.
   * 
   * @return <code>true</code> if the result set's rows will be displayed
   *         horizontally (everything on the same line), <code>false</code> if
   *         the rows will be displayed vertically (one row cell per line, one
   *         row after the other)
   */
  public static boolean toggleRowDisplay()
  {
    displayRowsHorizontally = !displayRowsHorizontally ;
    return displayRowsHorizontally;
  }
  
  public static void display(ResultSet rs, int fetchsize, Console console)
  {
    int totalLines = 0;
    String[] headers = getHeaders(rs);
    List cells;
    do {
      cells = getCells(rs, fetchsize);
      if (cells.size() == 0) {
        return;
      }
      totalLines += cells.size();
      String[][] matrix = (String[][]) cells.toArray(new String[cells.size()][]);
      String out = TableFormatter.format(headers, matrix, displayRowsHorizontally);
      console.println(out);
      if (fetchsize != 0 && cells.size() % fetchsize == 0) {
        try
        {
          console.readLine(ConsoleTranslate.get("sql.display.next.rows",
              new Integer[]{new Integer(fetchsize), new Integer(totalLines)}));
        }
        catch (ConsoleException e)
        {
        }
      }
    } while (cells != null && cells.size() > 0);
  }

  private static List getCells(ResultSet rs, int fetchsize)
  {
    try
    {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      List cells = new ArrayList();
      while (rs.next())
      {
        String[] cell = new String[columnCount];
        for (int j = 0; j < columnCount; j++)
        {
          String object = rs.getString(j + 1);
          String value = (object != null) ? object : "";
          cell[j] = value;
        }
        cells.add(cell);
        if (fetchsize != 0 && (cells.size() % fetchsize == 0))
        {
          return cells;
        }
      }
      return cells;
    }
    catch (SQLException e)
    {
      return null;
    }
  }

  private static String[] getHeaders(ResultSet rs)
  {
    try
    {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      String[] headers = new String[columnCount];
      meta = rs.getMetaData();
      for (int i = 0; i < columnCount; i++)
      {
        headers[i] = meta.getColumnName(i + 1);
      }
      return headers;
    }
    catch (SQLException e)
    {
      return new String[0];
    }
  }

  /**
   * Format and display the given <code>ResultSet</code> on the console.
   * 
   * @param rs the <code>ResultSet</code> to display
   * @param fetchsize fetchisze value
   * @param console console where the ResultSet will be displayed and he size of
   *          the result set)
   * @throws SQLException if an error occurs
   */
  public static void formatAndDisplay(ResultSet rs, int fetchsize,
      Console console) throws SQLException
  {
    // Get the metadata
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();

    console.println();

    appendSeparatorLine(columnCount, meta, console);

    // Print the column names
    console.print("|");
    for (int i = 1; i <= columnCount; i++)
    {
      console.print(" ");
      // Pad the column name and print it
      int size = meta.getColumnDisplaySize(i);
      String columnName = meta.getColumnName(i);
      if (size <= 0)
      {
        if (columnName != null)
          size = columnName.length();
        else
          size = 0;
      }
      appendPad(columnName, size, console);
      console.print(" |");
    }
    console.println();

    appendSeparatorLine(columnCount, meta, console);

    // Display the results
    String object;
    int line = 0;
    while (rs.next())
    {
      console.print("|");
      for (int i = 1; i <= columnCount; i++)
      {
        console.print(" ");
        String value = null;
        try
        {
          object = rs.getString(i);
          value = (object != null) ? object : "";
        }
        catch (SQLException e)
        {
          value = e.getMessage();
        }

        // Pad the value and print it
        int size = meta.getColumnDisplaySize(i);
        if (size <= 0)
        {
          if (value != null)
            size = value.length();
          else
            size = 0;
        }
        appendPad(value, size, console);
        console.print(" |");
      }
      console.println();
      line++;
      if (fetchsize != 0)
      {
        if (line % fetchsize == 0)
        {
          try
          {
            console.readLine(ConsoleTranslate.get("sql.display.next.rows",
                new Integer[]{new Integer(fetchsize), new Integer(line)}));
          }
          catch (ConsoleException ignore)
          {
          }
        }
      }
    }

    appendSeparatorLine(columnCount, meta, console);
  }

  private static void appendSeparatorLine(int columnCount,
      ResultSetMetaData meta, Console console) throws SQLException
  {

    console.print("+");
    for (int i = 1; i <= columnCount; i++)
    {
      int size = meta.getColumnDisplaySize(i);
      if (size > MAX_COLUMN_DISPLAY_WIDTH)
        size = MAX_COLUMN_DISPLAY_WIDTH;
      console.print("-");
      for (int j = 0; j < size; j++)
        console.print("-");
      console.print("-+");
    }
    console.println();
  }

  private static void appendPad(String text, int size, Console console)
  {
    if (size > MAX_COLUMN_DISPLAY_WIDTH)
      size = MAX_COLUMN_DISPLAY_WIDTH;
    if (size < text.length())
    {
      console.print(text.substring(0, size - 1) + "~");
      return;
    }
    StringBuffer toPad = new StringBuffer(size);
    toPad.insert(0, text);
    while (toPad.length() < size)
      toPad.append(' ');
    console.print(toPad.toString());
  }
}
