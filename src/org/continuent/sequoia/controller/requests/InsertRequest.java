/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Mathieu Peltier, Sara Bouchenak, Stephane Giron.
 */

package org.continuent.sequoia.controller.requests;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.sql.schema.TableColumn;

/**
 * An <code>InsertRequest</code> is an SQL request of the following syntax:
 * 
 * <pre>
 *  INSERT INTO table-name [(column-name[,column-name]*)] {VALUES (constant|null[,constant|null]*)}|{SELECT query}
 * </pre>
 * <code>VALUES<code> are ignored.
 *   *
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class InsertRequest extends AbstractWriteRequest implements Serializable
{
  private static final long    serialVersionUID              = -7395745061633156437L;

  private static final String  INSERT_REQUEST_PATTERN_STRING = "^insert\\s+(into\\s+)?(only\\s+)?([^(\\s|\\()]+)(\\s*\\(.*\\)\\s*|\\s+)(values|select)(.*)";

  private static final Pattern INSERT_REQUEST_PATTERN        = Pattern
                                                                 .compile(
                                                                     INSERT_REQUEST_PATTERN_STRING,
                                                                     Pattern.CASE_INSENSITIVE
                                                                         | Pattern.DOTALL);

  // These TABLENAME_POS and COLUMNS_POS respectively represent the position of
  // the table name and the list of columns in the previous regular expression.
  // Check that it did change when updating the regular expression
  // INSERT_REQUEST_PATTERN_STRING.
  private static final int     TABLENAME_POS                 = 3;
  private static final int     COLUMNS_POS                   = 4;

  /**
   * Creates a new <code>InsertRequest</code> instance. The caller must give
   * an SQL request, without any leading or trailing spaces and beginning with
   * 'create table ' (it will not be checked).
   * <p>
   * The request is not parsed but it can be done later by a call to
   * {@link #parse(DatabaseSchema, int, boolean)}.
   * 
   * @param sqlQuery the SQL request
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database ?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   * @see #parse(DatabaseSchema, int, boolean)
   */
  public InsertRequest(String sqlQuery, boolean escapeProcessing, int timeout,
      String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator,
        RequestType.INSERT);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersAggregateList()
   */
  public boolean altersAggregateList()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseCatalog()
   */
  public boolean altersDatabaseCatalog()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseSchema()
   */
  public boolean altersDatabaseSchema()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersMetadataCache()
   */
  public boolean altersMetadataCache()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersQueryResultCache()
   */
  public boolean altersQueryResultCache()
  {
    return true;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersSomething()
   */
  public boolean altersSomething()
  {
    return true;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersStoredProcedureList()
   */
  public boolean altersStoredProcedureList()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUserDefinedTypes()
   */
  public boolean altersUserDefinedTypes()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUsers()
   */
  public boolean altersUsers()
  {
    return false;
  }

  /**
   * @see AbstractRequest#cloneParsing(AbstractRequest)
   */
  public void cloneParsing(AbstractRequest request)
  {
    if (!request.isParsed())
      return;
    cloneTableNameAndColumns((AbstractWriteRequest) request);
    isParsed = true;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#needsMacroProcessing()
   */
  public boolean needsMacroProcessing()
  {
    return true;
  }

  /**
   * Parse the query to know which table is affected. Also checks for the
   * columns if the parsing granularity requires it.
   * 
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#parse(org.continuent.sequoia.common.sql.schema.DatabaseSchema,
   *      int, boolean)
   */
  public void parse(DatabaseSchema schema, int granularity,
      boolean isCaseSensitive) throws SQLException
  {

    if (granularity == ParsingGranularities.NO_PARSING)
    {
      isParsed = true;
      return;
    }

    Matcher matcher;
    String insertTable = "";
    String strColumns = null;

    String originalSQL = this.trimCarriageReturnAndTabs();

    matcher = INSERT_REQUEST_PATTERN.matcher(originalSQL);
    if (matcher.matches())
    {
      insertTable = matcher.group(TABLENAME_POS);
      strColumns = matcher.group(COLUMNS_POS).trim();
    }

    if (!isCaseSensitive)
      insertTable = insertTable.toLowerCase();

    if (schema == null)
    {
      // Lock this table in write
      writeLockedTables = new TreeSet<String>();
      writeLockedTables.add(insertTable);
      isParsed = true;
      // and stop parsing
      return;
    }

    DatabaseTable t = schema.getTable(insertTable, isCaseSensitive);
    if (t == null)
      throw new SQLException("Unknown table '" + insertTable
          + "' in this INSERT statement: '" + sqlQueryOrTemplate + "'");
    else
      tableName = t.getName();

    // Lock this table in write
    writeLockedTables = new TreeSet<String>();
    writeLockedTables.add(tableName);
    addDependingTables(schema, writeLockedTables);

    if ((granularity == ParsingGranularities.COLUMN)
        || (granularity == ParsingGranularities.COLUMN_UNIQUE))
    {
      if (strColumns.length() > 0)
      {
        // Removes '(' and ')' surrounding column names
        strColumns = strColumns.substring(1, strColumns.length() - 1);

        // get all column names
        StringTokenizer columnTokens = new StringTokenizer(strColumns, ",");

        this.columns = new ArrayList<TableColumn>();
        DatabaseColumn col = null;
        while (columnTokens.hasMoreTokens())
        {
          String token = columnTokens.nextToken().trim();
          if ((col = t.getColumn(token)) == null)
          {
            tableName = null;
            this.columns = null;
            throw new SQLException("Unknown column name '" + token
                + "' in this INSERT statement: '" + sqlQueryOrTemplate + "'");
          }
          else
          {
            this.columns.add(new TableColumn(tableName, col.getName()));
          }
        }
      }
      else
      {
        // All columns are affected
        this.columns = new ArrayList<TableColumn>();
        ArrayList<?> cols = t.getColumns();
        int size = cols.size();
        for (int j = 0; j < size; j++)
        {
          this.columns.add(new TableColumn(tableName, ((DatabaseColumn) cols
              .get(j)).getName()));
        }
      }
    }

    isParsed = true;
  }

  /**
   * Displays some debugging information about this request.
   */
  public void debug()
  {
    super.debug();
    if (tableName != null)
      System.out.println("Inserted table: " + tableName);
    else
      System.out.println("No information about inserted table");

    if (columns != null)
    {
      System.out.println("Inserted columns:");
      for (int i = 0; i < columns.size(); i++)
        System.out.println("  "
            + ((TableColumn) columns.get(i)).getColumnName());
    }
    else
      System.out.println("No information about inserted columns");

    System.out.println("");
  }

}