/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.requests;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TreeSet;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.sql.schema.TableColumn;

/**
 * This class defines a AlterRequest
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class AlterRequest extends AbstractWriteRequest
{
  private static final long          serialVersionUID          = 8386732943389593826L;

  /** The table to alter. */
  protected transient DatabaseTable  table                     = null;

  /** The column altered */
  protected transient DatabaseColumn column                    = null;

  protected transient boolean        isDrop                    = false;
  protected transient boolean        isAdd                     = false;

  // Be conservative, if we cannot parse the query, assumes it invalidates
  // everything
  protected boolean                  altersAggregateList       = true;
  protected boolean                  altersDatabaseCatalog     = true;
  protected boolean                  altersDatabaseSchema      = true;
  protected boolean                  altersMetadataCache       = true;
  protected boolean                  altersQueryResultCache    = true;
  protected boolean                  altersSomething           = true;
  protected boolean                  altersStoredProcedureList = true;
  protected boolean                  altersUserDefinedTypes    = true;
  protected boolean                  altersUsers               = true;

  /**
   * Creates a new <code>AlterRequest</code> instance. The caller must give an
   * SQL request, without any leading or trailing spaces and beginning with
   * 'alter table '
   * <p>
   * The request is not parsed but it can be done later by a call to
   * {@link #parse(DatabaseSchema, int, boolean)}.
   * 
   * @param sqlQuery the SQL request
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database ?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   * @see #parse
   */
  public AlterRequest(String sqlQuery, boolean escapeProcessing, int timeout,
      String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator, RequestType.ALTER);
    setMacrosAreProcessed(true); // no macro processing needed
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersAggregateList()
   */
  public boolean altersAggregateList()
  {
    return altersAggregateList;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseCatalog()
   */
  public boolean altersDatabaseCatalog()
  {
    return altersDatabaseCatalog;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseSchema()
   */
  public boolean altersDatabaseSchema()
  {
    return altersDatabaseSchema;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersMetadataCache()
   */
  public boolean altersMetadataCache()
  {
    return altersMetadataCache;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersQueryResultCache()
   */
  public boolean altersQueryResultCache()
  {
    return altersQueryResultCache;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersSomething()
   */
  public boolean altersSomething()
  {
    return altersSomething;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersStoredProcedureList()
   */
  public boolean altersStoredProcedureList()
  {
    return altersStoredProcedureList;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUserDefinedTypes()
   */
  public boolean altersUserDefinedTypes()
  {
    return altersUserDefinedTypes;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUsers()
   */
  public boolean altersUsers()
  {
    return altersUsers;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#cloneParsing(org.continuent.sequoia.controller.requests.AbstractRequest)
   */
  public void cloneParsing(AbstractRequest request)
  {
    if (!request.isParsed())
      return;
    AlterRequest alterRequest = (AlterRequest) request;
    cloneTableNameAndColumns((AbstractWriteRequest) request);
    table = alterRequest.getDatabaseTable();
    column = alterRequest.getColumn();
    isParsed = true;
  }

  /**
   * Returns the column value.
   * 
   * @return Returns the column.
   */
  public DatabaseColumn getColumn()
  {
    return column;
  }

  /**
   * Returns the table value.
   * 
   * @return Returns the table.
   */
  public DatabaseTable getDatabaseTable()
  {
    return table;
  }

  /**
   * Returns the isAdd value.
   * 
   * @return Returns the isAdd.
   */
  public boolean isAdd()
  {
    return isAdd;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#needsMacroProcessing()
   */
  public boolean needsMacroProcessing()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#parse(org.continuent.sequoia.common.sql.schema.DatabaseSchema,
   *      int, boolean)
   */
  public void parse(DatabaseSchema schema, int granularity,
      boolean isCaseSensitive) throws SQLException
  {
    /*
     * Example Alter statement: ALTER TABLE table_name ADD column_name datatype
     * ALTER TABLE table_name DROP COLUMN column_name
     */

    if (granularity == ParsingGranularities.NO_PARSING)
    {
      isParsed = true;
      return;
    }

    String originalSQL = this.trimCarriageReturnAndTabs();
    String sql = originalSQL.toLowerCase();

    // Strip 'alter table '
    int tableIdx = sql.indexOf("table");
    if (tableIdx == -1)
      throw new SQLException(
          "Malformed Alter Request. Should start with [ALTER TABLE]");
    sql = sql.substring(tableIdx + 5).trim();

    // Does the query contain a add?
    int addIdx = sql.indexOf(" add ");

    // Does the query contain a drop?
    int dropIdx = sql.indexOf(" drop ");

    if (addIdx != -1)
      isAdd = true;
    if (dropIdx != -1)
      isDrop = true;

    if (!isAdd && !isDrop)
      throw new SQLException(
          "Malformed Alter Request. No drop or add condition");

    if (isCaseSensitive) // Reverse to the original case
      sql = originalSQL.substring(originalSQL.length() - sql.length());

    int index = (isAdd) ? addIdx : dropIdx;

    tableName = sql.substring(0, index).trim();
    table = new DatabaseTable(tableName);
    writeLockedTables = new TreeSet<String>();
    writeLockedTables.add(tableName);
    addDependingTables(schema, writeLockedTables);

    if ((schema != null) && (granularity > ParsingGranularities.TABLE))
    {
      int subsIndex = index + 6 + 2; // index +
      // column.length()
      // + space
      if (isAdd)
        subsIndex += 3;
      else
        // Drop
        subsIndex += 4;

      columns = new ArrayList<TableColumn>();
      sql = sql.substring(subsIndex).trim();

      if (isAdd)
      {
        int colIndex = sql.indexOf(' ');
        String colName = sql.substring(0, colIndex);

        int uniqueIndex = sql.toLowerCase().indexOf("unique");
        int primary = sql.toLowerCase().indexOf("primary");
        if (uniqueIndex != -1 || primary != -1)
          column = new DatabaseColumn(colName, true);
        else
          column = new DatabaseColumn(colName, false);
        columns.add(new TableColumn(tableName, colName));
      }
      else if (isDrop)
      {
        String colName = sql.trim();
        column = schema.getTable(tableName).getColumn(colName);
        columns.add(new TableColumn(tableName, colName));
      }
    }
    isParsed = true;
  }

  /**
   * Does this request returns a ResultSet?
   * 
   * @return false
   */
  public boolean returnsResultSet()
  {
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getParsingResultsAsString()
   */
  public String getParsingResultsAsString()
  {
    StringBuffer sb = new StringBuffer(super.getParsingResultsAsString());
    sb.append(Translate.get("request.alter.table", table));
    sb.append(Translate.get("request.alter.column", column));
    sb.append(Translate.get("request.alter.is.drop", isDrop));
    sb.append(Translate.get("request.alter.is.add", isAdd));
    sb.append(Translate.get("request.alters",
        new String[]{String.valueOf(altersAggregateList()),
            String.valueOf(altersDatabaseCatalog()),
            String.valueOf(altersDatabaseSchema()),
            String.valueOf(altersMetadataCache()),
            String.valueOf(altersQueryResultCache()),
            String.valueOf(altersSomething()),
            String.valueOf(altersStoredProcedureList()),
            String.valueOf(altersUserDefinedTypes()),
            String.valueOf(altersUsers())}));

    return sb.toString();
  }
}