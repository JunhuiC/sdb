/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Initial developer(s): Julie Marguerite.
 * Contributor(s): Mathieu Peltier, Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.requests;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.sql.schema.TableColumn;

/**
 * A <code>CreateRequest</code> is a SQL request of the following syntax:
 * 
 * <pre>
 *  CREATE [TEMPORARY] TABLE table-name [(column-name column-type [,column-name colum-type]* [,table-constraint-definition]*)]
 * </pre>
 * 
 * We now also support SELECT INTO statements.
 * 
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class CreateRequest extends AbstractWriteRequest implements Serializable
{
  private static final long         serialVersionUID          = -8810498358284503952L;

  /** The table to create. */
  protected transient DatabaseTable table                     = null;

  /**
   * List of tables used to fill the created table in case of create query
   * containing a select.
   */
  protected transient Collection    fromTables                = null;

  // Be conservative, if we cannot parse the query, assumes it invalidates
  // everything
  protected boolean                 altersDatabaseCatalog     = true;
  protected boolean                 altersDatabaseSchema      = true;
  protected boolean                 altersSomething           = true;
  protected boolean                 altersStoredProcedureList = true;
  protected boolean                 altersUserDefinedTypes    = true;
  protected boolean                 altersUsers               = true;

  protected boolean                 altersAggregateList       = false;
  protected boolean                 altersMetadataCache       = false;
  protected boolean                 altersQueryResultCache    = false;

  protected boolean                 createsTemporaryTable    = false;

  /**
   * Creates a new <code>CreateRequest</code> instance. The caller must give
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
   * @see #parse
   */
  public CreateRequest(String sqlQuery, boolean escapeProcessing, int timeout,
      String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator,
        RequestType.CREATE);
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
   * Returns true if this create request alters the current database schema
   * (using create table, create schema, create view) and false otherwise
   * (create database, create index, create function, create method, create
   * procedure, create role, create trigger, create type).
   * 
   * @return Returns true if this query alters the database schema.
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
   * @see AbstractRequest#cloneParsing(AbstractRequest)
   */
  public void cloneParsing(AbstractRequest request)
  {
    if (!request.isParsed())
      return;
    CreateRequest createRequest = (CreateRequest) request;
    cloneTableNameAndColumns((AbstractWriteRequest) request);
    table = createRequest.getDatabaseTable();
    fromTables = createRequest.getFromTables();
    isParsed = true;
  }

  /**
   * Gets the database table created by this statement (in case of a CREATE
   * TABLE statement).
   * 
   * @return a <code>DatabaseTable</code> value
   */
  public DatabaseTable getDatabaseTable()
  {
    return table;
  }

  /**
   * Returns the list of tables used to fill the created table in case of create
   * query containing a select.
   * 
   * @return <code>Collection</code> of tables
   */
  public Collection getFromTables()
  {
    return fromTables;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#needsMacroProcessing()
   */
  public boolean needsMacroProcessing()
  {
    return false;
  }

  /**
   * Pattern for SELECT INTO statement. This permits to extract from the
   * statement the name of the table.
   */
  private static final String  CREATE_TABLE_SELECT_INTO                   = "^select.*into\\s+((temporary|temp)\\s+)?(table\\s+)?([^(\\s|\\()]+)(.*)?";
  private static final Pattern CREATE_TABLE_SELECT_INTO_STATEMENT_PATTERN = Pattern
                                                                              .compile(
                                                                                  CREATE_TABLE_SELECT_INTO,
                                                                                  Pattern.CASE_INSENSITIVE
                                                                                      | Pattern.DOTALL);

  /**
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

    String originalSQL = this.trimCarriageReturnAndTabs();
    String sql = originalSQL.toLowerCase();

    Matcher matcher = CREATE_TABLE_SELECT_INTO_STATEMENT_PATTERN.matcher(sql);
    if (matcher.matches())
    {
      // INSERT... INTO
      table = new DatabaseTable(matcher.group(4));

      altersDatabaseCatalog = false;
      altersDatabaseSchema = true;
      altersStoredProcedureList = false;
      altersUserDefinedTypes = false;
      altersUsers = false;
      isParsed = true;
      return;
    }

    // Strip create
    sql = sql.substring("create".length()).trim();

    // Check what kind of create we are facing
    if (sql.startsWith("database"))
    { // CREATE DATABASE alters only the database catalog
      altersDatabaseCatalog = true;
      altersDatabaseSchema = false;
      altersStoredProcedureList = false;
      altersUserDefinedTypes = false;
      altersUsers = false;
      return;
    }
    if (sql.startsWith("index") || sql.startsWith("unique")
        || sql.startsWith("role") || sql.startsWith("sequence"))
    { // Does not alter anything :
      // CREATE [UNIQUE] INDEX
      // CREATE ROLE
      // CREATE SEQUENCE
      altersSomething = false;
      return;
    }
    if (sql.startsWith("function") || sql.startsWith("method")
        || sql.startsWith("procedure") || sql.startsWith("trigger")
        || sql.startsWith("type"))
    { // CREATE FUNCTION, CREATE METHOD, CREATE PROCEDURE, CREATE TRIGGER and
      // CREATE TYPE only alters definitions
      altersDatabaseCatalog = false;
      altersDatabaseSchema = false;
      altersStoredProcedureList = true;
      altersUserDefinedTypes = true;
      altersUsers = false;
      return;
    }

    // From this point on, only the schema is affected
    altersDatabaseCatalog = false;
    altersDatabaseSchema = true;
    altersStoredProcedureList = false;
    altersUserDefinedTypes = false;
    altersUsers = false;
    if (sql.startsWith("schema") || sql.startsWith("view"))
      // No parsing to do
      return;

    // Let's try to check if we have a 'create [temporary] table '
    int tableIdx = sql.indexOf("table");
    if (tableIdx < 0)
      throw new SQLException("Unsupported CREATE statement: '"
          + sqlQueryOrTemplate + "'");

    //
    // Starting from here, everything is just to handle CREATE TABLE statements
    //

    // Strip up to 'table'
    sql = sql.substring(tableIdx + 5).trim();

    // Does the query contain a select?
    int selectIdx = sql.indexOf("select");
    if (selectIdx != -1 && sql.charAt(selectIdx + 6) != ' ')
      selectIdx = -1;

    if (isCaseSensitive) // Reverse to the original case
      sql = originalSQL.substring(originalSQL.length() - sql.length());

    if (selectIdx != -1)
    {
      // Get the table on which CREATE occurs
      int nextSpaceIdx = sql.indexOf(" ");
      tableName = sql.substring(0, nextSpaceIdx).trim();
      table = new DatabaseTable(tableName);
      // Parse the select
      sql = sql.substring(selectIdx).trim();
      SelectRequest select = new SelectRequest(sql, false, 60,
          getLineSeparator());
      select.parse(schema, granularity, isCaseSensitive);
      fromTables = select.getFrom();
      if (granularity > ParsingGranularities.TABLE)
      { // Update the columns and add them to the table
        columns = select.getSelect();
        int size = columns.size();
        for (int i = 0; i < size; i++)
        {
          TableColumn tc = (TableColumn) columns.get(i);
          table.addColumn(new DatabaseColumn(tc.getColumnName(), false));
        }
      }
    }
    else
    {
      // Get the table on which CREATE occurs
      // Look for the parenthesis
      int openParenthesisIdx = sql.indexOf("(");
      int closeParenthesisIdx = sql.lastIndexOf(")");
      if ((openParenthesisIdx == -1) && (closeParenthesisIdx == -1))
      {
        // no parenthesis found
        table = new DatabaseTable(sql.trim());
        if (granularity > ParsingGranularities.TABLE)
          columns = new ArrayList();
        isParsed = true;
        return;
      }
      else if ((openParenthesisIdx == -1) || (closeParenthesisIdx == -1)
          || (openParenthesisIdx > closeParenthesisIdx))
      {
        throw new SQLException("Syntax error in this CREATE statement: '"
            + sqlQueryOrTemplate + "'");
      }
      else
      {
        tableName = sql.substring(0, openParenthesisIdx).trim();
      }
      table = new DatabaseTable(tableName);

      // Get the column names
      if (granularity > ParsingGranularities.TABLE)
      {
        columns = new ArrayList();
        sql = sql.substring(openParenthesisIdx + 1, closeParenthesisIdx).trim();
        StringTokenizer columnTokens = new StringTokenizer(sql, ",");
        String word;
        String lowercaseWord;
        StringTokenizer wordTokens = null;
        String token;
        DatabaseColumn col = null;

        while (columnTokens.hasMoreTokens())
        {
          token = columnTokens.nextToken().trim();

          // work around to prevent bug: if the request contains for example:
          // INDEX foo (col1,col2)
          // we have to merge the 2 tokens: 'INDEX foo (col1' and 'col2)'
          if ((token.indexOf("(") != -1) && (token.indexOf(")") == -1))
          {
            if (columnTokens.hasMoreTokens())
              token = token + "," + columnTokens.nextToken().trim();
            else
            {
              tableName = null;
              columns = null;
              throw new SQLException("Syntax error in this CREATE statement: '"
                  + sqlQueryOrTemplate + "'");
            }
          }

          // First word of the line: either a column name or
          // a table constraint definition
          wordTokens = new StringTokenizer(token, " ");
          word = wordTokens.nextToken().trim();
          lowercaseWord = word.toLowerCase();

          // If it's a constraint, index or check keyword do not do anything
          // else parse the line
          if (!lowercaseWord.equals("constraint")
              && !lowercaseWord.equals("index")
              && !lowercaseWord.equals("check"))
          {
            String columnName;
            boolean isUnique = false;
            // Check for primary key or unique constraint
            if (lowercaseWord.equals("primary")
                || lowercaseWord.startsWith("unique"))
            {

              // Get the name of the column
              openParenthesisIdx = token.indexOf("(");
              closeParenthesisIdx = token.indexOf(")");
              if ((openParenthesisIdx == -1) || (closeParenthesisIdx == -1)
                  || (openParenthesisIdx > closeParenthesisIdx))
              {
                tableName = null;
                columns = null;
                throw new SQLException(
                    "Syntax error in this CREATE statement: '"
                        + sqlQueryOrTemplate + "'");
              }

              columnName = token.substring(openParenthesisIdx + 1,
                  closeParenthesisIdx).trim();

              int comma;
              while ((comma = columnName.indexOf(',')) != -1)
              {
                String col1 = columnName.substring(0, comma).trim();
                col = table.getColumn(col1);
                if (col == null)
                {
                  tableName = null;
                  columns = null;
                  throw new SQLException(
                      "Syntax error in this CREATE statement: '"
                          + sqlQueryOrTemplate + "'");
                }
                else
                  col.setIsUnique(true);
                columnName = columnName.substring(comma + 1);
              }

              // Set this column to unique
              col = table.getColumn(columnName);

              // Test first if dbTable contains this column. This can fail with
              // some invalid request, for example:
              // CREATE TABLE categories(id INT4, name TEXT, PRIMARY KEY((id))
              if (col == null)
              {
                tableName = null;
                columns = null;
                throw new SQLException(
                    "Syntax error in this CREATE statement: '"
                        + sqlQueryOrTemplate + "'");
              }
              else
                col.setIsUnique(true);
            }
            else
            {
              // It's a column name
              columnName = word;

              if (!wordTokens.hasMoreTokens())
              {
                // at least type declaration is required
                tableName = null;
                columns = null;
                throw new SQLException(
                    "Syntax error in this CREATE statement: '"
                        + sqlQueryOrTemplate + "'");
              }

              // Check for primary key or unique constraints
              do
              {
                word = wordTokens.nextToken().trim().toLowerCase();
                if (word.equals("primary") || word.startsWith("unique"))
                {
                  // Create the column as unique
                  isUnique = true;
                  break;
                }
              }
              while (wordTokens.hasMoreTokens());

              // Add the column to the parsed columns list and
              // to the create DatabaseTable
              columns.add(new TableColumn(tableName, columnName));
              table.addColumn(new DatabaseColumn(columnName, isUnique));
            }
          }
        }
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
    sb.append(Translate.get("request.create.table", table));
    if (fromTables != null)
    {
      sb.append(Translate.get("request.create.from.tables"));
      for (int i = 0; i < fromTables.size(); i++)
        sb.append(Translate.get("request.create.from.table", fromTables
            .toArray()[i]));
    }
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

  /**
   * Displays some debugging information about this request.
   */
  public void debug()
  {
    super.debug();
    if (tableName != null)
      System.out.println("Created table: " + tableName);
    else
      System.out.println("No information about created table");

    if (columns != null)
    {
      System.out.println("Created columns:");
      for (int i = 0; i < columns.size(); i++)
        System.out.println("  "
            + ((TableColumn) columns.get(i)).getColumnName());
    }
    else
      System.out.println("No information about created columns");

    System.out.println();
  }

  public boolean createsTemporaryTable()
  {
    return createsTemporaryTable;
  }

}