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
 * Initial developer(s): Julie Marguerite.
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.controller.requests;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;

/**
 * An <code>DropRequest</code> is an SQL request with the following syntax:
 * 
 * <pre>
 *  DROP TABLE table-name
 * </pre>
 * 
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public class DropRequest extends AbstractWriteRequest implements Serializable
{
  private static final long serialVersionUID          = 5702833689405251895L;

  // Be conservative, if we cannot parse the query, assumes it invalidates
  // everything
  protected boolean         altersAggregateList       = true;
  protected boolean         altersDatabaseCatalog     = true;
  protected boolean         altersDatabaseSchema      = true;
  protected boolean         altersMetadataCache       = true;
  protected boolean         altersQueryResultCache    = true;
  protected boolean         altersSomething           = true;
  protected boolean         altersStoredProcedureList = true;
  protected boolean         altersUserDefinedTypes    = true;
  protected boolean         altersUsers               = true;

  /**
   * Sorted list of table names that are being dropped. This will be used to
   * remove these tables from the schema.
   */
  protected SortedSet<String>       tablesToDrop              = null;

  /**
   * Creates a new <code>DropRequest</code> instance. The caller must give an
   * SQL request, without any leading or trailing spaces and beginning with
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
  public DropRequest(String sqlQuery, boolean escapeProcessing, int timeout,
      String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator, RequestType.DROP);
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
    return false;
  }

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

    // Strip drop
    sql = sql.substring("drop".length()).trim();
    originalSQL = originalSQL.substring("drop".length()).trim();

    // Check what kind of create we are facing
    if (sql.startsWith("database"))
    { // DROP DATABASE alters only the database catalog
      altersDatabaseCatalog = true;
      altersDatabaseSchema = false;
      altersStoredProcedureList = false;
      altersUserDefinedTypes = false;
      altersUsers = false;
      return;
    }

    if (sql.startsWith("index") || sql.startsWith("role")
        || sql.startsWith("sequence"))
    { // Does not alter anything :
      // DROP INDEX
      // DROP ROLE
      // DROP SEQUENCE
      altersSomething = false;
      return;
    }
    if (sql.startsWith("function") || sql.startsWith("method")
        || sql.startsWith("procedure") || sql.startsWith("trigger")
        || sql.startsWith("type"))
    { // DROP FUNCTION, DROP METHOD, DROP PROCEDURE, DROP TRIGGER and
      // DROP TYPE only alters definitions
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

    // Strip 'drop (temporary) table '
    int tableIdx = sql.indexOf("table");
    if (tableIdx < 0)
      throw new SQLException("Unsupported DROP statement: '"
          + sqlQueryOrTemplate + "'");

    if (isCaseSensitive)
      sql = originalSQL.substring(tableIdx + 5).trim();
    else
      sql = sql.substring(tableIdx + 5).trim();

    int endTableName = sql.indexOf(" ");

    if (endTableName >= 0)
      sql = sql.substring(0, endTableName).trim();

    if (schema == null)
      tableName = sql;
    else
    {
      // Get the table on which DROP occurs
      DatabaseTable t = schema.getTable(sql, isCaseSensitive);
      if (t == null)
        throw new SQLException("Unknown table '" + sql
            + "' in this DROP statement '" + sqlQueryOrTemplate + "'", "42P01");
      else
        tableName = t.getName();
    }
    writeLockedTables = new TreeSet<String>();
    tablesToDrop = new TreeSet<String>();
    writeLockedTables.add(tableName);
    tablesToDrop.add(tableName);
    addDependingTables(schema, writeLockedTables);
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
      System.out.println("Dropped table '" + tableName + "'");
    else
      System.out.println("No information about dropped table");

    System.out.println();
  }

  /**
   * Return the set of tables that are dropped by current request. This set is
   * different of writeLockedTables, as the latest also contains tables that
   * need to be locked (because of foreign key constraints).
   * 
   * @return set of tables that are being dropped by this request
   */
  public SortedSet<String> getTablesToDrop()
  {
    return tablesToDrop;
  }
}