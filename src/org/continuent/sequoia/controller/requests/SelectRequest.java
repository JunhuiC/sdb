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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Julie Marguerite, Mathieu Peltier, Sara Bouchenak.
 */

package org.continuent.sequoia.controller.requests;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.AliasedDatabaseTable;
import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.sql.schema.TableColumn;

/**
 * A <code>SelectRequest</code> is an SQL request returning a
 * {@link java.sql.ResultSet}. It may also have database side-effects.
 * <p>
 * It has the following syntax:
 * 
 * <pre>
 *  SELECT [ALL|DISTINCT] select-item[,select-item]* 
 *  FROM table-specification[,table-specification]* 
 *  [WHERE search-condition] 
 *  [GROUP BY grouping-column[,grouping-column]] 
 *  [HAVING search-condition] 
 *  [ORDER BY sort-specification[,sort-specification]] 
 *  [LIMIT ignored]
 * </pre>
 * 
 * Note that table-specification in the <code>FROM</code> clause can be a
 * sub-select. Everything after the end of the <code>WHERE</code> clause is
 * ignored.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Sara.Bouchenak@epfl.ch">Sara Bouchenak </a>
 * @version 1.0
 */
public class SelectRequest extends AbstractRequest implements Serializable
{
  private static final long      serialVersionUID = 6498520472410320514L;

  /**
   * Set to true if this SelectRequest must be broadcasted on the cluster
   * (useful for queries like SELECT FOR UPDATE
   */
  private boolean                mustBroadcast    = false;

  /** <code>ArrayList</code> of <code>TableColumn</code> objects. */
  protected transient ArrayList<TableColumn>  select;

  /** <code>ArrayList</code> of <code>String</code> objects. */
  protected transient Collection<Serializable> from;

  /** <code>ArrayList</code> of <code>AliasedTable</code> objects */
  protected transient Collection<Serializable> aliasFrom;

  /** <code>ArrayList</code> of <code>TableColumn</code> objects. */
  protected transient ArrayList<TableColumn>  where;

  /** <code>ArrayList</code> of <code>OrderBy</code> objects */
  protected transient ArrayList<?>  order;

  /** Some values to keep track of function in the SELECT request */
  public static final int        NO_FUNCTION      = 0;
  /** Represents a SQL max() macro */
  public static final int        MAX_FUNCTION     = 1;
  /** Represents a SQL min() macro */
  public static final int        MIN_FUNCTION     = 2;
  /** Represents a SQL average() macro */
  public static final int        AVERAGE_FUNCTION = 3;
  /** Represents a SQL count() macro */
  public static final int        COUNT_FUNCTION   = 4;
  /** Represents a SQL sum() macro */
  public static final int        SUM_FUNCTION     = 5;

  /** Need to keep track of type of query, e.g. MAX, COUNT, etc. */
  public transient int           funcType         = 0;

  /** Primary key value in case of a unique selection */
  protected transient String     pkValue          = null;

  /**
   * <code>Hashtable</code> of String keys corresponding to column names and
   * String values corresponding to the values associated with the UNIQUE
   * columns of a UNIQUE SELECT.
   * <p>
   * Used with the <code>COLUMN_UNIQUE_DELETE</code> granularity.
   * 
   * @see org.continuent.sequoia.controller.cache.result.CachingGranularities
   */
  protected transient Hashtable<?, ?>  whereValues;

  /**
   * Creates a new <code>SelectRequest</code> instance. The caller must give
   * an SQL request, without any leading or trailing spaces and beginning with
   * the 'select' keyword (it will not be checked).
   * <p>
   * The request is not parsed but it can be done later by a call to
   * {@link #parse(DatabaseSchema, int, boolean)}.
   * 
   * @param sqlQuery the SQL query
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database ?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   * @see #parse
   */
  public SelectRequest(String sqlQuery, boolean escapeProcessing, int timeout,
      String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator,
        RequestType.SELECT);
  }

  /**
   * @see AbstractRequest#AbstractRequest(java.lang.String, boolean, int,
   *      java.lang.String, int)
   */
  protected SelectRequest(String sqlQuery, boolean escapeProcessing,
      int timeout, String lineSeparator, int type)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator, type);
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
    return false;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersSomething()
   */
  public boolean altersSomething()
  {
    return false;
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
   * <p>
   * The result of the parsing is accessible through the {@link #getSelect()},
   * {@link #getFrom()}and {@link #getWhere()}functions.
   * 
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#parse(org.continuent.sequoia.common.sql.schema.DatabaseSchema,
   *      int, boolean)
   */
  public void parse(DatabaseSchema schema, int granularity,
      boolean isCaseSensitive) throws SQLException
  {
    if ((granularity == ParsingGranularities.NO_PARSING) || (schema == null))
    {
      cacheable = RequestType.UNCACHEABLE;
      isParsed = true;
      return;
    }

    String originalSQL = this.trimCarriageReturnAndTabs();

    while (originalSQL.charAt(0) == '(')
      originalSQL = originalSQL.substring(1);

    String sql = originalSQL.toLowerCase();
    if (!isCaseSensitive)
      originalSQL = sql;

    // Strip 'select'
    sql = sql.substring(6).trim();

    // Look for DISTINCT
    if (sql.startsWith("distinct"))
      sql = sql.substring(8).trim(); // Strip 'distinct '

    // Look for the begining of the FROM clause
    int fromIndex = sql.indexOf("from ");
    if (fromIndex == -1)
      // No FROM keyword found, stop here
      return;

    // Keep SELECT clause for later, we first have to check the
    // tables involved in the FROM clause
    int fshift = originalSQL.length() - sql.length();
    String selectClause = (isCaseSensitive) ? originalSQL.substring(fshift,
        fshift + fromIndex) : sql.substring(0, fromIndex);

    // Get rid of FROM
    sql = sql.substring(fromIndex + 5).trim();

    // Now find the boundaries of the FROM and WHERE clauses
    int whereIndex = 0;
    int parenthesis = 0;
    int lastParenthesisIdx = 0;
    boolean foundWhere = false;
    do
    {
      switch (sql.charAt(whereIndex))
      {
        case '(' :
          parenthesis++;
          break;
        case ')' :
          parenthesis--;
          lastParenthesisIdx = whereIndex;
          break;
        case 'w' :
          if (parenthesis == 0)
            try
            {
              foundWhere = (sql.charAt(whereIndex + 1) == 'h')
                  && (sql.charAt(whereIndex + 2) == 'e')
                  && (sql.charAt(whereIndex + 3) == 'r')
                  && (sql.charAt(whereIndex + 4) == 'e');
            }
            catch (StringIndexOutOfBoundsException ignore)
            {
              foundWhere = false;
            }
          break;
        default :
          break;
      }
      whereIndex++;
    }
    while ((!foundWhere) && (whereIndex < sql.length()));
    if (foundWhere)
      whereIndex--;
    else
      whereIndex = -1;

    // Warning! Here if whereIndex is -1 (no where clause)
    // endWhere is used to find the end of the FROM clause.
    // The variable name can be misleading but it's faster to do it this
    // way.
    int endWhere = sql.indexOf("group by ", lastParenthesisIdx);
    if (endWhere == -1)
    {
      endWhere = sql.indexOf("having ", lastParenthesisIdx);
      if (endWhere == -1)
      {
        endWhere = sql.indexOf("order by ", lastParenthesisIdx);
        if (endWhere == -1)
        {
          endWhere = sql.indexOf("limit ", lastParenthesisIdx);
          if (endWhere == -1)
            endWhere = sql.length();
        }
      }
    }
    int endFrom;
    if (whereIndex == -1)
      endFrom = endWhere;
    else
      endFrom = whereIndex;

    try
    {
      switch (granularity)
      {
        case ParsingGranularities.NO_PARSING :
          return;
        case ParsingGranularities.TABLE :
          int shift = originalSQL.length() - sql.length();
          from = getFromTables(originalSQL.substring(shift, shift + endFrom)
              .trim(), schema, isCaseSensitive);
          break;
        case ParsingGranularities.COLUMN :
        case ParsingGranularities.COLUMN_UNIQUE :
          shift = originalSQL.length() - sql.length();
          from = getFromTables(originalSQL.substring(shift, shift + endFrom)
              .trim(), schema, isCaseSensitive);
          // Find columns selected in the SELECT clause
          select = getSelectedColumns(selectClause, from, isCaseSensitive);
          if (whereIndex > 1)
            // Find columns involved in the WHERE clause (5="WHERE")
            where = getWhereColumns(originalSQL.substring(
                shift + whereIndex + 5, shift + endWhere).trim(), from,
                granularity == ParsingGranularities.COLUMN_UNIQUE,
                isCaseSensitive);
          break;
        default :
          throw new SQLException("Unsupported parsing granularity: '"
              + granularity + "'");
      }
    }
    catch (SQLException e)
    {
      from = null;
      select = null;
      where = null;
      cacheable = RequestType.UNCACHEABLE;
      throw e;
    }

    // Gokul added this
    // I need to have the aliases to determine if any of the OrderBy columns
    // are referenced using their alias

    aliasFrom = from;

    if (from != null)
    {
      // Convert 'from' to an ArrayList of String objects instead of
      // AliasedTables objects
      int size = from.size();
      ArrayList<Serializable> unaliased = new ArrayList<Serializable>(size);
      for (Iterator<Serializable> iter = from.iterator(); iter.hasNext();)
        unaliased
            .add(((AliasedDatabaseTable) iter.next()).getTable().getName());
      from = unaliased;
    }

    isParsed = true;
  }

  /**
   * @see AbstractRequest#cloneParsing(AbstractRequest)
   */
  public void cloneParsing(AbstractRequest request)
  {
    if (!request.isParsed())
      return;
    SelectRequest selectRequest = (SelectRequest) request;
    select = selectRequest.getSelect();
    from = selectRequest.getFrom();
    where = selectRequest.getWhere();
    cacheable = selectRequest.getCacheAbility();
    pkValue = selectRequest.getPkValue();
    isParsed = true;
  }

  /**
   * Extracts the tables from the given <code>FROM</code> clause and retrieves
   * their alias if any.
   * 
   * @param fromClause the <code>FROM</code> clause of the request (without
   *          the <code>FROM</code> keyword)
   * @param schema the <code>DatabaseSchema</code> this request refers to
   * @param isCaseSensitive true if table name parsing is case sensitive
   * @return an <code>ArrayList</code> of <code>AliasedDatabaseTable</code>
   *         objects
   * @exception SQLException if an error occurs
   */
  private Collection<Serializable> getFromTables(String fromClause, DatabaseSchema schema,
      boolean isCaseSensitive) throws SQLException
  {
    ArrayList<Serializable> result = new ArrayList<Serializable>();

    // Search for subselects in from clause
    try
    {
      int subSelect = fromClause.toLowerCase().indexOf("select ");
      while (subSelect != -1)
      {
        int subFromIndex = fromClause.indexOf("from", subSelect + 1) + 5;
        int bracket = subFromIndex;
        int parenthesis = 1;
        do
        {
          char c = fromClause.charAt(bracket);
          switch (c)
          {
            case '(' :
              parenthesis++;
              break;
            case ')' :
              parenthesis--;
              break;
            default :
              break;
          }
          bracket++;
        }
        while ((parenthesis > 0) && (bracket < fromClause.length()));

        SelectRequest subQuery = new SelectRequest(fromClause.substring(
            subSelect, bracket - 1).trim(), this.escapeProcessing, 0,
            getLineSeparator());
        subQuery.parse(schema, ParsingGranularities.TABLE, isCaseSensitive);
        for (Iterator<Serializable> iter = subQuery.getFrom().iterator(); iter.hasNext();)
        {
          result.add(new AliasedDatabaseTable(schema.getTable((String) iter
              .next(), isCaseSensitive), null));
        }

        if (subFromIndex + bracket > fromClause.length())
        {
          if (subSelect > 0)
          {
            fromClause = fromClause.substring(0, subSelect - 1).trim();
            if ((fromClause.length() > 0)
                && (fromClause.charAt(fromClause.length() - 1) == '('))
              fromClause = fromClause.substring(0, fromClause.length() - 1)
                  .trim();
          }
          else
            fromClause = "";
          break; // Nothing more to process
        }
        fromClause = (subSelect > 0 ? fromClause.substring(0, subSelect - 1)
            .trim() : "")
            + fromClause.substring(subFromIndex + bracket).trim();
        subSelect = fromClause.toLowerCase().indexOf("select");
      }
    }
    catch (RuntimeException e)
    {
      // Parsing failed, select everything
      Collection<?> unaliasedTables = schema.getTables().values();
      ArrayList<Serializable> fromAliasedTables = new ArrayList<Serializable>(unaliasedTables.size());
      for (Iterator<?> iter = unaliasedTables.iterator(); iter.hasNext();)
      {
        DatabaseTable t = (DatabaseTable) iter.next();
        fromAliasedTables.add(new AliasedDatabaseTable(t, null));
      }
      return fromAliasedTables;
    }

    // Use a brutal force technique by matching schema table names in the from
    // clause
    Collection<?> tables = schema.getTables().values();
    // Note that we use an iterator here since the tables might be modified
    // concurrently by a write query that alters the database schema. In case
    // of a concurrent modification, iter.next() will fail and we will restart
    // the parsing and this will prevent the disgracious error message reported
    // by BUG #303423.
    for (Iterator<?> iter = tables.iterator(); iter.hasNext();)
    {
      // Check if this table is found in the FROM string
      DatabaseTable t;
      try
      {
        t = (DatabaseTable) iter.next();
      }
      catch (ConcurrentModificationException race)
      {
        iter = tables.iterator();
        continue;
      }
      String tableName = t.getName();
      if (!isCaseSensitive)
        tableName = tableName.toLowerCase();

      // Check that we have a full match and not a partial match
      int index;
      int afterTableNameIndex = 0;
      boolean left;
      boolean right;
      do
      {
        index = fromClause.indexOf(tableName, afterTableNameIndex);
        if (index == -1)
          break;
        afterTableNameIndex = index + tableName.length();
        left = (index == 0)
            || ((index > 0) && ((fromClause.charAt(index - 1) == ' ')
                || (fromClause.charAt(index - 1) == '(')
                || (fromClause.charAt(index - 1) == ',') || (fromClause
                .charAt(index - 1) == getLineSeparator().charAt(
                getLineSeparator().length() - 1))));
        right = (afterTableNameIndex >= fromClause.length())
            || ((afterTableNameIndex < fromClause.length()) && ((fromClause
                .charAt(afterTableNameIndex) == ' ')
                || (fromClause.charAt(afterTableNameIndex) == ',')
                || (fromClause.charAt(afterTableNameIndex) == ')') || (fromClause
                .charAt(afterTableNameIndex) == getLineSeparator().charAt(0))));
      }
      while (!left || !right);
      if (index != -1)
      {
        // Check if the table has an alias
        // Example: SELECT x.price FROM item x
        String alias = null;
        index += tableName.length();
        if ((index < fromClause.length()) && (fromClause.charAt(index) == ' '))
        {
          char c;
          // Skip spaces before alias
          do
          {
            c = fromClause.charAt(index);
            index++;
          }
          while ((index < fromClause.length()) && (c != ' ')
              && (c != getLineSeparator().charAt(0)));
          if (index < fromClause.length())
          {
            int start = index;
            do
            {
              c = fromClause.charAt(index);
              index++;
            }
            while ((index < fromClause.length()) && (c != ' ') && (c != ',')
                && (c != getLineSeparator().charAt(0)));
            alias = fromClause.substring(start, index - 1);
          }
        }
        result.add(new AliasedDatabaseTable(t, alias));
      }
    }

    return result;
  }

  /**
   * Gets all the columns selected in the given <code>SELECT</code> clause.
   * <p>
   * The selected columns or tables must be found in the given
   * <code>ArrayList</code> of <code>AliasedDatabaseTable</code>
   * representing the <code>FROM</code> clause of the same request.
   * 
   * @param selectClause <code>SELECT</code> clause of the request (without
   *          the <code>SELECT</code> keyword)
   * @param aliasedFrom a <code>Collection</code> of
   *          <code>AliasedDatabaseTable</code>
   * @param isCaseSensitive true if column name parsing is case sensitive
   * @return an <code>ArrayList</code> of <code>TableColumn</code>
   */
  private ArrayList<TableColumn> getSelectedColumns(String selectClause,
      Collection<Serializable> aliasedFrom, boolean isCaseSensitive)
  {
    StringTokenizer selectTokens = new StringTokenizer(selectClause, ",");
    ArrayList<TableColumn> result = new ArrayList<TableColumn>();
    StringBuffer unresolvedTokens = null;

    while (selectTokens.hasMoreTokens())
    {
      String token = selectTokens.nextToken().trim();
      // Check if it is a function, e.g., MAX, COUNT, etc.
      if (isSqlFunction(token))
      {
        // token has the following form:
        // max(...)
        // or
        // count(...)
        int leftPar = token.indexOf("(");
        token = token.substring(leftPar + 1, token.length() - 1);
      }
      // Is it using an aliased table name (x.price for example) ?
      String alias = null;
      int aliasIdx = token.indexOf(".");
      if (aliasIdx != -1)
      {
        alias = token.substring(0, aliasIdx);
        token = token.substring(aliasIdx + 1); // Get rid of the '.'
      }

      // Discard any AS clause
      int as = token.indexOf(" as ");
      if (as != -1)
        token = token.substring(0, as).trim();

      // Now token only contains the column name

      // Deal with SELECT * or x.*
      if (token.indexOf("*") != -1)
      {
        if (alias == null)
        {
          // We have to take all colums of all tables of the FROM
          // clause
          for (Iterator<Serializable> iter = aliasedFrom.iterator(); iter.hasNext();)
          {
            DatabaseTable t = ((AliasedDatabaseTable) iter.next()).getTable();
            ArrayList<?> cols = t.getColumns();
            int colSize = cols.size();
            for (int j = 0; j < colSize; j++)
              result.add(new TableColumn(t.getName(), ((DatabaseColumn) cols
                  .get(j)).getName()));
          }
          return result;
        }
        else
        {
          // Add all colums of the table corresponding to the alias
          for (Iterator<Serializable> iter = aliasedFrom.iterator(); iter.hasNext();)
          {
            AliasedDatabaseTable adt = (AliasedDatabaseTable) iter.next();
            // The alias could be the full name of the table
            // instead of a "real" alias
            if (alias.equals(adt.getAlias())
                || alias.equals(adt.getTable().getName()))
            {
              DatabaseTable t = adt.getTable();
              ArrayList<?> cols = t.getColumns();
              int colSize = cols.size();
              for (int j = 0; j < colSize; j++)
                result.add(new TableColumn(t.getName(), ((DatabaseColumn) cols
                    .get(j)).getName()));
              break;
            }
          }
        }
        continue;
      }

      // First, we suppose that it's a simple column name.
      // If it fails, we will consider it later.
      DatabaseColumn col = null;

      if (alias == null)
      {
        for (Iterator<Serializable> iter = aliasedFrom.iterator(); iter.hasNext();)
        {
          DatabaseTable t = ((AliasedDatabaseTable) iter.next()).getTable();
          col = t.getColumn(token, isCaseSensitive);
          if (col != null)
          {
            result.add(new TableColumn(t.getName(), col.getName()));
            break;
          }
        }
      }
      else
      // same with an alias
      {
        for (Iterator<Serializable> iter = aliasedFrom.iterator(); iter.hasNext();)
        {
          AliasedDatabaseTable t = (AliasedDatabaseTable) iter.next();
          // It can be either an alias or the fully qualified name of
          // the table
          if (alias.equals(t.getAlias())
              || alias.equals(t.getTable().getName()))
          {
            col = t.getTable().getColumn(token, isCaseSensitive);
            if (col != null)
            {
              result
                  .add(new TableColumn(t.getTable().getName(), col.getName()));
              break;
            }
          }
        }
      }

      if (col == null)
      {
        if (unresolvedTokens == null)
          unresolvedTokens = new StringBuffer();
        unresolvedTokens.append(token);
        unresolvedTokens.append(" ");
      }
    }

    if (unresolvedTokens != null)
    {
      // Those tokens may be complex expressions, so instead of parsing
      // them, we use a brutal force technique and we try to directly
      // identify every column name of each table.
      DatabaseColumn col;

      String unresolvedTokensString = unresolvedTokens.toString();
      if (!isCaseSensitive)
        unresolvedTokensString = unresolvedTokensString.toLowerCase();

      for (Iterator<Serializable> iter = aliasedFrom.iterator(); iter.hasNext();)
      {
        DatabaseTable t = ((AliasedDatabaseTable) iter.next()).getTable();
        ArrayList<?> cols = t.getColumns();
        int size = cols.size();
        for (int j = 0; j < size; j++)
        {
          col = (DatabaseColumn) cols.get(j);
          String columnName = col.getName();
          if (!isCaseSensitive)
            columnName = columnName.toLowerCase();

          // if pattern found and column not already in result, it's a
          // dependency !
          int matchIdx = unresolvedTokensString.indexOf(columnName);
          if (matchIdx != -1)
            if ((matchIdx == 0)
                || (unresolvedTokens.charAt(matchIdx - 1) == ' ')
                || (unresolvedTokens.charAt(matchIdx - 1) == '(')
                || (unresolvedTokens.charAt(matchIdx - 1) == '.'))
            {
              TableColumn c = new TableColumn(t.getName(), col.getName());
              if (!result.contains(c))
                result.add(c);
            }
        }
      }
    }
    return result;
  }

  /**
   * Checks if the string parameter represents an SQL function, e. g., MAX,
   * COUNT, SUM, etc.
   * 
   * @param str A lower-case string that may represent an SQL function
   * @return boolean <code>true</code> if it is an SQL function and
   *         <code>false</code> otherwise.
   */
  private boolean isSqlFunction(String str)
  {

    if (str != null)
    {
      if (str.startsWith("max(") && str.endsWith(")"))
      {
        funcType = SelectRequest.MAX_FUNCTION;
        return true;
      }
      else if (str.startsWith("count(") && str.endsWith(")"))
      {
        funcType = SelectRequest.COUNT_FUNCTION;
        return true;
      }
      else if (str.startsWith("avg(") && str.endsWith(")"))
      {
        funcType = SelectRequest.AVERAGE_FUNCTION;
        return true;
      }
      else if (str.startsWith("min(") && str.endsWith(")"))
      {
        funcType = SelectRequest.MIN_FUNCTION;
        return true;
      }
      else if (str.startsWith("sum(") && str.endsWith(")"))
      {
        funcType = SelectRequest.SUM_FUNCTION;
        return true;
      }
      else
      {
        funcType = SelectRequest.NO_FUNCTION;
        return false;
      }
    }
    else
      return false;
  }

  /**
   * Gets all the columns involved in the given <code>WHERE</code> clause.
   * <p>
   * The selected columns or tables must be found in the given
   * <code>ArrayList</code> of <code>AliasedDatabaseTable</code>
   * representing the <code>FROM</code> clause of the same request.
   * 
   * @param whereClause <code>WHERE</code> clause of the request (without the
   *          <code>WHERE</code> keyword)
   * @param aliasedFrom a <code>Collection</code> of
   *          <code>AliasedDatabaseTable</code>
   * @param setUniqueCacheable true if we have to check is this select is
   *          <code>UNIQUE</code> or not
   * @param isCaseSensitive true if column name parsing is case sensitive
   * @return an <code>ArrayList</code> of <code>TableColumn</code>
   */
  private ArrayList<TableColumn> getWhereColumns(String whereClause, Collection<Serializable> aliasedFrom,
      boolean setUniqueCacheable, boolean isCaseSensitive)
  {
    ArrayList<TableColumn> result = new ArrayList<TableColumn>(); // TableColumn
    // objects

    if (!isCaseSensitive)
      whereClause = whereClause.toLowerCase();

    // Instead of parsing the clause, we use a brutal force technique
    // and we try to directly identify every column name of each table.
    DatabaseColumn col;
    for (Iterator<Serializable> iter = aliasedFrom.iterator(); iter.hasNext();)
    {
      DatabaseTable t = ((AliasedDatabaseTable) iter.next()).getTable();
      ArrayList<?> cols = t.getColumns();
      int size = cols.size();
      for (int j = 0; j < size; j++)
      {
        col = (DatabaseColumn) cols.get(j);
        // if pattern found and column not already in result, it's a
        // dependency !
        String columnName = col.getName();
        if (!isCaseSensitive)
          columnName = columnName.toLowerCase();

        int matchIdx = whereClause.indexOf(columnName);
        while (matchIdx > 0)
        {
          // Try to check that we got the full pattern and not a
          // sub-pattern
          char beforePattern = whereClause.charAt(matchIdx - 1);
          if (((beforePattern >= 'a') && (beforePattern <= 'z'))
              || ((beforePattern >= 'A') && (beforePattern <= 'Z'))
              || (beforePattern == '_'))
            matchIdx = whereClause.indexOf(columnName, matchIdx + 1);
          else
          {
            char afterPattern;
            try
            {
              afterPattern = whereClause.charAt(matchIdx + columnName.length());
              if (((afterPattern >= 'a') && (afterPattern <= 'z'))
                  || ((afterPattern >= 'A') && (afterPattern <= 'Z'))
                  || (afterPattern == '_'))
              {
                // This is a subset of the full name of another
                // column,
                // let's jump to next mathing pattern
                matchIdx = whereClause.indexOf(columnName, matchIdx + 1);
              }
              else
                break;
            }
            catch (IndexOutOfBoundsException e)
            {
              break;
            }
          }
        }
        if (matchIdx == -1)
          continue;
        result.add(new TableColumn(t.getName(), col.getName()));

        if (setUniqueCacheable)
        { // Check if this request selects a
          // unique row
          if (!col.isUnique())
          { // Column has no unicity constraint,
            // we can select multiple rows
            // with same value, give up.
            setUniqueCacheable = false;
            continue;
          }

          // Check if the column is in the left side of an equality
          // with a
          // constant.
          // e.g.: 'column_name1 = 10' is ok
          // but '5=table_name.column_name2' will fail

          int lookingForEqual = matchIdx + columnName.length();
          boolean searchReverse = false;
          try
          {
            while (whereClause.charAt(lookingForEqual) == ' ')
              lookingForEqual++;
          }
          catch (Exception e)
          {
            searchReverse = true;
          }

          String rightSide;

          if (searchReverse || (whereClause.charAt(lookingForEqual) != '='))
          {
            try
            {
              // try reverse
              StringBuffer sb = new StringBuffer(whereClause.substring(0,
                  lookingForEqual));
              String reverse = sb.reverse().toString();
              reverse = reverse.substring(reverse.indexOf('=') + 1);
              sb = new StringBuffer(reverse);
              // Get back the original values
              sb = sb.reverse();
              rightSide = sb.toString();
            }
            catch (Exception e)
            {
              // No equality, it is not unique cacheable
              setUniqueCacheable = false;
              continue;
            }
          }
          else
          {
            // We found it let's move to next char
            int nextSpace = lookingForEqual + 1;
            try
            {
              while (whereClause.charAt(nextSpace) == ' ')
                nextSpace++;
            }
            catch (Exception e1)
            { // This should not happen
              // unless we get a query like:
              // 'select ... where id= '
              setUniqueCacheable = false;
              continue;
            }

            rightSide = whereClause.substring(nextSpace);
          }
          char firstChar = rightSide.charAt(0);
          if ((firstChar == '\'') || (firstChar == '"')
              || ((firstChar >= '0') && (firstChar <= '9'))
              || (firstChar == '?'))
          { // Ok, the value is either
            // '...' or "..." or starts
            // with a
            // number which is enough for us to consider that it is
            // an
            // acceptable constant.
            pkValue = rightSide;
          }
          else
          {
            setUniqueCacheable = false;
            continue;
          }
        }
      }
    }

    if (setUniqueCacheable && !result.isEmpty())
      cacheable = RequestType.UNIQUE_CACHEABLE;

    return result;
  }

  /**
   * Returns a <code>Collection</code> of <code>AliasedDatabaseTable</code>
   * objects representing the table names found in the <code>FROM</code>
   * clause of this request.
   * 
   * @return a <code>Collection</code> of <code>AliasedDatabaseTable</code>
   */
  public Collection<Serializable> getAliasedFrom()
  {
    return aliasFrom;
  }

  /**
   * Returns a <code>Collection</code> of <code>String</code> objects
   * representing the table names found in the <code>FROM</code> clause of
   * this request.
   * 
   * @return a <code>Collection</code> of <code>String</code>
   */
  public Collection<Serializable> getFrom()
  {
    return from;
  }

  /**
   * Returns an <code>ArrayList</code> of <code>OrderBy</code> objects
   * representing the columns involved in the <code>ORDER BY</code> clause of
   * this request.
   * 
   * @return an <code>ArrayList</code> of <code>OrderBy</code>
   */
  public ArrayList<?> getOrderBy()
  {
    return order;
  }

  /**
   * @return Returns the pkValue.
   */
  public String getPkValue()
  {
    return pkValue;
  }

  /**
   * Returns an <code>ArrayList</code> of <code>DatabaseColumn</code>
   * objects representing the columns selected in the <code>SELECT</code>
   * clause of this request.
   * 
   * @return an <code>ArrayList</code> of <code>TableColumn</code>
   */
  public ArrayList<TableColumn> getSelect()
  {
    return select;
  }

  /**
   * Returns an <code>ArrayList</code> of <code>TableColumn</code> objects
   * representing the columns involved in the <code>WHERE</code> clause of
   * this request.
   * 
   * @return an <code>ArrayList</code> of <code>TableColumn</code>
   */
  public ArrayList<TableColumn> getWhere()
  {
    return where;
  }

  /**
   * Returns an <code>Hashtable</code> of <code>String</code> keys
   * representing unique column names and <code>String</code> values
   * associated with the columns involved in this request.
   * 
   * @return an <code>Hashtable</code> value
   */
  public Hashtable<?, ?> getWhereValues()
  {
    return whereValues;
  }

  /**
   * Returns the mustBroadcast value.
   * 
   * @return Returns the mustBroadcast.
   */
  public boolean isMustBroadcast()
  {
    return mustBroadcast;
  }

  /**
   * @return Returns true if mustBroadcast is true, else returns false
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#needsMacroProcessing()
   */
  public boolean needsMacroProcessing()
  {
    // replace macros only if the select does update(s) 
    return mustBroadcast; 
  }

  /**
   * Does this request returns a ResultSet?
   * 
   * @return true
   */
  public boolean returnsResultSet()
  {
    return true;
  }

  /**
   * Sets the mustBroadcast value.
   * 
   * @param mustBroadcast The mustBroadcast to set.
   */
  public void setMustBroadcast(boolean mustBroadcast)
  {
    this.mustBroadcast = mustBroadcast;
  }

  /**
   * @param pkValue The pkValue to set.
   */
  public void setPkValue(String pkValue)
  {
    this.pkValue = pkValue;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getParsingResultsAsString()
   */
  public String getParsingResultsAsString()
  {
    StringBuffer sb = new StringBuffer(super.getParsingResultsAsString());
    if (select != null && select.size() > 0)
    {
      sb.append(Translate.get("request.select.selects"));
      for (int i = 0; i < select.size(); i++)
      {
        sb.append(Translate.get("request.select.select", select.get(i)));
      }
    }
    if (from != null && from.size() > 0)
    {
      sb.append(Translate.get("request.select.froms"));
      for (int i = 0; i < from.size(); i++)
      {
        sb.append(Translate.get("request.select.from", from.toArray()[i]));
      }
    }
    if (aliasFrom != null && aliasFrom.size() > 0)
    {
      sb.append(Translate.get("request.select.alias.froms"));
      for (int i = 0; i < aliasFrom.size(); i++)
      {
        sb.append(Translate.get("request.select.alias.from",
            ((AliasedDatabaseTable) aliasFrom.toArray()[i]).getAlias()));
      }
    }
    if (where != null && where.size() > 0)
    {
      sb.append(Translate.get("request.select.wheres"));
      for (int i = 0; i < where.size(); i++)
      {
        sb.append(Translate.get("request.select.where", where.toArray()[i]));
      }
    }
    if (order != null && order.size() > 0)
    {
      sb.append(Translate.get("request.select.orders"));
      for (int i = 0; i < order.size(); i++)
      {
        sb.append(Translate.get("request.select.order", where.toArray()[i]));
      }
    }
    return sb.toString();
  }

  /**
   * Displays some debugging information about this request.
   */
  public void debug()
  {
    super.debug();
    if (select != null)
    {
      System.out.println("Selected columns:");
      for (int i = 0; i < select.size(); i++)
        System.out
            .println("  " + ((TableColumn) select.get(i)).getColumnName());
    }
    else
      System.out.println("No information on selected columns");

    if (select != null)
    {
      System.out.println("");
      System.out.println("From tables:");
      for (Iterator<Serializable> iter = from.iterator(); iter.hasNext();)
        for (int i = 0; i < from.size(); i++)
          System.out.println("  " + iter.next());
    }
    else
      System.out.println("No information on from tables");

    System.out.println("");
    System.out.println("Where columns:");
    if (where == null)
      System.out.println("  No Where clause");
    else
      for (int i = 0; i < where.size(); i++)
        System.out.print("  " + ((TableColumn) where.get(i)).getColumnName());

    System.out.println("");
    System.out.println("PK value: " + pkValue);
  }

}