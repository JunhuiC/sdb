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
 * Contributor(s): Sara Bouchenak.
 */

package org.continuent.sequoia.controller.cache.result.schema;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.requests.RequestType;

/**
 * A <code>CacheDatabaseTable</code> represents a database table and its
 * associated cache entries. It has an array of <code>CacheDatabaseColumn</code>
 * objects.
 * <p>
 * Keep it mind that <code>ArrayList</code> and <code>HashMap</code> are not
 * synchronized...
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Sara.Bouchenak@epfl.ch">Sara Bouchenak </a>
 * @version 1.0
 */
public class CacheDatabaseTable
{
  private String    name;
  private ArrayList<CacheDatabaseColumn> columns;
  private ArrayList<AbstractResultCacheEntry> cacheEntries;  // Cache entries depending on this table
  private HashMap<String, AbstractResultCacheEntry>   pkCacheEntries; // Cache entries corresponding to a pk value

  /**
   * Creates a new <code>CacheDatabaseTable</code> instance.
   * 
   * @param databaseTable the database table to cache
   */
  public CacheDatabaseTable(DatabaseTable databaseTable)
  {
    // Clone the name and the columns
    name = databaseTable.getName();
    ArrayList<?> origColumns = databaseTable.getColumns();
    int size = origColumns.size();
    columns = new ArrayList<CacheDatabaseColumn>(size);
    for (int i = 0; i < size; i++)
      columns.add(new CacheDatabaseColumn(((DatabaseColumn) origColumns.get(i))
          .getName()));

    // Create an empty cache
    cacheEntries = new ArrayList<AbstractResultCacheEntry>();
    pkCacheEntries = new HashMap<String, AbstractResultCacheEntry>();
  }

  /**
   * Gets the name of the table.
   * 
   * @return the table name
   */
  public String getName()
  {
    return name;
  }

  /**
   * Adds a <code>CacheDatabaseColumn</code> object to this table.
   * <p>
   * Warning! The underlying <code>ArrayList</code> is not synchronized.
   * 
   * @param column a <code>CacheDatabaseColumn</code> value
   */
  public void addColumn(CacheDatabaseColumn column)
  {
    columns.add(column);
  }

  /**
   * Merge the given table's columns with the current table. All missing columns
   * are added if no conflict is detected. An exception is thrown if the given
   * table columns conflicts with the current one.
   * 
   * @param t the table to merge
   * @throws SQLException if the schemas conflict
   */
  public void mergeColumns(CacheDatabaseTable t) throws SQLException
  {
    if (t == null)
      return;

    ArrayList<CacheDatabaseColumn> otherColumns = t.getColumns();
    if (otherColumns == null)
      return;

    int size = otherColumns.size();
    for (int i = 0; i < size; i++)
    {
      CacheDatabaseColumn c = (CacheDatabaseColumn) otherColumns.get(i);
      CacheDatabaseColumn original = getColumn(c.getName());
      if (original == null)
        addColumn(c);
      else
      {
        if (!original.equals(c))
          throw new SQLException("Column " + c.getName()
              + " definition mismatch.");
      }
    }
  }

  /**
   * Returns a list of <code>CacheDatabaseColumn</code> objects describing the
   * columns of this table.
   * <p>
   * Warning! The underlying <code>ArrayList</code> is not synchronized.
   * 
   * @return an <code>ArrayList</code> of <code>CacheDatabaseColumn</code>
   */
  public ArrayList<CacheDatabaseColumn> getColumns()
  {
    return columns;
  }

  /**
   * Returns the <code>CacheDatabaseColumn</code> object matching the given
   * column name or <code>null</code> if not found.
   * 
   * @param columnName column name to look for
   * @return a <code>CacheDatabaseColumn</code> value or <code>null</code>
   */
  public CacheDatabaseColumn getColumn(String columnName)
  {
    for (Iterator<CacheDatabaseColumn> i = columns.iterator(); i.hasNext();)
    {
      CacheDatabaseColumn c = (CacheDatabaseColumn) i.next();
      if (columnName.compareToIgnoreCase(c.getName()) == 0)
        return c;
    }
    return null;
  }

  /**
   * Two <code>CacheDatabaseColumn</code> are equals if they have the same
   * name and the same columns.
   * 
   * @param other the object to compare with
   * @return true if the objects are the same
   */
  public boolean equals(Object other)
  {
    if (!(other instanceof CacheDatabaseTable))
      return false;

    CacheDatabaseTable t = (CacheDatabaseTable) other;
    return t.getName().equals(name) && t.getColumns().equals(columns);
  }

  /**
   * Adds an <code>AbstractResultCacheEntry</code> object whose consistency
   * depends on this table.
   * 
   * @param ce an <code>AbstractResultCacheEntry</code> value
   */
  public synchronized void addCacheEntry(AbstractResultCacheEntry ce)
  {
    cacheEntries.add(ce);
  }

  /**
   * Adds an <code>AbstractResultCacheEntry</code> object associated to a pk
   * entry.
   * 
   * @param pk the pk entry
   * @param ce an <code>AbstractResultCacheEntry</code> value
   */
  public void addPkCacheEntry(String pk, AbstractResultCacheEntry ce)
  {
    synchronized (pkCacheEntries)
    {
      pkCacheEntries.put(pk, ce);
    }
  }

  /**
   * Gets a <code>CacheEntry</code> object associated to a pk entry.
   * 
   * @param pk the pk entry
   * @return the corresponding cache entry if any or null if nothing is found
   */
  public AbstractResultCacheEntry getPkResultCacheEntry(String pk)
  {
    if (pk == null)
      return null;
    synchronized (pkCacheEntries)
    {
      return (AbstractResultCacheEntry) pkCacheEntries.get(pk);
    }
  }

  /**
   * Remove a <code>CacheEntry</code> object associated to a pk entry.
   * 
   * @param pk the pk entry
   */
  public void removePkResultCacheEntry(Object pk)
  {
    synchronized (pkCacheEntries)
    {
      AbstractResultCacheEntry rce = (AbstractResultCacheEntry) pkCacheEntries
          .remove(pk);
      rce.invalidate();
    }
  }

  /**
   * Invalidates all cache entries of every column of this table. This does also
   * affect the entries based on pk values.
   */
  public void invalidateAll()
  {
    synchronized (this)
    {
      for (Iterator<AbstractResultCacheEntry> i = cacheEntries.iterator(); i.hasNext();)
        ((AbstractResultCacheEntry) i.next()).invalidate();
      cacheEntries.clear();

      for (int i = 0; i < columns.size(); i++)
        ((CacheDatabaseColumn) columns.get(i)).invalidateAll();
    }
    synchronized (pkCacheEntries)
    { // All pk cache entries have been invalidated as a side effect by the
      // above loop.
      pkCacheEntries.clear();
    }
  }

  /**
   * Invalidates all cache entries of every column of this table. This does not
   * affect the entries based on pk values.
   */
  public synchronized void invalidateAllExceptPk()
  {
    for (Iterator<AbstractResultCacheEntry> i = cacheEntries.iterator(); i.hasNext();)
    {
      AbstractResultCacheEntry qce = (AbstractResultCacheEntry) i.next();
      if (qce.getRequest().getCacheAbility() != RequestType.UNIQUE_CACHEABLE)
        qce.invalidate();
    }
    cacheEntries.clear();
  }

  /**
   * Returns information about the database table and its columns.
   * 
   * @param longFormat true for a long format, false for a short summary
   * @return String
   */
  public String getInformation(boolean longFormat)
  {
    String result = "Table " + name + ": ";
    int size = columns.size();
    for (int i = 0; i < size; i++)
    {
      CacheDatabaseColumn c = (CacheDatabaseColumn) columns.get(i);
      if (longFormat)
        result += "\n";
      result += c.getInformation();
      if (!longFormat && (i < size - 1))
        result += ",";
    }
    return result;
  }
}