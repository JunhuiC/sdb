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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.controller.cache.result.schema;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;

/**
 * A <code>CacheDatabaseSchema</code> describes all the tables and columns of
 * a database and its associated cache entries.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class CacheDatabaseSchema
{
  /** Database tables. */
  private ArrayList<CacheDatabaseTable> tables;

  /**
   * Creates a new <code>CacheDatabaseSchema</code> instance by cloning an
   * existing <code>DatabaseSchema</code>.
   * 
   * @param dbs the <code>DatabaseSchema</code> to clone
   */
  public CacheDatabaseSchema(DatabaseSchema dbs)
  {
    if (dbs == null)
    {
      tables = new ArrayList<CacheDatabaseTable>();
      return;
    }

    // Clone the tables
    Collection<?> origTables = dbs.getTables().values();
    int size = origTables.size();
    tables = new ArrayList<CacheDatabaseTable>(size);
    for (Iterator<?> iter = origTables.iterator(); iter.hasNext();)
      for (int i = 0; i < size; i++)
        tables.add(new CacheDatabaseTable((DatabaseTable) iter.next()));
  }

  /**
   * Adds a <code>CacheDatabaseTable</code> describing a table of the
   * database.
   * 
   * @param table the table to add
   */
  public void addTable(CacheDatabaseTable table)
  {
    tables.add(table);
  }

  /**
   * Removes a <code>CacheDatabaseTable</code> describing a table of the
   * database.
   * 
   * @param table the table to remove
   */
  public void removeTable(CacheDatabaseTable table)
  {
    tables.remove(table);
  }

  /**
   * Merge the given schema with the current one. All missing tables or columns
   * are added if no conflict is detected. An exception is thrown if the given
   * schema definition conflicts with the current one.
   * 
   * @param databaseSchema the schema to merge
   * @throws SQLException if the schemas conflict
   */
  public void mergeSchema(CacheDatabaseSchema databaseSchema)
      throws SQLException
  {
    if (databaseSchema == null)
      return;

    ArrayList<CacheDatabaseTable> otherTables = databaseSchema.getTables();
    if (otherTables == null)
      return;

    int size = otherTables.size();
    for (int i = 0; i < size; i++)
    {
      CacheDatabaseTable t = (CacheDatabaseTable) otherTables.get(i);
      CacheDatabaseTable original = getTable(t.getName());
      if (original == null)
        addTable(t);
      else
        original.mergeColumns(t);
    }
  }

  /**
   * Returns an <code>ArrayList</code> of <code>CacheDatabaseTable</code>
   * objects describing the database.
   * 
   * @return an <code>ArrayList</code> of <code>CacheDatabaseTable</code>
   */
  public ArrayList<CacheDatabaseTable> getTables()
  {
    return tables;
  }

  /**
   * Returns the <code>CacheDatabaseTable</code> object matching the given
   * table name or <code>null</code> if not found.
   * 
   * @param tableName the table name to look for
   * @return a <code>CacheDatabaseTable</code> value or null
   */
  public CacheDatabaseTable getTable(String tableName)
  {
    if (tableName == null)
      return null;

    int size = tables.size();
    for (int i = 0; i < size; i++)
    {
      CacheDatabaseTable t = (CacheDatabaseTable) tables.get(i);
      if (t.getName().compareTo(tableName) == 0)
        return t;
    }
    return null;
  }

  /**
   * Returns <code>true</code> if the given <code>TableName</code> is found
   * in this schema.
   * 
   * @param tableName the name of the table you are looking for
   * @return <code>true</code> if the table has been found
   */
  public boolean hasTable(String tableName)
  {
    int size = tables.size();
    for (int i = 0; i < size; i++)
    {
      CacheDatabaseTable t = (CacheDatabaseTable) tables.get(i);
      if (tableName.equals(t.getName()))
        return true;
    }
    return false;
  }

  /**
   * Two <code>CacheDatabaseSchema</code> are equals if they have the same
   * tables.
   * 
   * @param other the object to compare with
   * @return true if the 2 objects are the same.
   */
  public boolean equals(Object other)
  {
    if (!(other instanceof CacheDatabaseSchema))
      return false;

    if (tables == null)
      return ((CacheDatabaseSchema) other).getTables() == null;
    else
      return tables.equals(((CacheDatabaseSchema) other).getTables());
  }

  /**
   * Returns information about the database schema.
   * 
   * @param longFormat <code>true</code> for a long format, false for a short
   *          summary
   * @return a <code>String</code> value
   */
  public String getInformation(boolean longFormat)
  {
    String result = "";
    int size = tables.size();
    for (int i = 0; i < size; i++)
    {
      CacheDatabaseTable t = (CacheDatabaseTable) tables.get(i);
      result += t.getInformation(longFormat) + "\n";
    }
    return result;
  }

}
