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
 * Contributor(s): _________________________.
 */

package org.continuent.sequoia.controller.scheduler.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;

/**
 * A <code>SchedulerDatabaseSchema</code> describes all the tables and columns
 * of a database and its associated cache entries.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class SchedulerDatabaseSchema
{
  /** <code>ArrayList</code> of <code>SchedulerDatabaseTable</code>. */
  private ArrayList                tables;

  private TransactionExclusiveLock lock = new TransactionExclusiveLock();

  /**
   * Creates a new <code>SchedulerDatabaseSchema</code> instance by cloning an
   * existing <code>DatabaseSchema</code>.
   * 
   * @param schema the database schema to clone
   */
  public SchedulerDatabaseSchema(DatabaseSchema schema)
  {
    if (schema == null)
    {
      tables = new ArrayList();
      return;
    }

    // Clone the tables
    Collection origTables = schema.getTables().values();
    int size = origTables.size();
    tables = new ArrayList(size);
    for (Iterator iter = origTables.iterator(); iter.hasNext();)
      tables.add(new SchedulerDatabaseTable((DatabaseTable) iter.next()));
  }

  /**
   * Adds a <code>SchedulerDatabaseTable</code> describing a table of the
   * database.
   * 
   * @param table the table to add
   */
  public void addTable(SchedulerDatabaseTable table)
  {
    tables.add(table);
  }

  /**
   * Removes a <code>SchedulerDatabaseTable</code> describing a table of the
   * database.
   * 
   * @param table the table to remove
   */
  public void removeTable(SchedulerDatabaseTable table)
  {
    tables.remove(table);
  }

  /**
   * Merge the given schema with the current one. All missing tables are added
   * if no conflict is detected. An exception is thrown if the given schema
   * definition conflicts with the current one.
   * 
   * @param databaseSchema the schema to merge
   */
  public void mergeSchema(SchedulerDatabaseSchema databaseSchema)
  {
    if (databaseSchema == null)
      return;

    ArrayList otherTables = databaseSchema.getTables();
    if (otherTables == null)
      return;

    int size = otherTables.size();
    for (int i = 0; i < size; i++)
    {
      SchedulerDatabaseTable t = (SchedulerDatabaseTable) otherTables.get(i);
      SchedulerDatabaseTable original = getTable(t.getName());
      if (original == null)
        addTable(t);
    }
  }

  /**
   * Returns an <code>ArrayList</code> of <code>SchedulerDatabaseTable</code>
   * objects describing the database.
   * 
   * @return an <code>ArrayList</code> of <code>SchedulerDatabaseTable</code>
   */
  public ArrayList getTables()
  {
    return tables;
  }

  /**
   * Returns the <code>SchedulerDatabaseTable</code> object matching the given
   * table name or <code>null</code> if not found. Matching is case
   * insensitive.
   * 
   * @param tableName the table name to look for
   * @return a <code>SchedulerDatabaseTable</code> value or null
   */
  public SchedulerDatabaseTable getTable(String tableName)
  {
    SchedulerDatabaseTable t;
    int size = tables.size();
    for (int i = 0; i < size; i++)
    {
      t = (SchedulerDatabaseTable) tables.get(i);
      if (tableName.equalsIgnoreCase(t.getName()))
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
      SchedulerDatabaseTable t = (SchedulerDatabaseTable) tables.get(i);
      if (tableName.equals(t.getName()))
        return true;
    }
    return false;
  }

  /**
   * Returns the lock for this table.
   * 
   * @return a <code>TransactionExclusiveLock</code> instance
   * @see TransactionExclusiveLock
   */
  public TransactionExclusiveLock getLock()
  {
    return lock;
  }

  /**
   * Two <code>SchedulerDatabaseSchema</code> are equals if they have the same
   * tables.
   * 
   * @param other the object to compare with
   * @return true if the objects are the same
   */
  public boolean equals(Object other)
  {
    if (!(other instanceof SchedulerDatabaseSchema))
      return false;

    if (tables == null)
      return ((SchedulerDatabaseSchema) other).getTables() == null;
    else
      return tables.equals(((SchedulerDatabaseSchema) other).getTables());
  }

  /**
   * Returns information about the database schema.
   * 
   * @param longFormat <code>true</code> for a <code>long</code> format,
   *          <code>false</code> for a short summary
   * @return a <code>String</code> value
   */
  public String getInformation(boolean longFormat)
  {
    StringBuffer result = new StringBuffer();
    SchedulerDatabaseTable t;
    int size = tables.size();
    for (int i = 0; i < size; i++)
    {
      t = (SchedulerDatabaseTable) tables.get(i);
      result.append(t.getInformation(longFormat));
      result.append(System.getProperty("line.separator"));
    }
    return result.toString();
  }
}