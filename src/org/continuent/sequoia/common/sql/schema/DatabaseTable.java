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
 * Contributor(s): Mathieu Peltier, Sara Bouchenak.
 */

package org.continuent.sequoia.common.sql.schema;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.continuent.sequoia.common.locks.TransactionLogicalLock;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * A <code>DatabaseTable</code> represents a database table ! It is just an
 * array of <code>TableColumns</code> objects.
 * <p>
 * Keep it mind that <code>ArrayList</code> is not synchronized...
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier</a>
 * @author <a href="mailto:Sara.Bouchenak@epfl.ch">Sara Bouchenak</a>
 * @version 1.0
 */
public class DatabaseTable implements Serializable
{
  private static final long                serialVersionUID = 7138810420058450235L;

  /** Schema name this table belongs to */
  private String                           schema;

  /** Database table name. */
  private String                           name;

  /** <code>ArrayList</code> of <code>DatabaseColumn</code>. */
  private ArrayList                        columns;

  /** Lock for this table */
  private transient TransactionLogicalLock lock             = new TransactionLogicalLock();

  /**
   * List of tables that must be locked when this table is updated. This is
   * usually the list of tables that have a foreign key referencing this table.<br>
   * <code>ArrayList</code> of <code>String</code> (table names)
   */
  private ArrayList                        dependingTables;

  /**
   * containsAutoIncrementedKey indicates whether the table contains an
   * auto-incremented key or not. This information should be gathered from the
   * database schema.
   */
  private boolean                          containsAutoIncrementedKey;

  /**
   * Creates a new <code>DatabaseTable</code> instance.
   * 
   * @param name table name
   */
  public DatabaseTable(String name)
  {
    this(name, new ArrayList());
  }

  /**
   * Creates a new <code>DatabaseTable</code> instance.
   * 
   * @param name table name
   * @param nbOfColumns number of columns
   */
  public DatabaseTable(String name, int nbOfColumns)
  {
    this(name, new ArrayList(nbOfColumns));
  }

  /**
   * Creates a new <code>DatabaseTable</code> instance. New table shares
   * (only) its name and columns with the table passed as parameter.
   * 
   * @param dt database table
   */
  public DatabaseTable(DatabaseTable dt)
  {
    this(dt.getName(), dt.getColumns());
    if (dt.getDependingTables() != null)
      for (Iterator i = dt.getDependingTables().iterator(); i.hasNext();)
        addDependingTable((String) i.next());
  }

  /**
   * Creates a new <code>DatabaseTable</code> instance.
   * 
   * @param name table name
   * @param columns columns list
   */
  private DatabaseTable(String name, ArrayList columns)
  {
    if (name == null)
      throw new IllegalArgumentException(
          "Illegal null database table name in DatabaseTable constructor");

    this.name = name;
    this.columns = columns;
    this.containsAutoIncrementedKey = false;
  }

  /**
   * Add a depending table to this table. A depending table is locked when the
   * current table is updated. A depending table is usually a table that has a
   * foreign key referencing the current table.<br>
   * This method takes care of duplicates and a table name is only inserted
   * once.
   * 
   * @param tableName the depending table to add
   */
  public synchronized void addDependingTable(String tableName)
  {
    if (dependingTables == null)
      dependingTables = new ArrayList();
    if (!dependingTables.contains(tableName))
      dependingTables.add(tableName);
  }

  /**
   * Adds a <code>DatabaseColumn</code> object to this table.
   * <p>
   * Warning! The underlying <code>ArrayList</code> is not synchronized.
   * 
   * @param column a <code>DatabaseColumn</code> value
   */
  public void addColumn(DatabaseColumn column)
  {
    columns.add(column);
  }

  /**
   * Returns a list of <code>DatabaseColumn</code> objects describing the
   * columns of this table.
   * <p>
   * Warning! The underlying <code>ArrayList</code> is not synchronized.
   * 
   * @return an <code>ArrayList</code> of <code>DatabaseColumn</code>
   */
  public ArrayList getColumns()
  {
    return columns;
  }

  /**
   * Returns the <code>DatabaseColumn</code> object matching the given column
   * name or <code>null</code> if not found (the case is ignored).
   * 
   * @param columnName column name to look for
   * @return a <code>DatabaseColumn</code> value or <code>null</code>
   */
  public DatabaseColumn getColumn(String columnName)
  {
    DatabaseColumn c;
    for (Iterator i = columns.iterator(); i.hasNext();)
    {
      c = (DatabaseColumn) i.next();
      if (columnName.equalsIgnoreCase(c.getName()))
        return c;

    }
    return null;
  }

  /**
   * Returns the <code>DatabaseColumn</code> object matching the given column
   * name or <code>null</code> if not found (the case can be enforced).
   * 
   * @param columnName column name to look for
   * @param isCaseSensitive true if name matching must be case sensitive
   * @return a <code>DatabaseColumn</code> value or <code>null</code>
   */
  public DatabaseColumn getColumn(String columnName, boolean isCaseSensitive)
  {
    if (!isCaseSensitive)
      return getColumn(columnName);

    DatabaseColumn c;
    for (Iterator i = columns.iterator(); i.hasNext();)
    {
      c = (DatabaseColumn) i.next();
      if (columnName.equals(c.getName()))
        return c;

    }
    return null;
  }

  /**
   * Returns the dependingTables value.
   * 
   * @return Returns the dependingTables.
   */
  public ArrayList getDependingTables()
  {
    return dependingTables;
  }

  /**
   * Returns the lock for this table.
   * 
   * @return a <code>TransactionLogicalLock</code> instance
   */
  public TransactionLogicalLock getLock()
  {
    return lock;
  }

  /**
   * Retain locks held by a transaction when the schema is reloaded.
   * 
   * @param oldTable the previous version of this table
   */
  void setLock(DatabaseTable oldTable)
  {
    lock = oldTable.lock;
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
   * Returns the schema value.
   * 
   * @return Returns the schema.
   */
  public String getSchema()
  {
    return schema;
  }

  /**
   * Sets the schema value.
   * 
   * @param schema The schema to set.
   */
  public void setSchema(String schema)
  {
    this.schema = schema;
  }

  /**
   * Returns a list of <code>DatabaseColumn</code> objects representing the
   * unique columns of this table.
   * <p>
   * Warning! The underlying <code>ArrayList</code> is not synchronized.
   * 
   * @return an <code>ArrayList</code> of <code>DatabaseColumn</code>
   *         objects
   */
  public ArrayList getUniqueColumns()
  {
    ArrayList cols = new ArrayList();
    Iterator i;
    DatabaseColumn c;

    for (i = columns.iterator(); i.hasNext();)
    {
      c = (DatabaseColumn) i.next();
      if (c.isUnique())
        cols.add(c);
    }
    return cols;
  }

  /**
   * Merges this table with the given table. Both columns and depending tables
   * are merged.
   * <p>
   * All missing columns are added if no conflict is detected. An exception is
   * thrown if the given table columns conflicts with this one.
   * 
   * @param table the table to merge
   * @throws SQLException if the schemas conflict
   */
  public void merge(DatabaseTable table) throws SQLException
  {
    if (table == null)
      return;

    if (dependingTables == null)
      dependingTables = table.getDependingTables();
    else
    {
      ArrayList otherDependingTables = table.getDependingTables();
      if (otherDependingTables != null)
        for (Iterator iter = otherDependingTables.iterator(); iter.hasNext();)
          addDependingTable((String) iter.next());
    }

    ArrayList otherColumns = table.getColumns();
    if (otherColumns == null)
      return;

    DatabaseColumn c, original;
    int size = otherColumns.size();
    for (int i = 0; i < size; i++)
    {
      c = (DatabaseColumn) otherColumns.get(i);
      original = getColumn(c.getName());
      if (original == null)
        addColumn(c);
      else
      {
        if (!original.equalsIgnoreType(c))
          throw new SQLException("Unable to merge table [" + table.getName()
              + "]: column '" + c.getName() + "' definition mismatch");
      }
    }
  }

  /**
   * Drops a <code>DatabaseColumn</code> object from this table.
   * <p>
   * Warning! The underlying <code>ArrayList</code> is not synchronized.
   * 
   * @param columnName a <code>String</code> that maps to a
   *          <code>DatabaseColumn</code> value
   */
  public void removeColumn(String columnName)
  {
    columns.remove(getColumn(columnName));
  }

  /**
   * Drops a <code>DatabaseColumn</code> object from this table.
   * <p>
   * Warning! The underlying <code>ArrayList</code> is not synchronized.
   * 
   * @param column a <code>DatabaseColumn</code> value
   */
  public void removeColumn(DatabaseColumn column)
  {
    columns.remove(column);
  }

  /**
   * Updates this table with the given table's columns. All missing columns are
   * added and if the given table columns conflicts with this one, the current
   * column definition is overriden with the provided one. Note that existing
   * locks (if any) are preserved.
   * <p>
   * Warning! Data structures of the given table are not cloned.
   * 
   * @param table the table to use to update the current one
   */
  public void updateColumns(DatabaseTable table)
  {
    if (table == null)
      return;

    ArrayList otherColumns = table.getColumns();

    // Remove columns that do not exist anymore in new schema
    for (Iterator iter = columns.iterator(); iter.hasNext();)
    {
      DatabaseColumn c = (DatabaseColumn) iter.next();
      if (!otherColumns.contains(c))
        iter.remove();
    }

    // Add missing columns and update existing ones
    int size = otherColumns.size();
    for (int i = 0; i < size; i++)
    {
      DatabaseColumn c = (DatabaseColumn) otherColumns.get(i);
      DatabaseColumn originalColumn = getColumn(c.getName());
      if (originalColumn == null)
        addColumn(c);
      else
      {
        originalColumn.setType(c.getType());
        originalColumn.setIsUnique(c.isUnique());
      }
    }
  }

  /**
   * Two <code>DatabaseTable</code> are considered equal if they have the same
   * name and the same columns.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the tables are equal
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof DatabaseTable))
      return false;

    DatabaseTable t = (DatabaseTable) other;

    // Compare name
    if (!name.equals(t.getName()))
      return false;

    // Compare schema
    if (t.getSchema() == null)
    {
      if (schema != null)
        return false;
    }
    else
    {
      if (!t.getSchema().equals(schema))
        return false;
    }
    // Compare columns
    if (t.getColumns() == null)
      return columns == null;
    else
      return t.getColumns().equals(columns);
  }

  /**
   * This function is the same as equals but ignores the column type.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the table are equal ignoring the columns
   *         type
   * @see #equals(Object)
   */
  public boolean equalsIgnoreType(Object other)
  {
    if ((other == null) || !(other instanceof DatabaseTable))
      return false;

    DatabaseTable t = (DatabaseTable) other;
    // Compare name
    if (!name.equals(t.getName()))
      return false;

    // Compare schema
    if (t.getSchema() == null)
    {
      if (schema != null)
        return false;
    }
    else
    {
      if (!t.getSchema().equals(schema))
        return false;
    }

    DatabaseColumn c1, c2;
    Iterator iter = columns.iterator();
    while (iter.hasNext())
    {
      c1 = (DatabaseColumn) iter.next();
      c2 = t.getColumn(c1.getName());

      if (!c1.equalsIgnoreType(c2))
        return false; // Not compatible
    }
    return true;
  }

  /**
   * Get xml information about this table.
   * 
   * @return xml formatted information on this database table.
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_DatabaseTable + " "
        + DatabasesXmlTags.ATT_tableName + "=\"" + name + "\" "
        + DatabasesXmlTags.ATT_nbOfColumns + "=\"" + columns.size() + "\">");
    for (int i = 0; i < columns.size(); i++)
      info.append(((DatabaseColumn) columns.get(i)).getXml());
    info.append("</" + DatabasesXmlTags.ELT_DatabaseTable + ">");
    return info.toString();
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return name + "(" + columns + ")";
  }

  /**
   * setAutoIncrementedKey sets the containsAutoIncrementedKey to true (by
   * default, it is false). This implies that the table definition contains an
   * auto-incremented column.
   */
  public void setAutoIncrementedKey()
  {
    this.containsAutoIncrementedKey = true;
  }

  /**
   * getAutoIncrementedKey indicates whether a table contains an
   * auto-incremented column or not
   * 
   * @return true if the database schema contains an auto-incremented column
   */
  public boolean containsAutoIncrementedKey()
  {
    return containsAutoIncrementedKey;
  }
}
