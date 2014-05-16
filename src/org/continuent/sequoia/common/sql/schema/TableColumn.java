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
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.common.sql.schema;

/**
 * A <code>TableColumn</code> is used to carry parsing information and
 * contains a database table name and one of its column.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier</a>
 * @version 1.0
 */
public class TableColumn
{
  /** The table name. */
  private String tableName;

  /** The column name. */
  private String columnName;

  /**
   * Creates a new <code>TableColumn</code>.
   * 
   * @param tableName the table name
   * @param columnName the column name
   */
  public TableColumn(String tableName, String columnName)
  {
    if (tableName == null)
      throw new IllegalArgumentException("Illegal null table name in TableColumn constructor");

    if (columnName == null)
      throw new IllegalArgumentException("Illegal null column name in TableColumn constructor");

    this.tableName = tableName;
    this.columnName = columnName;
  }

  /**
   * Returns the column name.
   * 
   * @return the column name.
   */
  public String getColumnName()
  {
    return columnName;
  }

  /**
   * Returns the table name.
   * 
   * @return the table name.
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Sets the column name.
   * 
   * @param columnName the column to set
   */
  public void setColumnName(String columnName)
  {
    this.columnName = columnName;
  }

  /**
   * Sets the table name.
   * 
   * @param tableName the table to set
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * Two <code>TableColumn</code> objects are considered equal if they have
   * the same name and belong to the same table.
   * 
   * @param other the object to compare with
   * @return true if the 2 objects are the same
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof TableColumn))
      return false;

    TableColumn c = (TableColumn) other;
    return columnName.equals(c.getColumnName())
      && tableName.equals(c.getTableName());
  }
}
