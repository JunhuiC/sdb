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
 * Contributor(s): __________________.
 */

package org.continuent.sequoia.driver;

import java.sql.SQLException;

/**
 * ResultSet metadata provided for pretty printing of the ResultSet by a
 * console.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ResultSetMetaData implements java.sql.ResultSetMetaData
{
  private DriverResultSet rs;

  /**
   * Constructs a ResultSetMetaData from a Sequoia ResultSet.
   * 
   * @param rs the ResultSet
   */
  public ResultSetMetaData(DriverResultSet rs)
  {
    this.rs = rs;
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnCount()
   */
  public int getColumnCount() throws SQLException
  {
    return rs.nbOfColumns;
  }

  /**
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */
  public boolean isAutoIncrement(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isAutoIncrement();
  }

  /**
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */
  public boolean isCaseSensitive(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isCaseSensitive();
  }

  /**
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */
  public boolean isSearchable(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isSearchable();
  }

  /**
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */
  public boolean isCurrency(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isCurrency();
  }

  /**
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */
  public int isNullable(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isNullable();
  }

  /**
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */
  public boolean isSigned(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isSigned();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */
  public int getColumnDisplaySize(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getColumnDisplaySize();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnLabel(int)
   */
  public String getColumnLabel(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getFieldLabel();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnName(int)
   */
  public String getColumnName(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getFieldName();
  }

  /**
   * @see java.sql.ResultSetMetaData#getSchemaName(int)
   */
  public String getSchemaName(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getFullName();
  }

  /**
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */
  public int getPrecision(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getPrecision();
  }

  /**
   * @see java.sql.ResultSetMetaData#getScale(int)
   */
  public int getScale(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getScale();
  }

  /**
   * @see java.sql.ResultSetMetaData#getTableName(int)
   */
  public String getTableName(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getTableName();
  }

  /**
   * @see java.sql.ResultSetMetaData#getCatalogName(int)
   */
  public String getCatalogName(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return "";
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   */
  public int getColumnType(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getSqlType();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */
  public String getColumnTypeName(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getTypeName();
  }

  /**
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */
  public boolean isReadOnly(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isReadOnly();
  }

  /**
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */
  public boolean isWritable(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isWritable();
  }

  /**
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */
  public boolean isDefinitelyWritable(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].isDefinitelyWritable();
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */
  public String getColumnClassName(int column) throws SQLException
  {
    if ((column < 1) || (column > rs.nbOfColumns))
      throw new SQLException("Invalid column index " + column);
    return rs.fields[column - 1].getColumnClassName();
  }

@Override
public <T> T unwrap(Class<T> iface) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public boolean isWrapperFor(Class<?> iface) throws SQLException {
	// TODO Auto-generated method stub
	return false;
}

}
