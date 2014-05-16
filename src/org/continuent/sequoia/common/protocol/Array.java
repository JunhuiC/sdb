/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2008 Continuent, Inc.
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
 * Initial developer(s): Jussi Pajala
 * Contributor(s): 
 */

package org.continuent.sequoia.common.protocol;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.continuent.sequoia.common.exceptions.NotImplementedException;

/**
 * Array class implementing only needed and very limited parts of java.sql.Array
 * interface.
 * 
 * @see java.sql.Array
 * @author Jussi Pajala
 */
public class Array implements java.sql.Array
{
  private Object array;
  private int    baseType;
  private String baseTypeName;

  /**
   * Constructor.
   * 
   * @param array must be instance of Object[]
   * @param baseType type of the array, types defined in java.sql.Types
   * @param baseTypeName type of the array, as understood by back end database
   */
  public Array(Object array, int baseType, String baseTypeName)
  {
    this.array = array;
    this.baseType = baseType;
    this.baseTypeName = baseTypeName;
  }

  /**
   * Retrieves the contents of the SQL ARRAY value designated by this Array
   * object in the form of an array in the Java programming language.
   */
  public Object getArray()
  {
    return array;
  }

  /**
   * Retrieves the contents of the SQL ARRAY value designated by this Array
   * object in the form of an array in the Java programming language.
   */
  public Object getArray(long index, int count) throws NotImplementedException
  {
    throw new NotImplementedException();
  }

  /**
   * Retreives a slice of the SQL ARRAY value designated by this Array object,
   * beginning with the specified index and containing up to count successive
   * elements of the SQL array.
   */
//  public Object getArray(long index, int count, Map map)
//      throws NotImplementedException
//  {
//    throw new NotImplementedException();
//  }

  /**
   * Retrieves the contents of the SQL ARRAY value designated by this Array
   * object.
   */
//  public Object getArray(Map map) throws SQLException, NotImplementedException
//  {
//    throw new NotImplementedException();
//  }

  /**
   * Retrieves the JDBC type of the elements in the array designated by this
   * Array object.
   */
  public int getBaseType()
  {
    return baseType;
  }

  /**
   * Retrieves the SQL type name of the elements in the array designated by this
   * Array object.
   */
  public String getBaseTypeName()
  {
    return new String(baseTypeName);
  }

  /**
   * Retrieves a result set that contains the elements of the SQL ARRAY value
   * designated by this Array object.
   */
  public ResultSet getResultSet() throws NotImplementedException
  {
    throw new NotImplementedException();
  }

  /**
   * Retrieves a result set holding the elements of the subarray that starts at
   * index index and contains up to count successive elements.
   */
  public ResultSet getResultSet(long index, int count)
      throws NotImplementedException
  {
    throw new NotImplementedException();
  }

  /**
   * Retrieves a result set holding the elements of the subarray that starts at
   * index index and contains up to count successive elements.
   */
//  public ResultSet getResultSet(long index, int count, Map map)
//      throws NotImplementedException
//  {
//    throw new NotImplementedException();
//  }

  /**
   * Retrieves a result set that contains the elements of the SQL ARRAY value
   * designated by this Array object.
   */
//  public ResultSet getResultSet(Map map) throws NotImplementedException
//  {
//    throw new NotImplementedException();
//  }

  private void getArrayString(StringBuffer sb, Object o)
  {
    if (o instanceof Object[])
    {
      int length = ((Object[]) o).length;
      for (int i = 0; i < length; i++)
      {
        if (i == 0)
          sb.append("{");
        else
          sb.append(",");
        getArrayString(sb, ((Object[]) o)[i]);
      }
      sb.append("}");
    }
    else
      sb.append(o.toString());
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    getArrayString(sb, array);
    return sb.toString();
  }

@Override
public Object getArray(Map<String, Class<?>> map) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Object getArray(long index, int count, Map<String, Class<?>> map)
		throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
		throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void free() throws SQLException {
	// TODO Auto-generated method stub
	
}
}