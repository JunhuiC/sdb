/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2007 Continuent, Inc.
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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.sql.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import org.continuent.sequoia.common.exceptions.driver.DriverSQLException;
import org.continuent.sequoia.common.exceptions.driver.protocol.BackendDriverException;
import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;

/**
 * This class defines a SequoiaParameterMetaData, sequoia implementation of the
 * <code>ParameterMetaData</code> interface, which can be sent/received over
 * the network.
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 * @param <E>
 */
public class SequoiaParameterMetaData
    implements
      ParameterMetaData,
      Serializable
{
  private static final long serialVersionUID   = 755705234599780316L;

  /**
   * Each array corresponds to a (static) metadata type. The data inside each
   * array corresponds the actual metadata of each parameter which can be an
   * exception if the driver threw it. This is read-only, no need for
   * synchronization.
   */
//  private ArrayList<Object>[]       data               = (ArrayList<Object>[])new ArrayList[8];
  private ArrayList<Object>[]       data               = null;  
  /** Number of objects in each vector */
  private int               numberOfParameters;
  // data types:
  private static int        nullable           = 0;
  private static int        signed             = 1;
  private static int        precision          = 2;
  private static int        scale              = 3;
  private static int        parameterType      = 4;
  private static int        parameterTypeName  = 5;
  private static int        parameterClassName = 6;
  private static int        parameterMode      = 7;

  /**
   * Creates a new <code>SequoiaParameterMetaData</code> object from a generic
   * {@link ParameterMetaData}.<br>
   * Retrieves and stores all metadata info into internal storage space. If the
   * driver throws exception during this metadata walkthrough, the exception is
   * also stored so it can be reliably rethrown when the user accesses it.
   * 
   * @param from parameter metadata to build this instance from
   * @throws SQLException if a database error occurs
   */
  public SequoiaParameterMetaData(ParameterMetaData from) throws SQLException
  {
    numberOfParameters = from.getParameterCount();
    data[nullable] = new ArrayList<Object>(numberOfParameters);
    data[signed] = new ArrayList<Object>(numberOfParameters);
    data[precision] = new ArrayList<Object>(numberOfParameters);
    data[scale] = new ArrayList<Object>(numberOfParameters);
    data[parameterType] = new ArrayList<Object>(numberOfParameters);
    data[parameterTypeName] = new ArrayList<Object>(numberOfParameters);
    data[parameterClassName] = new ArrayList<Object>(numberOfParameters);
    data[parameterMode] = new ArrayList<Object>(numberOfParameters);
    // we must be careful with indexes. Storage arrays start at zero whereas
    // jdbc starts at 1
    for (int i = 0; i < numberOfParameters; i++)
    {
      try
      {
        data[nullable].add(new Integer(from.isNullable(i + 1)));
      }
      catch (SQLException e)
      {
        data[nullable].add(new BackendDriverException(e));
      }
      try
      {
        data[signed].add(new Boolean(from.isSigned(i + 1)));
      }
      catch (SQLException e)
      {
        data[signed].add(new BackendDriverException(e));
      }
      try
      {
        data[precision].add(new Integer(from.getPrecision(i + 1)));
      }
      catch (SQLException e)
      {
        data[precision].add(new BackendDriverException(e));
      }
      try
      {
        data[scale].add(new Integer(from.getScale(i + 1)));
      }
      catch (SQLException e)
      {
        data[scale].add(new BackendDriverException(e));
      }
      try
      {
        data[parameterType].add(new Integer(from.getParameterType(i + 1)));
      }
      catch (SQLException e)
      {
        data[parameterType].add(new BackendDriverException(e));
      }
      try
      {
        data[parameterTypeName].add(from.getParameterTypeName(i + 1));
      }
      catch (SQLException e)
      {
        data[parameterTypeName].add(new BackendDriverException(e));
      }
      try
      {
        data[parameterClassName].add(from.getParameterClassName(i + 1));
      }
      catch (SQLException e)
      {
        data[parameterClassName].add(new BackendDriverException(e));
      }
      try
      {
        data[parameterMode].add(new Integer(from.getParameterMode(i + 1)));
      }
      catch (SQLException e)
      {
        data[parameterMode].add(new BackendDriverException(e));
      }
    }
  }

  /**
   * Creates a new <code>SequoiaParameterMetaData</code> object from the
   * stream.<br>
   * The info will be sent by the controller on the metadata will be
   * reconstructed inside the driver
   * 
   * @param stream to read from
   * @throws IOException if an error occurs on the stream
   */
  public SequoiaParameterMetaData(DriverBufferedInputStream in)
      throws IOException
  {
    numberOfParameters = in.readInt();
    data[nullable] = new ArrayList<Object>(numberOfParameters);
    data[signed] = new ArrayList<Object>(numberOfParameters);
    data[precision] = new ArrayList<Object>(numberOfParameters);
    data[scale] = new ArrayList<Object>(numberOfParameters);
    data[parameterType] = new ArrayList<Object>(numberOfParameters);
    data[parameterTypeName] = new ArrayList<Object>(numberOfParameters);
    data[parameterClassName] = new ArrayList<Object>(numberOfParameters);
    data[parameterMode] = new ArrayList<Object>(numberOfParameters);
    for (int i = 0; i < numberOfParameters; i++)
    {
      if (in.readBoolean())
        data[nullable].add(new Integer(in.readInt()));
      else
        data[nullable].add(new BackendDriverException(in));
      if (in.readBoolean())
        data[signed].add(new Boolean(in.readBoolean()));
      else
        data[signed].add(new BackendDriverException(in));
      if (in.readBoolean())
        data[precision].add(new Integer(in.readInt()));
      else
        data[precision].add(new BackendDriverException(in));
      if (in.readBoolean())
        data[scale].add(new Integer(in.readInt()));
      else
        data[scale].add(new BackendDriverException(in));
      if (in.readBoolean())
        data[parameterType].add(new Integer(in.readInt()));
      else
        data[parameterType].add(new BackendDriverException(in));
      if (in.readBoolean())
        data[parameterTypeName].add(in.readLongUTF());
      else
        data[parameterTypeName].add(new BackendDriverException(in));
      if (in.readBoolean())
        data[parameterClassName].add(in.readLongUTF());
      else
        data[parameterClassName].add(new BackendDriverException(in));
      if (in.readBoolean())
        data[parameterMode].add(new Integer(in.readInt()));
      else
        data[parameterMode].add(new BackendDriverException(in));
    }
  }

  /**
   * Serializes a <code>SequoiaParameterMetaData</code> on the output stream
   * by sending all stored metadata in order to reconstruct it in the driver.
   * MUST mirror the deserialization method
   * {@link #SequoiaParameterMetaData(DriverBufferedInputStream)}
   * 
   * @param output destination stream
   * @throws IOException if a network error occurs
   */

  public void sendToStream(DriverBufferedOutputStream output)
      throws IOException
  {
    // Send size first so data can be allocated
    output.writeInt(numberOfParameters);
    for (int i = 0; i < numberOfParameters; i++)
    {
      if (data[nullable].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[nullable].get(i)).sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeInt(((Integer) data[nullable].get(i)).intValue());
      }
      if (data[signed].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[signed].get(i)).sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeBoolean(((Boolean) data[signed].get(i)).booleanValue());
      }
      if (data[precision].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[precision].get(i)).sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeInt(((Integer) data[precision].get(i)).intValue());
      }
      if (data[scale].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[scale].get(i)).sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeInt(((Integer) data[scale].get(i)).intValue());
      }
      if (data[parameterType].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[parameterType].get(i))
            .sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeInt(((Integer) data[parameterType].get(i)).intValue());
      }
      if (data[parameterTypeName].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[parameterTypeName].get(i))
            .sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeLongUTF((String) data[parameterTypeName].get(i));
      }
      if (data[parameterClassName].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[parameterClassName].get(i))
            .sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeLongUTF((String) data[parameterClassName].get(i));
      }
      if (data[parameterMode].get(i) instanceof BackendDriverException)
      {
        output.writeBoolean(false);
        ((BackendDriverException) data[parameterMode].get(i))
            .sendToStream(output);
      }
      else
      {
        output.writeBoolean(true);
        output.writeInt(((Integer) data[parameterMode].get(i)).intValue());
      }
    }
  }

  /**
   * Checks that the given index is in the range 1-numberOfParameters
   * 
   * @param idx index to check
   * @throws SQLException if the given index is out of range
   */
  private void checkIndex(int idx) throws SQLException
  {
    if (idx > numberOfParameters || idx < 1)
      throw new SQLException("Column index " + idx + "out of range (>"
          + numberOfParameters + ").");
  }

  public int getParameterCount() throws SQLException
  {
    return numberOfParameters;
  }

  public int isNullable(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[nullable].get(param - 1);
    if (d instanceof Integer)
      return ((Integer) d).intValue();
    throw new DriverSQLException((BackendDriverException) d);
  }

  public boolean isSigned(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[signed].get(param - 1);
    if (d instanceof Boolean)
      return ((Boolean) d).booleanValue();
    throw new DriverSQLException((BackendDriverException) d);
  }

  public int getPrecision(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[precision].get(param - 1);
    if (d instanceof Integer)
      return ((Integer) d).intValue();
    throw new DriverSQLException((BackendDriverException) d);
  }

  public int getScale(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[scale].get(param - 1);
    if (d instanceof Integer)
      return ((Integer) d).intValue();
    throw new DriverSQLException((BackendDriverException) d);
  }

  public int getParameterType(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[parameterType].get(param - 1);
    if (d instanceof Integer)
      return ((Integer) d).intValue();
    throw new DriverSQLException((BackendDriverException) d);
  }

  public String getParameterTypeName(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[parameterTypeName].get(param - 1);
    if (d instanceof String || d == null) // the string can be null
      return (String) d;
    throw new DriverSQLException((BackendDriverException) d);
  }

  public String getParameterClassName(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[parameterClassName].get(param - 1);
    if (d instanceof String || d == null) // the string can be null
      return (String) d;
    throw new DriverSQLException((BackendDriverException) d);
  }

  public int getParameterMode(int param) throws SQLException
  {
    checkIndex(param);
    Object d = data[parameterMode].get(param - 1);
    if (d instanceof Integer)
      return ((Integer) d).intValue();
    throw new DriverSQLException((BackendDriverException) d);
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
