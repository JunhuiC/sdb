/**
 * Sequoia: Database clustering technology.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.protocol;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.continuent.sequoia.common.sql.filters.AbstractBlobFilter;
import org.continuent.sequoia.common.util.Strings;
import org.continuent.sequoia.controller.requests.StoredProcedure;

/**
 * This class contains the data used to serialize PreparedStatement.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public final class PreparedStatementSerialization
{

  /**
   * Tags used to serialize the name of the setter used by the JDBC client
   * application; one tag for each setter method. All tags must have the same
   * length {@link #setPreparedStatement(String, java.sql.PreparedStatement)}
   */
  /** @see java.sql.PreparedStatement#setArray(int, Array) */
  public static final String ARRAY_TAG                         = "A|";
  /** @see java.sql.PreparedStatement#setByte(int, byte) */
  public static final String BYTE_TAG                          = "b|";
  /** @see java.sql.PreparedStatement#setBytes(int, byte[]) */
  public static final String BYTES_TAG                         = "B|";
  /** @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob) */
  public static final String BLOB_TAG                          = "c|";
  /** @see java.sql.PreparedStatement#setClob(int, java.sql.Clob) */
  public static final String CLOB_TAG                          = "C|";
  /** @see java.sql.PreparedStatement#setBoolean(int, boolean) */
  public static final String BOOLEAN_TAG                       = "0|";
  /**
   * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
   */
  public static final String BIG_DECIMAL_TAG                   = "1|";
  /** @see java.sql.PreparedStatement#setDate(int, java.sql.Date) */
  public static final String DATE_TAG                          = "d|";
  /** @see java.sql.PreparedStatement#setDouble(int, double) */
  public static final String DOUBLE_TAG                        = "D|";
  /** @see java.sql.PreparedStatement#setFloat(int, float) */
  public static final String FLOAT_TAG                         = "F|";
  /** @see java.sql.PreparedStatement#setInt(int, int) */
  public static final String INTEGER_TAG                       = "I|";
  /** @see java.sql.PreparedStatement#setLong(int, long) */
  public static final String LONG_TAG                          = "L|";

  /** Encoding of a named parameter in a CallableStatement */
  public static final String NAMED_PARAMETER_TAG               = "P|";

  /** Encoding of a NULL value. Also used to as the _TAG for setNull() */
  public static final String NULL_VALUE                        = "N|";
  /**
   * Special escape _type_ tag used when the string parameter is unfortunately
   * equal to an encoded null value.
   */
  public static final String NULL_STRING_TAG                   = "n|";

  /**
   * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
   */
  public static final String OBJECT_TAG                        = "O|";
  /** @see java.sql.PreparedStatement#setRef(int, java.sql.Ref) */
  public static final String REF_TAG                           = "R|";
  /** @see java.sql.PreparedStatement#setShort(int, short) */
  public static final String SHORT_TAG                         = "s|";
  /**
   * @see java.sql.PreparedStatement#setString(int, java.lang.String)
   */
  public static final String STRING_TAG                        = "S|";
  /** @see java.sql.PreparedStatement#setTime(int, java.sql.Time) */
  public static final String TIME_TAG                          = "t|";
  /**
   * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
   */
  public static final String TIMESTAMP_TAG                     = "T|";
  /** @see java.sql.PreparedStatement#setURL(int, java.net.URL) */
  public static final String URL_TAG                           = "U|";

  /** Escape for callable statement out parameter */
  public static final String REGISTER_OUT_PARAMETER            = "o|";
  /** Escape for callable statement out parameter with scale */
  public static final String REGISTER_OUT_PARAMETER_WITH_SCALE = "w|";
  /** Escape for callable statement out parameter with type name */
  public static final String REGISTER_OUT_PARAMETER_WITH_NAME  = "W|";
  /**
   * Escape for 'void' placeholder when compiling callable statement OUT or
   * named parameter
   */
  public static final String CS_PARAM_TAG                      = "V|";

  /** Tag maker for parameters */
  public static final String TAG_MARKER                        = "!%";
  /** Escape for tag maker */
  public static final String TAG_MARKER_ESCAPE                 = TAG_MARKER
                                                                   + ";";

  /** Tag for parameters start delimiter */
  public static final String START_PARAM_TAG                   = "<"
                                                                   + TAG_MARKER;
  /** Tag for parameters end delimiter */
  public static final String END_PARAM_TAG                     = "|"
                                                                   + TAG_MARKER
                                                                   + ">";
  /** Tag for start and end delimiter for serializing array base type name string */
  public static final String ARRAY_BASETYPE_NAME_SEPARATOR     = "\"";

  /**
   * Static method to initialize a backend PreparedStatement by calling the
   * appropriate setXXX methods on the request skeleton. Has to extract the
   * tagged and inlined parameters from the sql String beforehand. Used by the
   * controller.
   * 
   * @param parameters encoded parameters to set
   * @param backendPS the preparedStatement to set
   * @throws SQLException if an error occurs
   * @see org.continuent.sequoia.driver.PreparedStatement#setParameterWithTag(int,
   *      String, String)
   */
  public static void setPreparedStatement(String parameters,
      java.sql.PreparedStatement backendPS) throws SQLException
  {
    int i = 0;
    int paramIdx = 0;

    // Set all parameters
    while ((i = parameters.indexOf(START_PARAM_TAG, i)) > -1)
    {
      paramIdx++;

      int typeStart = i + START_PARAM_TAG.length();

      // Here we assume that all tags have the same length as the boolean tag.
      String paramType = parameters.substring(typeStart, typeStart
          + BOOLEAN_TAG.length());
      String paramValue = parameters.substring(
          typeStart + BOOLEAN_TAG.length(), parameters
              .indexOf(END_PARAM_TAG, i));
      paramValue = Strings.replace(paramValue, TAG_MARKER_ESCAPE, TAG_MARKER);

      if (!performCallOnPreparedStatement(backendPS, paramIdx, paramType,
          paramValue))
      {
        // invalid parameter, we want to be able to store strings like
        // <?xml version="1.0" encoding="ISO-8859-1"?>
        paramIdx--;
      }
      i = typeStart + paramValue.length();
    }
  }

  /**
   * Static method to initialize a backend PreparedStatement by calling the
   * appropriate setXXX methods on the request skeleton. Has to extract the
   * tagged and inlined parameters from the sql String beforehand. Used by the
   * controller.
   * 
   * @param parameters encoded parameters to set
   * @param cs the CallableStatement to set
   * @param proc the StoredProcedure that is called
   * @throws SQLException if an error occurs
   * @see org.continuent.sequoia.driver.PreparedStatement#setParameterWithTag(int,
   *      String, String)
   * @see org.continuent.sequoia.driver.CallableStatement#setNamedParameterWithTag(String,
   *      String, String)
   */
  public static void setCallableStatement(String parameters,
      CallableStatement cs, StoredProcedure proc) throws SQLException
  {
    int i = 0;
    int paramIdx = 0;

    // Set all parameters
    while ((i = parameters.indexOf(START_PARAM_TAG, i)) > -1)
    {
      paramIdx++;

      int typeStart = i + START_PARAM_TAG.length();

      // Here we assume that all tags have the same length as the boolean tag.
      String paramType = parameters.substring(typeStart, typeStart
          + BOOLEAN_TAG.length());
      String paramValue = parameters.substring(
          typeStart + BOOLEAN_TAG.length(), parameters
              .indexOf(END_PARAM_TAG, i));
      paramValue = Strings.replace(paramValue, TAG_MARKER_ESCAPE, TAG_MARKER);

      if (!performCallOnPreparedStatement(cs, paramIdx, paramType, paramValue))
      {
        // Not a standard PreparedStatement call, let's check for OUT paramters
        // and named parameters
        int comma = paramValue.indexOf(",");
        String paramName = paramValue.substring(0, comma);
        if (setOutParameter(paramName, paramType, paramValue
            .substring(comma + 1), cs, proc))
        {
          // Success, this was an out parameter
          // else try a named parameter
        }
        else if (paramType.equals(NAMED_PARAMETER_TAG))
        {
          // Value is composed of: paramName,paramTypeparamValue
          paramType = paramValue.substring(comma + 1, comma + 1
              + BOOLEAN_TAG.length());
          paramValue = paramValue.substring(comma + 1 + BOOLEAN_TAG.length());
          paramValue = Strings.replace(paramValue, TAG_MARKER_ESCAPE,
              TAG_MARKER);

          proc.setNamedParameterName(paramName);
          setNamedParameterOnCallableStatement(cs, paramName, paramType,
              paramValue);
        }
        else
        {
          // invalid parameter, we want to be able to store strings like
          // <?xml version="1.0" encoding="ISO-8859-1"?>
          paramIdx--;
        }
      }
      i = typeStart;
    }
  }

  private static boolean setOutParameter(String paramName, String paramType,
      String paramValue, CallableStatement cs, StoredProcedure proc)
      throws SQLException
  {
    if (paramType.equals(REGISTER_OUT_PARAMETER))
    {
      int sqlType = Integer.valueOf(paramValue).intValue();
      try
      {
        int paramIdx = Integer.parseInt(paramName);
        proc.setOutParameterIndex(paramIdx);
        cs.registerOutParameter(paramIdx, sqlType);
      }
      catch (NumberFormatException e)
      { // This is a real named parameter
        proc.setNamedParameterName(paramName);
        cs.registerOutParameter(paramName, sqlType);
      }
      return true;
    }
    else if (paramType.equals(REGISTER_OUT_PARAMETER_WITH_SCALE))
    {
      int comma = paramValue.indexOf(',');
      int sqlType = Integer.valueOf(paramValue.substring(0, comma)).intValue();
      int scale = Integer.valueOf(paramValue.substring(comma + 1)).intValue();
      try
      {
        int paramIdx = Integer.parseInt(paramName);
        proc.setOutParameterIndex(paramIdx);
        cs.registerOutParameter(paramIdx, sqlType, scale);
      }
      catch (NumberFormatException e)
      { // This is a real named parameter
        proc.setNamedParameterName(paramName);
        cs.registerOutParameter(paramName, sqlType, scale);
      }
      return true;
    }
    else if (paramType.equals(REGISTER_OUT_PARAMETER_WITH_NAME))
    {
      int comma = paramValue.indexOf(',');
      int sqlType = Integer.valueOf(paramValue.substring(0, comma)).intValue();
      try
      {
        int paramIdx = Integer.parseInt(paramName);
        proc.setOutParameterIndex(paramIdx);
        cs.registerOutParameter(paramIdx, sqlType, paramValue
            .substring(comma + 1));
      }
      catch (NumberFormatException e)
      { // This is a real named parameter
        proc.setNamedParameterName(paramName);
        cs.registerOutParameter(paramName, sqlType, paramValue
            .substring(comma + 1));
      }
      return true;
    }
    else
      return false;
  }

  private static void setNamedParameterOnCallableStatement(
      CallableStatement cs, String paramName, String paramType,
      String paramValue) throws SQLException
  {
    // Test tags in alphabetical order (to make the code easier to read)
    if (paramType.equals(BIG_DECIMAL_TAG))
    {
      BigDecimal t = null;
      if (!paramValue.equals(NULL_VALUE))
        t = new BigDecimal(paramValue);
      cs.setBigDecimal(paramName, t);
    }
    else if (paramType.equals(BOOLEAN_TAG))
      cs.setBoolean(paramName, Boolean.valueOf(paramValue).booleanValue());
    else if (paramType.equals(BYTE_TAG))
    {
      byte t = new Integer(paramValue).byteValue();
      cs.setByte(paramName, t);
    }
    else if (paramType.equals(BYTES_TAG))
    {
      /**
       * encoded by the driver at {@link #setBytes(int, byte[])}in order to
       * inline it in the request (no database encoding here).
       */
      byte[] t = AbstractBlobFilter.getDefaultBlobFilter().decode(paramValue);
      cs.setBytes(paramName, t);
    }
    else if (paramType.equals(DATE_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        cs.setDate(paramName, null);
      else
        try
        {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          Date t = new Date(sdf.parse(paramValue).getTime());
          cs.setDate(paramName, t);
        }
        catch (ParseException p)
        {
          cs.setDate(paramName, null);
          throw new SQLException("Couldn't format date!!!");
        }
    }
    else if (paramType.equals(DOUBLE_TAG))
      cs.setDouble(paramName, Double.valueOf(paramValue).doubleValue());
    else if (paramType.equals(FLOAT_TAG))
      cs.setFloat(paramName, Float.valueOf(paramValue).floatValue());
    else if (paramType.equals(INTEGER_TAG))
      cs.setInt(paramName, Integer.valueOf(paramValue).intValue());
    else if (paramType.equals(LONG_TAG))
      cs.setLong(paramName, Long.valueOf(paramValue).longValue());
    else if (paramType.equals(NULL_VALUE))
      cs.setNull(paramName, Integer.valueOf(paramValue).intValue());
    else if (paramType.equals(OBJECT_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        cs.setObject(paramName, null);
      else
      {
        final String commonMsg = "Failed to deserialize object parameter of setObject()";
        Object obj;
        try
        {
          byte[] decoded = AbstractBlobFilter.getDefaultBlobFilter().decode(
              paramValue);
          obj = new ObjectInputStream(new ByteArrayInputStream(decoded))
              .readObject();
        }
        catch (ClassNotFoundException cnfe)
        {
          throw (SQLException) new SQLException(commonMsg
              + ", class not found on controller").initCause(cnfe);
        }
        catch (IOException ioe) // like for instance invalid stream header
        {
          throw (SQLException) new SQLException(commonMsg + ", I/O exception")
              .initCause(ioe);
        }
        cs.setObject(paramName, obj);
      }
    }
    else if (paramType.equals(SHORT_TAG))
    {
      short t = new Integer(paramValue).shortValue();
      cs.setShort(paramName, t);
    }
    else if (paramType.equals(STRING_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        cs.setString(paramName, null);
      else
        cs.setString(paramName, paramValue);
    }
    else if (paramType.equals(NULL_STRING_TAG))
    {
      cs.setString(paramName, null);
    }
    else if (paramType.equals(TIME_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        cs.setTime(paramName, null);
      else
        try
        {
          SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
          Time t = new Time(sdf.parse(paramValue).getTime());
          cs.setTime(paramName, t);
        }
        catch (ParseException p)
        {
          cs.setTime(paramName, null);
          throw new SQLException("Couldn't format time!!!");
        }
    }
    else if (paramType.equals(TIMESTAMP_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        cs.setTimestamp(paramName, null);
      else
        try
        {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
          Timestamp t = new Timestamp(sdf.parse(paramValue).getTime());
          cs.setTimestamp(paramName, t);
        }
        catch (ParseException p)
        {
          cs.setTimestamp(paramName, null);
          throw new SQLException("Couldn't format timestamp!!!");
        }
    }
    else if (paramType.equals(URL_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        cs.setURL(paramName, null);
      else
        try
        {
          cs.setURL(paramName, new URL(paramValue));
        }
        catch (MalformedURLException e)
        {
          throw new SQLException("Unable to create URL " + paramValue + " ("
              + e + ")");
        }
    }
    else
    {
      throw new SQLException("Unsupported named parameter type: " + paramType);
    }
  }

  private static boolean performCallOnPreparedStatement(
      java.sql.PreparedStatement backendPS, int paramIdx, String paramType,
      String paramValue) throws SQLException
  {
    // Test tags in alphabetical order (to make the code easier to read)
    if (paramType.equals(ARRAY_TAG))
    {
      // in case of Array, the String contains:
      // - integer: sql type
      // - string: sql type name, delimited by ARRAY_BASETYPE_NAME_SEPARATOR
      // - serialized object array
      final String commonMsg = "Failed to deserialize Array parameter of setObject()";
      int baseType;
      String baseTypeName;

      // Get start and end of the type name string
      int startBaseTypeName = paramValue.indexOf(ARRAY_BASETYPE_NAME_SEPARATOR);
      int endBaseTypeName = paramValue.indexOf(ARRAY_BASETYPE_NAME_SEPARATOR,
          startBaseTypeName + 1);

      baseType = Integer.valueOf(paramValue.substring(0, startBaseTypeName))
          .intValue();
      baseTypeName = paramValue.substring(startBaseTypeName + 1,
          endBaseTypeName);

      // create a new string without type information in front.
      paramValue = paramValue.substring(endBaseTypeName + 1);

      // rest of the array deserialization code is copied from OBJECT_TAG

      Object obj;
      try
      {
        byte[] decoded = AbstractBlobFilter.getDefaultBlobFilter().decode(
            paramValue);
        obj = new ObjectInputStream(new ByteArrayInputStream(decoded))
            .readObject();
      }
      catch (ClassNotFoundException cnfe)
      {
        throw (SQLException) new SQLException(commonMsg
            + ", class not found on controller").initCause(cnfe);
      }
      catch (IOException ioe) // like for instance invalid stream header
      {
        throw (SQLException) new SQLException(commonMsg + ", I/O exception")
            .initCause(ioe);
      }

      // finally, construct the java.sql.array interface object using
      // deserialized object, and type information
      org.continuent.sequoia.common.protocol.Array array = new org.continuent.sequoia.common.protocol.Array(
          obj, baseType, baseTypeName);
      backendPS.setArray(paramIdx, array);
    }
    else if (paramType.equals(BIG_DECIMAL_TAG))
    {
      BigDecimal t = null;
      if (!paramValue.equals(NULL_VALUE))
        t = new BigDecimal(paramValue);
      backendPS.setBigDecimal(paramIdx, t);
    }
    else if (paramType.equals(BOOLEAN_TAG))
      backendPS
          .setBoolean(paramIdx, Boolean.valueOf(paramValue).booleanValue());
    else if (paramType.equals(BYTE_TAG))
    {
      byte t = new Integer(paramValue).byteValue();
      backendPS.setByte(paramIdx, t);
    }
    else if (paramType.equals(BYTES_TAG))
    {
      /**
       * encoded by the driver at {@link #setBytes(int, byte[])}in order to
       * inline it in the request (no database encoding here).
       */
      byte[] t = AbstractBlobFilter.getDefaultBlobFilter().decode(paramValue);
      backendPS.setBytes(paramIdx, t);
    }
    else if (paramType.equals(BLOB_TAG))
    {
      ByteArrayBlob b = null;
      // encoded by the driver at {@link #setBlob(int, java.sql.Blob)}
      if (!paramValue.equals(NULL_VALUE))
        b = new ByteArrayBlob(AbstractBlobFilter.getDefaultBlobFilter().decode(
            paramValue));
      backendPS.setBlob(paramIdx, b);
    }
    else if (paramType.equals(CLOB_TAG))
    {
      StringClob c = null;
      if (!paramValue.equals(NULL_VALUE))
        c = new StringClob(paramValue);
      backendPS.setClob(paramIdx, c);
    }
    else if (paramType.equals(DATE_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        backendPS.setDate(paramIdx, null);
      else
        try
        {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          Date t = new Date(sdf.parse(paramValue).getTime());
          backendPS.setDate(paramIdx, t);
        }
        catch (ParseException p)
        {
          backendPS.setDate(paramIdx, null);
          throw new SQLException("Couldn't format date!!!");
        }
    }
    else if (paramType.equals(DOUBLE_TAG))
      backendPS.setDouble(paramIdx, Double.valueOf(paramValue).doubleValue());
    else if (paramType.equals(FLOAT_TAG))
      backendPS.setFloat(paramIdx, Float.valueOf(paramValue).floatValue());
    else if (paramType.equals(INTEGER_TAG))
      backendPS.setInt(paramIdx, Integer.valueOf(paramValue).intValue());
    else if (paramType.equals(LONG_TAG))
      backendPS.setLong(paramIdx, Long.valueOf(paramValue).longValue());
    else if (paramType.equals(NULL_VALUE))
      backendPS.setNull(paramIdx, Integer.valueOf(paramValue).intValue());
    else if (paramType.equals(OBJECT_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        backendPS.setObject(paramIdx, null);
      else
      {
        final String commonMsg = "Failed to deserialize object parameter of setObject()";
        Object obj;
        try
        {
          byte[] decoded = AbstractBlobFilter.getDefaultBlobFilter().decode(
              paramValue);
          obj = new ObjectInputStream(new ByteArrayInputStream(decoded))
              .readObject();
        }
        catch (ClassNotFoundException cnfe)
        {
          throw (SQLException) new SQLException(commonMsg
              + ", class not found on controller").initCause(cnfe);
        }
        catch (IOException ioe) // like for instance invalid stream header
        {
          throw (SQLException) new SQLException(commonMsg + ", I/O exception")
              .initCause(ioe);
        }
        backendPS.setObject(paramIdx, obj);
      }
    }
    else if (paramType.equals(REF_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        backendPS.setRef(paramIdx, null);
      else
        throw new SQLException("Ref type not supported");
    }
    else if (paramType.equals(SHORT_TAG))
    {
      short t = new Integer(paramValue).shortValue();
      backendPS.setShort(paramIdx, t);
    }
    else if (paramType.equals(STRING_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        backendPS.setString(paramIdx, null);
      else
        backendPS.setString(paramIdx, paramValue);
    }
    else if (paramType.equals(NULL_STRING_TAG))
    {
      backendPS.setString(paramIdx, null);
    }
    else if (paramType.equals(TIME_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        backendPS.setTime(paramIdx, null);
      else
        try
        {
          SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
          Time t = new Time(sdf.parse(paramValue).getTime());
          backendPS.setTime(paramIdx, t);
        }
        catch (ParseException p)
        {
          backendPS.setTime(paramIdx, null);
          throw new SQLException("Couldn't format time!!!");
        }
    }
    else if (paramType.equals(TIMESTAMP_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        backendPS.setTimestamp(paramIdx, null);
      else
        try
        {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
          Timestamp t = new Timestamp(sdf.parse(paramValue).getTime());
          backendPS.setTimestamp(paramIdx, t);
        }
        catch (ParseException p)
        {
          backendPS.setTimestamp(paramIdx, null);
          throw new SQLException("Couldn't format timestamp!!!");
        }
    }
    else if (paramType.equals(URL_TAG))
    {
      if (paramValue.equals(NULL_VALUE))
        backendPS.setURL(paramIdx, null);
      else
        try
        {
          backendPS.setURL(paramIdx, new URL(paramValue));
        }
        catch (MalformedURLException e)
        {
          throw new SQLException("Unable to create URL " + paramValue + " ("
              + e + ")");
        }
    }
    else if (paramType.equals(CS_PARAM_TAG))
      return true; // ignore, will be treated in the named parameters
    else
      return false;

    return true;
  }

}
