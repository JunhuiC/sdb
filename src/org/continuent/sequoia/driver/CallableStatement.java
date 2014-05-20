/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 Continuent, Inc.
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

package org.continuent.sequoia.driver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.continuent.sequoia.common.protocol.PreparedStatementSerialization;
import org.continuent.sequoia.common.sql.Request;
import org.continuent.sequoia.common.sql.RequestWithResultSetParameters;
import org.continuent.sequoia.common.sql.filters.AbstractBlobFilter;
import org.continuent.sequoia.common.util.Strings;

/**
 * This class is used to execute SQL stored procedures. The JDBC API provides a
 * stored procedure SQL escape syntax that allows stored procedures to be called
 * in a standard way for all RDBMSs. The syntax accepted by this implementation
 * is as follows:
 * 
 * <pre>
 *  {call &lt;procedure-name&gt;[&lt;arg1&gt;,&lt;arg2&gt;, ...]}
 * </pre>
 * 
 * or (since Sequoia 2.7)
 * 
 * <pre>
 *  {?= call &lt;procedure-name&gt;[&lt;arg1&gt;,&lt;arg2&gt;, ...]}
 * </pre>
 * 
 * Parameters are referred to sequentially, by number, with the first parameter
 * being 1. IN parameter values are set using the <code>set</code> methods
 * inherited from {@link PreparedStatement}.
 * <p>
 * OUT and JDBC 3 named parameters are implemented in this class.
 * <p>
 * A <code>CallableStatement</code> can return one {@link DriverResultSet}
 * object or multiple <code>ResultSet</code> objects. Multiple
 * <code>ResultSet</code> objects are handled using operations inherited from
 * {@link Statement}.
 * 
 * @see org.continuent.sequoia.driver.Connection#prepareCall(String)
 * @see DriverResultSet
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class CallableStatement extends PreparedStatement
    implements
      java.sql.CallableStatement
{
  // Values returned by out parameters
  private HashMap<?, ?> outParameters             = null;
  // Out and named parameter types set by the application
  private HashMap<String, Object> outAndNamedParameterTypes = null;
  // Named parameter returned values
  private HashMap<?, ?> namedParameterValues      = null;
  private int     lastOutParameterIndex     = -1;

  /**
   * <code>CallableStatements</code> syntax is {call procedure_name[(?, ?,
   * ...)]}. Note that {? = call ...} is not supported by this implementation.
   * 
   * @param connection the instanatiating connection
   * @param sql the SQL statement with ? for IN markers
   * @param driver the Driver used to create connections
   */
  public CallableStatement(Connection connection, String sql, Driver driver)
  {
    super(connection, sql, driver);
  }

  /**
   * Add names parameters at the end of regular parameters.
   * 
   * @see PreparedStatement#compileParameters(boolean)
   */
  protected synchronized String compileParameters() throws SQLException
  {
    // Call PreparedStatement.compileParameters()
    String parameters = super.compileParameters(true);

    if (outAndNamedParameterTypes == null)
      return parameters;

    sbuf.append(parameters);
    
    // Append named parameters but reuse sbuf set by super.compileParameters()
    // for more efficiency
    for (Iterator<?> iter = outAndNamedParameterTypes.values().iterator(); iter
        .hasNext();)
    {
      String namedParam = (String) iter.next();
      sbuf.append(namedParam);
    }
    return trimStringBuffer();
  }

  /**
   * @see org.continuent.sequoia.driver.PreparedStatement#execute()
   */
  @SuppressWarnings("unchecked")
public boolean execute() throws SQLException
  {
    if (isClosed())
    {
      throw new SQLException("Unable to execute query on a closed statement");
    }
    RequestWithResultSetParameters proc = new RequestWithResultSetParameters(
        sql, compileParameters(), escapeProcessing, timeout);
    setReadRequestParameters(proc);
    ResultAndWarnings result = connection.callableStatementExecute(proc);
    warnings = result.getStatementWarnings();
    List<?> resultWithParameters = result.getResultList();
    resultList = (LinkedList<Object>) resultWithParameters.get(0);
    resultListIterator = resultList.iterator();
    outParameters = (HashMap<?, ?>) resultWithParameters.get(1);
    namedParameterValues = (HashMap<?, ?>) resultWithParameters.get(2);

    return getMoreResults();
  }

  /**
   * Execute a batch of commands
   * 
   * @return an array containing update count that corresponding to the commands
   *         that executed successfully
   * @exception BatchUpdateException if an error occurs on one statement (the
   *              number of updated rows for the successfully executed
   *              statements can be found in
   *              BatchUpdateException.getUpdateCounts())
   */
  public int[] executeBatch() throws BatchUpdateException
  {
    if (batch == null || batch.isEmpty())
      return new int[0];

    int size = batch.size();
    int[] nbsRowsUpdated = new int[size];
    int i = 0;
    // must keep warning in a separate place otherwise they will be erased at
    // each call to executeUpdate()
    SQLWarning allWarnings = null;
    try
    {
      for (i = 0; i < size; i++)
      {
        BatchElement be = (BatchElement) batch.elementAt(i);
        Request proc = new Request(be.getSqlTemplate(), be.getParameters(),
            escapeProcessing, timeout);

        ResultAndWarnings result = connection
            .callableStatementExecuteUpdate(proc);
        if (result.getStatementWarnings() != null)
          addWarningTo(result.getStatementWarnings(), allWarnings);
        List<?> resultWithParameters = result.getResultList();
        nbsRowsUpdated[i] = ((Integer) resultWithParameters.get(0)).intValue();
        // Not sure what to do with OUT and named parameters. Just keep them in
        // case someone wants to access the ones returned by the last executed
        // statement.
        outParameters = (HashMap<?, ?>) resultWithParameters.get(1);
        namedParameterValues = (HashMap<?, ?>) resultWithParameters.get(2);
      }
      // make one chain with all generated warnings
      warnings = allWarnings;
      return nbsRowsUpdated;
    }
    catch (SQLException e)
    {
      String message = "Batch failed for request " + i + ": "
          + ((BatchElement) batch.elementAt(i)).getSqlTemplate() + " (" + e
          + ")";

      // shrink the returned array
      int[] updateCounts = new int[i];
      System.arraycopy(nbsRowsUpdated, 0, updateCounts, 0, i);

      throw new BatchUpdateException(message, updateCounts);
    }
    finally
    {
      batch.removeAllElements();
    }
  }

  /**
   * @see org.continuent.sequoia.driver.PreparedStatement#executeQuery()
   */
  public ResultSet executeQuery() throws SQLException
  {
    if (isClosed())
    {
      throw new SQLException("Unable to execute query on a closed statement");
    }
    updateCount = -1; // invalidate the last write result
    if (result != null)
    { // Discard the previous result
      result.close();
      result = null;
    }

    RequestWithResultSetParameters proc = new RequestWithResultSetParameters(
        sql, compileParameters(), escapeProcessing, timeout);
    setReadRequestParameters(proc);
    ResultAndWarnings res = connection.callableStatementExecuteQuery(proc);
    warnings = res.getStatementWarnings();
    List<?> resultWithParameters = res.getResultList();
    result = (ResultSet) resultWithParameters.get(0);
    outParameters = (HashMap<?, ?>) resultWithParameters.get(1);
    namedParameterValues = (HashMap<?, ?>) resultWithParameters.get(2);
    if (result instanceof DriverResultSet)
      ((DriverResultSet) result).setStatement(this);
    return result;
  }

  /**
   * @see org.continuent.sequoia.driver.PreparedStatement#executeUpdate()
   */
  public int executeUpdate() throws SQLException
  {
    if (isClosed())
    {
      throw new SQLException("Unable to execute query on a closed statement");
    }
    if (result != null)
    { // Discard the previous result
      result.close();
      result = null;
    }
    Request proc = new Request(sql, compileParameters(), escapeProcessing,
        timeout);
    ResultAndWarnings result = connection.callableStatementExecuteUpdate(proc);
    this.warnings = result.getStatementWarnings();
    List<?> resultWithParameters = result.getResultList();
    updateCount = ((Integer) resultWithParameters.get(0)).intValue();
    outParameters = (HashMap<?, ?>) resultWithParameters.get(1);
    namedParameterValues = (HashMap<?, ?>) resultWithParameters.get(2);

    return updateCount;
  }

  /**
   * Registers the OUT parameter in ordinal position <code>parameterIndex</code>
   * to the JDBC type <code>sqlType</code>. All OUT parameters must be
   * registered before a stored procedure is executed.
   * <p>
   * The JDBC type specified by <code>sqlType</code> for an OUT parameter
   * determines the Java type that must be used in the <code>get</code> method
   * to read the value of that parameter.
   * <p>
   * If the JDBC type expected to be returned to this output parameter is
   * specific to this particular database, <code>sqlType</code> should be
   * <code>java.sql.Types.OTHER</code>. The method {@link #getObject(int)}
   * retrieves the value.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @param sqlType the JDBC type code defined by <code>java.sql.Types</code>.
   *          If the parameter is of JDBC type <code>NUMERIC</code> or
   *          <code>DECIMAL</code>, the version of
   *          <code>registerOutParameter</code> that accepts a scale value
   *          should be used.
   * @exception SQLException if a database access error occurs
   * @see java.sql.Types
   */
  public void registerOutParameter(int parameterIndex, int sqlType)
      throws SQLException
  {
    setOutParameterWithTag(String.valueOf(parameterIndex),
        PreparedStatementSerialization.REGISTER_OUT_PARAMETER, String
            .valueOf(sqlType));
  }

  /**
   * Registers the parameter in ordinal position <code>parameterIndex</code>
   * to be of JDBC type <code>sqlType</code>. This method must be called
   * before a stored procedure is executed.
   * <p>
   * The JDBC type specified by <code>sqlType</code> for an OUT parameter
   * determines the Java type that must be used in the <code>get</code> method
   * to read the value of that parameter.
   * <p>
   * This version of <code>registerOutParameter</code> should be used when the
   * parameter is of JDBC type <code>NUMERIC</code> or <code>DECIMAL</code>.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @param sqlType the SQL type code defined by <code>java.sql.Types</code>.
   * @param scale the desired number of digits to the right of the decimal
   *          point. It must be greater than or equal to zero.
   * @exception SQLException if a database access error occurs
   * @see java.sql.Types
   */
  public void registerOutParameter(int parameterIndex, int sqlType, int scale)
      throws SQLException
  {
    setOutParameterWithTag(String.valueOf(parameterIndex),
        PreparedStatementSerialization.REGISTER_OUT_PARAMETER_WITH_SCALE,
        sqlType + "," + scale);
  }

  /**
   * Registers the designated output parameter. This version of the method
   * <code>registerOutParameter</code> should be used for a user-defined or
   * <code>REF</code> output parameter. Examples of user-defined types
   * include: <code>STRUCT</code>,<code>DISTINCT</code>,
   * <code>JAVA_OBJECT</code>, and named array types.
   * <p>
   * Before executing a stored procedure call, you must explicitly call
   * <code>registerOutParameter</code> to register the type from
   * <code>java.sql.Types</code> for each OUT parameter. For a user-defined
   * parameter, the fully-qualified SQL type name of the parameter should also
   * be given, while a <code>REF</code> parameter requires that the
   * fully-qualified type name of the referenced type be given. A JDBC driver
   * that does not need the type code and type name information may ignore it.
   * To be portable, however, applications should always provide these values
   * for user-defined and <code>REF</code> parameters.
   * <p>
   * Although it is intended for user-defined and <code>REF</code> parameters,
   * this method may be used to register a parameter of any JDBC type. If the
   * parameter does not have a user-defined or <code>REF</code> type, the
   * <i>typeName </i> parameter is ignored.
   * <p>
   * <b>Note: </b> When reading the value of an out parameter, you must use the
   * getter method whose Java type corresponds to the parameter's registered SQL
   * type.
   * 
   * @param paramIndex the first parameter is 1, the second is 2,...
   * @param sqlType a value from {@link java.sql.Types}
   * @param typeName the fully-qualified name of an SQL structured type
   * @exception SQLException if a database access error occurs
   * @see java.sql.Types
   * @since 1.2
   */
  public void registerOutParameter(int paramIndex, int sqlType, String typeName)
      throws SQLException
  {
    setOutParameterWithTag(String.valueOf(paramIndex),
        PreparedStatementSerialization.REGISTER_OUT_PARAMETER_WITH_NAME,
        sqlType + "," + typeName);
  }

  /**
   * Get the Java SQL type of an OUT parameter. Throws an SQLException if the
   * parameter was not an OUT parameter.
   * 
   * @param parameterIndex parameter index
   * @return Java SQL type
   * @throws SQLException if the parameter is not an out parameter or an invalid
   *           parameter
   */
  private int getOutParameterType(int parameterIndex) throws SQLException
  {
    lastOutParameterIndex = -1;

    if (outParameters == null)
      throw new SQLException("No OUT parameter has been returned");

    lastOutParameterIndex = parameterIndex;

    String type = (String) outAndNamedParameterTypes.get(Integer
        .toString(parameterIndex));
    if (type != null)
    {
      int typeStart = PreparedStatementSerialization.START_PARAM_TAG.length();
      String paramType = type.substring(typeStart, typeStart
          + PreparedStatementSerialization.BOOLEAN_TAG.length());
      String paramValue = type.substring(typeStart
          + PreparedStatementSerialization.BOOLEAN_TAG.length(), type
          .indexOf(PreparedStatementSerialization.END_PARAM_TAG));

      // paramType is a NAMED_PARAMETER_TAG

      // Value is composed of: paramName,paramTypeparamValue
      // paramName is equal to parameterIndex (just ignore it)
      // paramTypeparamValue should be the out parameter and its value
      int comma = paramValue.indexOf(",");
      paramValue = paramValue.substring(comma + 1);

      if (paramType
          .startsWith(PreparedStatementSerialization.REGISTER_OUT_PARAMETER))
        return Integer.parseInt(paramValue);
      if (paramType
          .startsWith(PreparedStatementSerialization.REGISTER_OUT_PARAMETER_WITH_NAME))
        return Integer.parseInt(paramValue
            .substring(0, paramValue.indexOf(',')));
      if (paramType
          .startsWith(PreparedStatementSerialization.REGISTER_OUT_PARAMETER_WITH_SCALE))
        return Integer.parseInt(paramValue
            .substring(0, paramValue.indexOf(',')));
    }
    throw new SQLException("Parameter " + parameterIndex
        + " is not a registered OUT parameter");
  }

  /**
   * Retrieves whether the last OUT parameter read had the value of SQL
   * <code>NULL</code>. Note that this method should be called only after
   * calling a getter method; otherwise, there is no value to use in determining
   * whether it is <code>null</code> or not.
   * 
   * @return <code>true</code> if the last parameter read was SQL
   *         <code>NULL</code>;<code>false</code> otherwise
   * @exception SQLException if a database access error occurs
   */
  public boolean wasNull() throws SQLException
  {
    if (lastOutParameterIndex == -1)
      return false;
    return (outParameters.get(new Integer(lastOutParameterIndex)) == null);
  }

  /**
   * Retrieves the value of the designated JDBC <code>CHAR</code>,
   * <code>VARCHAR</code>, or <code>LONGVARCHAR</code> parameter as a
   * <code>String</code> in the Java programming language.
   * <p>
   * For the fixed-length type JDBC <code>CHAR</code>, the
   * <code>String</code> object returned has exactly the same value the JDBC
   * <code>CHAR</code> value had in the database, including any padding added
   * by the database.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setString
   */
  public String getString(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.CHAR :
      case Types.VARCHAR :
      case Types.LONGVARCHAR :
        return (String) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>BIT</code> parameter as
   * a <code>boolean</code> in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>false</code>.
   * @exception SQLException if a database access error occurs
   * @see #setBoolean
   */
  public boolean getBoolean(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.BIT :
        Boolean b = (Boolean) outParameters.get(new Integer(parameterIndex));
        if (b == null)
          return false;
        else
          return b.booleanValue();
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>TINYINT</code> parameter
   * as a <code>byte</code> in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setByte
   */
  public byte getByte(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.TINYINT :
        Number b = (Number) outParameters.get(new Integer(parameterIndex));
        if (b == null)
          return 0;
        else
          return b.byteValue();
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>SMALLINT</code>
   * parameter as a <code>short</code> in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setShort
   */
  public short getShort(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.SMALLINT :
        Number s = (Number) outParameters.get(new Integer(parameterIndex));
        if (s == null)
          return 0;
        else
          return s.shortValue();
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>INTEGER</code> parameter
   * as an <code>int</code> in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setInt
   */
  public int getInt(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.INTEGER :
        Integer i = (Integer) outParameters.get(new Integer(parameterIndex));
        if (i == null)
          return 0;
        else
          return i.intValue();
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>BIGINT</code> parameter
   * as a <code>long</code> in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setLong
   */
  public long getLong(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.BIGINT :
        Long l = (Long) outParameters.get(new Integer(parameterIndex));
        if (l == null)
          return 0;
        else
          return l.longValue();
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>FLOAT</code> parameter
   * as a <code>float</code> in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setFloat
   */
  public float getFloat(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.FLOAT :
      case Types.REAL :
        Float f = (Float) outParameters.get(new Integer(parameterIndex));
        if (f == null)
          return 0;
        else
          return f.floatValue();
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>DOUBLE</code> parameter
   * as a <code>double</code> in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setDouble
   */
  public double getDouble(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.DOUBLE :
      case Types.FLOAT :
        Double d = (Double) outParameters.get(new Integer(parameterIndex));
        if (d == null)
          return 0;
        else
          return d.doubleValue();
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>NUMERIC</code> parameter
   * as a <code>java.math.BigDecimal</code> object with <i>scale </i> digits
   * to the right of the decimal point.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @param scale the number of digits to the right of the decimal point
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @deprecated use <code>getBigDecimal(int parameterIndex)</code> or
   *             <code>getBigDecimal(String parameterName)</code>
   * @see #setBigDecimal
   */
  public BigDecimal getBigDecimal(int parameterIndex, int scale)
      throws SQLException
  {
    return getBigDecimal(parameterIndex);
  }

  /**
   * Retrieves the value of the designated JDBC <code>BINARY</code> or
   * <code>VARBINARY</code> parameter as an array of <code>byte</code>
   * values in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setBytes
   */
  public byte[] getBytes(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.BINARY :
        return (byte[]) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }

  }

  /**
   * Retrieves the value of the designated JDBC <code>DATE</code> parameter as
   * a <code>java.sql.Date</code> object.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setDate(String, Date)
   */
  public Date getDate(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.DATE :
        return (Date) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>TIME</code> parameter as
   * a <code>java.sql.Time</code> object.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTime(String, Time)
   */
  public Time getTime(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.TIME :
        return (Time) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>TIMESTAMP</code>
   * parameter as a <code>java.sql.Timestamp</code> object.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTimestamp(String, Timestamp)
   */
  public Timestamp getTimestamp(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.TIMESTAMP :
        return (Timestamp) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  // ----------------------------------------------------------------------
  // Advanced features:

  /**
   * Retrieves the value of the designated parameter as an <code>Object</code>
   * in the Java programming language. If the value is an SQL <code>NULL</code>,
   * the driver returns a Java <code>null</code>.
   * <p>
   * This method returns a Java object whose type corresponds to the JDBC type
   * that was registered for this parameter using the method
   * <code>registerOutParameter</code>. By registering the target JDBC type
   * as <code>java.sql.Types.OTHER</code>, this method can be used to read
   * database-specific abstract data types.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return A <code>java.lang.Object</code> holding the OUT parameter value
   * @exception SQLException if a database access error occurs
   * @see java.sql.Types
   * @see #setObject(String, Object)
   */
  public Object getObject(int parameterIndex) throws SQLException
  {
    // Check type first (throw an exception if invalid)
    getOutParameterType(parameterIndex);
    return outParameters.get(new Integer(parameterIndex));
  }

  // --------------------------JDBC 2.0-----------------------------

  /**
   * Retrieves the value of the designated JDBC <code>NUMERIC</code> parameter
   * as a <code>java.math.BigDecimal</code> object with as many digits to the
   * right of the decimal point as the value contains.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value in full precision. If the value is SQL
   *         <code>NULL</code>, the result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setBigDecimal
   * @since 1.2
   */
  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.NUMERIC :
      case Types.DECIMAL :
        return (BigDecimal) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Returns an object representing the value of OUT parameter <code>i</code>
   * and uses <code>map</code> for the custom mapping of the parameter value.
   * <p>
   * This method returns a Java object whose type corresponds to the JDBC type
   * that was registered for this parameter using the method
   * <code>registerOutParameter</code>. By registering the target JDBC type
   * as <code>java.sql.Types.OTHER</code>, this method can be used to read
   * database-specific abstract data types.
   * 
   * @param i the first parameter is 1, the second is 2, and so on
   * @param map the mapping from SQL type names to Java classes
   * @return a <code>java.lang.Object</code> holding the OUT parameter value
   * @exception SQLException if a database access error occurs
   * @see #setObject(String, Object)
   * @since 1.2
   */
//  public Object getObject(int i, Map map) throws SQLException
//  {
//    throw new NotImplementedException("getObject");
//  }

  /**
   * Retrieves the value of the designated JDBC
   * <code>REF(&lt;structured-type&gt;)</code> parameter as a <code>Ref</code>
   * object in the Java programming language.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @return the parameter value as a <code>Ref</code> object in the Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.2
   */
  public Ref getRef(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.REF :
        return (Ref) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>BLOB</code> parameter as
   * a {@link Blob}object in the Java programming language.
   * 
   * @param i the first parameter is 1, the second is 2, and so on
   * @return the parameter value as a <code>Blob</code> object in the Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.2
   */
  public Blob getBlob(int i) throws SQLException
  {
    int type = getOutParameterType(i);
    switch (type)
    {
      case Types.BLOB :
        return (Blob) outParameters.get(new Integer(i));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>CLOB</code> parameter as
   * a <code>Clob</code> object in the Java programming language.
   * 
   * @param i the first parameter is 1, the second is 2, and so on
   * @return the parameter value as a <code>Clob</code> object in the Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.2
   */
  public Clob getClob(int i) throws SQLException
  {
    int type = getOutParameterType(i);
    switch (type)
    {
      case Types.CLOB :
        return (Clob) outParameters.get(new Integer(i));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>ARRAY</code> parameter
   * as an {@link Array}object in the Java programming language.
   * 
   * @param i the first parameter is 1, the second is 2, and so on
   * @return the parameter value as an <code>Array</code> object in the Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.2
   */
  public Array getArray(int i) throws SQLException
  {
    int type = getOutParameterType(i);
    switch (type)
    {
      case Types.ARRAY :
        return (Array) outParameters.get(new Integer(i));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Retrieves the value of the designated JDBC <code>DATE</code> parameter as
   * a <code>java.sql.Date</code> object, using the given
   * <code>Calendar</code> object to construct the date. With a
   * <code>Calendar</code> object, the driver can calculate the date taking
   * into account a custom timezone and locale. If no <code>Calendar</code>
   * object is specified, the driver uses the default timezone and locale.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the date
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setDate(String, Date)
   * @since 1.2
   */
  public Date getDate(int parameterIndex, Calendar cal) throws SQLException
  {
    return getDate(parameterIndex);
  }

  /**
   * Retrieves the value of the designated JDBC <code>TIME</code> parameter as
   * a <code>java.sql.Time</code> object, using the given
   * <code>Calendar</code> object to construct the time. With a
   * <code>Calendar</code> object, the driver can calculate the time taking
   * into account a custom timezone and locale. If no <code>Calendar</code>
   * object is specified, the driver uses the default timezone and locale.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the time
   * @return the parameter value; if the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTime(String, Time)
   * @since 1.2
   */
  public Time getTime(int parameterIndex, Calendar cal) throws SQLException
  {
    return getTime(parameterIndex);
  }

  /**
   * Retrieves the value of the designated JDBC <code>TIMESTAMP</code>
   * parameter as a <code>java.sql.Timestamp</code> object, using the given
   * <code>Calendar</code> object to construct the <code>Timestamp</code>
   * object. With a <code>Calendar</code> object, the driver can calculate the
   * timestamp taking into account a custom timezone and locale. If no
   * <code>Calendar</code> object is specified, the driver uses the default
   * timezone and locale.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2, and so on
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the timestamp
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTimestamp(String, Timestamp)
   * @since 1.2
   */
  public Timestamp getTimestamp(int parameterIndex, Calendar cal)
      throws SQLException
  {
    return getTimestamp(parameterIndex);
  }

  /**
   * Retrieves the value of the designated JDBC <code>DATALINK</code>
   * parameter as a <code>java.net.URL</code> object.
   * 
   * @param parameterIndex the first parameter is 1, the second is 2,...
   * @return a <code>java.net.URL</code> object that represents the JDBC
   *         <code>DATALINK</code> value used as the designated parameter
   * @exception SQLException if a database access error occurs, or if the URL
   *              being returned is not a valid URL on the Java platform
   * @see #setURL
   * @since 1.4
   */
  public URL getURL(int parameterIndex) throws SQLException
  {
    int type = getOutParameterType(parameterIndex);
    switch (type)
    {
      case Types.DATALINK :
        return (URL) outParameters.get(new Integer(parameterIndex));
      default :
        throw new SQLException("Cannot convert OUT parameter of type " + type
            + " to the requested type.");
    }
  }

  /**
   * Registers the OUT parameter named <code>parameterName</code> to the JDBC
   * type <code>sqlType</code>. All OUT parameters must be registered before
   * a stored procedure is executed.
   * <p>
   * The JDBC type specified by <code>sqlType</code> for an OUT parameter
   * determines the Java type that must be used in the <code>get</code> method
   * to read the value of that parameter.
   * <p>
   * If the JDBC type expected to be returned to this output parameter is
   * specific to this particular database, <code>sqlType</code> should be
   * <code>java.sql.Types.OTHER</code>. The method {@link #getObject(String)}
   * retrieves the value.
   * 
   * @param parameterName the name of the parameter
   * @param sqlType the JDBC type code defined by <code>java.sql.Types</code>.
   *          If the parameter is of JDBC type <code>NUMERIC</code> or
   *          <code>DECIMAL</code>, the version of
   *          <code>registerOutParameter</code> that accepts a scale value
   *          should be used.
   * @exception SQLException if a database access error occurs
   * @since 1.4
   * @see java.sql.Types
   */
  public void registerOutParameter(String parameterName, int sqlType)
      throws SQLException
  {
    setOutParameterWithTag(parameterName,
        PreparedStatementSerialization.REGISTER_OUT_PARAMETER, String
            .valueOf(sqlType));
  }

  /**
   * Registers the parameter named <code>parameterName</code> to be of JDBC
   * type <code>sqlType</code>. This method must be called before a stored
   * procedure is executed.
   * <p>
   * The JDBC type specified by <code>sqlType</code> for an OUT parameter
   * determines the Java type that must be used in the <code>get</code> method
   * to read the value of that parameter.
   * <p>
   * This version of <code>registerOutParameter</code> should be used when the
   * parameter is of JDBC type <code>NUMERIC</code> or <code>DECIMAL</code>.
   * 
   * @param parameterName the name of the parameter
   * @param sqlType SQL type code defined by <code>java.sql.Types</code>.
   * @param scale the desired number of digits to the right of the decimal
   *          point. It must be greater than or equal to zero.
   * @exception SQLException if a database access error occurs
   * @since 1.4
   * @see java.sql.Types
   */
  public void registerOutParameter(String parameterName, int sqlType, int scale)
      throws SQLException
  {
    setOutParameterWithTag(parameterName,
        PreparedStatementSerialization.REGISTER_OUT_PARAMETER_WITH_SCALE,
        String.valueOf(sqlType) + "," + scale);
  }

  /**
   * Registers the designated output parameter. This version of the method
   * <code>registerOutParameter</code> should be used for a user-named or REF
   * output parameter. Examples of user-named types include: STRUCT, DISTINCT,
   * JAVA_OBJECT, and named array types.
   * <p>
   * Before executing a stored procedure call, you must explicitly call
   * <code>registerOutParameter</code> to register the type from
   * <code>java.sql.Types</code> for each OUT parameter. For a user-named
   * parameter the fully-qualified SQL type name of the parameter should also be
   * given, while a REF parameter requires that the fully-qualified type name of
   * the referenced type be given. A JDBC driver that does not need the type
   * code and type name information may ignore it. To be portable, however,
   * applications should always provide these values for user-named and REF
   * parameters.
   * <p>
   * Although it is intended for user-named and REF parameters, this method may
   * be used to register a parameter of any JDBC type. If the parameter does not
   * have a user-named or REF type, the typeName parameter is ignored.
   * <p>
   * <b>Note: </b> When reading the value of an out parameter, you must use the
   * <code>getXXX</code> method whose Java type XXX corresponds to the
   * parameter's registered SQL type.
   * 
   * @param parameterName the name of the parameter
   * @param sqlType a value from {@link java.sql.Types}
   * @param typeName the fully-qualified name of an SQL structured type
   * @exception SQLException if a database access error occurs
   * @see java.sql.Types
   * @since 1.4
   */
  public void registerOutParameter(String parameterName, int sqlType,
      String typeName) throws SQLException
  {
    setOutParameterWithTag(parameterName,
        PreparedStatementSerialization.REGISTER_OUT_PARAMETER_WITH_NAME, String
            .valueOf(sqlType)
            + "," + typeName);
  }

  /**
   * Stores an out parameter and its type as a <em>quoted</em> String, so the
   * controller can decode them back.
   * 
   * @param paramName the name of the parameter (index must be converted to
   *          String)
   * @param typeTag type of the parameter
   * @param paramValue the parameter string to be stored
   * @see PreparedStatementSerialization#setPreparedStatement(String,
   *      java.sql.PreparedStatement)
   */
  private void setOutParameterWithTag(String paramName, String typeTag,
      String paramValue)
  {
    if (outAndNamedParameterTypes == null)
      outAndNamedParameterTypes = new HashMap<String, Object>();

    /**
     * insert TAGS so the controller can parse and "unset" the request using
     * {@link PreparedStatementSerialization#setPreparedStatement(String, java.sql.PreparedStatement) 
     */
    outAndNamedParameterTypes.put(paramName,
        PreparedStatementSerialization.START_PARAM_TAG + typeTag + paramName
            + "," + paramValue + PreparedStatementSerialization.END_PARAM_TAG);
  }

  /**
   * Stores a named parameter and its type as a <em>quoted</em> String, so the
   * controller can decode them back.
   * 
   * @param paramName the name of the parameter
   * @param typeTag type of the parameter
   * @param param the parameter string to be stored
   * @see PreparedStatementSerialization#setPreparedStatement(String,
   *      java.sql.PreparedStatement)
   */
  private void setNamedParameterWithTag(String paramName, String typeTag,
      String param)
  {
    if (outAndNamedParameterTypes == null)
      outAndNamedParameterTypes = new HashMap<String, Object>();

    /**
     * insert TAGS so the controller can parse and "unset" the request using
     * {@link PreparedStatementSerialization#setPreparedStatement(String, java.sql.PreparedStatement) 
     */
    outAndNamedParameterTypes.put(paramName,
        PreparedStatementSerialization.START_PARAM_TAG
            + PreparedStatementSerialization.NAMED_PARAMETER_TAG
            + paramName
            + ","
            + typeTag
            + Strings.replace(param, PreparedStatementSerialization.TAG_MARKER,
                PreparedStatementSerialization.TAG_MARKER_ESCAPE)
            + PreparedStatementSerialization.END_PARAM_TAG);
  }

  /**
   * Sets the designated parameter to the given <code>java.net.URL</code>
   * object. The driver converts this to an SQL <code>DATALINK</code> value
   * when it sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param url the parameter value
   * @exception SQLException if a database access error occurs, or if a URL is
   *              malformed
   * @see #getURL(String)
   * @since 1.4
   */
  public void setURL(String parameterName, URL url) throws SQLException
  {
    if (url == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.URL_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.URL_TAG, url.toString());
  }

  /**
   * Sets the designated parameter to SQL <code>NULL</code>.
   * <p>
   * <b>Note: </b> you must specify the parameter's SQL type.
   * 
   * @param parameterName the name of the parameter
   * @param sqlType the SQL type code defined in <code>java.sql.Types</code>
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public void setNull(String parameterName, int sqlType) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.NULL_VALUE, String.valueOf(sqlType));
  }

  /**
   * Sets the designated parameter to the given Java <code>boolean</code>
   * value. The driver converts this to an SQL <code>BIT</code> value when it
   * sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getBoolean(String)
   * @since 1.4
   */
  public void setBoolean(String parameterName, boolean x) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.BOOLEAN_TAG, String.valueOf(x));
  }

  /**
   * Sets the designated parameter to the given Java <code>byte</code> value.
   * The driver converts this to an SQL <code>TINYINT</code> value when it
   * sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getByte(String)
   * @since 1.4
   */
  public void setByte(String parameterName, byte x) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.BYTE_TAG, Integer.toString(x));
  }

  /**
   * Sets the designated parameter to the given Java <code>short</code> value.
   * The driver converts this to an SQL <code>SMALLINT</code> value when it
   * sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getShort(String)
   * @since 1.4
   */
  public void setShort(String parameterName, short x) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.SHORT_TAG, Integer.toString(x));
  }

  /**
   * Sets the designated parameter to the given Java <code>int</code> value.
   * The driver converts this to an SQL <code>INTEGER</code> value when it
   * sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getInt(String)
   * @since 1.4
   */
  public void setInt(String parameterName, int x) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.INTEGER_TAG, Integer.toString(x));
  }

  /**
   * Sets the designated parameter to the given Java <code>long</code> value.
   * The driver converts this to an SQL <code>BIGINT</code> value when it
   * sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getLong(String)
   * @since 1.4
   */
  public void setLong(String parameterName, long x) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.LONG_TAG, Long.toString(x));
  }

  /**
   * Sets the designated parameter to the given Java <code>float</code> value.
   * The driver converts this to an SQL <code>FLOAT</code> value when it sends
   * it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getFloat(String)
   * @since 1.4
   */
  public void setFloat(String parameterName, float x) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.FLOAT_TAG, Float.toString(x));
  }

  /**
   * Sets the designated parameter to the given Java <code>double</code>
   * value. The driver converts this to an SQL <code>DOUBLE</code> value when
   * it sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getDouble(String)
   * @since 1.4
   */
  public void setDouble(String parameterName, double x) throws SQLException
  {
    setNamedParameterWithTag(parameterName,
        PreparedStatementSerialization.DOUBLE_TAG, Double.toString(x));
  }

  /**
   * Sets the designated parameter to the given
   * <code>java.math.BigDecimal</code> value. The driver converts this to an
   * SQL <code>NUMERIC</code> value when it sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getBigDecimal(String)
   * @since 1.4
   */
  public void setBigDecimal(String parameterName, BigDecimal x)
      throws SQLException
  {
    if (x == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.BIG_DECIMAL_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.BIG_DECIMAL_TAG, x.toString());
  }

  /**
   * Sets the designated parameter to the given Java <code>String</code>
   * value. The driver converts this to an SQL <code>VARCHAR</code> or
   * <code>LONGVARCHAR</code> value (depending on the argument's size relative
   * to the driver's limits on <code>VARCHAR</code> values) when it sends it
   * to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getString(String)
   * @since 1.4
   */
  public void setString(String parameterName, String x) throws SQLException
  {
    if (x == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.STRING_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
    {
      if (PreparedStatementSerialization.NULL_VALUE.equals(x))
      { // Someone is trying to set a String that matches our NULL tag, a real
        // bad luck, use our special NULL_STRING_TAG!
        setNamedParameterWithTag(parameterName,
            PreparedStatementSerialization.NULL_STRING_TAG, x);
      }
      else
      { // No escape processing is needed for queries not being parsed into
        // statements.
        setNamedParameterWithTag(parameterName,
            PreparedStatementSerialization.STRING_TAG, x);
      }
    }
  }

  /**
   * Sets the designated parameter to the given Java array of bytes. The driver
   * converts this to an SQL <code>VARBINARY</code> or
   * <code>LONGVARBINARY</code> (depending on the argument's size relative to
   * the driver's limits on <code>VARBINARY</code> values) when it sends it to
   * the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getBytes(String)
   * @since 1.4
   */
  public void setBytes(String parameterName, byte[] x) throws SQLException
  {
    try
    {
      /**
       * Encoded only for request inlining. Decoded right away by the controller
       * at static
       * {@link #setPreparedStatement(String, java.sql.PreparedStatement)}
       */
      String encodedString = AbstractBlobFilter.getDefaultBlobFilter()
          .encode(x);
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.BYTES_TAG, encodedString);
    }
    catch (OutOfMemoryError oome)
    {
      System.gc();
      throw new SQLException("Out of memory while encoding bytes");
    }
  }

  /**
   * Sets the designated parameter to the given <code>java.sql.Date</code>
   * value. The driver converts this to an SQL <code>DATE</code> value when it
   * sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getDate(String)
   * @since 1.4
   */
  public void setDate(String parameterName, Date x) throws SQLException
  {
    if (x == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.DATE_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.DATE_TAG, new java.sql.Date(x
              .getTime()).toString());
  }

  /**
   * Sets the designated parameter to the given <code>java.sql.Time</code>
   * value. The driver converts this to an SQL <code>TIME</code> value when it
   * sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getTime(String)
   * @since 1.4
   */
  public void setTime(String parameterName, Time x) throws SQLException
  {
    if (x == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.TIME_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.TIME_TAG, x.toString());
  }

  /**
   * Sets the designated parameter to the given <code>java.sql.Timestamp</code>
   * value. The driver converts this to an SQL <code>TIMESTAMP</code> value
   * when it sends it to the database.
   * 
   * @param parameterName the name of the parameter
   * @param x the parameter value
   * @exception SQLException if a database access error occurs
   * @see #getTimestamp(String)
   * @since 1.4
   */
  public void setTimestamp(String parameterName, Timestamp x)
      throws SQLException
  {
    if (x == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.TIMESTAMP_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
    {
      if (x.getClass().equals(Timestamp.class))
        setNamedParameterWithTag(parameterName,
            PreparedStatementSerialization.TIMESTAMP_TAG, new Timestamp(x
                .getTime()).toString());
    }
  }

  /**
   * Sets the designated parameter to the given input stream, which will have
   * the specified number of bytes. When a very large ASCII value is input to a
   * <code>LONGVARCHAR</code> parameter, it may be more practical to send it
   * via a <code>java.io.InputStream</code>. Data will be read from the
   * stream as needed until end-of-file is reached. The JDBC driver will do any
   * necessary conversion from ASCII to the database char format.
   * <p>
   * <b>Note: </b> This stream object can either be a standard Java stream
   * object or your own subclass that implements the standard interface.
   * 
   * @param parameterName the name of the parameter
   * @param x the Java input stream that contains the ASCII parameter value
   * @param length the number of bytes in the stream
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public void setAsciiStream(String parameterName, InputStream x, int length)
      throws SQLException
  {
    setBinaryStream(parameterName, x, length);
  }

  /**
   * Sets the designated parameter to the given input stream, which will have
   * the specified number of bytes. When a very large binary value is input to a
   * <code>LONGVARBINARY</code> parameter, it may be more practical to send it
   * via a <code>java.io.InputStream</code> object. The data will be read from
   * the stream as needed until end-of-file is reached.
   * <p>
   * <b>Note: </b> This stream object can either be a standard Java stream
   * object or your own subclass that implements the standard interface.
   * 
   * @param parameterName the name of the parameter
   * @param x the java input stream which contains the binary parameter value
   * @param length the number of bytes in the stream
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public void setBinaryStream(String parameterName, InputStream x, int length)
      throws SQLException
  {
    byte[] data = new byte[length];
    try
    {
      x.read(data, 0, length);
    }
    catch (Exception ioe)
    {
      throw new SQLException("Problem with streaming of data");
    }
    // TODO: optimize me and avoid the copy thanks to a new setBytesFromStream()
    // setBytes does the blob filter encoding.
    setBytes(parameterName, data);
  }

  /**
   * Sets the value of the designated parameter with the given object. The
   * second argument must be an object type; for integral values, the
   * <code>java.lang</code> equivalent objects should be used.
   * <p>
   * The given Java object will be converted to the given
   * <code>targetSqlType</code> before being sent to the database.
   * <p>
   * If the object has a custom mapping (is of a class implementing the
   * interface <code>SQLData</code>), the JDBC driver should call the method
   * <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on
   * the other hand, the object is of a class implementing <code>Ref</code>,
   * <code>Blob</code>,<code>Clob</code>,<code>Struct</code>, or
   * <code>Array</code>, the driver should pass it to the database as a value
   * of the corresponding SQL type.
   * <p>
   * Note that this method may be used to pass datatabase-specific abstract data
   * types.
   * 
   * @param parameterName the name of the parameter
   * @param x the object containing the input parameter value
   * @param targetSqlType the SQL type (as defined in
   *          <code>java.sql.Types</code>) to be sent to the database. The
   *          scale argument may further qualify this type.
   * @param scale for <code>java.sql.Types.DECIMAL</code> or
   *          <code>java.sql.Types.NUMERIC</code> types, this is the number of
   *          digits after the decimal point. For all other types, this value
   *          will be ignored.
   * @exception SQLException if a database access error occurs
   * @see java.sql.Types
   * @see #getObject(String, Map)
   * @since 1.4
   */
  public void setObject(String parameterName, Object x, int targetSqlType,
      int scale) throws SQLException
  {
    if (x == null)
    {
      setNull(parameterName, targetSqlType);
      return;
    }

    try
    {
      boolean failed = false;
      switch (targetSqlType)
      {
        /**
         * Reference is table "Conversions Performed by setObject()..." in JDBC
         * Reference Book (table 47.9.5 in 2nd edition, 50.5 in 3rd edition).
         * Also available online in Sun's "JDBC Technology Guide: Getting
         * Started", section "Mapping SQL and Java Types".
         */
        // Some drivers (at least postgresql > 8.1) don't accept setInt for tiny
        // and small ints. We can safely use setShort instead. See bug
        // SEQUOIA-543
        // setShort().
        case Types.TINYINT :
        case Types.SMALLINT :
          if (x instanceof Number)
            setShort(parameterName, ((Number) x).shortValue());
          else if (x instanceof Boolean)
            setShort(parameterName, ((Boolean) x).booleanValue()
                ? (short) 1
                : (short) 0);
          else if (x instanceof String)
            setInt(parameterName, Short.parseShort((String) x));
          else
            failed = true;
          break;
        // setInt()
        case Types.INTEGER :
          if (x instanceof Number)
            setInt(parameterName, ((Number) x).intValue());
          else if (x instanceof Boolean)
            setInt(parameterName, ((Boolean) x).booleanValue() ? 1 : 0);
          else if (x instanceof String)
            setInt(parameterName, Integer.parseInt((String) x));
          else
            failed = true;
          break;
        // setLong()
        case Types.BIGINT :
          if (x instanceof Number)
            setLong(parameterName, ((Number) x).longValue());
          else if (x instanceof String)
            setLong(parameterName, Long.parseLong((String) x));
          else if (x instanceof Boolean)
            setLong(parameterName, ((Boolean) x).booleanValue() ? 1 : 0);
          else
            failed = true;
          break;
        // setDouble()
        case Types.REAL :
        case Types.FLOAT :
        case Types.DOUBLE :
          if (x instanceof Number)
            setDouble(parameterName, ((Number) x).doubleValue());
          else if (x instanceof String)
            setDouble(parameterName, Double.parseDouble((String) x));
          else if (x instanceof Boolean)
            setDouble(parameterName, ((Boolean) x).booleanValue() ? 1 : 0);
          else
            failed = true;
          break;
        // setBigDecimal()
        case Types.DECIMAL :
        case Types.NUMERIC :
          BigDecimal bd;
          if (x instanceof Boolean)
            bd = new BigDecimal(((Boolean) x).booleanValue() ? 1d : 0d);
          else if (x instanceof Number)
            bd = new BigDecimal(((Number) x).toString());
          else if (x instanceof String)
            bd = new BigDecimal((String) x);
          else
          {
            failed = true;
            break;
          }
          bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
          setBigDecimal(parameterName, bd);
          break;
        // setBoolean()
        case Types.BIT :
        case Types.BOOLEAN :
          if (x instanceof Number)
            setBoolean(parameterName, 0 != ((Number) x).longValue());
          else if (x instanceof Boolean)
            setBoolean(parameterName, ((Boolean) x).booleanValue());
          else if (x instanceof String)
            setBoolean(parameterName, Boolean.valueOf((String) x)
                .booleanValue());
          else
            failed = true;
          break;
        // setString()
        case Types.CHAR :
        case Types.VARCHAR :
        case Types.LONGVARCHAR :
          setString(parameterName, x.toString());
          break;
        // setBytes(), setBlob(),...
        case Types.BINARY :
        case Types.VARBINARY :
        case Types.LONGVARBINARY :
          if (x instanceof byte[])
            setBytes(parameterName, (byte[]) x);
          else if (x instanceof Serializable)
            // Try it as an Object (serialized in bytes in setObject below)
            setObject(parameterName, x);
          else
            failed = true;
          break;
        // setDate()
        case Types.DATE :
          if (x instanceof String)
            setDate(parameterName, java.sql.Date.valueOf((String) x));
          else if (x instanceof java.sql.Date)
            setDate(parameterName, (java.sql.Date) x);
          else if (x instanceof Timestamp)
            setDate(parameterName, new java.sql.Date(((Timestamp) x).getTime()));
          else
            failed = true;
          break;
        // setTime()
        case Types.TIME :
          if (x instanceof String)
            setTime(parameterName, Time.valueOf((String) x));
          else if (x instanceof Time)
            setTime(parameterName, (Time) x);
          else if (x instanceof Timestamp)
            setTime(parameterName, new Time(((Timestamp) x).getTime()));
          else
            failed = true;
          break;
        // setTimeStamp()
        case Types.TIMESTAMP :
          if (x instanceof String)
            setTimestamp(parameterName, Timestamp.valueOf((String) x));
          else if (x instanceof Date)
            setTimestamp(parameterName, new Timestamp(((Date) x).getTime()));
          else if (x instanceof Timestamp)
            setTimestamp(parameterName, (Timestamp) x);
          else
            failed = true;
          break;
        // setBlob()
        case Types.BLOB :
          failed = true;
          break;
        // setURL()
        case Types.DATALINK :
          if (x instanceof java.net.URL)
            setURL(parameterName, (java.net.URL) x);
          else
            setURL(parameterName, new java.net.URL(x.toString()));
          break;
        case Types.JAVA_OBJECT :
        case Types.OTHER :
          // let's ignore the unknown target type given.
          setObject(parameterName, x);
          break;
        default :
          throw new SQLException("Unsupported type value");
      }
      if (true == failed)
        throw new IllegalArgumentException(
            "Attempt to perform an illegal conversion");
    }
    catch (Exception e)
    {
      SQLException outE = new SQLException("Exception while converting type "
          + x.getClass() + " to SQL type " + targetSqlType);
      outE.initCause(e);
      throw outE;
    }
  }

  /**
   * Sets the value of the designated parameter with the given object. This
   * method is like the method <code>setObject</code> above, except that it
   * assumes a scale of zero.
   * 
   * @param parameterName the name of the parameter
   * @param x the object containing the input parameter value
   * @param targetSqlType the SQL type (as defined in
   *          <code>java.sql.Types</code>) to be sent to the database
   * @exception SQLException if a database access error occurs
   * @see #getObject(String, Map)
   * @since 1.4
   */
  public void setObject(String parameterName, Object x, int targetSqlType)
      throws SQLException
  {
    setObject(parameterName, x, targetSqlType, 0);
  }

  /**
   * Sets the value of the designated parameter with the given object. The
   * second parameter must be of type <code>Object</code>; therefore, the
   * <code>java.lang</code> equivalent objects should be used for built-in
   * types.
   * <p>
   * The JDBC specification specifies a standard mapping from Java
   * <code>Object</code> types to SQL types. The given argument will be
   * converted to the corresponding SQL type before being sent to the database.
   * <p>
   * Note that this method may be used to pass datatabase-specific abstract data
   * types, by using a driver-specific Java type.
   * <p>
   * If the object is of a class implementing the interface <code>SQLData</code>,
   * the JDBC driver should call the method <code>SQLData.writeSQL</code> to
   * write it to the SQL data stream. If, on the other hand, the object is of a
   * class implementing <code>Ref</code>,<code>Blob</code>,
   * <code>Clob</code>,<code>Struct</code>, or <code>Array</code>, the
   * driver should pass it to the database as a value of the corresponding SQL
   * type.
   * <p>
   * This method throws an exception if there is an ambiguity, for example, if
   * the object is of a class implementing more than one of the interfaces named
   * above.
   * 
   * @param parameterName the name of the parameter
   * @param x the object containing the input parameter value
   * @exception SQLException if a database access error occurs or if the given
   *              <code>Object</code> parameter is ambiguous
   * @see #getObject(String, Map)
   * @since 1.4
   */
  public void setObject(String parameterName, Object x) throws SQLException
  {
    if (x == null)
    {
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.OBJECT_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    }
    else
    { // This is an optimization, faster than going through
      // the generic setObject() method and calling instanceof again.
      // This has to be in the end equivalent to
      // setObject(index, object, DEFAULT_targetSqlType, 0)
      // where DEFAULT_targetSqlType is defined in table:
      // "Java Object Type Mapped to JDBC Types".
      // It's currently not exactly the same, since generic setObject()
      // is not strict enough in its conversions. For instance
      // setObject(target=Float) actually calls "setDouble()" -- MH.

      if (x instanceof String)
        setString(parameterName, (String) x);
      else if (x instanceof BigDecimal)
        setBigDecimal(parameterName, (BigDecimal) x);
      else if (x instanceof Boolean)
        setBoolean(parameterName, ((Boolean) x).booleanValue());
      else if (x instanceof Short)
        setShort(parameterName, ((Short) x).shortValue());
      else if (x instanceof Integer)
        setInt(parameterName, ((Integer) x).intValue());
      else if (x instanceof Long)
        setLong(parameterName, ((Long) x).longValue());
      else if (x instanceof Float)
        setFloat(parameterName, ((Float) x).floatValue());
      else if (x instanceof Double)
        setDouble(parameterName, ((Double) x).doubleValue());
      else if (x instanceof byte[])
        setBytes(parameterName, (byte[]) x);
      else if (x instanceof java.sql.Date)
        setDate(parameterName, (java.sql.Date) x);
      else if (x instanceof Time)
        setTime(parameterName, (Time) x);
      else if (x instanceof Timestamp)
        setTimestamp(parameterName, (Timestamp) x);
      else if (x instanceof java.net.URL)
        setURL(parameterName, (java.net.URL) x);
      else if (x instanceof Serializable)
      {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        try
        {
          // Serialize object to byte array
          ObjectOutputStream objectOutputStream = new ObjectOutputStream(
              byteOutputStream);
          objectOutputStream.writeObject(x);
          objectOutputStream.close();
          synchronized (this.sbuf)
          {
            this.sbuf.setLength(0);
            /**
             * Encoded only for request inlining. Decoded right away by the
             * controller
             */
            this.sbuf.append(AbstractBlobFilter.getDefaultBlobFilter().encode(
                byteOutputStream.toByteArray()));
            setNamedParameterWithTag(parameterName,
                PreparedStatementSerialization.OBJECT_TAG, trimStringBuffer());
          }
        }
        catch (IOException e)
        {
          throw new SQLException("Failed to serialize object: " + e);
        }
      }
      else
        throw new SQLException("Objects of type " + x.getClass()
            + " are not supported.");
    }
  }

  /**
   * Sets the designated parameter to the given <code>Reader</code> object,
   * which is the given number of characters long. When a very large UNICODE
   * value is input to a <code>LONGVARCHAR</code> parameter, it may be more
   * practical to send it via a <code>java.io.Reader</code> object. The data
   * will be read from the stream as needed until end-of-file is reached. The
   * JDBC driver will do any necessary conversion from UNICODE to the database
   * char format.
   * <p>
   * <b>Note: </b> This stream object can either be a standard Java stream
   * object or your own subclass that implements the standard interface.
   * 
   * @param parameterName the name of the parameter
   * @param reader the <code>java.io.Reader</code> object that contains the
   *          UNICODE data used as the designated parameter
   * @param length the number of characters in the stream
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public void setCharacterStream(String parameterName, Reader reader, int length)
      throws SQLException
  {
    char[] data = new char[length];
    try
    {
      reader.read(data, 0, length);
    }
    catch (Exception ioe)
    {
      throw new SQLException("Problem with streaming of data");
    }
    setString(parameterName, new String(data));
  }

  /**
   * Sets the designated parameter to the given <code>java.sql.Date</code>
   * value, using the given <code>Calendar</code> object. The driver uses the
   * <code>Calendar</code> object to construct an SQL <code>DATE</code>
   * value, which the driver then sends to the database. With a a
   * <code>Calendar</code> object, the driver can calculate the date taking
   * into account a custom timezone. If no <code>Calendar</code> object is
   * specified, the driver uses the default timezone, which is that of the
   * virtual machine running the application.
   * 
   * @param parameterName the name of the parameter
   * @param d the parameter value
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the date
   * @exception SQLException if a database access error occurs
   * @see #getDate(String, Calendar)
   * @since 1.4
   */
  public void setDate(String parameterName, Date d, Calendar cal)
      throws SQLException
  {
    if (d == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.DATE_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
    {
      if (cal == null)
        setDate(parameterName, d);
      else
      {
        cal.setTime(d);
        setDate(parameterName, new java.sql.Date(cal.getTime().getTime()));
      }
    }
  }

  /**
   * Sets the designated parameter to the given <code>java.sql.Time</code>
   * value, using the given <code>Calendar</code> object. The driver uses the
   * <code>Calendar</code> object to construct an SQL <code>TIME</code>
   * value, which the driver then sends to the database. With a a
   * <code>Calendar</code> object, the driver can calculate the time taking
   * into account a custom timezone. If no <code>Calendar</code> object is
   * specified, the driver uses the default timezone, which is that of the
   * virtual machine running the application.
   * 
   * @param parameterName the name of the parameter
   * @param t the parameter value
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the time
   * @exception SQLException if a database access error occurs
   * @see #getTime(String, Calendar)
   * @since 1.4
   */
  public void setTime(String parameterName, Time t, Calendar cal)
      throws SQLException
  {
    if (t == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.TIME_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
    {
      if (cal == null)
        setTime(parameterName, t);
      else
      {
        cal.setTime(t);
        setTime(parameterName, new java.sql.Time(cal.getTime().getTime()));
      }
    }
  }

  /**
   * Sets the designated parameter to the given <code>java.sql.Timestamp</code>
   * value, using the given <code>Calendar</code> object. The driver uses the
   * <code>Calendar</code> object to construct an SQL <code>TIMESTAMP</code>
   * value, which the driver then sends to the database. With a a
   * <code>Calendar</code> object, the driver can calculate the timestamp
   * taking into account a custom timezone. If no <code>Calendar</code> object
   * is specified, the driver uses the default timezone, which is that of the
   * virtual machine running the application.
   * 
   * @param parameterName the name of the parameter
   * @param t the parameter value
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the timestamp
   * @exception SQLException if a database access error occurs
   * @see #getTimestamp(String, Calendar)
   * @since 1.4
   */
  public void setTimestamp(String parameterName, Timestamp t, Calendar cal)
      throws SQLException
  {
    if (t == null)
      setNamedParameterWithTag(parameterName,
          PreparedStatementSerialization.TIMESTAMP_TAG,
          PreparedStatementSerialization.NULL_VALUE);
    else
    {
      if (cal == null)
        setTimestamp(parameterName, t);
      else
      {
        cal.setTime(t);
        setTimestamp(parameterName, new java.sql.Timestamp(cal.getTime()
            .getTime()));
      }
    }
  }

  /**
   * Sets the designated parameter to SQL <code>NULL</code>. This version of
   * the method <code>setNull</code> should be used for user-defined types and
   * REF type parameters. Examples of user-defined types include: STRUCT,
   * DISTINCT, JAVA_OBJECT, and named array types.
   * <p>
   * <b>Note: </b> to be portable, applications must give the SQL type code and
   * the fully-qualified SQL type name when specifying a NULL user-defined or
   * REF parameter. In the case of a user-defined type the name is the type name
   * of the parameter itself. For a REF parameter, the name is the type name of
   * the referenced type. If a JDBC driver does not need the type code or type
   * name information, it may ignore it.
   * <p>
   * Although it is intended for user-defined and Ref parameters, this method
   * may be used to set a null parameter of any JDBC type. If the parameter does
   * not have a user-defined or REF type, the given typeName is ignored.
   * 
   * @param parameterName the name of the parameter
   * @param sqlType a value from <code>java.sql.Types</code>
   * @param typeName the fully-qualified name of an SQL user-defined type;
   *          ignored if the parameter is not a user-defined type or SQL
   *          <code>REF</code> value
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public void setNull(String parameterName, int sqlType, String typeName)
      throws SQLException
  {
    setNull(parameterName, sqlType);
  }

  /**
   * Retrieves the value of a JDBC <code>CHAR</code>,<code>VARCHAR</code>,
   * or <code>LONGVARCHAR</code> parameter as a <code>String</code> in the
   * Java programming language.
   * <p>
   * For the fixed-length type JDBC <code>CHAR</code>, the
   * <code>String</code> object returned has exactly the same value the JDBC
   * <code>CHAR</code> value had in the database, including any padding added
   * by the database.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setString
   * @since 1.4
   */
  public String getString(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (String) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>BIT</code> parameter as a
   * <code>boolean</code> in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>false</code>.
   * @exception SQLException if a database access error occurs
   * @see #setBoolean
   * @since 1.4
   */
  public boolean getBoolean(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      Boolean b = (Boolean) namedParameterValues.get(parameterName);
      if (b == null)
        return false;
      else
        return b.booleanValue();
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>TINYINT</code> parameter as a
   * <code>byte</code> in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setByte
   * @since 1.4
   */
  public byte getByte(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      Byte b = (Byte) namedParameterValues.get(parameterName);
      if (b == null)
        return 0;
      else
        return b.byteValue();
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>SMALLINT</code> parameter as a
   * <code>short</code> in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setShort
   * @since 1.4
   */
  public short getShort(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      Short s = (Short) namedParameterValues.get(parameterName);
      if (s == null)
        return 0;
      else
        return s.shortValue();
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>INTEGER</code> parameter as an
   * <code>int</code> in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setInt
   * @since 1.4
   */
  public int getInt(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      Integer i = (Integer) namedParameterValues.get(parameterName);
      if (i == null)
        return 0;
      else
        return i.intValue();
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>BIGINT</code> parameter as a
   * <code>long</code> in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setLong
   * @since 1.4
   */
  public long getLong(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      Long l = (Long) namedParameterValues.get(parameterName);
      if (l == null)
        return 0;
      else
        return l.longValue();
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>FLOAT</code> parameter as a
   * <code>float</code> in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setFloat
   * @since 1.4
   */
  public float getFloat(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      Float f = (Float) namedParameterValues.get(parameterName);
      if (f == null)
        return 0;
      else
        return f.floatValue();
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>DOUBLE</code> parameter as a
   * <code>double</code> in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>0</code>.
   * @exception SQLException if a database access error occurs
   * @see #setDouble
   * @since 1.4
   */
  public double getDouble(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      Double d = (Double) namedParameterValues.get(parameterName);
      if (d == null)
        return 0;
      else
        return d.doubleValue();
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>BINARY</code> or
   * <code>VARBINARY</code> parameter as an array of <code>byte</code>
   * values in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setBytes
   * @since 1.4
   */
  public byte[] getBytes(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (byte[]) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>DATE</code> parameter as a
   * <code>java.sql.Date</code> object.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setDate(String, Date)
   * @since 1.4
   */
  public Date getDate(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (Date) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>TIME</code> parameter as a
   * <code>java.sql.Time</code> object.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTime(String, Time)
   * @since 1.4
   */
  public Time getTime(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (Time) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
   * <code>java.sql.Timestamp</code> object.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTimestamp(String, Timestamp)
   * @since 1.4
   */
  public Timestamp getTimestamp(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (Timestamp) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a parameter as an <code>Object</code> in the Java
   * programming language. If the value is an SQL <code>NULL</code>, the
   * driver returns a Java <code>null</code>.
   * <p>
   * This method returns a Java object whose type corresponds to the JDBC type
   * that was registered for this parameter using the method
   * <code>registerOutParameter</code>. By registering the target JDBC type
   * as <code>java.sql.Types.OTHER</code>, this method can be used to read
   * database-specific abstract data types.
   * 
   * @param parameterName the name of the parameter
   * @return A <code>java.lang.Object</code> holding the OUT parameter value.
   * @exception SQLException if a database access error occurs
   * @see java.sql.Types
   * @see #setObject(String, Object)
   * @since 1.4
   */
  public Object getObject(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>NUMERIC</code> parameter as a
   * <code>java.math.BigDecimal</code> object with as many digits to the right
   * of the decimal point as the value contains.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value in full precision. If the value is SQL
   *         <code>NULL</code>, the result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setBigDecimal
   * @since 1.4
   */
  public BigDecimal getBigDecimal(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (BigDecimal) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Returns an object representing the value of OUT parameter <code>i</code>
   * and uses <code>map</code> for the custom mapping of the parameter value.
   * <p>
   * This method returns a Java object whose type corresponds to the JDBC type
   * that was registered for this parameter using the method
   * <code>registerOutParameter</code>. By registering the target JDBC type
   * as <code>java.sql.Types.OTHER</code>, this method can be used to read
   * database-specific abstract data types.
   * 
   * @param parameterName the name of the parameter
   * @param map the mapping from SQL type names to Java classes
   * @return a <code>java.lang.Object</code> holding the OUT parameter value
   * @exception SQLException if a database access error occurs
   * @see #setObject(String, Object)
   * @since 1.4
   */
//  public Object getObject(String parameterName, Map map) throws SQLException
//  {
//    throw new NotImplementedException("getObject");
//  }

  /**
   * Retrieves the value of a JDBC <code>REF(&lt;structured-type&gt;)</code>
   * parameter as a <code>Ref</code> object in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value as a <code>Ref</code> object in the Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public Ref getRef(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (Ref) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>BLOB</code> parameter as a
   * {@link Blob}object in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value as a <code>Blob</code> object in the Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public Blob getBlob(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (Blob) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>CLOB</code> parameter as a
   * <code>Clob</code> object in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value as a <code>Clob</code> object in the Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public Clob getClob(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (Clob) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>ARRAY</code> parameter as an
   * {@link Array}object in the Java programming language.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value as an <code>Array</code> object in Java
   *         programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs
   * @since 1.4
   */
  public Array getArray(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (Array) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

  /**
   * Retrieves the value of a JDBC <code>DATE</code> parameter as a
   * <code>java.sql.Date</code> object, using the given <code>Calendar</code>
   * object to construct the date. With a <code>Calendar</code> object, the
   * driver can calculate the date taking into account a custom timezone and
   * locale. If no <code>Calendar</code> object is specified, the driver uses
   * the default timezone and locale.
   * 
   * @param parameterName the name of the parameter
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the date
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setDate(String, Date, Calendar)
   * @since 1.4
   */
  public Date getDate(String parameterName, Calendar cal) throws SQLException
  {
    return getDate(executeUpdate());
  }

  /**
   * Retrieves the value of a JDBC <code>TIME</code> parameter as a
   * <code>java.sql.Time</code> object, using the given <code>Calendar</code>
   * object to construct the time. With a <code>Calendar</code> object, the
   * driver can calculate the time taking into account a custom timezone and
   * locale. If no <code>Calendar</code> object is specified, the driver uses
   * the default timezone and locale.
   * 
   * @param parameterName the name of the parameter
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the time
   * @return the parameter value; if the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTime(String, Time, Calendar)
   * @since 1.4
   */
  public Time getTime(String parameterName, Calendar cal) throws SQLException
  {
    return getTime(executeUpdate());
  }

  /**
   * Retrieves the value of a JDBC <code>TIMESTAMP</code> parameter as a
   * <code>java.sql.Timestamp</code> object, using the given
   * <code>Calendar</code> object to construct the <code>Timestamp</code>
   * object. With a <code>Calendar</code> object, the driver can calculate the
   * timestamp taking into account a custom timezone and locale. If no
   * <code>Calendar</code> object is specified, the driver uses the default
   * timezone and locale.
   * 
   * @param parameterName the name of the parameter
   * @param cal the <code>Calendar</code> object the driver will use to
   *          construct the timestamp
   * @return the parameter value. If the value is SQL <code>NULL</code>, the
   *         result is <code>null</code>.
   * @exception SQLException if a database access error occurs
   * @see #setTimestamp(String, Timestamp, Calendar)
   * @since 1.4
   */
  public Timestamp getTimestamp(String parameterName, Calendar cal)
      throws SQLException
  {
    return getTimestamp(executeUpdate());
  }

  /**
   * Retrieves the value of a JDBC <code>DATALINK</code> parameter as a
   * <code>java.net.URL</code> object.
   * 
   * @param parameterName the name of the parameter
   * @return the parameter value as a <code>java.net.URL</code> object in the
   *         Java programming language. If the value was SQL <code>NULL</code>,
   *         the value <code>null</code> is returned.
   * @exception SQLException if a database access error occurs, or if there is a
   *              problem with the URL
   * @see #setURL
   * @since 1.4
   */
  public URL getURL(String parameterName) throws SQLException
  {
    if (!outAndNamedParameterTypes.containsKey(parameterName))
      throw new SQLException("Invalid named parameter " + parameterName);

    try
    {
      return (URL) namedParameterValues.get(parameterName);
    }
    catch (Exception e)
    {
      throw new SQLException("Unable to convert named parameter "
          + parameterName + " to the requested type");
    }
  }

@Override
public void setRowId(int parameterIndex, RowId x) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNString(int parameterIndex, String value) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNCharacterStream(int parameterIndex, Reader value, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNClob(int parameterIndex, NClob value) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setClob(int parameterIndex, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBlob(int parameterIndex, InputStream inputStream, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNClob(int parameterIndex, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setAsciiStream(int parameterIndex, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBinaryStream(int parameterIndex, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setCharacterStream(int parameterIndex, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setAsciiStream(int parameterIndex, InputStream x)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBinaryStream(int parameterIndex, InputStream x)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setCharacterStream(int parameterIndex, Reader reader)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNCharacterStream(int parameterIndex, Reader value)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setClob(int parameterIndex, Reader reader) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBlob(int parameterIndex, InputStream inputStream)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNClob(int parameterIndex, Reader reader) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setPoolable(boolean poolable) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public boolean isPoolable() throws SQLException {
	// TODO Auto-generated method stub
	return false;
}

@Override
public void closeOnCompletion() throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public boolean isCloseOnCompletion() throws SQLException {
	// TODO Auto-generated method stub
	return false;
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

@Override
public Object getObject(int parameterIndex, Map<String, Class<?>> map)
		throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Object getObject(String parameterName, Map<String, Class<?>> map)
		throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public RowId getRowId(int parameterIndex) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public RowId getRowId(String parameterName) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setRowId(String parameterName, RowId x) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNString(String parameterName, String value) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNCharacterStream(String parameterName, Reader value, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNClob(String parameterName, NClob value) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setClob(String parameterName, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBlob(String parameterName, InputStream inputStream, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNClob(String parameterName, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public NClob getNClob(int parameterIndex) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public NClob getNClob(String parameterName) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setSQLXML(String parameterName, SQLXML xmlObject)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public SQLXML getSQLXML(int parameterIndex) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public SQLXML getSQLXML(String parameterName) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public String getNString(int parameterIndex) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public String getNString(String parameterName) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Reader getNCharacterStream(int parameterIndex) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Reader getNCharacterStream(String parameterName) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Reader getCharacterStream(int parameterIndex) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Reader getCharacterStream(String parameterName) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setBlob(String parameterName, Blob x) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setClob(String parameterName, Clob x) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setAsciiStream(String parameterName, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBinaryStream(String parameterName, InputStream x, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setCharacterStream(String parameterName, Reader reader, long length)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setAsciiStream(String parameterName, InputStream x)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBinaryStream(String parameterName, InputStream x)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setCharacterStream(String parameterName, Reader reader)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNCharacterStream(String parameterName, Reader value)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setClob(String parameterName, Reader reader) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setBlob(String parameterName, InputStream inputStream)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNClob(String parameterName, Reader reader) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}
}