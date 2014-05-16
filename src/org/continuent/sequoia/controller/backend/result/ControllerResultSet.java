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
 * Contributor(s): Diego Malpica.
 */

package org.continuent.sequoia.controller.backend.result;

import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.exceptions.driver.protocol.BackendDriverException;
import org.continuent.sequoia.common.protocol.Field;
import org.continuent.sequoia.common.protocol.SQLDataSerialization;
import org.continuent.sequoia.common.protocol.TypeTag;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * A <code>ControllerResultSet</code> is a lightweight ResultSet for the
 * controller side. It only contains row data and column metadata. The real
 * ResultSet is constructed on by the driver (DriverResultSet object) on the
 * client side from the ControllerResultSet information.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ControllerResultSet extends AbstractResult implements Serializable
{
  private static final long                 serialVersionUID  = 4109773059200535129L;

  /** The results */
  private ArrayList                         data              = null;
  /** The fields */
  private Field[]                           fields            = null;
  /** Cursor name for this ResultSet (not used yet) */
  private String                            cursorName        = null;
  /** Fetch size if we need to fetch only a subset of the ResultSet */
  private int                               fetchSize         = 0;
  /** Backend ResultSet. We need to hold a ref to it when streaming. */
  private transient ResultSet               dbResultSet       = null;
  /**
   * Temporary reference to Statement when we are built from a dbResultSet.
   * Maybe this should be a local variable.
   */
  private transient Statement               owningStatement   = null;
  /** True if the underlying database ResultSet is closed */
  private boolean                           dbResultSetClosed = true;
  /** True if there is still more data to fetch from dbResultSet */
  private boolean                           hasMoreData       = false;
  /** Maximum number of rows remaining to fetch */
  private int                               maxRows           = 0;
  /** Pointers to column-specific de/serializer */
  private SQLDataSerialization.Serializer[] serializers;
  /** SQL Warning attached to this resultset */
  private SQLWarning                        warnings          = null;
  
  /** 
   * Connection associated with result set when fetching rows over multiple 
   * calls.
   */
  private transient ConnectionResource      connectionResource = null;

  /**
   * Build a Sequoia ResultSet from a database specific ResultSet. The metadata
   * can be retrieved from the MetadataCache if provided. If a metadata cache is
   * provided but the data is not in the cache, the MetadataCache is updated
   * accordingly. The remaining code is a straightforward copy of both metadata
   * and data. Used for actual queries, as opposed to metadata ResultSets.
   * <p>
   * The statement used to execute the query will be closed when the ResultSet
   * has been completely copied or when the ResultSet is closed while in
   * streaming mode.
   * 
   * @param request Request to which this ResultSet belongs
   * @param rs The database specific ResultSet
   * @param metadataCache MetadataCache (null if none)
   * @param s Statement used to get rs
   * @param isPartOfMultipleResults true if this ResultSet is part of a call to
   *          a query that returns multiple ResultSets
   * @throws SQLException if an error occurs
   */
  public ControllerResultSet(AbstractRequest request, java.sql.ResultSet rs,
      MetadataCache metadataCache, Statement s, boolean isPartOfMultipleResults)
      throws SQLException
  {
    this.owningStatement = s;
    try
    {
      if (rs == null)
        throw new SQLException(
            "Cannot build a ControllerResultSet with a null java.sql.ResultSet");

      // This is already a result coming from another controller.
      // if (rs instanceof org.continuent.sequoia.driver.ResultSet)
      // return (org.continuent.sequoia.driver.ResultSet) rs;

      // Build the ResultSet metaData
      if ((metadataCache != null) && !isPartOfMultipleResults)
        fields = metadataCache.getMetadata(request);

      if (fields == null)
      { // Metadata Cache miss or part of multiple results
        // Build the fields from the MetaData
        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        fields = ControllerConstants.CONTROLLER_FACTORY
            .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                metadataCache);
        if ((metadataCache != null) && !isPartOfMultipleResults)
          metadataCache.addMetadata(request, fields);
      }
      // Copy the warnings
      warnings = null;
      if (request.getRetrieveSQLWarnings())
      {
        warnings = rs.getWarnings();
      }

      // Build the ResultSet data
      data = new ArrayList();
      if (rs.next()) // not empty RS
      {
        cursorName = request.getCursorName();
        fetchSize = request.getFetchSize();
        maxRows = request.getMaxRows();
        if (maxRows == 0)
          maxRows = Integer.MAX_VALUE; // Infinite number of rows

        // Note that fetchData updates the data field
        dbResultSet = rs;
        fetchData();
        if (hasMoreData && (cursorName == null))
          // hashCode() is not guaranteed to be injective in theory,
          // but returns the address of the object in practice.
          cursorName = String.valueOf(dbResultSet.hashCode());
      }
      else
      // empty RS
      {
        hasMoreData = false;
        dbResultSet = null;
        dbResultSetClosed = true;
        rs.close();
        if (owningStatement != null)
        {
          try
          {
            owningStatement.close();
          }
          catch (SQLException ignore)
          {
          }
          owningStatement = null;
        }
      }
    }
    catch (SQLException e)
    {
      throw (SQLException) new SQLException(
          "Error while building Sequoia ResultSet (" + e.getLocalizedMessage()
              + ")", e.getSQLState(), e.getErrorCode()).initCause(e);
    }
  }

  /**
   * Creates a new <code>ControllerResultSet</code> object from already built
   * data. Used for ResultSets holding metadata.
   * 
   * @param fields ResultSet metadata fields
   * @param data ResultSet data (an ArrayList of Object[] representing row
   *          content)
   */
  public ControllerResultSet(Field[] fields, ArrayList data)
  {
    if (data == null)
      throw new IllegalArgumentException(
          "Cannot build a ControllerResultSet with null data ArrayList");

    this.fields = fields;
    this.data = data;
    warnings = null;
  }

  /**
   * Saves the connection resource associated with the result set.  If the 
   * resource is not needed it is freed immediately.  
   */
  public void saveConnectionResource(ConnectionResource resource)
  {
    connectionResource = resource;
    if (dbResultSet == null || dbResultSetClosed)
    {
      connectionResource.release();
    }
  }
  
  /**
   * Releases the connection resource.  This call is idempotent but should *always* 
   * be made when the result set is no longer needed or the connection may not be 
   * released properly.  
   */
  private void releaseConnectionResource()
  {
    if (connectionResource != null)
    {
      connectionResource.release();
      connectionResource = null;
    }
  }

  /**
   * Closes the database ResultSet to release the resource and garbage collect
   * data.
   */
  public synchronized void closeResultSet()
  {
    if ((dbResultSet != null) && !dbResultSetClosed)
    {
      try
      {
        dbResultSet.close();
        releaseConnectionResource();
      }
      catch (SQLException ignore)
      {
      }
      dbResultSet = null; // to allow GC to work properly

      // TODO: explain how owningStatement could be not null since we set it to
      // null at end of constructor ??
      if (owningStatement != null)
      {
        try
        {
          owningStatement.close();
        }
        catch (SQLException ignore)
        {
        }
        owningStatement = null;
      }
    }
  }

  /**
   * Sets the fetch size and calls fetchData()
   * 
   * @param fetchSizeParam the number of rows to fetch
   * @throws SQLException if an error occurs
   * @see #fetchData()
   */
  public void fetchData(int fetchSizeParam) throws SQLException
  {
    this.fetchSize = fetchSizeParam;
    fetchData();
    if (!hasMoreData)
    {
      if (owningStatement != null)
      {
        try
        {
          owningStatement.close();
        }
        catch (SQLException ignore)
        {
        }
        owningStatement = null;
      }
    }
  }

  /**
   * Fetch the next rows of data from dbResultSet according to fetchSize and
   * maxRows parameters. This methods directly updates the data and hasMoreData
   * fields returned by getData() and hadMoreData() accessors.
   * 
   * @throws SQLException from the backend or if dbResultSet is closed. Maybe we
   *           should use a different type internally.
   */
  public void fetchData() throws SQLException
  {
    if (dbResultSet == null)
      throw new SQLException("Backend ResultSet is closed");

    Object[] row;
    // We directly update the data field

    // Re-use the existing ArrayList with the same size: more efficient in the
    // usual case (constant fetchSize)
    data.clear();
    int toFetch;
    if (fetchSize > 0)
    {
      toFetch = fetchSize < maxRows ? fetchSize : maxRows;
      // instead of remembering how much we sent, it's simpler to decrease how
      // much we still may send.
      maxRows -= toFetch;
    }
    else
      toFetch = maxRows;
    int nbColumn = fields.length;
    Object object;
    do
    {
      row = new Object[nbColumn];
      for (int i = 0; i < nbColumn; i++)
      {
        object = ControllerConstants.CONTROLLER_FACTORY
            .getBackendObjectConverter().convertResultSetObject(dbResultSet, i,
                fields[i]);
        row[i] = object;
      }
      data.add(row);
      toFetch--;
      hasMoreData = dbResultSet.next();
    }
    while (hasMoreData && (toFetch > 0));
    if (hasMoreData && (fetchSize > 0) && (maxRows > 0))
    { // More data to fetch later on
      maxRows += toFetch;
      dbResultSetClosed = false;
    }
    else
    {
      hasMoreData = false;
      dbResultSet.close();
      if (owningStatement != null)
        owningStatement.close();
      releaseConnectionResource();
      dbResultSet = null;
      dbResultSetClosed = true;
    }
  }

  /**
   * Get the name of the SQL cursor used by this ResultSet
   * 
   * @return the ResultSet's SQL cursor name.
   */
  public String getCursorName()
  {
    return cursorName;
  }

  /**
   * Returns the data value.
   * 
   * @return Returns the data.
   */
  public ArrayList getData()
  {
    return data;
  }

  /**
   * Returns the fields value.
   * 
   * @return Returns the fields.
   */
  public Field[] getFields()
  {
    return fields;
  }

  /**
   * Returns the hasMoreData value.
   * 
   * @return Returns the hasMoreData.
   */
  public boolean hasMoreData()
  {
    return hasMoreData;
  }

  //
  // Serialization
  //

  /**
   * Serialize the <code>DriverResultSet</code> on the output stream by
   * sending only the needed parameters to reconstruct it on the driver. Caller
   * MUST have called #initSerializers() before. MUST mirror the following
   * deserialization method:
   * {@link org.continuent.sequoia.driver.DriverResultSet#DriverResultSet(org.continuent.sequoia.driver.Connection)}
   * 
   * @param output destination stream
   * @throws IOException if a network error occurs
   */

  public void sendToStream(
      org.continuent.sequoia.common.stream.DriverBufferedOutputStream output)
      throws IOException
  {
    // Serialize SQL warning chain first (in case of result streaming, results
    // must be the last ones to be sent
    if (warnings != null)
    {
      output.writeBoolean(true);
      new BackendDriverException(warnings).sendToStream(output);
    }
    else
      output.writeBoolean(false);

    int nbOfColumns = fields.length;
    int nbOfRows = data.size();
    // serialize columns information
    output.writeInt(nbOfColumns);
    for (int f = 0; f < nbOfColumns; f++)
      this.fields[f].sendToStream(output);

    TypeTag.COL_TYPES.sendToStream(output);

    // This could be just a boolean, see next line. But there is no real need
    // for change.
    output.writeInt(nbOfRows);

    // Send Java columns type. We need to do it only once: not for every row!
    if (nbOfRows > 0)
    {
      if (null == this.serializers)
        throw new IllegalStateException(
            "Bug: forgot to initialize serializers of a non empty ControllerResultSet");

      for (int col = 0; col < nbOfColumns; col++)
      {
        if (serializers[col] == null)
          TypeTag.JAVA_NULL.sendToStream(output);
        else
          serializers[col].getTypeTag().sendToStream(output);
      }
    }

    // Finally send the actual data
    sendRowsToStream(output);

    if (this.hasMoreData)
    { // Send the cursor name for further references
      output.writeLongUTF(this.cursorName);
    }
    output.flush();
  }

  /**
   * Initialize serializers based on the analysis of actual Java Objects of the
   * ResultSet to send (typically issued by backend's driver readObject()
   * method). MUST be called before #sendToStream()
   * 
   * @throws NotImplementedException in case we don't know how to serialize
   *           something
   */
  public void initSerializers() throws NotImplementedException
  {
    boolean dataFound = false;
    /* we don't expect the column types of "this" result set to change */
    if (this.serializers != null)
      return;

    if (data.size() == 0)
      return;

    final int nbOfColumns = fields.length;
    this.serializers = new SQLDataSerialization.Serializer[nbOfColumns];

    for (int col = 0; col < nbOfColumns; col++)
    {
      int rowIdx = -1;
      while (serializers[col] == null)
      {
        dataFound = false;
        rowIdx++;

        // We browsed the whole column and found nothing but NULLs
        if (rowIdx >= data.size()) // ? || rowIdx > 100)
          break;

        final Object[] row = (Object[]) data.get(rowIdx);
        final Object sqlObj = row[col];

        /*
         * If SQL was NULL, we only have a null reference and can't do much with
         * it. Move down to next row
         */
        if (sqlObj == null)
          continue;
        dataFound = true;
        getSerializer(col, sqlObj);

      } // while (serializers[col] == null)

      if (serializers[col] == null) // we found nothing
      {
        // TODO: add the following SQLWarning() to this resultset
        // "The whole column number " + col + " was null"

        /**
         * The whole column is null. Fall back on the JDBC type provided by
         * backend's metaData.getColumnType(), hoping it's right. Since we are
         * sending just nulls, a wrong typing should not do much harm anyway ?
         * 
         * @see org.continuent.sequoia.controller.virtualdatabase.ControllerResultSet#ControllerResultSet(AbstractRequest,
         *      java.sql.ResultSet, MetadataCache, Statement)
         */
        /**
         * We could (should ?) also use {@link Field#getColumnClassName()}, and
         * do some reflection instead. This is depending on the behaviour and
         * quality of the JDBC driver of the backends we want to support.
         */

        final TypeTag javaObjectType = TypeTag.jdbcToJavaObjectType(fields[col]
            .getSqlType());

        if (!TypeTag.TYPE_ERROR.equals(javaObjectType))

          serializers[col] = SQLDataSerialization.getSerializer(javaObjectType);

        else
        {
          if (dataFound)
            // data was found : set the undefined serializer (we promise not to
            // use it)
            serializers[col] = SQLDataSerialization.getSerializer(null);

          // else leave it to null : we will set it later, if needed

          // TODO: add the following SQLWarning() to this resultset
          // "The whole column number " + col + " was null"
          // **AND** there was an unknown JDBC type number

          // throw new NotImplementedException(
          // "Could not guess type of column number " + (col + 1)
          // + ". Whole column is null and backend provides "
          // + "unknown JDBC type number: " + fields[col].getSqlType());
        }

      } // if (serializers[col] == null) we found nothing for this whole column

    } // for (column)
  }

  /**
   * Get the serializer to use for the object sqlObj that was found in the column col
   * 
   * @param col Number of the column in which the object sqlObj was found
   * @param sqlObj Object for which we are trying to find the correct serializer to use
   * @throws NotImplementedException if no serializer can be found for the sqlObj object
   */
  private void getSerializer(int col, Object sqlObj)
      throws NotImplementedException
  {
    try
    {
      serializers[col] = SQLDataSerialization.getSerializer(sqlObj);
    }
    catch (NotImplementedException nie)
    {
      if (sqlObj instanceof Short)
      {
        /**
         * This is a workaround for a bug in (at least) PostgreSQL's driver.
         * This bug has been only very recently fixed: 8 jun 2005 in version
         * 1.75 of source file
         * pgjdbc/org/postgresql/jdbc2/AbstractJdbc2ResultSet.java
         * http://jdbc.postgresql.org/development/cvs.html.
         * <p>
         * It seems this java.lang.Short bug happens with multiple DBMS:
         * http://archives.postgresql.org/pgsql-jdbc/2005-07/threads.php#00382
         */

        // FIXME: we should probably convert to Integer sooner in
        // backendObjectConverter. Or just set a different serializer.
        // Unfortunately we have not access to any logger at this point.
        // TODO: append the following SQLwarning() to this resultset
        // "Buggy backend driver returns a java.lang.Short"
        // + " for column number " + col + ", converting to Integer"
        serializers[col] = SQLDataSerialization.getSerializer(TypeTag.INTEGER);

      } // no known serialization workaround
      else
      {
        NotImplementedException betterNIE = new NotImplementedException(
            "Backend driver gave an object of an unsupported java type:"
                + sqlObj.getClass().getName() + ", at colum number " + col
                + " of name " + fields[col].getFieldName());
        /**
         * No need for this, see
         * {@link SQLDataSerialization#getSerializer(Object)}
         */
        // betterNIE.initCause(nie);
        throw betterNIE;
      }
    }
  }

  /**
   * Serialize only rows, not any metadata. Useful for streaming. Called by the
   * controller side. This method MUST mirror the following deserialization
   * method: {@link org.continuent.sequoia.driver.DriverResultSet#receiveRows()}
   * 
   * @param output destination stream
   * @throws IOException on stream error
   */
  public void sendRowsToStream(DriverBufferedOutputStream output)
      throws IOException
  {

    output.writeInt(data.size());

    boolean[] nulls = new boolean[fields.length];

    Iterator rowsIter = this.data.iterator();
    while (rowsIter.hasNext())
    {
      Object[] row = (Object[]) rowsIter.next();
      TypeTag.ROW.sendToStream(output);

      // first flag null values
      for (int col = 0; col < row.length; col++)
      {
        if (null == row[col])
          nulls[col] = true;
        else
          nulls[col] = false;
        // TODO: we should compress this
        output.writeBoolean(nulls[col]);
      }

      for (int col = 0; col < row.length; col++)
        if (!nulls[col]) // send only non-nulls
        {
          try
          {
            /**
             * Here we are sure that serializers are initialized because:
             * <p>
             * (1) we went through
             * {@link #sendToStream(DriverBufferedOutputStream)} at least once
             * before
             * <p>
             * (2) and there was a non-zero ResultSet transfered, else we would
             * not come here again.
             */
            if (serializers[col] == null && row[col] != null)
            {
              // The serializer could not be set before now
              try
              {
                getSerializer(col, row[col]);
              }
              catch (NotImplementedException e)
              {
                serializers[col] = SQLDataSerialization.getSerializer(null);
              }
              serializers[col].getTypeTag().sendToStream(output);
            }
            serializers[col].sendToStream(row[col], output);
          }
          catch (ClassCastException cce1)
          {
            ClassCastException cce2 = new ClassCastException("Serializer "
                + serializers[col] + " failed on Java object: " + row[col]
                + " found in column: " + col + ", because of unexpected type "
                + row[col].getClass().getName());
            cce2.initCause(cce1);
            throw cce2;
          }
        } // if !null

    } // while (rows)

    output.writeBoolean(this.hasMoreData);
    output.flush();
  }
}