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
 * Initial developer(s): Julie Marguerite.
 * Contributor(s): Emmanuel Cecchet, Nicolas Modrzyk, Damian Arregui.
 */

package org.continuent.sequoia.controller.virtualdatabase;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.continuent.hedera.adapters.MulticastRequestAdapter;
import org.continuent.hedera.adapters.MulticastResponse;
import org.continuent.hedera.channel.NotConnectedException;
import org.continuent.sequoia.common.authentication.AuthenticationManager;
import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.UnreachableBackendException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.protocol.Field;
import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedure;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureParameter;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.connection.AbstractConnectionManager;
import org.continuent.sequoia.controller.connection.PooledConnection;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.UnknownReadRequest;
import org.continuent.sequoia.controller.virtualdatabase.protocol.GetMetadata;

/**
 * Class that gathers the dynamic metadata for a virtual database, that means
 * all the metadata subject to changes during the lifetime of the application.
 * 
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie.Marguerite </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a> *
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 */
public class VirtualDatabaseDynamicMetaData
{

  /** Detect a null valu for int */
  public static final int NULL_VALUE = -999;

  private VirtualDatabase vdb;
  private String          vdbName;
  private RequestManager  requestManager;

  /** Logger instance. */
  private Trace           logger     = null;

  /**
   * Reference the database for this metadata. Do not fetch any data at this
   * time
   * 
   * @param database to link this metadata to
   */
  public VirtualDatabaseDynamicMetaData(VirtualDatabase database)
  {
    this.vdb = database;
    this.vdbName = database.getDatabaseName();
    requestManager = database.getRequestManager();
    if (requestManager == null)
      throw new RuntimeException(
          "Null request manager in VirtualDatabaseMetaData");

    this.logger = Trace
        .getLogger("org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread."
            + vdbName + ".metadata");
  }

  /**
   * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  public ControllerResultSet getAttributes(ConnectionContext connContext,
      String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException
  {
    // This is a JDBC 3.0 feature
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetAttributes(connContext, catalog, schemaPattern,
            typeNamePattern, attributeNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, String.class};
      Object[] args = {connContext, catalog, schemaPattern, typeNamePattern,
          attributeNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetAttributes", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(getAttributesFields, data);
    return rs;
  }

  /**
   * @see #getAttributes(String, String, String, String, String)
   */
  private ControllerResultSet doGetAttributes(ConnectionContext connContext,
      String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getAttributes(catalog, schemaPattern, typeNamePattern,
          attributeNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[21];
        row[0] = cols.getObject(1); // TYPE_CAT
        row[1] = cols.getObject(2); // TYPE_SCHEM
        row[2] = cols.getObject(3); // TYPE_NAME
        row[3] = cols.getObject(4); // DATA_TYPE
        row[4] = cols.getObject(5); // ATTR_NAME
        row[5] = cols.getObject(6); // ATTR_TYPE_NAME
        row[6] = cols.getObject(7); // ATTR_SIZE
        row[7] = cols.getObject(8); // DECIMAL_DIGITS
        row[8] = cols.getObject(9); // NUM_PREC_RADIX
        row[9] = cols.getObject(10); // NULLABLE
        row[10] = cols.getObject(11); // REMARKS
        row[11] = cols.getObject(12); // ATTR_DEF
        row[12] = cols.getObject(13); // SQL_DATA_TYPE
        row[13] = cols.getObject(14); // SQL_DATETIME_SUB
        row[14] = cols.getObject(15); // CHAR_OCTET_LENGTH
        row[15] = cols.getObject(16); // ORDINAL_POSITION
        row[16] = cols.getObject(17); // IS_NULLABLE
        row[17] = cols.getObject(18); // SCOPE_CATALOG
        row[18] = cols.getObject(19); // SCOPE_SCHEMA
        row[19] = cols.getObject(20); // SCOPE_TABLE
        row[20] = cols.getObject(21); // SOURCE_DATA_TYPE
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getAttributesFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getAttributesFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String,
   *      java.lang.String, java.lang.String, int, boolean)
   */
  public ControllerResultSet getBestRowIdentifier(
      ConnectionContext connContext, String catalog, String schema,
      String table, int scope, boolean nullable) throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetBestRowIdentifier(connContext, catalog, schema, table,
            scope, nullable);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, int.class, boolean.class};
      Object[] args = {connContext, catalog, schema, table, new Integer(scope),
          Boolean.valueOf(nullable)};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetBestRowIdentifier", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(
        getBestRowIdentifierAndVersionColumnsFields, data);
    return rs;
  }

  /**
   * @see #getBestRowIdentifier(ConnectionContext, String, String, String, int,
   *      boolean)
   */
  public ControllerResultSet doGetBestRowIdentifier(
      ConnectionContext connContext, String catalog, String schema,
      String table, int scope, boolean nullable) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getBestRowIdentifier(catalog, schema, table, scope,
          nullable);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[8];
        row[0] = cols.getObject(1); // SCOPE
        row[1] = cols.getObject(2); // COLUMN_NAME
        row[2] = cols.getObject(3); // DATA_TYPE
        row[3] = cols.getObject(4); // TYPE_NAME
        row[4] = cols.getObject(5); // COLUMN_SIZE
        row[5] = cols.getObject(6); // BUFFER_LENGTH
        row[6] = cols.getObject(7); // DECIMAL_DIGITS
        row[7] = cols.getObject(8); // PSEUDO_COLUMN
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getBestRowIdentifierAndVersionColumnsFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getBestRowIdentifierAndVersionColumnsFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * Build a list of Catalogs from a givem list of virtual database names
   * 
   * @param list of virtual database from the controller
   * @return <code>ResultSet</code> with list of catalogs
   */
  public ControllerResultSet getCatalogs(ArrayList list)
  {
    int size = list.size();
    ArrayList data = new ArrayList(size);
    for (int i = 0; i < size; i++)
    {
      Object[] row = new Object[1];
      row[0] = list.get(i);
      data.add(row);
    }
    ControllerResultSet rs = new ControllerResultSet(getCatalogsFields, data);
    return rs;
  }

  /**
   * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  public ControllerResultSet getColumnPrivileges(ConnectionContext connContext,
      String catalog, String schema, String table, String columnNamePattern)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetColumnPrivileges(connContext, catalog, schema, table,
            columnNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, String.class};
      Object[] args = {connContext, catalog, schema, table, columnNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetColumnPrivileges", argTypes, args);
      if (crs != null)
        return crs;
    }

    AuthenticationManager manager = requestManager.getVirtualDatabase()
        .getAuthenticationManager();

    DatabaseSchema dbs = requestManager.getDatabaseSchema();
    if (dbs == null)
      throw new SQLException("Unable to fetch the virtual database schema");

    if (columnNamePattern == null)
      // if null is passed then select all tables
      columnNamePattern = "%";

    DatabaseTable dbTable = dbs.getTable(table);
    if (dbTable == null)
      throw new SQLException("Unable to find table " + table);

    ArrayList columns = dbTable.getColumns();
    int size = columns.size();
    ArrayList data = new ArrayList();

    ArrayList virtualLogins = manager.getVirtualLogins();
    int vsize = virtualLogins.size();
    VirtualDatabaseUser vu;

    for (int i = 0; i < size; i++)
    {
      DatabaseColumn c = (DatabaseColumn) columns.get(i);
      if (columnNamePattern.equals("%")
          || columnNamePattern.equals(c.getName()))
      {
        for (int j = 0; j < vsize; j++)
        {
          vu = (VirtualDatabaseUser) virtualLogins.get(0);

          if (logger.isDebugEnabled())
            logger.debug("Found privilege for user:" + vu.getLogin()
                + " on column:" + c.getName());
          Object[] row = new Object[8];
          row[0] = vdbName; // table cat
          row[1] = null; // table schema
          row[2] = table; // table name
          row[3] = c.getName(); // column name
          row[4] = null; // grantor
          row[5] = vu.getLogin(); // grantee
          row[6] = "UPDATE"; // privilege
          row[7] = "NO"; // IS_GRANTABLE
          data.add(row);
        }
      }
    }

    ControllerResultSet rs = new ControllerResultSet(getColumnPrivilegesFields,
        data);
    return rs;
  }

  /**
   * @see #getColumnPrivileges(ConnectionContext, String, String, String,
   *      String)
   */
  public ControllerResultSet doGetColumnPrivileges(
      ConnectionContext connContext, String catalog, String schema,
      String table, String columnNamePattern) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getColumnPrivileges(catalog, schema, table,
          columnNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[8];
        row[0] = cols.getObject(1); // TABLE_CAT
        row[1] = cols.getObject(2); // TABLE_SCHEM
        row[2] = cols.getObject(3); // TABLE_NAME
        row[3] = cols.getObject(4); // COLUMN_NAME
        row[4] = cols.getObject(5); // GRANTOR
        row[5] = cols.getObject(6); // GRANTEE
        row[6] = cols.getObject(7); // PRIVILEGE
        row[7] = cols.getObject(8); // IS_GRANTABLE
        data.add(row);
      }
      return new ControllerResultSet(getColumnPrivilegesFields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getColumns(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  public ControllerResultSet getColumns(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Getting columns for " + vdbName);

    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetColumns(connContext, catalog, schemaPattern,
            tableNamePattern, columnNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, String.class};
      Object[] args = {connContext, catalog, schemaPattern, tableNamePattern,
          columnNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController("doGetColumns",
          argTypes, args);
      if (crs != null)
        return crs;
    }

    // Ok from this point on, this is RAIDb-0 or RAIDb-2 and we have to build
    // the results ourselves.
    DatabaseSchema dbs = requestManager.getDatabaseSchema();
    if (dbs == null)
      throw new SQLException("Unable to fetch the virtual database schema");

    if (tableNamePattern == null)
      tableNamePattern = "%"; // if null is passed then select
    // all tables

    if (columnNamePattern == null)
      columnNamePattern = "%"; // if null is passed then

    // Build the ResultSet
    Collection tables = dbs.getTables().values();
    ArrayList data = new ArrayList(tables.size());

    for (Iterator iter = tables.iterator(); iter.hasNext();)
    {
      DatabaseTable t = (DatabaseTable) iter.next();

      if (tableNamePattern.equals("%") || tableNamePattern.equals(t.getName()))
      {
        if (logger.isDebugEnabled())
          logger.debug("Found table " + t.getName());
        ArrayList columns = t.getColumns();
        for (int j = 0; j < columns.size(); j++)
        {
          DatabaseColumn c = (DatabaseColumn) columns.get(j);
          if (columnNamePattern.equals("%")
              || columnNamePattern.equals(c.getName()))
          {
            if (logger.isDebugEnabled())
              logger.debug("Found column " + c.getName());
            Object[] row = new Object[22];
            row[0] = vdbName; // TABLE_CAT
            row[1] = null; // TABLE_SCHEM
            row[2] = t.getName(); // TABLE_NAME
            row[3] = c.getName(); // COLUMN_NAME
            row[4] = new Integer(c.getType()); // DATA_TYPE
            row[5] = null; // TYPE_NAME
            row[6] = null; // COLUMN_SIZE
            row[7] = null; // BUFFER_LENGTH
            row[8] = null; // DECIMAL_DIGITS
            row[9] = null; // NUM_PREC_RADIX
            row[10] = null; // NULLABLE
            row[11] = null; // REMARKS
            row[12] = null; // COLUMN_DEF
            row[13] = null; // SQL_DATA_TYPE
            row[14] = null; // SQL_DATETIME_SUB
            row[15] = null; // CHAR_OCTET_LENGTH
            row[16] = null; // ORDINAL_POSITION
            row[17] = ""; // IS_NULLABLE
            row[18] = null; // SCOPE_CATALOG
            row[19] = null;// SCOPE_SCHEMA
            row[20] = null;// SCOPE_TABLE
            row[21] = null; // SOURCE_DATA_TYPE
            data.add(row);
          }
        }
      }
    }
    ControllerResultSet rs = new ControllerResultSet(getColumnsFields, data);
    return rs;
  }

  /**
   * @see #getColumns(ConnectionContext, String, String, String, String)
   */
  public ControllerResultSet doGetColumns(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getColumns(catalog, schemaPattern, tableNamePattern,
          columnNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[22];
        row[0] = cols.getObject(1); // TABLE_CAT
        row[1] = cols.getObject(2); // TABLE_SCHEM
        row[2] = cols.getObject(3); // TABLE_NAME
        row[3] = cols.getObject(4); // COLUMN_NAME
        row[4] = cols.getObject(5); // DATA_TYPE
        row[5] = cols.getObject(6); // TYPE_NAME
        row[6] = cols.getObject(7); // COLUMN_SIZE
        row[7] = cols.getObject(8); // BUFFER_LENGTH
        row[8] = cols.getObject(9); // DECIMAL_DIGITS
        row[9] = cols.getObject(10); // NUM_PREC_RADIX
        row[10] = cols.getObject(11); // NULLABLE
        row[11] = cols.getObject(12); // REMARKS
        row[12] = cols.getObject(13); // COLUMN_DEF
        row[13] = cols.getObject(14); // SQL_DATA_TYPE
        row[14] = cols.getObject(15); // SQL_DATETIME_SUB
        row[15] = cols.getObject(16); // CHAR_OCTET_LENGTH
        row[16] = cols.getObject(17); // ORDINAL_POSITION
        row[17] = cols.getObject(18); // IS_NULLABLE
        // JDBC 3.0 starts here
        try
        {
          row[18] = cols.getObject(19); // SCOPE_CATALOG
          row[19] = cols.getObject(20); // SCOPE_SCHEMA
          row[20] = cols.getObject(21); // SCOPE_TABLE
          row[21] = cols.getObject(22); // SOURCE_DATA_TYPE
        }
        catch (Exception e)
        { // Driver does not support JDBC 3.0 cut here
          row[18] = null; // SCOPE_CATALOG
          row[19] = null;// SCOPE_SCHEMA
          row[20] = null;// SCOPE_TABLE
          row[21] = null; // SOURCE_DATA_TYPE
        }
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getColumnsFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getColumnsFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public ControllerResultSet getCrossReference(ConnectionContext connContext,
      String primaryCatalog, String primarySchema, String primaryTable,
      String foreignCatalog, String foreignSchema, String foreignTable)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetCrossReference(connContext, primaryCatalog, primarySchema,
            primaryTable, foreignCatalog, foreignSchema, foreignTable);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, String.class, String.class, String.class};
      Object[] args = {connContext, primaryCatalog, primarySchema,
          primaryTable, foreignCatalog, foreignSchema, foreignTable};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetCrossReference", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(
        getCrossReferenceOrImportExportedKeysFields, data);
    return rs;
  }

  /**
   * @see #getCrossReference(ConnectionContext, String, String, String, String,
   *      String, String)
   */
  public ControllerResultSet doGetCrossReference(ConnectionContext connContext,
      String primaryCatalog, String primarySchema, String primaryTable,
      String foreignCatalog, String foreignSchema, String foreignTable)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getCrossReference(primaryCatalog, primarySchema,
          primaryTable, foreignCatalog, foreignSchema, foreignTable);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[14];
        row[0] = cols.getObject(1); // PKTABLE_CAT
        row[1] = cols.getObject(2); // PKTABLE_SCHEM
        row[2] = cols.getObject(3); // PKTABLE_NAME
        row[3] = cols.getObject(4); // PKCOLUMN_NAME
        row[4] = cols.getObject(5); // FKTABLE_CAT
        row[5] = cols.getObject(6); // FKTABLE_SCHEM
        row[6] = cols.getObject(7); // FKTABLE_NAME
        row[7] = cols.getObject(8); // FKCOLUMN_NAME
        row[8] = cols.getObject(9); // KEY_SEQ
        row[9] = cols.getObject(10); // UPDATE_RULE
        row[10] = cols.getObject(11); // DELETE_RULE
        row[11] = cols.getObject(12); // FK_NAME
        row[12] = cols.getObject(13); // PK_NAME
        row[13] = cols.getObject(14); // DEFERRABILITY
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getCrossReferenceOrImportExportedKeysFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getCrossReferenceOrImportExportedKeysFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public ControllerResultSet getExportedKeys(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetExportedKeys(connContext, catalog, schema, table);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schema, table};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetExportedKeys", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(
        getCrossReferenceOrImportExportedKeysFields, data);
    return rs;
  }

  /**
   * @see #getExportedKeys(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetExportedKeys(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getExportedKeys(catalog, schema, table);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[14];
        row[0] = cols.getObject(1); // PKTABLE_CAT
        row[1] = cols.getObject(2); // PKTABLE_SCHEM
        row[2] = cols.getObject(3); // PKTABLE_NAME
        row[3] = cols.getObject(4); // PKCOLUMN_NAME
        row[4] = cols.getObject(5); // FKTABLE_CAT
        row[5] = cols.getObject(6); // FKTABLE_SCHEM
        row[6] = cols.getObject(7); // FKTABLE_NAME
        row[7] = cols.getObject(8); // FKCOLUMN_NAME
        row[8] = cols.getObject(9); // KEY_SEQ
        row[9] = cols.getObject(10); // UPDATE_RULE
        row[10] = cols.getObject(11); // DELETE_RULE
        row[11] = cols.getObject(12); // FK_NAME
        row[12] = cols.getObject(13); // PK_NAME
        row[13] = cols.getObject(14); // DEFERRABILITY
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getCrossReferenceOrImportExportedKeysFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getCrossReferenceOrImportExportedKeysFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public ControllerResultSet getImportedKeys(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetImportedKeys(connContext, catalog, schema, table);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schema, table};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetImportedKeys", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(
        getCrossReferenceOrImportExportedKeysFields, data);
    return rs;
  }

  /**
   * @see #doGetImportedKeys(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetImportedKeys(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getImportedKeys(catalog, schema, table);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[14];
        row[0] = cols.getObject(1); // PKTABLE_CAT
        row[1] = cols.getObject(2); // PKTABLE_SCHEM
        row[2] = cols.getObject(3); // PKTABLE_NAME
        row[3] = cols.getObject(4); // PKCOLUMN_NAME
        row[4] = cols.getObject(5); // FKTABLE_CAT
        row[5] = cols.getObject(6); // FKTABLE_SCHEM
        row[6] = cols.getObject(7); // FKTABLE_NAME
        row[7] = cols.getObject(8); // FKCOLUMN_NAME
        row[8] = cols.getObject(9); // KEY_SEQ
        row[9] = cols.getObject(10); // UPDATE_RULE
        row[10] = cols.getObject(11); // DELETE_RULE
        row[11] = cols.getObject(12); // FK_NAME
        row[12] = cols.getObject(13); // PK_NAME
        row[13] = cols.getObject(14); // DEFERRABILITY
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getCrossReferenceOrImportExportedKeysFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getCrossReferenceOrImportExportedKeysFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String,
   *      java.lang.String, java.lang.String, boolean, boolean)
   */
  public ControllerResultSet getIndexInfo(ConnectionContext connContext,
      String catalog, String schema, String table, boolean unique,
      boolean approximate) throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetIndexInfo(connContext, catalog, schema, table, unique,
            approximate);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, boolean.class, boolean.class};
      Object[] args = {connContext, catalog, schema, table,
          Boolean.valueOf(unique), Boolean.valueOf(approximate)};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetIndexInfo", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet
    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(getIndexInfoFields, data);
    return rs;
  }

  /**
   * @see #getIndexInfo(ConnectionContext, String, String, String, boolean,
   *      boolean)
   */
  public ControllerResultSet doGetIndexInfo(ConnectionContext connContext,
      String catalog, String schema, String table, boolean unique,
      boolean approximate) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getIndexInfo(catalog, schema, table, unique,
          approximate);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[13];
        row[0] = cols.getObject(1); // TABLE_CAT
        row[1] = cols.getObject(2); // TABLE_SCHEM
        row[2] = cols.getObject(3); // TABLE_NAME
        row[3] = cols.getObject(4); // NON_UNIQUE
        row[4] = cols.getObject(5); // INDEX_QUALIFIER
        row[5] = cols.getObject(6); // INDEX_NAME
        row[6] = cols.getObject(7); // TYPE
        row[7] = cols.getObject(8); // ORDINAL_POSITION
        row[8] = cols.getObject(9); // COLUMN_NAME
        row[9] = cols.getObject(10); // ASC_OR_DESC
        row[10] = cols.getObject(11); // CARDINALITY
        row[11] = cols.getObject(12); // PAGES
        row[12] = cols.getObject(13); // FILTER_CONDITION
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getIndexInfoFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getIndexInfoFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * Gets a description of a table's primary key columns for the given login.
   * Primary keys are ordered by COLUMN_NAME.
   * 
   * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public ControllerResultSet getPrimaryKeys(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Getting getPrimaryKeys for " + vdbName);

    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetPrimaryKeys(connContext, catalog, schema, table);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schema, table};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetPrimaryKeys", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Ok from this point on, this is RAIDb-0 or RAIDb-2 and we have to build
    // the results ourselves.
    DatabaseSchema dbs = requestManager.getDatabaseSchema();
    if (dbs == null)
      throw new SQLException("Unable to fetch the virtual database schema");

    if (table == null)
      table = "%"; // if null is passed then
    // select all tables

    // Build the ResultSet
    Collection tables = dbs.getTables().values();
    ArrayList data = new ArrayList(tables.size());

    for (Iterator iter = tables.iterator(); iter.hasNext();)
    {
      DatabaseTable t = (DatabaseTable) iter.next();
      if (table.equals("%") || table.equals(t.getName()))
      {
        ArrayList columns = t.getColumns();
        for (int j = 0; j < columns.size(); j++)
        {
          DatabaseColumn c = (DatabaseColumn) columns.get(j);
          if (c.isUnique())
          {
            if (logger.isDebugEnabled())
              logger.debug("Found primary key" + c.getName());
            Object[] row = new Object[6];
            row[0] = vdbName; // TABLE_CAT
            row[1] = null; // TABLE_SCHEM
            row[2] = t.getName(); // TABLE_NAME
            row[3] = c.getName(); // COLUMN_NAME
            row[4] = new Integer(c.getType()); // KEY_SEQ
            row[5] = c.getName(); // PK_NAME
            data.add(row);
          }
          else
          {
            if (logger.isDebugEnabled())
              logger.debug("Key " + c.getName() + " is not unique");
          }
        }
      }
    }
    ControllerResultSet rs = new ControllerResultSet(getPrimaryKeysFields, data);
    return rs;
  }

  /**
   * @see #getPrimaryKeys(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetPrimaryKeys(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet pks = m.getPrimaryKeys(catalog, schema, table);
      ArrayList data = new ArrayList();
      while (pks.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[6];
        row[0] = pks.getObject(1); // TABLE_CAT
        row[1] = pks.getObject(2); // TABLE_SCHEM
        row[2] = pks.getObject(3); // TABLE_NAME
        row[3] = pks.getObject(4); // COLUMN_NAME
        row[4] = pks.getObject(5); // KEY_SEQ
        row[5] = pks.getObject(6); // PK_NAME
        data.add(row);
      }
      return new ControllerResultSet(getPrimaryKeysFields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getProcedureColumns
   */
  public ControllerResultSet getProcedureColumns(ConnectionContext connContext,
      String catalog, String schemaPattern, String procedureNamePattern,
      String columnNamePattern) throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetProcedureColumns(connContext, catalog, schemaPattern,
            procedureNamePattern, columnNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, String.class};
      Object[] args = {connContext, catalog, schemaPattern,
          procedureNamePattern, columnNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetProcedureColumns", argTypes, args);
      if (crs != null)
        return crs;
    }

    DatabaseSchema dbs = requestManager.getDatabaseSchema();
    if (dbs == null)
      throw new SQLException("Unable to fetch the virtual database schema");

    if (procedureNamePattern == null)
      procedureNamePattern = "%";

    if (columnNamePattern == null)
      columnNamePattern = "%";

    // Build the ResultSet
    Collection procedures = dbs.getProcedures().values();
    ArrayList data = new ArrayList(procedures.size());

    for (Iterator iter = procedures.iterator(); iter.hasNext();)
    {
      DatabaseProcedure sp = (DatabaseProcedure) iter.next();
      if (procedureNamePattern.equals("%")
          || procedureNamePattern.equals(sp.getName()))
      {
        if (logger.isDebugEnabled())
          logger.debug("Found matching procedure " + sp.getName());

        ArrayList params = sp.getParameters();
        int sizep = params.size();
        DatabaseProcedureParameter param;
        for (int k = 0; k < sizep; k++)
        {
          param = (DatabaseProcedureParameter) params.get(k);
          if (columnNamePattern.equals("%")
              || columnNamePattern.equals(sp.getName()))
          {
            if (logger.isDebugEnabled())
              logger.debug("Found matching procedure parameter"
                  + param.getName());

            Object[] row = new Object[13];
            row[0] = vdbName; // PROCEDURE_CAT
            row[1] = null; // PROCEDURE_SCHEM
            row[2] = sp.getName(); // PROCEDURE_NAME
            row[3] = param.getName(); // COLUMN_NAME
            row[4] = new Integer(param.getColumnType()); // COLUMN_TYPE
            row[5] = new Integer(param.getDataType()); // DATA_TYPE
            row[6] = param.getTypeName(); // TYPE_NAME
            row[7] = new Float(param.getPrecision()); // PRECISION
            row[8] = new Integer(param.getLength()); // LENGTH
            row[9] = new Integer(param.getScale()); // SCALE
            row[10] = new Integer(param.getRadix()); // RADIX
            row[11] = new Integer(param.getNullable()); // NULLABLE
            row[12] = param.getRemarks();

            data.add(row);
          }
        }
      }
    }
    ControllerResultSet rs = new ControllerResultSet(getProcedureColumnsFields,
        data);
    return rs;
  }

  /**
   * @see #getProcedureColumns(ConnectionContext, String, String, String,
   *      String)
   */
  public ControllerResultSet doGetProcedureColumns(
      ConnectionContext connContext, String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getColumns(catalog, schemaPattern,
          procedureNamePattern, columnNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[13];
        row[0] = cols.getObject(1); // PROCEDURE_CAT
        row[1] = cols.getObject(2); // PROCEDURE_SCHEM
        row[2] = cols.getObject(3); // PROCEDURE_NAME
        row[3] = cols.getObject(4); // COLUMN_NAME
        row[4] = cols.getObject(5); // COLUMN_TYPE
        row[5] = cols.getObject(6); // DATA_TYPE
        row[6] = cols.getObject(7); // TYPE_NAME
        row[7] = cols.getObject(8); // PRECISION
        row[8] = cols.getObject(9); // LENGTH
        row[9] = cols.getObject(10); // SCALE
        row[10] = cols.getObject(11); // RADIX
        row[11] = cols.getObject(12); // NULLABLE
        row[12] = cols.getObject(13); // REMARKS
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getProcedureColumnsFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getProcedureColumnsFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getProcedures(String,
   *      String, String)
   */
  public ControllerResultSet getProcedures(ConnectionContext connContext,
      String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetProcedures(connContext, catalog, schemaPattern,
            procedureNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schemaPattern,
          procedureNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetProcedures", argTypes, args);
      if (crs != null)
        return crs;
    }

    DatabaseSchema dbs = requestManager.getDatabaseSchema();
    if (dbs == null)
      throw new SQLException("Unable to fetch the virtual database schema");

    if (procedureNamePattern == null)
      procedureNamePattern = "%"; // if null is passed then
    // select all procedures

    // Build the ResultSet
    Collection procedures = dbs.getProcedures().values();
    ArrayList data = new ArrayList(procedures.size());

    for (Iterator iter = procedures.iterator(); iter.hasNext();)
    {
      DatabaseProcedure sp = (DatabaseProcedure) iter.next();
      if (procedureNamePattern.equals("%")
          || procedureNamePattern.equals(sp.getName()))
      {
        if (logger.isDebugEnabled())
          logger.debug("Found procedure " + sp.getName());
        Object[] row = new Object[8];
        row[0] = vdbName; // PROCEDURE_CAT
        row[1] = null; // PROCEDURE_SCHEM
        row[2] = sp.getName(); // PROCEDURE_NAME
        row[3] = null; // reserved for future use
        row[4] = null; // reserved for future use
        row[5] = null; // reserved for future use
        row[6] = sp.getRemarks(); // REMARKS
        row[7] = new Integer(sp.getProcedureType()); // PROCEDURE_TYPE
        data.add(row);
      }
    }
    ControllerResultSet rs = new ControllerResultSet(getProceduresFields, data);
    return rs;
  }

  /**
   * @see #getProcedures(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetProcedures(ConnectionContext connContext,
      String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getProcedures(catalog, schemaPattern,
          procedureNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[8];
        row[0] = cols.getObject(1); // PROCEDURE_CAT
        row[1] = cols.getObject(2); // PROCEDURE_SCHEM
        row[2] = cols.getObject(3); // PROCEDURE_NAME
        row[3] = cols.getObject(4); // reserved for future use
        row[4] = cols.getObject(5); // reserved for future use
        row[5] = cols.getObject(6); // reserved for future use
        row[6] = cols.getObject(7); // REMARKS
        row[7] = cols.getObject(8); // PROCEDURE_TYPE
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getProceduresFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getProceduresFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * Will return the schema from the call to getSchemas() on the first available
   * node.
   * 
   * @see java.sql.DatabaseMetaData#getSchemas()
   */
  public ControllerResultSet getSchemas(ConnectionContext connContext)
      throws SQLException
  {
    try
    { // Forward directly to the underlying backend
      return doGetSchemas(connContext);
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class};
      Object[] args = {connContext};
      ControllerResultSet crs = getMetaDataFromRemoteController("doGetSchemas",
          argTypes, args);
      if (crs != null)
        return crs;
    }

    Object[] row = new Object[2];
    row[0] = vdbName; // TABLE_SCHEM
    row[1] = null; // TABLE_CATALOG
    ArrayList data = new ArrayList();
    data.add(row);
    return new ControllerResultSet(getSchemasFields, data);
  }

  /**
   * @see #getSchemas(String)
   */
  public ControllerResultSet doGetSchemas(ConnectionContext connContext)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getSchemas();
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[2];
        row[0] = cols.getObject(1); // TABLE_SCHEM
        // JDBC 3.0 starts here
        try
        {
          row[1] = cols.getObject(2); // TABLE_CATALOG
        }
        catch (Exception e)
        { // Driver does not support JDBC 3.0 cut here
          row[1] = null; // TABLE_SCHEM
        }
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getSchemasFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getSchemasFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public ControllerResultSet getSuperTables(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetSuperTables(connContext, catalog, schemaPattern,
            tableNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schemaPattern, tableNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetSuperTables", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(getSuperTablesFields, data);
    return rs;
  }

  /**
   * @see #getSuperTables(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetSuperTables(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getSuperTables(catalog, schemaPattern,
          tableNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[4];
        row[0] = cols.getObject(1); // TABLE_CAT
        row[1] = cols.getObject(2); // TABLE_SCHEM
        row[2] = cols.getObject(3); // TABLE_NAME
        row[3] = cols.getObject(4); // SUPERTABLE_NAME
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getSuperTablesFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getSuperTablesFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public ControllerResultSet getSuperTypes(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetSuperTypes(connContext, catalog, schemaPattern,
            tableNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schemaPattern, tableNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetSuperTypes", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(getSuperTypesFields, data);
    return rs;
  }

  /**
   * @see #getSuperTypes(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetSuperTypes(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m
          .getSuperTypes(catalog, schemaPattern, tableNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[5];
        row[0] = cols.getObject(1); // TYPE_CAT
        row[1] = cols.getObject(2); // TYPE_SCHEM
        row[2] = cols.getObject(3); // TYPE_NAME
        row[3] = cols.getObject(4); // SUPERTYPE_CAT
        row[4] = cols.getObject(5); // SUPERTYPE_SCHEM
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getSuperTypesFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getSuperTypesFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getTablePrivileges(String,
   *      String, String)
   */
  public ControllerResultSet getTablePrivileges(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetTablePrivileges(connContext, catalog, schemaPattern,
            tableNamePattern);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schemaPattern, tableNamePattern};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetTablePrivileges", argTypes, args);
      if (crs != null)
        return crs;
    }

    AuthenticationManager manager = requestManager.getVirtualDatabase()
        .getAuthenticationManager();

    DatabaseSchema dbs = requestManager.getDatabaseSchema();
    if (dbs == null)
      throw new SQLException("Unable to fetch the virtual database schema");

    if (tableNamePattern == null)
      // if null is passed then select all tables
      tableNamePattern = "%";

    ArrayList virtualLogins = manager.getVirtualLogins();
    int vsize = virtualLogins.size();
    VirtualDatabaseUser vu;

    Collection tables = dbs.getTables().values();
    ArrayList data = new ArrayList(tables.size());
    for (Iterator iter = tables.iterator(); iter.hasNext();)
    {
      DatabaseTable t = (DatabaseTable) iter.next();
      if (tableNamePattern.equals("%") || tableNamePattern.equals(t.getName()))
      {
        for (int j = 0; j < vsize; j++)
        {
          vu = (VirtualDatabaseUser) virtualLogins.get(0);

          if (logger.isDebugEnabled())
            logger.debug("Found privilege for user:" + vu.getLogin()
                + " on table:" + t.getName());
          Object[] row = new Object[7];
          row[0] = vdbName; // table cat
          row[1] = null; // table schema
          row[2] = t.getName(); // table name
          row[3] = null; // grantor
          row[4] = vu.getLogin(); // grantee
          row[5] = "UPDATE"; // privilege
          row[6] = "NO"; // IS_GRANTABLE
          data.add(row);
        }
      }
    }

    ControllerResultSet rs = new ControllerResultSet(getTablePrivilegesFields,
        data);
    return rs;
  }

  /**
   * @see #getTablePrivileges(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetTablePrivileges(
      ConnectionContext connContext, String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getTablePrivileges(catalog, schemaPattern,
          tableNamePattern);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[7];
        row[0] = cols.getObject(1); // TABLE_CAT
        row[1] = cols.getObject(2); // TABLE_SCHEM
        row[2] = cols.getObject(3); // TABLE_NAME
        row[3] = cols.getObject(4); // GRANTOR
        row[4] = cols.getObject(5); // GRANTEE
        row[5] = cols.getObject(6); // PRIVILEGE
        row[6] = cols.getObject(7); // IS_GRANTABLE
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getTablePrivilegesFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getTablePrivilegesFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getTables(String,
   *      String, String, String[])
   */
  public ControllerResultSet getTables(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern,
      String[] types) throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetTables(connContext, catalog, schemaPattern,
            tableNamePattern, types);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, String[].class};
      Object[] args = {connContext, catalog, schemaPattern, tableNamePattern,
          types};
      ControllerResultSet crs = getMetaDataFromRemoteController("doGetTables",
          argTypes, args);
      if (crs != null)
        return crs;
    }

    DatabaseSchema dbs = requestManager.getDatabaseSchema();
    if (dbs == null)
      throw new SQLException(
          "No virtual database schema can be found possibly because no backend is enabled on that controller");

    if (tableNamePattern == null)
      // if null is passed then select all tables
      tableNamePattern = "%";

    // Build the ResultSet
    Collection tables = dbs.getTables().values();
    ArrayList data = new ArrayList(tables.size());

    for (Iterator iter = tables.iterator(); iter.hasNext();)
    {
      DatabaseTable t = (DatabaseTable) iter.next();
      if (tableNamePattern.equals("%")
          || t.getName().indexOf(tableNamePattern) != -1)
      {
        if (logger.isDebugEnabled())
          logger.debug("Found table " + t.getName());
        Object[] row = new Object[10];
        row[0] = vdbName; // TABLE_CAT
        row[1] = null; // TABLE_SCHEM
        row[2] = t.getName(); // TABLE_NAME
        row[3] = "TABLE"; // TABLE_TYPE
        row[4] = null; // REMARKS
        row[5] = null; // TYPE_CAT
        row[6] = null; // TYPE_SCHEM
        row[7] = null; // TYPE_NAME
        row[8] = null; // SELF_REFERENCING_COL_NAME
        row[9] = "SYSTEM"; // REF_GENERATION
        data.add(row);
      }
    }
    ControllerResultSet rs = new ControllerResultSet(getTablesFields, data);
    return rs;
  }

  /**
   * @see #getTables(ConnectionContext, String, String, String, String[])
   */
  public ControllerResultSet doGetTables(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern,
      String[] types) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getTables(catalog, schemaPattern, tableNamePattern,
          types);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[10];
        row[0] = cols.getObject(1); // TABLE_CAT
        row[1] = cols.getObject(2); // TABLE_SCHEM
        row[2] = cols.getObject(3); // TABLE_NAME
        row[3] = cols.getObject(4); // TABLE_TYPE
        row[4] = cols.getObject(5); // REMARKS

        // JDBC 3.0 starts here
        try
        {
          row[5] = cols.getObject(6); // TYPE_CAT
          row[6] = cols.getObject(7); // TYPE_SCHEM
          row[7] = cols.getObject(8); // TYPE_NAME
          row[8] = cols.getObject(9); // SELF_REFERENCING_COL_NAME
          row[9] = cols.getObject(10); // REF_GENERATION
        }
        catch (Exception e)
        { // Driver does not support JDBC 3.0 cut here
          row[5] = null;
          row[6] = null;
          row[7] = null;
          row[8] = null;
          row[9] = null;
        }
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getTablesFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getTablesFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getTableTypes()
   */
  public ControllerResultSet getTableTypes(ConnectionContext connContext)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetTableTypes(connContext);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class};
      Object[] args = {connContext};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetTableTypes", argTypes, args);
      if (crs != null)
        return crs;
    }

    ArrayList list = new ArrayList(1);
    Object[] row = new Object[1];
    row[0] = "TABLE"; // TABLE_TYPE
    list.add(row);
    ControllerResultSet rs = new ControllerResultSet(getTableTypesFields, list);
    return rs;
  }

  /**
   * @see #getTableTypes(ConnectionContext)
   */
  public ControllerResultSet doGetTableTypes(ConnectionContext connContext)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getTableTypes();
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[1];
        row[0] = cols.getObject(1); // TABLE_TYPE
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getTableTypesFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getTableTypesFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getTypeInfo()
   */
  public ControllerResultSet getTypeInfo(ConnectionContext connContext)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetTypeInfo(connContext);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class};
      Object[] args = {connContext};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetTypeInfo", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(getTypeInfoFields, data);
    return rs;
  }

  /**
   * @see #getTypeInfo(ConnectionContext)
   */
  public ControllerResultSet doGetTypeInfo(ConnectionContext connContext)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getTypeInfo();
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[18];
        row[0] = cols.getObject(1); // TYPE_NAME
        row[1] = cols.getObject(2); // DATA_TYPE
        row[2] = cols.getObject(3); // PRECISION
        row[3] = cols.getObject(4); // LITERAL_PREFIX
        row[4] = cols.getObject(5); // LITERAL_SUFFIX
        row[5] = cols.getObject(6); // CREATE_PARAMS
        row[6] = cols.getObject(7); // NULLABLE
        row[7] = cols.getObject(8); // CASE_SENSITIVE
        row[8] = cols.getObject(9); // SEARCHABLE
        row[9] = cols.getObject(10); // UNSIGNED_ATTRIBUTE
        row[10] = cols.getObject(11); // FIXED_PREC_SCALE
        row[11] = cols.getObject(12); // AUTO_INCREMENT
        row[12] = cols.getObject(13); // LOCAL_TYPE_NAME
        row[13] = cols.getObject(14); // MINIMUM_SCALE
        row[14] = cols.getObject(15); // MAXIMUM_SCALE
        row[15] = cols.getObject(16); // SQL_DATA_TYPE
        row[16] = cols.getObject(17); // SQL_DATETIME_SUB
        row[17] = cols.getObject(18); // NUM_PREC_RADIX
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getTypeInfoFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getTypeInfoFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getUDTs(String, String,
   *      String, int[])
   */
  public ControllerResultSet getUDTs(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern, int[] types)
      throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetUDTs(connContext, catalog, schemaPattern, tableNamePattern,
            types);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class, int[].class};
      Object[] args = {connContext, catalog, schemaPattern, tableNamePattern,
          types};
      ControllerResultSet crs = getMetaDataFromRemoteController("doGetUDTs",
          argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(getUDTsFields, data);
    return rs;
  }

  /**
   * @see #getUDTs(ConnectionContext, String, String, String, int[])
   */
  public ControllerResultSet doGetUDTs(ConnectionContext connContext,
      String catalog, String schemaPattern, String tableNamePattern, int[] types)
      throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getUDTs(catalog, schemaPattern, tableNamePattern,
          types);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[7];
        row[0] = cols.getObject(1); // TYPE_CAT
        row[1] = cols.getObject(2); // TYPE_SCHEM
        row[2] = cols.getObject(3); // TYPE_NAME
        row[3] = cols.getObject(4); // CLASS_NAME
        row[4] = cols.getObject(5); // DATA_TYPE
        row[5] = cols.getObject(6); // REMARKS

        // JDBC 3.0 starts here
        try
        {
          row[6] = cols.getObject(7); // BASE_TYPE
        }
        catch (Exception e)
        { // Driver does not support JDBC 3.0 cut here
          row[6] = null; // BASE_TYPE
        }

        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getUDTsFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getUDTsFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getVersionColumns(String,
   *      String, String)
   */
  public ControllerResultSet getVersionColumns(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    try
    {
      int raidbLevel = requestManager.getLoadBalancer().getRAIDbLevel();
      if ((raidbLevel == RAIDbLevels.RAIDb1)
          || (raidbLevel == RAIDbLevels.SingleDB))
      { // Forward directly to the underlying backend
        return doGetVersionColumns(connContext, catalog, schema, table);
      }
    }
    catch (NoMoreBackendException ignore)
    {
      // No backend is available, try getting metadata from a remote controller
      Class[] argTypes = {ConnectionContext.class, String.class, String.class,
          String.class};
      Object[] args = {connContext, catalog, schema, table};
      ControllerResultSet crs = getMetaDataFromRemoteController(
          "doGetVersionColumns", argTypes, args);
      if (crs != null)
        return crs;
    }

    // Feature not supported in RAIDb-0 and RAIDb-2, return an empty ResultSet

    ArrayList data = new ArrayList();
    ControllerResultSet rs = new ControllerResultSet(
        getBestRowIdentifierAndVersionColumnsFields, data);
    return rs;
  }

  /**
   * @see #getVersionColumns(ConnectionContext, String, String, String)
   */
  public ControllerResultSet doGetVersionColumns(ConnectionContext connContext,
      String catalog, String schema, String table) throws SQLException
  {
    ConnectionAndDatabaseMetaData info = null;
    try
    {
      info = getMetaDataFromFirstAvailableBackend(connContext);
      DatabaseMetaData m = info.getDatabaseMetaData();
      ResultSet cols = m.getVersionColumns(catalog, schema, table);
      ArrayList data = new ArrayList();
      while (cols.next())
      { // Unroll the loop for comments (and speed?)
        Object[] row = new Object[8];
        row[0] = cols.getObject(1); // SCOPE
        row[1] = cols.getObject(2); // COLUMN_NAME
        row[2] = cols.getObject(3); // DATA_TYPE
        row[3] = cols.getObject(4); // TYPE_NAME
        row[4] = cols.getObject(5); // COLUMN_SIZE
        row[5] = cols.getObject(6); // BUFFER_LENGTH
        row[6] = cols.getObject(7); // DECIMAL_DIGITS
        row[7] = cols.getObject(8); // PSEUDO_COLUMN
        data.add(row);
      }
      Field[] fields;
      if (vdb.useStaticResultSetMetaData())
        fields = getBestRowIdentifierAndVersionColumnsFields;
      else
      { // Fetch metdata as well
        ResultSetMetaData metaData = cols.getMetaData();
        if (metaData == null)
          fields = getBestRowIdentifierAndVersionColumnsFields;
        else
          fields = ControllerConstants.CONTROLLER_FACTORY
              .getResultSetMetaDataFactory().copyResultSetMetaData(metaData,
                  null);
      }
      return new ControllerResultSet(fields, data);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      releaseConnection(info);
    }
  }

  /**
   * Get DatabaseMetaData from the first available backend.
   * 
   * @param login the login to use to fetch metadata
   * @return the DatabaseMetaData obtained from the first available backend
   *         among with the connection information
   * @throws NoMoreBackendException if no backend is enabled on this controller
   * @throws SQLException if an error occured while getting MetaData
   */
  private ConnectionAndDatabaseMetaData getMetaDataFromFirstAvailableBackend(
      ConnectionContext connContext) throws NoMoreBackendException,
      SQLException
  {
    DatabaseBackend b = requestManager.getVirtualDatabase()
        .getFirstAvailableBackend();
    if (b == null)
      throw new NoMoreBackendException(
          "No backend is enabled for virtual database " + vdbName);
    AbstractConnectionManager cm = b.getConnectionManager(connContext
        .getLogin());
    if (cm == null)
      throw new SQLException("Invalid login " + connContext.getLogin()
          + " on backend " + b.getName());

    PooledConnection c = null;
    try
    {
      if (connContext.isStartedTransaction())
      { // Retrieve connection in the proper transaction context
        c = cm.retrieveConnectionForTransaction(connContext.getTransactionId());
        // If the transaction was not started yet, we fallback as if we were in
        // autocommit to prevent issues related to lazy transaction start
      }
      if (c == null)
      { // Retrieve the appropriate connection if the connection is persistent
        // or allocate a new one.
        AbstractRequest fakeRequest = new UnknownReadRequest("metadata", false,
            0, "\n");
        fakeRequest.setIsAutoCommit(true);
        fakeRequest.setPersistentConnection(connContext
            .isPersistentConnection());
        fakeRequest.setPersistentConnectionId(connContext
            .getPersistentConnectionId());
        // Cleanup transactional context in case we fallback from a failure to
        // retrieve connection from transaction because the transaction was not
        // started yet (see previous if statement above)
        connContext.setStartedTransaction(false);
        c = cm.retrieveConnectionInAutoCommit(fakeRequest);
      }
    }
    catch (UnreachableBackendException e)
    {
      throw new SQLException("Unable to get a connection for login "
          + connContext);
    }
    return new ConnectionAndDatabaseMetaData(c, cm, c.getConnection()
        .getMetaData(), connContext);
  }

  /**
   * Release the connection used to fetch the metadata
   * 
   * @param info the connection information returned by
   *          getMetaDataFromFirstAvailableBackend
   * @see #getMetaDataFromFirstAvailableBackend(String)
   */
  private void releaseConnection(ConnectionAndDatabaseMetaData info)
  {
    if (info == null)
      return;

    /*
     * Don't release the connection if it is persistent or in a transaction (it
     * should only be done at closing time for persistent connections and at
     * commit/rollback time for transactions
     */
    if (info.getConnectionContext().isPersistentConnection()
        || info.getConnectionContext().isStartedTransaction())
      return;
    info.getConnectionManager().releaseConnectionInAutoCommit(null,
        info.getConnection());
  }

  private ControllerResultSet getMetaDataFromRemoteController(
      String methodName, Class[] argTypes, Object[] args) throws SQLException
  {
    // If vdb is NOT distributed return null, else try remote controllers
    if (!vdb.isDistributed())
      return null;

    DistributedVirtualDatabase dvdb = (DistributedVirtualDatabase) vdb;
    try
    {
      MulticastResponse rspList = dvdb.getMulticastRequestAdapter()
          .multicastMessage(
              dvdb.getAllMemberButUs(),
              new GetMetadata(methodName, argTypes, args),
              MulticastRequestAdapter.WAIT_ALL,
              dvdb.getMessageTimeouts()
                  .getVirtualDatabaseConfigurationTimeout());

      Map results = rspList.getResults();
      if (results.size() == 0)
        if (logger.isWarnEnabled())
          logger
              .warn("No response while getting metadata from remote controller");
      for (Iterator iter = results.values().iterator(); iter.hasNext();)
      {
        Object response = iter.next();
        if (response instanceof ControllerException)
        {
          if (logger.isErrorEnabled())
          {
            logger.error("Error while getting metadata from remote controller");
          }
        }
        else
        {
          // Here we succeded in getting metadata from a remote controller
          return (ControllerResultSet) response;
        }
      }
    }
    catch (NotConnectedException e)
    {
      if (logger.isErrorEnabled())
        logger
            .error(
                "Channel unavailable while getting metadata from remote controller",
                e);
    }

    // Here we didn't succeded in getting metadata from another controller
    // Throw exception (fixes SEQUOIA-500)
    throw new SQLException(
        "Unable to fetch metadata from any backend in the cluster (probably no backend is enabled)");
  }

  /**
   * This class defines a ConnectionAndDatabaseMetaData used to carry metadata
   * and connection related information to properly release the connection later
   * on.
   * 
   * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
   * @version 1.0
   */
  private class ConnectionAndDatabaseMetaData
  {
    DatabaseMetaData          databaseMetaData;
    AbstractConnectionManager connectionManager;
    PooledConnection          connection;
    ConnectionContext         connContext;

    /**
     * Creates a new <code>ConnectionAndDatabaseMetaData</code> object
     * 
     * @param c the connection used to get the metadata
     * @param cm the connection manager used to get the connection
     * @param metadata the metadata fetched from the connection
     * @param connContext connection context
     */
    public ConnectionAndDatabaseMetaData(PooledConnection c,
        AbstractConnectionManager cm, DatabaseMetaData metadata,
        ConnectionContext connContext)
    {
      this.connection = c;
      this.connectionManager = cm;
      this.databaseMetaData = metadata;
      this.connContext = connContext;
    }

    /**
     * Returns the connection value.
     * 
     * @return Returns the connection.
     */
    public PooledConnection getConnection()
    {
      return connection;
    }

    /**
     * Returns the connection context value.
     * 
     * @return Returns the connContext.
     */
    public final ConnectionContext getConnectionContext()
    {
      return connContext;
    }

    /**
     * Returns the connectionManager value.
     * 
     * @return Returns the connectionManager.
     */
    public AbstractConnectionManager getConnectionManager()
    {
      return connectionManager;
    }

    /**
     * Returns the databaseMetaData value.
     * 
     * @return Returns the databaseMetaData.
     */
    public DatabaseMetaData getDatabaseMetaData()
    {
      return databaseMetaData;
    }

  }

  /**
   * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private static Field[] getAttributesFields                         = new Field[]{
      new Field("TYPE_CAT", "TYPE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TYPE_SCHEM", "TYPE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("DATA_TYPE", "DATA_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("ATTR_NAME", "ATTR_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("ATTR_TYPE_NAME", "ATTR_TYPE_NAME", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("ATTR_SIZE", "ATTR_SIZE", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("DECIMAL_DIGITS", "DECIMAL_DIGITS", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("NUM_PREC_RADIX", "NUM_PREC_RADIX", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("NULLABLE", "NULLABLE", 10, Types.INTEGER, "INTEGER", "Integer"),
      new Field("REMARKS", "REMARKS", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("ATTR_DEF", "ATTR_DEF", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("SQL_DATA_TYPE", "SQL_DATA_TYPE", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("CHAR_OCTET_LENGTH", "CHAR_OCTET_LENGTH", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("ORDINAL_POSITION", "ORDINAL_POSITION", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("IS_NULLABLE", "IS_NULLABLE", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SCOPE_CATALOG", "SCOPE_CATALOG", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SCOPE_SCHEMA", "SCOPE_SCHEMA", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SCOPE_TABLE", "SCOPE_TABLE", 10, Types.VARCHAR, "VARCHAR",
          "String")                                                  };

  /**
   * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String,
   *      java.lang.String, java.lang.String, int, boolean)
   */
  private static Field[] getBestRowIdentifierAndVersionColumnsFields = new Field[]{
      new Field("SCOPE", "SCOPE", 10, Types.SMALLINT, "SMALLINT", "Short"),
      new Field("COLUMN_NAME", "COLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("DATA_TYPE", "DATA_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("COLUMN_SIZE", "COLUMN_SIZE", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("BUFFER_LENGTH", "BUFFER_LENGTH", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("DECIMAL_DIGITS", "DECIMAL_DIGITS", 10, Types.SMALLINT,
          "SMALLINT", "Short"),
      new Field("PSEUDO_COLUMN", "PSEUDO_COLUMN", 10, Types.SMALLINT,
          "SMALLINT", "Short")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getCatalogs()
   */
  private static Field[] getCatalogsFields                           = new Field[]{new Field(
                                                                         "TABLE_CAT",
                                                                         "TABLE_CAT",
                                                                         9,
                                                                         Types.VARCHAR,
                                                                         "VARCHAR",
                                                                         "String")};

  /**
   * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private static Field[] getColumnPrivilegesFields                   = new Field[]{
      new Field("TABLE_CAT", "TABLE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_NAME", "TABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("COLUMN_NAME", "COLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("GRANTOR", "GRANTOR", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("GRANTEE", "GRANTEE", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("PRIVILEGE", "PRIVILEGE", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("IS_GRANTABLE", "IS_GRANTABLE", 10, Types.VARCHAR, "VARCHAR",
          "String"),                                                 };

  /**
   * @see java.sql.DatabaseMetaData#getColumns(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private static Field[] getColumnsFields                            = new Field[]{
      new Field("TABLE_CAT", "TABLE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_NAME", "TABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("COLUMN_NAME", "COLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("DATA_TYPE", "DATA_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("COLUMN_SIZE", "COLUMN_SIZE", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("BUFFER_LENGTH", "BUFFER_LENGTH", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("DECIMAL_DIGITS", "DECIMAL_DIGITS", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("NUM_PREC_RADIX", "NUM_PREC_RADIX", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("NULLABLE", "NULLABLE", 10, Types.INTEGER, "INTEGER", "Integer"),
      new Field("REMARKS", "REMARKS", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("COLUMN_DEF", "COLUMN_DEF", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SQL_DATA_TYPE", "SQL_DATA_TYPE", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("CHAR_OCTET_LENGTH", "CHAR_OCTET_LENGTH", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("ORDINAL_POSITION", "ORDINAL_POSITION", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("IS_NULLABLE", "IS_NULLABLE", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SCOPE_CATALOG", "SCOPE_CATALOG", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SCOPE_SCHEMA", "SCOPE_SCHEMA", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SCOPE_TABLE", "SCOPE_TABLE", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SOURCE_DATA_TYPE", "SOURCE_DATA_TYPE", 10, Types.SMALLINT,
          "SMALLINT", "Short")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.lang.String)
   * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private static Field[] getCrossReferenceOrImportExportedKeysFields = new Field[]{
      new Field("PKTABLE_CAT", "PKTABLE_CAT", 9, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("PKTABLE_SCHEM", "PKTABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("PKTABLE_NAME", "PKTABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("PKCOLUMN_NAME", "PKCOLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("FKTABLE_CAT", "FKTABLE_CAT", 9, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("FKTABLE_SCHEM", "FKTABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("FKTABLE_NAME", "FKTABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("FKCOLUMN_NAME", "FKCOLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("KEY_SEQ", "KEY_SEQ", 10, Types.SMALLINT, "SMALLINT", "Short"),
      new Field("UPDATE_RULE", "UPDATE_RULE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("DELETE_RULE", "DELETE_RULE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("FK_NAME", "FK_NAME", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("PK_NAME", "PK_NAME", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("DEFERRABILITY", "DEFERRABILITY", 10, Types.SMALLINT,
          "SMALLINT", "Short")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String,
   *      java.lang.String, java.lang.String, boolean, boolean)
   */
  private static Field[] getIndexInfoFields                          = new Field[]{
      new Field("TABLE_CAT", "TABLE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_NAME", "TABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("NON_UNIQUE", "NON_UNIQUE", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("INDEX_QUALIFIER", "INDEX_QUALIFIER", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("INDEX_NAME", "INDEX_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TYPE", "TYPE", 10, Types.SMALLINT, "SMALLINT", "Short"),
      new Field("ORDINAL_POSITION", "ORDINAL_POSITION", 10, Types.SMALLINT,
          "SMALLINT", "Short"),
      new Field("COLUMN_NAME", "COLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("ASC_OR_DESC", "ASC_OR_DESC", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("CARDINALITY", "CARDINALITY", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("PAGES", "PAGES", 10, Types.INTEGER, "INTEGER", "Integer"),
      new Field("FILTER_CONDITION", "FILTER_CONDITION", 10, Types.VARCHAR,
          "VARCHAR", "String")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private static Field[] getPrimaryKeysFields                        = new Field[]{
      new Field("TABLE_CAT", "TABLE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_NAME", "TABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("COLUMN_NAME", "COLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("KEY_SEQ", "KEY_SEQ", 10, Types.SMALLINT, "SMALLINT", "Short"),
      new Field("PK_NAME", "PK_NAME", 10, Types.VARCHAR, "VARCHAR", "String")};
  /**
   * @see java.sql.DatabaseMetaData#getProcedureColumns(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private static Field[] getProcedureColumnsFields                   = new Field[]{
      new Field("PROCEDURE_CAT", "PROCEDURE_CAT", 9, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("PROCEDURE_SCHEM", "PROCEDURE_SCHEM", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("PROCEDURE_NAME", "PROCEDURE_NAME", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("COLUMN_NAME", "COLUMN_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("COLUMN_TYPE", "COLUMN_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("DATA_TYPE", "DATA_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("PRECISION", "PRECISION", 10, Types.FLOAT, "FLOAT", "Float"),
      new Field("LENGTH", "LENGTH", 10, Types.INTEGER, "INTEGER", "Integer"),
      new Field("SCALE", "SCALE", 10, Types.SMALLINT, "SMALLINT", "Short"),
      new Field("RADIX", "RADIX", 10, Types.SMALLINT, "SMALLINT", "Short"),
      new Field("NULLABLE", "NULLABLE", 10, Types.SMALLINT, "SMALLINT", "Short"),
      new Field("REMARKS", "REMARKS", 10, Types.VARCHAR, "VARCHAR", "String")};

  /**
   * @see java.sql.DatabaseMetaData#getProcedures(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private static Field[] getProceduresFields                         = new Field[]{
      new Field("PROCEDURE_CAT", "PROCEDURE_CAT", 9, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("PROCEDURE_SCHEM", "PROCEDURE_SCHEM", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("PROCEDURE_NAME", "PROCEDURE_NAME", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("", "", 0, Types.VARCHAR, "VARCHAR", "String"),
      new Field("", "", 0, Types.VARCHAR, "VARCHAR", "String"),
      new Field("", "", 0, Types.VARCHAR, "VARCHAR", "String"),
      new Field("REMARKS", "REMARKS", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("PROCEDURE_TYPE", "PROCEDURE_TYPE", 10, Types.SMALLINT,
          "SMALLINT", "Short")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getSchemas()
   */
  private static Field[] getSchemasFields                            = new Field[]{
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 9, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_CATALOG", "TABLE_CATALOG", 9, Types.VARCHAR, "VARCHAR",
          "String")                                                  };

  /**
   * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private static Field[] getSuperTablesFields                        = new Field[]{
      new Field("TABLE_CAT", "TABLE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_NAME", "TABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SUPERTABLE_NAME", "SUPERTABLE_NAME", 10, Types.VARCHAR,
          "VARCHAR", "String")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private static Field[] getSuperTypesFields                         = new Field[]{
      new Field("TYPE_CAT", "TYPE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TYPE_SCHEM", "TYPE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SUPERTYPE_CAT", "SUPERTYPE_CAT", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SUPERTYPE_SCHEM", "SUPERTYPE_SCHEM", 10, Types.VARCHAR,
          "VARCHAR", "String")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getTablePrivileges(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private static Field[] getTablePrivilegesFields                    = new Field[]{
      new Field("TABLE_CAT", "TABLE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_NAME", "TABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("GRANTOR", "GRANTOR", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("GRANTEE", "GRANTEE", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("PRIVILEGE", "PRIVILEGE", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("IS_GRANTABLE", "IS_GRANTABLE", 10, Types.VARCHAR, "VARCHAR",
          "String"),                                                 };

  /**
   * @see java.sql.DatabaseMetaData#getTables(String, String, String, String[])
   */
  private static Field[] getTablesFields                             = new Field[]{
      new Field("TABLE_CAT", "TABLE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TABLE_SCHEM", "TABLE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_NAME", "TABLE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TABLE_TYPE", "TABLE_TYPE", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("REMARKS", "REMARKS", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TYPE_CAT", "TYPE_CAT", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TYPE_SCHEM", "TYPE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("SELF_REFERENCING_COL_NAME", "SELF_REFERENCING_COL_NAME", 25,
          Types.VARCHAR, "VARCHAR", "String"),
      new Field("REF_GENERATION", "REF_GENERATION", 15, Types.VARCHAR,
          "VARCHAR", "String")                                       };

  /**
   * @see java.sql.DatabaseMetaData#getTableTypes()
   */
  private static Field[] getTableTypesFields                         = new Field[]{new Field(
                                                                         "TABLE_TYPE",
                                                                         "TABLE_TYPE",
                                                                         9,
                                                                         Types.VARCHAR,
                                                                         "VARCHAR",
                                                                         "String")};

  /**
   * @see java.sql.DatabaseMetaData#getTypeInfo()
   */
  private static Field[] getTypeInfoFields                           = new Field[]{
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("DATA_TYPE", "DATA_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("PRECISION", "PRECISION", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("LITERAL_PREFIX", "LITERAL_PREFIX", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("LITERAL_SUFFIX", "LITERAL_SUFFIX", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("CREATE_PARAMS", "CREATE_PARAMS", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("NULLABLE", "NULLABLE", 10, Types.INTEGER, "INTEGER", "Integer"),
      new Field("CASE_SENSITIVE", "CASE_SENSITIVE", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("SEARCHABLE", "SEARCHABLE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("UNSIGNED_ATTRIBUTE", "UNSIGNED_ATTRIBUTE", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("FIXED_PREC_SCALE", "FIXED_PREC_SCALE", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("AUTO_INCREMENT", "AUTO_INCREMENT", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("LOCAL_TYPE_NAME", "LOCAL_TYPE_NAME", 10, Types.VARCHAR,
          "VARCHAR", "String"),
      new Field("MINIMUM_SCALE", "MINIMUM_SCALE", 10, Types.SMALLINT,
          "SMALLINT", "Short"),
      new Field("MAXIMUM_SCALE", "MAXIMUM_SCALE", 10, Types.SMALLINT,
          "SMALLINT", "Short"),
      new Field("SQL_DATA_TYPE", "SQL_DATA_TYPE", 10, Types.INTEGER, "INTEGER",
          "Integer"),
      new Field("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", 10, Types.INTEGER,
          "INTEGER", "Integer"),
      new Field("NUM_PREC_RADIX", "NUM_PREC_RADIX", 10, Types.INTEGER,
          "INTEGER", "Integer")                                      };

  /**
   * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String,
   *      java.lang.String, int[])
   */
  private static Field[] getUDTsFields                               = new Field[]{
      new Field("TYPE_CAT", "TYPE_CAT", 9, Types.VARCHAR, "VARCHAR", "String"),
      new Field("TYPE_SCHEM", "TYPE_SCHEM", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("CLASS_NAME", "CLASS_NAME", 10, Types.VARCHAR, "VARCHAR",
          "String"),
      new Field("DATA_TYPE", "DATA_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short"),
      new Field("REMARKS", "REMARKS", 10, Types.VARCHAR, "VARCHAR", "String"),
      new Field("BASE_TYPE", "BASE_TYPE", 10, Types.SMALLINT, "SMALLINT",
          "Short")                                                   };

}