/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet, Edward Archibald.
 */

package org.continuent.sequoia.controller.backend;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedure;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureParameter;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.controller.core.ControllerConstants;

/**
 * This class defines a DatabaseSQLMetaData. It is used to collect metadata from
 * a live connection to a database
 * 
 * @author <a href="mailto:ed.archibald@continuent.com">Edward Archibald</a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class DatabaseSQLMetaData
{
  private Trace      logger;
  private Connection connection;
  private int        dynamicPrecision;
  private boolean    gatherSystemTables;
  private String     schemaPattern;

  /**
   * Creates a new <code>MetaData</code> object
   * 
   * @param logger the log4j logger to output to
   * @param connection a jdbc connection to a database
   * @param dynamicPrecision precision used to create the schema
   * @param gatherSystemTables should we gather system tables
   * @param schemaPattern schema pattern to look for (reduce the scope of
   *          gathering if not null)
   */
  public DatabaseSQLMetaData(Trace logger, Connection connection,
      int dynamicPrecision, boolean gatherSystemTables, String schemaPattern)
  {
    super();
    this.logger = logger;
    this.connection = connection;
    this.dynamicPrecision = dynamicPrecision;
    this.gatherSystemTables = gatherSystemTables;
    this.schemaPattern = schemaPattern;
  }

  /**
   * Create a database schema from the given connection
   * 
   * @param vdbName the virtual database name this schema represents
   * @return <code>DataSchema</code> contructed from the information collected
   *         through jdbc
   * @throws SQLException if an error occurs with the given connection
   */
  public final DatabaseSchema createDatabaseSchema(String vdbName)
      throws SQLException
  {
    SchemaFactory schemaFactory = ControllerConstants.CONTROLLER_FACTORY
        .getSchemaFactory();

    return schemaFactory.createDatabaseSchema(vdbName, logger, connection,
        dynamicPrecision, gatherSystemTables, schemaPattern, this);
  }

  /**
   * @see java.sql.DatabaseMetaData#getProcedures
   * @see java.sql.DatabaseMetaData#getProcedureColumns
   */
  public void getProcedures(DatabaseMetaData metaData, DatabaseSchema schema)
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("backend.meta.get.procedures"));

    ResultSet rs = null;
    ResultSet rs2 = null;
    try
    {
      // Get Procedures meta data
      rs = metaData.getProcedures(null, schemaPattern, "%");

      if (rs == null)
      {
        logger.warn(Translate.get("backend.meta.get.procedures.failed",
            metaData.getConnection().getCatalog()));
        return;
      }

      while (rs.next())
      {
        // Each row is a procedure description
        // 3 = PROCEDURE_NAME
        // 7 = REMARKS
        // 8 = PROCEDURE_TYPE
        DatabaseProcedure procedure = new DatabaseProcedure(rs.getString(3), rs
            .getString(7), rs.getShort(8));

        // Check if we need to fetch the procedure parameters
        if (dynamicPrecision < DatabaseBackendSchemaConstants.DynamicPrecisionProcedures)
        {
          if (logger.isDebugEnabled()
              && schema.getProcedure(procedure.getName()) != null)
          {
            logger.debug(Translate
                .get("backend.meta.procedure.already.in.schema", procedure
                    .getName()));
          }
          continue;
        }

        // Get the column information
        rs2 = metaData
            .getProcedureColumns(null, null, procedure.getName(), "%");
        if (rs2 == null)
          logger.warn(Translate.get("backend.meta.get.procedure.params.failed",
              procedure.getName()));
        else
        {
          while (rs2.next())
          {
            // Each row is a parameter description for the current procedure
            // 4 = COLUMN_NAME
            // 5 = COLUMN_TYPE
            // 6 = DATA_TYPE
            // 7 = TYPE_NAME
            // 8 = PRECISION
            // 9 = LENGTH
            // 10 = SCALE
            // 11 = RADIX
            // 12 = NULLABLE
            // 13 = REMARKS
            DatabaseProcedureParameter param = new DatabaseProcedureParameter(
                rs2.getString(4), rs2.getInt(5), rs2.getInt(6), rs2
                    .getString(7), rs2.getFloat(8), rs2.getInt(9), rs2
                    .getInt(10), rs2.getInt(11), rs2.getInt(12), rs2
                    .getString(13));
            procedure.addParameter(param);
            if (logger.isDebugEnabled())
              logger.debug(procedure.getName() + ": adding parameter "
                  + param.getName());
          }
          rs2.close();
        }

        // Add procedure only if it is not already in schema
        if (!schema.getProcedures().containsValue(procedure))
        {
          schema.addProcedure(procedure);
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("backend.meta.procedure.added",
                procedure.getKey()));
        }
        else if (logger.isDebugEnabled())
        {
          logger.debug(Translate.get(
              "backend.meta.procedure.already.in.schema", procedure.getName()));
        }
      }
    }
    catch (Exception e)
    {
      logger.error(Translate.get("backend.meta.get.procedures.failed", e
          .getMessage()), e);
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        rs2.close();
      }
      catch (Exception ignoreAsWell)
      {
      }
    }

    /*
     * EXPERIMENTAL: Further initialize the DatabaseProcedure objects to reflect
     * the semantic information if it exists. For now, just assume that the
     * tables exist that store the semantic information. Just log a message if
     * we don't find any semantic information.
     */
    getProcedureSemantics(metaData, schema);

  }

  /**
   * This method does a query against the tables which provide the semantic
   * information for stored procedures. As it processes each row returned, a
   * lookup is done in the database schema and if the procedure is found, the
   * information from this query is used to set up the semantics fields in the
   * DatabaseProcedure object.
   * 
   * @param metaData reference to a database meta data provider.
   * @param schema reference to the current database schema.
   */

  private void getProcedureSemantics(DatabaseMetaData metaData,
      DatabaseSchema schema)
  {
    ResultSet rs = null;
    DatabaseProcedure foundProc = null;
    int paramCount;
    String objectName = null;
    String procedureKey = null;
    PreparedStatement getBaseStmt = null;

    try
    {
      getBaseStmt = connection
          .prepareStatement("select * from sequoiaSABase where objectType=?");
      getBaseStmt.setInt(1, 1);

      /**
       * Execute the query to get the base semantic information for the
       * procedures. This call will return a single row for each procedure that
       * has semantics information.
       */
      rs = getBaseStmt.executeQuery();
      if (rs == null)
      { // Sanity check but a JDBC compliant driver never returns null
        logger.warn(Translate.get(
            "backend.meta.procedure.semantics.get.failed",
            "Query returned null"));
        return;
      }

      /**
       * For each row of base semantic information, see if we have the procedure
       * in the schema already. We should have it.... Then set up the basic
       * semantics information fields and, finally, call a method that will fill
       * out all of the tables referenced by recursively calling itself, if
       * necessary, to follow nested references to other stored procedures.
       */
      while (rs.next())
      {
        objectName = rs.getString("objectName");
        paramCount = rs.getInt("paramCount");

        procedureKey = DatabaseProcedure.buildKey(objectName, paramCount);
        if ((foundProc = schema.getProcedure(procedureKey)) == null)
        {
          logger.warn(Translate.get("backend.meta.procedure.notfound",
              procedureKey));
          continue;
        }

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("backend.meta.procedure.found.semantics",
              procedureKey));

        /**
         * set up the base semantics for the DatabaseProcedure.
         */
        DatabaseProcedureSemantic procedureSemantics = new DatabaseProcedureSemantic(
            rs.getBoolean("hasSelect"), rs.getBoolean("hasInsert"), rs
                .getBoolean("hasUpdate"), rs.getBoolean("hasDelete"), rs
                .getBoolean("hasDDLWrite"), rs.getBoolean("hasTransaction"), rs
                .getBoolean("isCausallyDependent"), rs
                .getBoolean("isCommutative"));
        foundProc.setSemantic(procedureSemantics);

        /**
         * call a method that will set up the tables referenced.
         */
        setProcedureTablesReferenced(metaData, schema, objectName, foundProc);

      }
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "backend.meta.procedure.semantics.get.failed", e.getMessage()), e);
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  private void setProcedureTablesReferenced(DatabaseMetaData metaData,
      DatabaseSchema schema, String procName, DatabaseProcedure proc)
  {
    ResultSet rs = null;
    int referenceType = 0;

    PreparedStatement getReferencesStmt = null;

    try
    {
      getReferencesStmt = connection
          .prepareStatement("select sequoiaSABase.objectName as baseObjectName, sequoiaSAReferences.objectName as referencedObjectName, sequoiaSAReferences.objectType as referencedObjectType,"
              + " referencedInSelect, referencedInInsert, referencedInUpdate, referencedInDelete, referencedInReplace, referencedInDDLWrite, referencedInDDLRead"
              + " FROM sequoiaSAReferences, sequoiaSABase"
              + " WHERE sequoiaSAReferences.baseObjectName = sequoiaSABase.objectName AND sequoiaSAReferences.baseObjectName=?");
      getReferencesStmt.setString(1, procName);

      rs = getReferencesStmt.executeQuery();

      if (rs == null)
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get(
              "backend.meta.procedure.semantics.no.references", procName));

        getReferencesStmt.close();
        return;
      }

      while (rs.next())
      {
        referenceType = rs.getInt("referencedObjectType");

        String referencedObjectName = rs.getString("referencedObjectName");
        if (logger.isDebugEnabled())
          logger.debug(Translate.get(
              "backend.meta.procedure.semantics.references.found",
              new Object[]{proc.getName(),
                  (referenceType == 1) ? "stored procedure" : "table",
                  rs.getString("baseObjectName"), referencedObjectName}));

        /**
         * If the reference is a stored procedure, do a recursive call to get
         * the tables. Otherwise, just add the table to the list.
         */
        if (referenceType == 1)
        {
          setProcedureTablesReferenced(metaData, schema, referencedObjectName,
              proc);
        }
        else
        {
          if (!proc.getSemantic().isReadOnly())
          {
            if (logger.isDebugEnabled())
              logger.debug(Translate.get(
                  "backend.meta.procedure.semantics.add.reference",
                  new Object[]{proc.getName(), referencedObjectName}));

            proc.getSemantic().addWriteTable(referencedObjectName);
          }
          else
            logger
                .error("Invalid semantic detected for stored procedure "
                    + procName
                    + " - Procedure cannot be read-only since it has a non read-only reference "
                    + referencedObjectName);
        }
      }
    }
    catch (Exception e)
    {
      logger.error(
          Translate.get("backend.meta.procedures.semantics.set.table.failed", e
              .getMessage()), e);
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Gets the list of columns of a given database table. The caller must ensure
   * that the parameters are not <code>null</code>.
   * 
   * @param metaData the database meta data
   * @param table the database table
   * @exception SQLException if an error occurs
   */
  public void getColumns(DatabaseMetaData metaData, DatabaseTable table)
      throws SQLException
  {
    ResultSet rs = null;
    try
    {
      // Get columns meta data
      // getColumns() gets a description of columns matching the catalog,
      // schema, table name and column name pattern. Sending in null for
      // catalog and schema drops them from the selection criteria. The
      // column pattern "%" allows to obtain all columns.
      rs = metaData.getColumns(null, table.getSchema(), table.getName(), "%");

      if (rs == null)
      {
        logger.warn(Translate.get("backend.meta.get.columns.failed", table
            .getSchema()
            + "." + table.getName()));
        return;
      }

      DatabaseColumn column = null;
      int type;
      while (rs.next())
      {
        // 1 is table catalog, 2 is table schema, 3 is table name,
        // 4 is column name, 5 is data type
        type = rs.getShort(5);
        column = new DatabaseColumn(rs.getString(4), false, type);
        table.addColumn(column);

        if (logger.isDebugEnabled())
          logger.debug(Translate.get("backend.meta.found.column", rs
              .getString(4)));
      }
    }
    catch (SQLException e)
    {
      throw new SQLException(Translate.get("backend.meta.get.columns.failed",
          table.getSchema() + "." + table.getName()));
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Gets the foreign keys of the database. They will be automatically added to
   * the list of depending tables of the referenced table and the referencing
   * table.<br>
   * The caller must ensure that the parameters are not <code>null</code>.
   * 
   * @param metaData the database meta data
   * @param databaseSchema the database schema
   */
  protected void getForeignKeys(DatabaseMetaData metaData,
      DatabaseSchema databaseSchema)
  {
    ResultSet rs = null;
    try
    {
      // Get foreign keys meta data
      // getExportedKeys() gets a description of foreign keys matching the
      // catalog, schema, and table name. Sending in null for catalog and schema
      // and '%' for all tables returns all the foreign keys of the database.

      rs = metaData.getExportedKeys(null, null, "%");

      if (rs == null)
      {
        logger.warn(Translate.get("backend.meta.get.foreign.keys.failed"));
        return;
      }

      DatabaseTable referencedTable = null;
      DatabaseTable referencingTable = null;
      while (rs.next())
      {
        // 3. PKTABLE_NAME
        // 7. FKTABLE_NAME (8. FK_COLUMN_NAME)
        referencedTable = databaseSchema.getTable(rs.getString(3));
        referencingTable = databaseSchema.getTable(rs.getString(7));
        if (referencedTable != null && referencingTable != null)
        {
          referencedTable.addDependingTable(referencingTable.getName());
          referencingTable.addDependingTable(referencedTable.getName());

          if (logger.isDebugEnabled())
            logger.debug(Translate.get("backend.meta.found.foreign.key",
                referencingTable.getName(), referencedTable.getName()));
        }
      }
    }
    catch (SQLException e)
    {
      logger.warn(Translate.get("backend.meta.get.foreign.keys.failed"), e);
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Gets the exported keys of a given database table. The exported keys will be
   * automatically added to the list of depending tables of the given table.<br>
   * The caller must ensure that the parameters are not <code>null</code>.
   * 
   * @param metaData the database meta data
   * @param table the database table
   */
  protected void getExportedKeys(DatabaseMetaData metaData, DatabaseTable table)
  {
    ResultSet rs = null;
    try
    {
      // Get exported keys meta data
      // getExportedKeys() gets a description of exported keys matching the
      // catalog, schema, and table name. Sending in null for catalog and
      // schema drop them from the selection criteria.

      rs = metaData.getExportedKeys(null, table.getSchema(), table.getName());

      if (rs == null)
      {
        logger.warn(Translate.get("backend.meta.get.exported.keys.failed",
            table.getSchema() + "." + table.getName()));
        return;
      }

      String referencingTableName = null;
      while (rs.next())
      {
        // 7. FKTABLE_NAME (8. FK_COLUMN_NAME)
        referencingTableName = rs.getString(7);
        if (referencingTableName == null)
          continue;
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("backend.meta.found.exported.key",
              referencingTableName));

        // Set the column to unique
        table.addDependingTable(referencingTableName);
      }
    }
    catch (SQLException e)
    {
      logger.warn(Translate.get("backend.meta.get.exported.keys.failed", table
          .getSchema()
          + "." + table.getName()), e);
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Gets the imported keys of a given database table. The imported keys will be
   * automatically added to the list of depending tables of the given table.<br>
   * The caller must ensure that the parameters are not <code>null</code>.
   * 
   * @param metaData the database meta data
   * @param table the database table
   */
  protected void getImportedKeys(DatabaseMetaData metaData, DatabaseTable table)
  {
    ResultSet rs = null;
    try
    {
      // Get imported keys meta data
      // getImportedKeys() gets a description of imported keys matching the
      // catalog, schema, and table name. Sending in null for catalog and
      // schema drop them from the selection criteria.

      rs = metaData.getImportedKeys(null, table.getSchema(), table.getName());

      if (rs == null)
      {
        logger.warn(Translate.get("backend.meta.get.imported.keys.failed",
            table.getSchema() + "." + table.getName()));
        return;
      }

      String referencedTableName = null;
      while (rs.next())
      {
        // 3. PKTABLE_NAME (4. PKCOLUMN_NAME)
        referencedTableName = rs.getString(3);
        if (referencedTableName == null)
          continue;
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("backend.meta.found.imported.key",
              referencedTableName));

        // Set the column to unique
        table.addDependingTable(referencedTableName);
      }
    }
    catch (SQLException e)
    {
      logger.warn(Translate.get("backend.meta.get.imported.keys.failed", table
          .getSchema()
          + "." + table.getName()), e);
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Gets the primary keys of a given database table. The caller must ensure
   * that the parameters are not <code>null</code>.
   * 
   * @param metaData the database meta data
   * @param table the database table
   * @exception SQLException if an error occurs
   */
  public void getPrimaryKeys(DatabaseMetaData metaData, DatabaseTable table)
      throws SQLException
  {
    ResultSet rs = null;
    try
    {
      // Get primary keys meta data
      // getPrimaryKeys() gets a description of primary keys matching the
      // catalog, schema, and table name. Sending in null for catalog and
      // schema drop them from the selection criteria.

      rs = metaData.getPrimaryKeys(null, table.getSchema(), table.getName());

      if (rs == null)
      {
        logger.warn(Translate.get("backend.meta.get.primary.keys.failed", table
            .getSchema()
            + "." + table.getName()));
        return;
      }

      String columnName = null;
      while (rs.next())
      {

        // 1 is table catalog, 2 is table schema, 3 is table name, 4 is column
        // name
        columnName = rs.getString(4);
        if (columnName == null)
          continue;
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("backend.meta.found.primary.key",
              columnName));

        // Set the column to unique
        table.getColumn(columnName).setIsUnique(true);
      }
    }
    catch (SQLException e)
    {
      throw new SQLException(Translate.get(
          "backend.meta.get.primary.keys.failed", table.getSchema() + "."
              + table.getName()));
    }
    finally
    {
      try
      {
        rs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }
}
