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
 * Initial developer(s):
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;

/**
 * This class defines a SequoiaSchemaFactory
 * 
 * @author <a href="mailto:joe.daly@continuent.com">Joe Daly</a>
 * @version 1.0
 */
public class SequoiaSchemaFactory implements SchemaFactory
{
  /**
   * @see org.continuent.sequoia.controller.backend.SchemaFactory#createDatabaseSchema(String,
   *      Trace, Connection, int, boolean, String, DatabaseSQLMetaData)
   */
  public DatabaseSchema createDatabaseSchema(String vdbName, Trace logger,
      Connection connection, int dynamicPrecision, boolean gatherSystemTables,
      String schemaPattern, DatabaseSQLMetaData databaseSQLMetaData)
      throws SQLException
  {
    ResultSet rs = null;

    boolean connectionWasAutocommit = connection.getAutoCommit();

    if (connectionWasAutocommit)
      connection.setAutoCommit(false); // Needed for Derby Get DatabaseMetaData

    DatabaseMetaData metaData = connection.getMetaData();
    if (metaData == null)
    {
      logger.warn(Translate.get("backend.meta.received.null"));
      return null;
    }

    if (logger.isDebugEnabled())
      logger.debug("Fetching schema with precision '"
          + DatabaseBackendSchemaConstants
              .getDynamicSchemaLevel(dynamicPrecision)
          + (gatherSystemTables ? "'" : "' not") + " including system tables.");

    DatabaseSchema databaseSchema = new DatabaseSchema(vdbName);

    // Check if we should get system tables or not
    String[] types;
    if (gatherSystemTables)
    {
      schemaPattern = null;
      types = new String[]{"TABLE", "VIEW", "SYSTEM TABLE", "SYSTEM VIEW",
          "SEQUENCE"};
    }
    else
      types = new String[]{"TABLE", "VIEW", "SEQUENCE"};

    // Get tables meta data
    // getTables() gets a description of tables matching the catalog, schema,
    // table name pattern and type. Sending in null for catalog and schema
    // drops them from the selection criteria. The table name pattern "%"
    // means match any substring of 0 or more characters.
    // Last argument allows to obtain only database tables
    try
    {
      rs = metaData.getTables(null, schemaPattern, "%", types);
    }
    catch (Exception e)
    {
      // VIEWS cannot be retrieved with this backend
      logger.error(Translate.get("backend.meta.view.not.supported"), e);
      if (gatherSystemTables)
        types = new String[]{"TABLE", "SYSTEM TABLE"};
      else
        types = new String[]{"TABLE"};
      rs = metaData.getTables(null, schemaPattern, "%", types);
    }

    if (rs == null)
    {
      logger.warn(Translate.get("backend.meta.received.null"));
      if (connectionWasAutocommit)
        connection.commit();
      return null;
    }

    // Check if we can get all foreign keys for this database
    boolean optimized = false;
    try
    {
      optimized = metaData.getExportedKeys(null, null, "%") != null;
    }
    catch (SQLException ignored)
    {
      logger
          .warn("Got an exception when executing getExportedKeys(null, null, \"%\") : "
              + ignored);
    }

    String tableName;
    String schemaName;
    DatabaseTable table = null;
    while (rs.next())
    {
      // 1 is table catalog, 2 is table schema, 3 is table name, 4 is type
      schemaName = rs.getString(2);
      tableName = rs.getString(3);
      String type = rs.getString(4);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("backend.meta.found.table", schemaName + "."
            + tableName));

      // Create a new table and add it to the database schema
      table = new DatabaseTable(tableName);
      table.setSchema(schemaName);
      if (!type.equals("VIEW") && !optimized)
      {
        databaseSQLMetaData.getExportedKeys(metaData, table);
        databaseSQLMetaData.getImportedKeys(metaData, table);
      }
      databaseSchema.addTable(table);
      if (dynamicPrecision >= DatabaseBackendSchemaConstants.DynamicPrecisionColumn)
      {
        // Get information about this table columns
        databaseSQLMetaData.getColumns(metaData, table);
        // Get information about this table primary keys
        if (!type.equals("VIEW"))
        {
          databaseSQLMetaData.getPrimaryKeys(metaData, table);
        }
      }
    }

    // Get Foreign keys for this database
    if (optimized)
      databaseSQLMetaData.getForeignKeys(metaData, databaseSchema);

    // Get Procedures for this database
    if (dynamicPrecision >= DatabaseBackendSchemaConstants.DynamicPrecisionProcedures)
      databaseSQLMetaData.getProcedures(metaData, databaseSchema);

    try
    {
      rs.close();
    }
    catch (Exception ignore)
    {
    }

    if (connectionWasAutocommit)
    {
      try
      {
        connection.commit();
      }
      catch (Exception ignore)
      {
        // This was a read-only transaction
      }

      try
      {
        // restore connection
        connection.setAutoCommit(true);
      }
      catch (SQLException e1)
      {
        // ignore, transaction is no more valid
      }
    }

    return databaseSchema;
  }
}
