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
 * Initial developer(s): Joe Daly.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend;

import java.sql.SQLException;
import java.sql.Connection;

import org.continuent.sequoia.common.log.Trace;


import org.continuent.sequoia.common.sql.schema.DatabaseSchema;

/**
 * This class defines a SchemaFactory to construct a DatabaseSchema.
 * 
 * @author <a href="mailto:joe.daly@continuent.com">Joe Daly</a>
 * @version 1.0
 */

public interface SchemaFactory
{
  /**
   * Create a database schema from the given connection
   * 
   * @param vdbName the virtual database name this schema represents
   * @param logger the log4j logger to output to
   * @param connection a jdbc connection to a database
   * @param dynamicPrecision precision used to create the schema
   * @param gatherSystemTables should we gather system tables
   * @param schemaPattern schema pattern to look for (reduce the scope of
   *          gathering if not null)
   * @param databaseSQLMetaData instance used in schema construction
   * @return <code>DatabaseSchema</code> contructed from the information collected
   *         through jdbc
   * @throws SQLException if an error occurs with the given connection         
   */ 
  DatabaseSchema createDatabaseSchema(String vdbName, Trace logger,
      Connection connection, int dynamicPrecision, boolean gatherSystemTables,
      String schemaPattern, DatabaseSQLMetaData databaseSQLMetaData)
      throws SQLException;
}
