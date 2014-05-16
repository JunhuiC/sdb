/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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

package org.continuent.sequoia.controller.backend;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.continuent.sequoia.common.protocol.Field;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;

/**
 * This class defines a SequoiaResultSetMetaDataFactory
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class SequoiaResultSetMetaDataFactory
    implements
      ResultSetMetaDataFactory
{

  /**
   * @see org.continuent.sequoia.controller.backend.ResultSetMetaDataFactory#copyResultSetMetaData(java.sql.ResultSetMetaData,
   *      org.continuent.sequoia.controller.cache.metadata.MetadataCache)
   */
  public Field[] copyResultSetMetaData(ResultSetMetaData metaData,
      MetadataCache metadataCache) throws SQLException
  {
    if (metaData == null)
    {
      // Nothing to fetch
      return new Field[0];
      // At least two drivers (pgjdbc & hsqldb) return a null reference
      // in this case, so we are not transparent here. See SEQUOIA-262.
    }

    int nbColumn = metaData.getColumnCount();
    Field[] fields = new Field[nbColumn];
    for (int i = 1; i <= nbColumn; i++)
    { // 1st column is 1
      String columnName = metaData.getColumnName(i);
      String fTableName = null;
      try
      {
        fTableName = metaData.getTableName(i);
      }
      catch (Exception ignore)
      {
      }
      String columnLabel = null;
      try
      {
        columnLabel = metaData.getColumnLabel(i);
      }
      catch (Exception ignore)
      {
      }
      if (metadataCache != null)
      { // Check Field cache
        fields[i - 1] = metadataCache.getField(fTableName + "." + columnName
            + "." + columnLabel);
        if (fields[i - 1] != null)
          continue; // Cache hit
      }
      // Field cache miss
      int fColumnDisplaySize = 0;
      try
      {
        fColumnDisplaySize = metaData.getColumnDisplaySize(i);
      }
      catch (Exception ignore)
      {
      }
      int columnType = -1;
      try
      {
        columnType = metaData.getColumnType(i);
      }
      catch (Exception ignore)
      {
      }
      String columnTypeName = null;
      try
      {
        columnTypeName = metaData.getColumnTypeName(i);
      }
      catch (Exception ignore)
      {
      }
      String fColumnClassName = null;
      try
      {
        fColumnClassName = metaData.getColumnClassName(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsAutoIncrement = false;
      try
      {
        fIsAutoIncrement = metaData.isAutoIncrement(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsCaseSensitive = false;
      try
      {
        fIsCaseSensitive = metaData.isCaseSensitive(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsCurrency = false;
      try
      {
        fIsCurrency = metaData.isCurrency(i);
      }
      catch (Exception ignore)
      {
      }
      int fIsNullable = ResultSetMetaData.columnNullableUnknown;
      try
      {
        fIsNullable = metaData.isNullable(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsReadOnly = false;
      try
      {
        fIsReadOnly = metaData.isReadOnly(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsWritable = false;
      try
      {
        fIsWritable = metaData.isWritable(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsDefinitelyWritable = false;
      try
      {
        fIsDefinitelyWritable = metaData.isDefinitelyWritable(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsSearchable = false;
      try
      {
        fIsSearchable = metaData.isSearchable(i);
      }
      catch (Exception ignore)
      {
      }
      boolean fIsSigned = false;
      try
      {
        fIsSigned = metaData.isSigned(i);
      }
      catch (Exception ignore)
      {
      }
      int fPrecision = 0;
      try
      {
        fPrecision = metaData.getPrecision(i);
      }
      catch (Exception ignore)
      {
      }
      int fScale = 0;
      try
      {
        fScale = metaData.getScale(i);
      }
      catch (Exception ignore)
      {
      }
      String encoding = null;
      // Here we trust the backend's driver not to change behind our back the
      // metadata Strings it gave to us (this would be a rather weird
      // interpretation of the JDBC spec)
      fields[i - 1] = new Field(fTableName, columnName, columnLabel,
          fColumnDisplaySize, columnType, columnTypeName, fColumnClassName,
          fIsAutoIncrement, fIsCaseSensitive, fIsCurrency, fIsNullable,
          fIsReadOnly, fIsWritable, fIsDefinitelyWritable, fIsSearchable,
          fIsSigned, fPrecision, fScale, encoding);

      if (metadataCache != null)
        // Add field to cache
        metadataCache.addField(fTableName + "." + columnName + "."
            + columnLabel, fields[i - 1]);
    } // for
    return fields;
  }

}
