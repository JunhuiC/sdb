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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend;

import java.sql.SQLException;

import org.continuent.sequoia.common.protocol.Field;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;

/**
 * This class defines a ResultSetMetaDataFactory to copy database specific
 * metadata into Sequoia metadata.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface ResultSetMetaDataFactory
{

  /**
   * Creates a new <code>Field[]</code> object
   * 
   * @param metaData Generate a Sequoia Field[] from a generic ResultSetMetadata
   * @param metadataCache Optional metadata cache (null if none)
   * @return a Sequoia Field[] containing the ResultSetMedaData
   * @throws SQLException if the metadata is null
   */
  Field[] copyResultSetMetaData(java.sql.ResultSetMetaData metaData,
      MetadataCache metadataCache) throws SQLException;

}
