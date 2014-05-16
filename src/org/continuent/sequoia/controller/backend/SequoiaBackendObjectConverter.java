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

package org.continuent.sequoia.controller.backend;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.continuent.sequoia.common.protocol.Field;

/**
 * This class defines a SequoiaBackendObjectConverter that just returns the
 * ResultSet object.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class SequoiaBackendObjectConverter implements BackendObjectConverter
{

  /**
   * @see org.continuent.sequoia.controller.backend.BackendObjectConverter#convertResultSetObject(java.sql.ResultSet,
   *      int, Field)
   */
  public Object convertResultSetObject(ResultSet dbResultSet, int i, Field f)
      throws SQLException
  {
    Object object = dbResultSet.getObject(i + 1);
    return object;
  }

}
