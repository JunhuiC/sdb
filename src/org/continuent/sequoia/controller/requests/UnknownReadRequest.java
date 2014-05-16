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

package org.continuent.sequoia.controller.requests;

import java.sql.SQLException;

import org.continuent.sequoia.common.sql.schema.DatabaseSchema;

/**
 * This class defines an UnknownReadRequest used for all SQL statements that are
 * not SELECT but should be executed as read requests.<br>
 * An UnknownReadRequest is a request that returns a ResultSet and that we are
 * not able to parse (we cannot know which tables are accessed, if any).
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class UnknownReadRequest extends SelectRequest
{
  private static final long serialVersionUID = -1056060047939721758L;

  /**
   * Creates a new <code>UnknownReadRequest</code> object
   * 
   * @param sqlQuery the SQL query
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database ?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   */
  public UnknownReadRequest(String sqlQuery, boolean escapeProcessing,
      int timeout, String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator,
        RequestType.UNKNOWN_READ);
    setMacrosAreProcessed(true); // no macro processing needed
  }

  /**
   * Request is not parsed (isParsed is just set to true).
   * 
   * @see AbstractRequest#cloneParsing(AbstractRequest)
   */
  public void cloneParsing(AbstractRequest request)
  {
    isParsed = true;
  }

  /**
   * Request is not parsed (isParsed is just set to true).
   * 
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#parse(org.continuent.sequoia.common.sql.schema.DatabaseSchema,
   *      int, boolean)
   */
  public void parse(DatabaseSchema schema, int granularity,
      boolean isCaseSensitive) throws SQLException
  { // No parsing for unknown read request
    // Just let database try to execute the request
    isParsed = true;
  }

  /**
   * Always returns RequestType.UNCACHEABLE
   * 
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getCacheAbility()
   */
  public int getCacheAbility()
  {
    return RequestType.UNCACHEABLE;
  }

  /**
   * Always returns true since the request cannot be parsed anyway.
   * 
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#isParsed()
   */
  public boolean isParsed()
  {
    return true;
  }

}
