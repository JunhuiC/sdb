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

import java.util.List;

/**
 * This class defines a factory to create Sequoia requests based on a given SQL
 * query.<br>
 * It defines the regexp to use for request parsing and implements the
 * getXXXRequest() methods that create the corresponding request with custom
 * types
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 2.0
 */
public class SequoiaRequestFactory extends RequestFactory
{
  /**
   * Creates a new SequoiaRequestFactory by specifying a SequoiaRequestRegExp as
   * regular expressions to be used
   */
  public SequoiaRequestFactory()
  {
    super(new SequoiaRequestRegExp());
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getAlterRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public AlterRequest getAlterRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new AlterRequest(sqlQuery, escapeProcessing, timeout, lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getCreateRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public CreateRequest getCreateRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new CreateRequest(sqlQuery, escapeProcessing, timeout, lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getDeleteRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public DeleteRequest getDeleteRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new DeleteRequest(sqlQuery, escapeProcessing, timeout, lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getDropRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public DropRequest getDropRequest(String sqlQuery, boolean escapeProcessing,
      int timeout, String lineSeparator)
  {
    return new DropRequest(sqlQuery, escapeProcessing, timeout, lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getInsertRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public InsertRequest getInsertRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new InsertRequest(sqlQuery, escapeProcessing, timeout, lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getSelectRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public SelectRequest getSelectRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new SelectRequest(sqlQuery, escapeProcessing, timeout, lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getStoredProcedure(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public StoredProcedure getStoredProcedure(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new StoredProcedure(sqlQuery, escapeProcessing, timeout,
        lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getUnknownReadRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public UnknownReadRequest getUnknownReadRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new UnknownReadRequest(sqlQuery, escapeProcessing, timeout,
        lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getUnknownWriteRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public UnknownWriteRequest getUnknownWriteRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new UnknownWriteRequest(sqlQuery, escapeProcessing, timeout,
        lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#getUpdateRequest(java.lang.String,
   *      boolean, int, java.lang.String)
   */
  public UpdateRequest getUpdateRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    return new UpdateRequest(sqlQuery, escapeProcessing, timeout, lineSeparator);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestFactory#checkIfSelectRequiresBroadcast(java.lang.String,
   *      java.util.List)
   */
  public boolean checkIfSelectRequiresBroadcast(String sqlQuery,
      List functionsList)
  {
    return false;
  }
}
