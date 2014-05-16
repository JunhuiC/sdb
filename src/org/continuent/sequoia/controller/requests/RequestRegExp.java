/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.requests;

import java.util.regex.Pattern;

/**
 * This class defines the interface for request regular expressions.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface RequestRegExp
{

  /**
   * Pattern used to determine if a SQL statement is an AlterRequest
   * 
   * @return Returns the ALTER_REQUEST_PATTERN.
   */
  Pattern getAlterRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a CreateRequest
   * 
   * @return Returns the CREATE_REQUEST_PATTERN.
   */
  Pattern getCreateRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a DeleteRequest
   * 
   * @return Returns the DELETE_REQUEST_PATTERN.
   */
  Pattern getDeleteRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a DropRequest
   * 
   * @return Returns the DROP_REQUEST_PATTERN.
   */
  Pattern getDropRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a InsertRequest
   * 
   * @return Returns the INSERT_REQUEST_PATTERN.
   */
  Pattern getInsertQueryPattern();

  /**
   * Pattern used to determine if a SQL statement is a SELECT ... FOR UPDATE
   * query.
   * 
   * @return Returns the SELECT_FOR_UPDATE_PATTERN.
   */
  Pattern getSelectForUpdatePattern();

  /**
   * Pattern used to determine if a SQL statement is a SelectRequest
   * 
   * @return Returns the SELECT_REQUEST_PATTERN.
   */
  Pattern getSelectRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a StoredProcedure
   * 
   * @return Returns the STORED_PROCEDURE_PATTERN.
   */
  Pattern getStoredProcedurePattern();

  /**
   * Pattern used to determine if a SQL statement is a UnknownReadRequest
   * 
   * @return Returns the UNKNOWN_READ_REQUEST_PATTERN.
   */
  Pattern getUnknownReadRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a UnknownReadRequest
   * 
   * @return Returns the UNKNOWN_WRITE_REQUEST_PATTERN.
   */
  Pattern getUnknownWriteRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a UpdateRequest
   * 
   * @return Returns the UPDATE_REQUEST_PATTERN.
   */
  Pattern getUpdateRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is a request that needs to be
   * executed with statementExecute
   * 
   * @return Returns the STATEMENT_EXECUTE_REQUEST_PATTERN.
   */
  Pattern getStatementExecuteRequestPattern();

  /**
   * Pattern used to determine if a SQL statement is not authorized to execute
   * on the cluster.
   * 
   * @return Returns the STATEMENT_EXECUTE_REQUEST_PATTERN.
   */
  Pattern getUnauthorizedRequestsPattern();

}