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

import java.sql.Connection;
import java.util.List;

/**
 * This class defines a RequestFactory.<br>
 * Given a SQL query, it creates a full request of the correct type. These
 * request types are defined by the implementing classes, as well as the regular
 * expressions to be used for the SQL query parsing
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 2.0
 */
public abstract class RequestFactory
{
  /** Reqular expressions to be used for request parsing */
  protected RequestRegExp driverRegExp = null;

  /**
   * Sets the custom regular expressions
   * 
   * @param regexp regular expression to be used for request parsing
   */
  protected RequestFactory(RequestRegExp regexp)
  {
    driverRegExp = regexp;
  }

  /**
   * Parses the given string:
   * <ul>
   * <li>if a read query is identified, creates and returns the appropriate
   * read request
   * <li>else if a write query is identified, creates and returns an unknown
   * write request
   * <li>else returns null
   * </ul>
   * 
   * @param sqlQuery sql statement to parse
   * @param escapeProcessing query parameter to set
   * @param timeout query parameter to set
   * @param lineSeparator query parameter to set
   * @return a <code>SelectRequest</code>, an <code>UnknownReadRequest</code>,
   *         an <code>UnknownWriteRequest</code> or <code>null</code>
   */
  protected AbstractRequest decodeReadRequestFromString(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    AbstractRequest decodedRequest = null;
    if (isSelect(sqlQuery))
    {
      decodedRequest = getSelectRequest(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
      if (driverRegExp.getSelectForUpdatePattern().matcher(sqlQuery).matches())
        ((SelectRequest) decodedRequest).setMustBroadcast(true);
    }
    if (isUnknownWrite(sqlQuery))
    {
      decodedRequest = getUnknownWriteRequest(sqlQuery, escapeProcessing,
          timeout, lineSeparator);
    }
    else if (isUnknownRead(sqlQuery))
    {
      decodedRequest = getUnknownReadRequest(sqlQuery, escapeProcessing,
          timeout, lineSeparator);
      if (driverRegExp.getSelectForUpdatePattern().matcher(sqlQuery).matches())
        ((SelectRequest) decodedRequest).setMustBroadcast(true);
    }
    return decodedRequest;
  }

  /**
   * Parses the given string and creates/returns the appropriate write request,
   * stored procedure or <code>null</code> if no write query was identified
   * 
   * @param sqlQuery sql statement to parse
   * @param escapeProcessing query parameter to set
   * @param timeout query parameter to set
   * @param lineSeparator query parameter to set
   * @return an <code>InsertRequest</code>, an <code>UpdateRequest</code>,
   *         a <code>DeleteRequest</code>, a <code>CreateRequest</code>, a
   *         <code>DropRequest</code>, an <code>AlterRequest</code>, a
   *         <code>StoredProcedure</code> or <code>null</code>
   */
  protected AbstractRequest decodeWriteRequestFromString(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator)
  {
    AbstractRequest decodedRequest = null;
    if (isInsert(sqlQuery))
    {
      decodedRequest = getInsertRequest(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
    }
    else if (isUpdate(sqlQuery))
    {
      decodedRequest = getUpdateRequest(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
    }
    else if (isDelete(sqlQuery))
    {
      decodedRequest = getDeleteRequest(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
    }
    else if (isCreate(sqlQuery))
    {
      decodedRequest = getCreateRequest(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
    }
    else if (isDrop(sqlQuery))
    {
      decodedRequest = getDropRequest(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
    }
    else if (isAlter(sqlQuery))
    {
      decodedRequest = getAlterRequest(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
    }
    else if (isStoredProcedure(sqlQuery))
    {
      decodedRequest = getStoredProcedure(sqlQuery, escapeProcessing, timeout,
          lineSeparator);
    }
    return decodedRequest;
  }

  /**
   * Given a sql statement string, creates the appropriate read or write
   * request, or <code>null</code> if no sql query could be identified.
   * Priority of the parsing and the parameters of the request to be created are
   * configurable tries to identify read or write first)
   * 
   * @param sqlQuery sql statement to parse
   * @param isProbablyAReadRequest set to true if the given sql query is
   *          probably a read statement. This will give priority to the parsing
   *          of read requests
   * @param escapeProcessing query parameter to set
   * @param timeout query parameter to set
   * @param lineSeparator query parameter to set
   * @return an <code>InsertRequest</code>, an <code>UpdateRequest</code>,
   *         a <code>DeleteRequest</code>, a <code>CreateRequest</code>, a
   *         <code>DropRequest</code>, an <code>AlterRequest</code>, a
   *         <code>StoredProcedure</code>, a <code>SelectRequest</code>,
   *         an <code>UnknownReadRequest</code>, an
   *         <code>UnknownWriteRequest</code> or <code>null</code>
   */
  public AbstractRequest requestFromString(String sqlQuery,
      boolean isProbablyAReadRequest, boolean escapeProcessing, int timeout,
      String lineSeparator)
  {
    AbstractRequest decodedRequest = null;
    if (isProbablyAReadRequest)
    {
      // more probably a read, let's try it first
      decodedRequest = decodeReadRequestFromString(sqlQuery, escapeProcessing,
          timeout, lineSeparator);
    }
    if (decodedRequest == null || decodedRequest instanceof UnknownWriteRequest)
    {
      // either isProbablyAReadRequest == false or decodeRead found an unknown
      // write
      decodedRequest = decodeWriteRequestFromString(sqlQuery, escapeProcessing,
          timeout, lineSeparator);
    }
    if (decodedRequest == null && !isProbablyAReadRequest)
    {
      // decodeWrite didn't find anything, let's try decodeRead
      decodedRequest = decodeReadRequestFromString(sqlQuery, escapeProcessing,
          timeout, lineSeparator);
    }
    if (decodedRequest == null)
    {
      // nothing found, safer to say that it is an unknown read
      decodedRequest = getUnknownReadRequest(sqlQuery, escapeProcessing,
          timeout, lineSeparator);
    }
    return decodedRequest;
  }

  /**
   * Returns true if the query has to be executed with statement.execute() or if
   * it contains an inline batch such as "SELECT xxx ; INSERT xxx ; DROP xxx ;
   * ..."
   * 
   * @param request the request to check
   * @return true if the request has to be executed with statement.execute() or
   *         contains an inline batch
   */
  public boolean requestNeedsExecute(AbstractRequest request)
  {
    // Check if the request match the StatementExecuteRequest pattern
    if (driverRegExp.getStatementExecuteRequestPattern().matcher(
        request.getSqlOrTemplate()).matches())
      return true;

    // Otherwise, check if the query contains an inline batch such as "SELECT
    // xxx ; INSERT xxx ; DROP xxx ; ..."
    String sql = request.getSqlOrTemplate();
    // Remove all parameters content from the query - see SEQUOIA-812
    sql = sql.replaceAll("\\\\", "");
    sql = sql.replaceAll("'[^']*", "''");
    
    int semiColonIdx = sql.indexOf(';');

    // Test for no semicolon
    if (semiColonIdx == -1)
      return false;

    // Test for multiple semicolons
    // TODO: Check that semicolon does not belong to a parameter
    if (sql.indexOf(';', semiColonIdx + 1) != -1)
      return true;

    // Single semicolon, check that this was not just appended at the end of a
    // single query
    if (sql.trim().endsWith(";"))
      return false; // query ends by semicolon
    else
      return true;
  }

  /**
   * Check whether the given sql is authorized to execute on the cluster or not.
   * 
   * @param sql the SQL statement to check
   * @return false if the statement is not authorized and must be blocked, true
   *         otherwise
   */
  public boolean isAuthorizedRequest(String sql)
  {
    return !driverRegExp.getUnauthorizedRequestsPattern().matcher(sql)
        .matches();
  }

  /**
   * Determines whether or not a select statement should be broadcast based on
   * the setting of the transaction isolation level. By default request at
   * transaction isolation level serializable are broadcast.
   * 
   * @param transactionIsolationLevel the transaction isolation level for the
   *          current request.
   * @return true if the select should be broadcast.
   */
  public boolean isBroadcastRequired(int transactionIsolationLevel)
  {
    return transactionIsolationLevel == Connection.TRANSACTION_SERIALIZABLE;
  }

  /**
   * @see AlterRequest#AlterRequest(String, boolean, int, String)
   */
  protected abstract AlterRequest getAlterRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see CreateRequest#CreateRequest(String, boolean, int, String)
   */
  protected abstract CreateRequest getCreateRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see DeleteRequest#DeleteRequest(String, boolean, int, String)
   */
  protected abstract DeleteRequest getDeleteRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see DropRequest#DropRequest(String, boolean, int, String)
   */
  protected abstract DropRequest getDropRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see InsertRequest#InsertRequest(String, boolean, int, String)
   */
  protected abstract InsertRequest getInsertRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see SelectRequest#SelectRequest(String, boolean, int, String)
   */
  protected abstract SelectRequest getSelectRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see StoredProcedure#StoredProcedure(String, boolean, int, String)
   */
  protected abstract StoredProcedure getStoredProcedure(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see UnknownReadRequest#UnknownReadRequest(String, boolean, int, String)
   */
  protected abstract UnknownReadRequest getUnknownReadRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * @see UnknownWriteRequest#UnknownWriteRequest(String, boolean, int, String)
   */
  protected abstract UnknownWriteRequest getUnknownWriteRequest(
      String sqlQuery, boolean escapeProcessing, int timeout,
      String lineSeparator);

  /**
   * @see UpdateRequest#UpdateRequest(String, boolean, int, String)
   */
  protected abstract UpdateRequest getUpdateRequest(String sqlQuery,
      boolean escapeProcessing, int timeout, String lineSeparator);

  /**
   * Check if a SELECT statement has update side-effects that require it to be
   * broadcast.
   * 
   * @param sqlQuery the SQL statement
   * @param functionsList
   * @return
   */
  public abstract boolean checkIfSelectRequiresBroadcast(String sqlQuery,
      List<?> functionsList);

  /**
   * isAlter checks if the provided query matches with an ALTER statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is an ALTER statement
   */
  protected boolean isAlter(String sqlQuery)
  {
    return driverRegExp.getAlterRequestPattern().matcher(sqlQuery).matches();
  }

  /**
   * isCreate checks if the provided query matches with a CREATE statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is a CREATE statement
   */
  protected boolean isCreate(String sqlQuery)
  {
    return driverRegExp.getCreateRequestPattern().matcher(sqlQuery).matches();
  }

  /**
   * isDelete checks if the provided query matches with a DELETE statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is a DELETE statement
   */
  protected boolean isDelete(String sqlQuery)
  {
    return driverRegExp.getDeleteRequestPattern().matcher(sqlQuery).matches();
  }

  /**
   * isDrop checks if the provided query matches with a DROP statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is a DROP statement
   */
  protected boolean isDrop(String sqlQuery)
  {
    return driverRegExp.getDropRequestPattern().matcher(sqlQuery).matches();
  }

  /**
   * isInsert checks if the provided query matches with an INSERT statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is an INSERT statement
   */
  protected boolean isInsert(String sqlQuery)
  {
    return driverRegExp.getInsertQueryPattern().matcher(sqlQuery).matches();
  }

  /**
   * isSelect checks if the provided query matches with a SELECT statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is a SELECT statement
   */
  protected boolean isSelect(String sqlQuery)
  {
    return driverRegExp.getSelectRequestPattern().matcher(sqlQuery).matches();
  }

  /**
   * isStoredProcedure checks if the provided query matches with a STORE
   * PROCEDURE CALL statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is a STORE PROCEDURE CALL statement
   */
  protected boolean isStoredProcedure(String sqlQuery)
  {
    return driverRegExp.getStoredProcedurePattern().matcher(sqlQuery).matches();
  }

  /**
   * isUnknownRead checks if the provided query matches with an UNKNOWN READ
   * statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is an UNKNOWN READ statement
   */
  protected boolean isUnknownRead(String sqlQuery)
  {
    return driverRegExp.getUnknownReadRequestPattern().matcher(sqlQuery)
        .matches();
  }

  /**
   * isUnknownWrite checks if the provided query matches with an UNKNOWN WRITE
   * statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is an UNKNOWN WRITE statement
   */
  protected boolean isUnknownWrite(String sqlQuery)
  {
    return driverRegExp.getUnknownWriteRequestPattern().matcher(sqlQuery)
        .matches();
  }

  /**
   * isUpdate checks if the provided query matches with an UPDATE statement.
   * 
   * @param sqlQuery is the query to be checked
   * @return TRUE if the query is an UPDATE statement
   */
  protected boolean isUpdate(String sqlQuery)
  {
    return driverRegExp.getUpdateRequestPattern().matcher(sqlQuery).matches();
  }

}
