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

import java.util.regex.Pattern;

/**
 * This class defines the regular expressions to be used with generic ANSI SQL
 * requests.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public final class SequoiaRequestRegExp implements RequestRegExp
{
  private static final String  ALTER_REQUEST_PATTERN_STRING             = "^alter.*";
  private static final String  CREATE_REQUEST_PATTERN_STRING            = "^(create|select\\s.+\\sinto\\s).*";
  private static final String  DELETE_REQUEST_PATTERN_STRING            = "^(delete|truncate).*";
  private static final String  DROP_REQUEST_PATTERN_STRING              = "^drop.*";
  private static final String  INSERT_REQUEST_PATTERN_STRING            = "^insert.*";
  private static final String  SELECT_REQUEST_PATTERN_STRING            = "^(\\s|\\()*select.*";
  private static final String  SELECT_FOR_UPDATE_PATTERN_STRING         = ".*select.*\\s+for\\s+update\\s*.*";
  private static final String  STORED_PROCEDURE_PATTERN_STRING          = "^\\{(\\s*\\?\\s*=)?\\s*call.*";
  private static final String  UPDATE_REQUEST_PATTERN_STRING            = "^update.*";
  private static final String  UNKNOWN_READ_REQUEST_PATTERN_STRING      = "^show.*";
  private static final String  UNKNOWN_WRITE_REQUEST_PATTERN_STRING     = "^(grant|lock|revoke|set).*";
  private static final String  STATEMENT_EXECUTE_REQUEST_PATTERN_STRING = "^(explain\\s+analyze).*";
  private static final String  UNAUTHORIZED_REQUEST_PATTERN_STRING      = "^(shutdown|kill).*";

  // Note that we use Patterns and not Matchers because these objects are
  // potentially accessed concurrently and parsing could occur in parallel. This
  // way, each caller will have to call Pattern.matcher() that will allocate a
  // matcher for its own use in a thread-safe way.

  private static final Pattern ALTER_REQUEST_PATTERN                    = Pattern
                                                                            .compile(
                                                                                ALTER_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern CREATE_REQUEST_PATTERN                   = Pattern
                                                                            .compile(
                                                                                CREATE_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern DELETE_REQUEST_PATTERN                   = Pattern
                                                                            .compile(
                                                                                DELETE_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);
  private static final Pattern DROP_REQUEST_PATTERN                     = Pattern
                                                                            .compile(
                                                                                DROP_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern INSERT_REQUEST_PATTERN                   = Pattern
                                                                            .compile(
                                                                                INSERT_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern SELECT_REQUEST_PATTERN                   = Pattern
                                                                            .compile(
                                                                                SELECT_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);
  private static final Pattern SELECT_FOR_UPDATE_PATTERN                = Pattern
                                                                            .compile(
                                                                                SELECT_FOR_UPDATE_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern STORED_PROCEDURE_PATTERN                 = Pattern
                                                                            .compile(
                                                                                STORED_PROCEDURE_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern UPDATE_REQUEST_PATTERN                   = Pattern
                                                                            .compile(
                                                                                UPDATE_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern UNKNOWN_READ_REQUEST_PATTERN             = Pattern
                                                                            .compile(
                                                                                UNKNOWN_READ_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern UNKNOWN_WRITE_REQUEST_PATTERN            = Pattern
                                                                            .compile(
                                                                                UNKNOWN_WRITE_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern STATEMENT_EXECUTE_REQUEST_PATTERN        = Pattern
                                                                            .compile(
                                                                                STATEMENT_EXECUTE_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern UNAUTHORIZED_REQUEST_PATTERN             = Pattern
                                                                            .compile(
                                                                                UNAUTHORIZED_REQUEST_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getAlterRequestPattern()
   */
  public Pattern getAlterRequestPattern()
  {
    return ALTER_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getCreateRequestPattern()
   */
  public Pattern getCreateRequestPattern()
  {
    return CREATE_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getDeleteRequestPattern()
   */
  public Pattern getDeleteRequestPattern()
  {
    return DELETE_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getDropRequestPattern()
   */
  public Pattern getDropRequestPattern()
  {
    return DROP_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getInsertQueryPattern()
   */
  public Pattern getInsertQueryPattern()
  {
    return INSERT_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getSelectForUpdatePattern()
   */
  public Pattern getSelectForUpdatePattern()
  {
    return SELECT_FOR_UPDATE_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getSelectRequestPattern()
   */
  public Pattern getSelectRequestPattern()
  {
    return SELECT_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getStoredProcedurePattern()
   */
  public Pattern getStoredProcedurePattern()
  {
    return STORED_PROCEDURE_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getUnknownReadRequestPattern()
   */
  public Pattern getUnknownReadRequestPattern()
  {
    return UNKNOWN_READ_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getUnknownWriteRequestPattern()
   */
  public Pattern getUnknownWriteRequestPattern()
  {
    return UNKNOWN_WRITE_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getUpdateRequestPattern()
   */
  public Pattern getUpdateRequestPattern()
  {
    return UPDATE_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getStatementExecuteRequestPattern()
   */
  public Pattern getStatementExecuteRequestPattern()
  {
    return STATEMENT_EXECUTE_REQUEST_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.RequestRegExp#getUnauthorizedRequestsPattern()
   */
  public Pattern getUnauthorizedRequestsPattern()
  {
    return UNAUTHORIZED_REQUEST_PATTERN;
  }

}
