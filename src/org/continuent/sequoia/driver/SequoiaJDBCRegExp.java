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

package org.continuent.sequoia.driver;

import java.util.regex.Pattern;

/**
 * This class defines the regular expressions to map generic ANSI SQL requests
 * to JDBC API calls.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public final class SequoiaJDBCRegExp implements JDBCRegExp
{
  /** Regexp patterns for Statement class */
  private static final String  BEGIN_PATTERN_STRING                     = "^begin.*";
  private static final String  COMMIT_PATTERN_STRING                    = "^commit.*";
  private static final String  RELEASE_SAVEPOINT_PATTERN_STRING         = "^release\\s+savepoint.*";
  private static final String  ROLLBACK_PATTERN_STRING                  = "^rollback.*";
  private static final String  ROLLBACK_TO_SAVEPOINT_PATTERN_STRING     = "^rollback\\s+to\\s+.*";
  private static final String  SET_ISOLATION_LEVEL_PATTERN_STRING       = "^set.*transaction.*isolation\\s+level.*";
  private static final String  SET_READ_ONLY_TRANSACTION_PATTERN_STRING = "^set.*transaction\\s+read\\s+only.*";
  private static final String  SET_SAVEPOINT_PATTERN_STRING             = "^savepoint.*";
  private static final String  SET_AUTOCOMMIT_1_PATTERN_STRING          = "";

  // Note that we use Patterns and not Matchers because these objects are
  // potentially accessed concurrently and parsing could occur in parallel. This
  // way, each caller will have to call Pattern.matcher() that will allocate a
  // matcher for its own use in a thread-safe way.

  private static final Pattern BEGIN_PATTERN                            = Pattern
                                                                            .compile(
                                                                                BEGIN_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern COMMIT_PATTERN                           = Pattern
                                                                            .compile(
                                                                                COMMIT_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern RELEASE_SAVEPOINT_PATTERN                = Pattern
                                                                            .compile(
                                                                                RELEASE_SAVEPOINT_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern ROLLBACK_PATTERN                         = Pattern
                                                                            .compile(
                                                                                ROLLBACK_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern ROLLBACK_TO_SAVEPOINT_PATTERN            = Pattern
                                                                            .compile(
                                                                                ROLLBACK_TO_SAVEPOINT_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern SET_ISOLATION_LEVEL_PATTERN              = Pattern
                                                                            .compile(
                                                                                SET_ISOLATION_LEVEL_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern SET_READ_ONLY_TRANSACTION_PATTERN        = Pattern
                                                                            .compile(
                                                                                SET_READ_ONLY_TRANSACTION_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  private static final Pattern SET_SAVEPOINT_PATTERN                    = Pattern
                                                                            .compile(
                                                                                SET_SAVEPOINT_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);
  private static final Pattern SET_AUTOCOMMIT_1_PATTERN                 = Pattern
                                                                            .compile(
                                                                                SET_AUTOCOMMIT_1_PATTERN_STRING,
                                                                                Pattern.CASE_INSENSITIVE
                                                                                    | Pattern.DOTALL);

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getBeginPattern()
   */
  public Pattern getBeginPattern()
  {
    return BEGIN_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getCommitPattern()
   */
  public Pattern getCommitPattern()
  {
    return COMMIT_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getReleaseSavepointPattern()
   */
  public Pattern getReleaseSavepointPattern()
  {
    return RELEASE_SAVEPOINT_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getRollbackPattern()
   */
  public Pattern getRollbackPattern()
  {
    return ROLLBACK_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getRollbackToSavepointPattern()
   */
  public Pattern getRollbackToSavepointPattern()
  {
    return ROLLBACK_TO_SAVEPOINT_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getSetIsolationLevelPattern()
   */
  public Pattern getSetIsolationLevelPattern()
  {
    return SET_ISOLATION_LEVEL_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getSetReadOnlyTransactionPattern()
   */
  public Pattern getSetReadOnlyTransactionPattern()
  {
    return SET_READ_ONLY_TRANSACTION_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getSetSavepointPattern()
   */
  public Pattern getSetSavepointPattern()
  {
    return SET_SAVEPOINT_PATTERN;
  }

  /**
   * @see org.continuent.sequoia.driver.JDBCRegExp#getSetAutocommitPattern()
   */
  public Pattern getSetAutocommit1Pattern()
  {
    return SET_AUTOCOMMIT_1_PATTERN;
  }

}
