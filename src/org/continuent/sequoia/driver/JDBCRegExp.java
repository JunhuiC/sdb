/**
 * Sequoia: Database clustering technology.
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

package org.continuent.sequoia.driver;

import java.util.regex.Pattern;

/**
 * This interface defines the methods for SQL to JDBC API mapping using regular
 * expressions.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface JDBCRegExp
{

  /**
   * Pattern maching a transaction BEGIN that should be mapped on a
   * setAutocommit(false) call.
   * 
   * @return Returns the BEGIN_PATTERN.
   */
  Pattern getBeginPattern();

  /**
   * Pattern maching a transaction COMMIT that should be mapped on a
   * connection.commit() call.
   * 
   * @return Returns the COMMIT_PATTERN.
   */
  Pattern getCommitPattern();

  /**
   * Pattern maching a RELEASE SAVEPOINT statememt that should be mapped on a
   * connection.releaseSavepoint(Savepoint) call.
   * 
   * @return Returns the RELEASE_SAVEPOINT_PATTERN.
   */
  Pattern getReleaseSavepointPattern();

  /**
   * Pattern maching a transaction ROLLBACK that should be mapped on a
   * connection.rollback() call.
   * 
   * @return Returns the ROLLBACK_PATTERN.
   */
  Pattern getRollbackPattern();

  /**
   * Pattern maching a transaction ROLLBACK TO SAVEPOINT that should be mapped
   * on a connection.rollback(Savepoint) call.
   * 
   * @return Returns the ROLLBACK_TO_SAVEPOINT_PATTERN.
   */
  Pattern getRollbackToSavepointPattern();

  /**
   * Pattern maching a transaction isolation level setting that should be mapped
   * on a connection.setIsolationLevel() call.
   * 
   * @return Returns the SET_ISOLATION_LEVEL_PATTERN.
   */
  Pattern getSetIsolationLevelPattern();

  /**
   * Pattern maching a transaction read-only setting that should be mapped on a
   * connection.setReadOnly() call.
   * 
   * @return Returns the SET_READ_ONLY_TRANSACTION_PATTERN.
   */
  Pattern getSetReadOnlyTransactionPattern();

  /**
   * Pattern maching a transaction SET SAVEPOINT that should be mapped on a
   * connection.setSavepoint(Savepoint) call.
   * 
   * @return Returns the SET_SAVEPOINT_PATTERN.
   */
  Pattern getSetSavepointPattern();

  /**
   * Pattern maching a transaction SET AUTOCOMMIT=1 that should be mapped on a
   * connection.setAutocommit(true) call.
   * 
   * @return Returns the SET_AUTOCOMMIT_PATTERN.
   */
  Pattern getSetAutocommit1Pattern();

}