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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.driver;

import java.sql.SQLWarning;
import java.util.List;

/**
 * Holds the result of XXXExecuteUpdate() and XXXExecute() execution plus the
 * possible SQL Warning chain that was generated by the database.<br>
 * This holder contains a warning chain, that can be null, and either an update
 * count (for XXXExecuteUpdate()) or a result list (for XXXExecute()), depending
 * on the function call that generated the result.<br>
 * <i>Note:</i> All methods of this class are package private, nobody but the
 * driver itself should have access to it
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class ResultAndWarnings
{
  /** warning chain, null means no warning */
  private SQLWarning statementWarnings = null;

  private int        updateCount       = -1;
  private List<?>       resultList        = null;

  /**
   * Constructs a <code>ResultAndWarning</code> that will hold an updateCount
   * and the given SQLWarnings.<br>
   * This constructor will typically be called by XXXexecuteUpdate() functions
   * 
   * @param uc the update count
   * @param warns the warnings to set, can be null
   * @see Connection#statementExecuteUpdate(org.continuent.sequoia.common.sql.Request,
   *      SQLWarning)
   * @see Connection#statementExecuteUpdateWithKeys(org.continuent.sequoia.common.sql.Request)
   * @see Connection#callableStatementExecuteUpdate(org.continuent.sequoia.common.sql.Request)
   */
  ResultAndWarnings(int uc, SQLWarning warns)
  {
    statementWarnings = warns;
    updateCount = uc;
  }

  /**
   * Constructs a <code>ResultAndWarning</code> that will hold a resultList
   * and the given SQLWarnings.<br>
   * This constructor will typically be called by XXXexecute() functions
   * 
   * @param reslist list of results
   * @param warns the warnings to set, can be null
   * @see Connection#statementExecute(org.continuent.sequoia.common.sql.RequestWithResultSetParameters)
   * @see Connection#callableStatementExecute(org.continuent.sequoia.common.sql.RequestWithResultSetParameters)
   */
  ResultAndWarnings(List<?> reslist, SQLWarning warns)
  {
    statementWarnings = warns;
    resultList = reslist;
  }

  /**
   * Gets the warning chain associated to the statement that generated the
   * result
   * 
   * @return a <code>SQLWarning</code> chain or null if no warnings
   */
  SQLWarning getStatementWarnings()
  {
    return statementWarnings;
  }

  /**
   * Gets the updateCount. <i>Note:</i> if the held value is not an update
   * count, then the returned value is unspecified
   * 
   * @return the updateCount value
   */
  int getUpdateCount()
  {
    return updateCount;
  }

  /**
   * Gets the ResultList. <i>Note:</i> if the held value is not an ResultList,
   * then the returned value is unspecified
   * 
   * @return the resultList value
   */
  List<?> getResultList()
  {
    return resultList;
  }
}
