/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Marc Wick.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * This class defines a SQLExceptionFactory
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class SQLExceptionFactory
{

  /**
   * creates a new SQLException with the sequoiaMessage
   * 
   * @param sqlEx the original exception
   * @param sequoiaMessage the sequoia message to use for the new sqlexception
   * @return a new SQLException
   */
  public static SQLException getSQLException(SQLException sqlEx,
      String sequoiaMessage)
  {
    SQLException newException = new SQLException(sequoiaMessage, sqlEx
        .getSQLState(), sqlEx.getErrorCode());
    // TODO: shouldn't we use the new initCause() standard chaining instead ?
    // if we move to a new "SyntheticSQLException" type we will have to do
    // it anyway.
    // See also same issue below.
    newException.setNextException(sqlEx);
    return newException;
  }

  /**
   * creates a new SQLException with the sequoiaMessage, if all exceptions in
   * the list have the same errorcode and sqlstate the returned SQLExcepion will
   * be constructed with this values otherwise with null and 0
   * 
   * @param exceptions list of exceptions
   * @param sequoiaMessage the Sequoia message
   * @return a new SQLException
   */
  public static SQLException getSQLException(List exceptions,
      String sequoiaMessage)
  {
    String sqlState = null;
    int errorCode = 0;
    for (int i = 0; i < exceptions.size(); i++)
    {
      SQLException ex = (SQLException) exceptions.get(i);
      if (ex == null)
        continue;
      sequoiaMessage += ex.getMessage() + "\n";
      if (sqlState == null)
      {
        // first exception
        sqlState = ex.getSQLState();
        errorCode = ex.getErrorCode();
      }
      // We ignore if backend reports different SQL states or error codes. We
      // report the error of the first backend that failed. Details can be
      // retrieved for each backend in the exception chaining below.
    }
    SQLException newHead = new SQLException(sequoiaMessage, sqlState, errorCode);
    Iterator exIter = exceptions.iterator();

    // TODO: shouldn't we use the new initCause() standard chaining instead ?
    // See more comments above.
    while (exIter.hasNext())
      newHead.setNextException((SQLException) exIter.next());

    return newHead;
  }

}
