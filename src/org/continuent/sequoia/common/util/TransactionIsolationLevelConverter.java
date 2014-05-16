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

package org.continuent.sequoia.common.util;

import java.sql.Connection;

/**
 * Provides transaction isolation level to string converter
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 */
public class TransactionIsolationLevelConverter
{
  /**
   * Converts the given transaction isolation level to a human readable string,
   * identical to the constant name found in Connection or
   * "TRANSACTION_UNDEFINED" if the level does not exist
   * 
   * @param level one of the transaction isolation level as found in
   *          <code>java.sql.Connection</code>
   * @return a string representation of the given level, one of:
   *         <code>"TRANSACTION_READ_UNCOMMITTED"</code>,
   *         <code>"TRANSACTION_READ_COMMITTED"</code>,
   *         <code>"TRANSACTION_REPEATABLE_READ"</code>,
   *         <code>"TRANSACTION_SERIALIZABLE"</code>,
   *         <code>"TRANSACTION_NONE"</code>,
   *         <code>"TRANSACTION_UNDEFINED"</code> because it specifies that
   *         transactions are not supported.)
   * @see java.sql.Connection#getTransactionIsolation()
   */
  public static String toString(int level)
  {
    switch (level)
    {
      case Connection.TRANSACTION_READ_UNCOMMITTED :
        return "TRANSACTION_READ_UNCOMMITTED";
      case Connection.TRANSACTION_READ_COMMITTED :
        return "TRANSACTION_READ_COMMITTED";
      case Connection.TRANSACTION_REPEATABLE_READ :
        return "TRANSACTION_REPEATABLE_READ";
      case Connection.TRANSACTION_SERIALIZABLE :
        return "TRANSACTION_SERIALIZABLE";
      case Connection.TRANSACTION_NONE :
        return "TRANSACTION_NONE";
      default :
        return "TRANSACTION_UNDEFINED";
    }
  }
}
