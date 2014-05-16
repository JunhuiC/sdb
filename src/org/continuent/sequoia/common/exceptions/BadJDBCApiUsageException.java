/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Continuent Inc.
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

package org.continuent.sequoia.common.exceptions;

/**
 * A <code>BadJDBCApiUsageException</code> is thrown when a client does an
 * inappropriate usage of the JDBC API. For example calling executeQuery() for a
 * query that returns an update count.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class BadJDBCApiUsageException extends SequoiaException
{
  private static final long serialVersionUID = 1325042260073279124L;

  /**
   * Creates a new <code>BadJDBCApiUsageException</code> instance.
   * 
   * @param cause the root cause
   */
  public BadJDBCApiUsageException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Creates a new <code>BadJDBCApiUsageException</code> object
   * 
   * @param message the error message
   */
  public BadJDBCApiUsageException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>BadJDBCApiUsageException</code> instance.
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public BadJDBCApiUsageException(String message, Throwable cause)
  {
    super(message, cause);
  }

}
