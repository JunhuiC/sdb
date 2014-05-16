/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks
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
 * Initial developer(s): Marc Herbert
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions.driver.protocol;

import java.io.IOException;
import java.sql.SQLException;

import org.continuent.sequoia.common.stream.DriverBufferedInputStream;

/**
 * This class is an SQLException (typically from backend) made serializable.
 * 
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @version 1.0
 */
public class BackendDriverException extends SerializableException
{
  private static final long serialVersionUID = -4044262679874226846L;

  /**
   * @see SerializableException#SerializableException(DriverBufferedInputStream)
   */
  public BackendDriverException(DriverBufferedInputStream in) throws IOException
  {
    super(in);
  }

  /**
   * Converts a chain of Throwables to a new chain of SerializableException
   * starting with a <code>BackendDriverException</code>. The returned chain
   * has the same length. We don't use super's method but re-implement it since
   * we want to also convert SQLException old-style chaining into a new style
   * chain. "SyntheticSQLException-s" from
   * {@link org.continuent.sequoia.common.exceptions.SQLExceptionFactory} also
   * currently use old style chaining (with setNextException).
   * 
   * @param start head of chain to convert.
   * @see SerializableException#SerializableException(Throwable)
   */
  public BackendDriverException(Throwable start)
  {
    super(start.getMessage(), convertNext(start)); // recursion here
    convertStackTrace(start);

    if (start instanceof SQLException) // hopefully, else why are we here?
    {
      SQLException sqlE = (SQLException) start;
      setSQLState(sqlE.getSQLState());
      setErrorCode(sqlE.getErrorCode());
    }
  }

  /**
   * Get the first cause found (new or old style), and convert it to a new
   * BackendDriverException object (which is Serializable)
   */

  private static SerializableException convertNext(Throwable regularEx)
  {
    /*
     * If we find that the new standard 1.4 chain is used, then we don't even
     * look at the old SQLException chain.
     */
    /*
     * We could also <em>not</em> lose this information by: adding another
     * separated chain to this class, serialize both chains, and convert
     * everything back to SQLExceptions on the driver side so both chains can
     * separately be offered to the JDBC client...
     */
    Throwable newStyleCause = regularEx.getCause();
    if (null != newStyleCause)
      return new BackendDriverException(newStyleCause);

    // check legacy style chaining
    if (regularEx instanceof SQLException)
    {
      SQLException nextE = ((SQLException) regularEx).getNextException();
      if (null != nextE)
        return new BackendDriverException(nextE);
    }

    // found no more link, stop condition
    return null;

  }
}