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

package org.continuent.sequoia.common.exceptions.driver;

/**
 * A <code>VirtualDatabaseUnavailableException</code> is thrown to a client
 * when a requested virtual database does not exist, is shutting down, starting
 * or shut down on all controllers the client is trying to connect to.<br>
 * Internally (inside the driver), the
 * <code>VirtualDatabaseUnavailableException</code> is thrown when <b>one</b>
 * controller's virtual database is not available.
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat </a>
 * @version 1.0
 */
public class VirtualDatabaseUnavailableException extends DriverSQLException
{
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new <code>VirtualDatabaseUnavailableException</code> object
   */
  public VirtualDatabaseUnavailableException()
  {
    super();
  }

  /**
   * Creates a new <code>VirtualDatabaseUnavailableException</code> object
   * 
   * @param reason the error message
   */
  public VirtualDatabaseUnavailableException(String reason)
  {
    super(reason);
  }
}
