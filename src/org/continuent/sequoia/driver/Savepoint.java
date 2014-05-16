/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Jean-Bernard van Zuylen.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.driver;

import java.sql.SQLException;

/**
 * This class defines a Savepoint
 * 
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public class Savepoint implements java.sql.Savepoint
{
  private int    savepointId;

  private String savepointName;

  /**
   * Creates a new un-named <code>Savepoint</code> object
   * 
   * @param savepointId the generated ID for this savepoint
   */
  public Savepoint(int savepointId)
  {
    this.savepointId = savepointId;
  }

  /**
   * Creates a new named <code>Savepoint</code> object
   * 
   * @param savepointName the name of the savepoint
   */
  public Savepoint(String savepointName)
  {
    this.savepointName = savepointName;
  }

  /**
   * @see java.sql.Savepoint#getSavepointId()
   */
  public int getSavepointId() throws SQLException
  {
    if (this.savepointName != null)
      throw new SQLException("This is a named savepoint");

    return this.savepointId;
  }

  /**
   * @see java.sql.Savepoint#getSavepointName()
   */
  public String getSavepointName() throws SQLException
  {
    if (this.savepointName == null)
      throw new SQLException("This is an unnamed savepoint");

    return this.savepointName;
  }
}
