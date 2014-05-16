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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): _________________________.
 */

package org.continuent.sequoia.controller.scheduler.schema;

import org.continuent.sequoia.common.sql.schema.DatabaseTable;

/**
 * A <code>CacheDatabaseTable</code> represents a database table and its
 * associated cache entries. It has an array of <code>CacheDatabaseColumn</code>
 * objects.
 * <p>
 * Keep it mind that <code>ArrayList</code> is not synchronized...
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class SchedulerDatabaseTable
{
  /** Database table name. */
  private String                   name;

  private TransactionExclusiveLock lock = new TransactionExclusiveLock();

  /**
   * Creates a new <code>CacheDatabaseTable</code> instance.
   * 
   * @param databaseTable the database table
   */
  public SchedulerDatabaseTable(DatabaseTable databaseTable)
  {
    // Clone the name and the columns
    name = databaseTable.getName();
  }

  /**
   * Gets the name of the table.
   * 
   * @return the table name
   */
  public String getName()
  {
    return name;
  }

  /**
   * Returns the lock for this table.
   * 
   * @return a <code>TransactionExclusiveLock</code> instance
   * @see TransactionExclusiveLock
   */
  public TransactionExclusiveLock getLock()
  {
    return lock;
  }

  /**
   * Two <code>CacheDatabaseColumn</code> are equals if they have the same
   * name and the same columns.
   * 
   * @param other the object to compare with
   * @return true if the 2 objects are the same
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof SchedulerDatabaseTable))
      return false;
    else
      return name.equals(((SchedulerDatabaseTable) other).getName());
  }

  /**
   * Returns information about the database table and its columns.
   * 
   * @param longFormat <code>true</code> for a long format, <code>false</code>
   *          for a short summary
   * @return a <code>String</code> value
   */
  public String getInformation(boolean longFormat)
  {
    return "Table " + name + ": ";
  }
}
