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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.common.sql.schema;

import java.io.Serializable;

/**
 * An <code>AliasedDatabaseTable</code> represents a database table with an
 * alias name. Example:
 * 
 * <pre>
 *  SELECT x.price FROM item x
 * </pre>
 * 
 * <p>
 * In this case, the <code>item</code> table has an alias named <code>x</code>.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class AliasedDatabaseTable implements Serializable
{
  private static final long serialVersionUID = 7082201367853814224L;

  /** Database table. */
  private DatabaseTable     table;

  /** Alias name or <code>null</code> if no alias is defined. */
  private String            alias;

  /**
   * Creates a new <code>AliasedDatabaseTable</code> instance.
   * 
   * @param table a <code>DatabaseTable</code> value
   * @param alias the alias name, <code>null</code> if no alias is defined
   */
  public AliasedDatabaseTable(DatabaseTable table, String alias)
  {
    if (table == null)
      throw new IllegalArgumentException(
          "Illegal null database table in AliasedDatabaseTable constructor");

    this.table = table;
    this.alias = alias;
  }

  /**
   * Returns the <code>DatabaseTable</code> object corresponding to this
   * database.
   * 
   * @return a <code>DatabaseTable</code> value
   */
  public DatabaseTable getTable()
  {
    return table;
  }

  /**
   * Gets the alias name.
   * 
   * @return the alias name. Returns <code>null</code> if no alias is set.
   */
  public String getAlias()
  {
    return alias;
  }

  /**
   * Two <code>AliasedDatabaseTable</code> are considered equal if they
   * represent the same table and have the same alias.
   * 
   * @param other the object to compare with
   * @return true if the 2 objects are the same
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof AliasedDatabaseTable))
      return false;

    AliasedDatabaseTable ad = (AliasedDatabaseTable) other;
    if (alias == null)
      return (ad.getAlias() == null) && table.equals(ad.getTable());
    else
      return alias.equals(ad.getAlias()) && table.equals(ad.getTable());
  }
}
