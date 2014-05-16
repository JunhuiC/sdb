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
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.controller.requests;

import java.util.ArrayList;

import org.continuent.sequoia.common.i18n.Translate;

/**
 * <code>AbstractWriteRequest</code> is the super-class of all requests which
 * do NOT return any ResultSet. They do have side-effects (else what would they
 * useful for?!)
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public abstract class AbstractWriteRequest extends AbstractRequest
{
  /** Name of the table involved in this write query. */
  protected transient String    tableName;

  /**
   * <code>ArrayList</code> of <code>TableColumn</code> involved in this
   * write query.
   */
  protected transient ArrayList columns;

  /** <code>true</code> if this request might block. */
  protected transient boolean   blocking = true;

  /** Primary key value */
  protected transient String    pkValue  = null;

  /**
   * True if the request requires to take a global lock.
   */
  protected boolean requiresGlobalLock = false;

  /**
   * Creates a new <code>AbstractWriteRequest</code> object
   * 
   * @param sqlQuery the SQL query
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database ?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   * @param requestType the request type as defined in RequestType class
   * @see RequestType
   */
  public AbstractWriteRequest(String sqlQuery, boolean escapeProcessing,
      int timeout, String lineSeparator, int requestType)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator, requestType);
  }

  /**
   * Clones table name and columns from an already parsed request.
   * 
   * @param abstractWriteRequest the already parsed request
   */
  protected void cloneTableNameAndColumns(
      AbstractWriteRequest abstractWriteRequest)
  {
    tableName = abstractWriteRequest.getTableName();
    columns = abstractWriteRequest.getColumns();
    pkValue = abstractWriteRequest.getPk();
    cacheable = abstractWriteRequest.getCacheAbility();
    writeLockedTables = abstractWriteRequest.getWriteLockedDatabaseTables();
  }

  /**
   * Returns an <code>ArrayList</code> of <code>TableColumn</code> objects
   * representing the columns affected by this statement.
   * 
   * @return an <code>ArrayList</code> value
   */
  public ArrayList getColumns()
  {
    return columns;
  }

  /**
   * @return Returns the pk.
   */
  public String getPk()
  {
    return pkValue;
  }

  /**
   * Returns the name of the table affected by this statement.
   * 
   * @return a <code>String</code> value
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Tests if this request might block.
   * 
   * @return <code>true</code> if this request might block
   */
  public boolean mightBlock()
  {
    return blocking;
  }

  /**
   * Sets if this request might block.
   * 
   * @param blocking a <code>boolean</code> value
   */
  public void setBlocking(boolean blocking)
  {
    this.blocking = blocking;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getParsingResultsAsString()
   */
  public String getParsingResultsAsString()
  {
    StringBuffer sb = new StringBuffer(super.getParsingResultsAsString());
    sb.append(Translate.get("request.table.involved", tableName));
    if (columns != null && columns.size() > 0)
    {
      sb.append(Translate.get("request.table.column"));
      for (int i = 0; i < columns.size(); i++)
      {
        sb.append(Translate.get("request.table.column", columns.get(i)));
      }
    }
    sb.append(Translate.get("request.blocking", blocking));
    sb.append(Translate.get("request.primary.key", pkValue));
    return sb.toString();
  }

  /**
   * Return true if the query requires a global locking.
   * 
   * @return the requiresGlobalLock value.
   */
  public boolean requiresGlobalLock()
  {
    return requiresGlobalLock;
  }
}