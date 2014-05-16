/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2007 Continuent, Inc.
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
 * Initial developer(s): Robert Hodges.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.interceptors;

/**
 * Provides a wrapper around SQL request data to constrain access
 * to request contents by interceptors.   
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface RequestFacade
{
  /** 
   * Returns the unique, monotonically increasing request ID. 
   */
  public abstract long getId();

  /**
   * Returns the SQL statement that is being submitted. 
   */
  public abstract String getSql();

  /**
   * Returns the transaction ID or -1 if there is no transaction. 
   */
  public abstract long getTransactionId();

  /**
   * Returns true if this is a read-only request. 
   */
  public abstract boolean isReadOnly();

  /**
   * Sets the SQL statement.  
   */
  public abstract void setSql(String sql);
}