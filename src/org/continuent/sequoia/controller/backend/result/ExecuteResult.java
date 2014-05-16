/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Continuent, Inc.
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

package org.continuent.sequoia.controller.backend.result;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * This class stores the result of a call to Statement.execute() and chains the
 * updateCount and ResultSets in a list. Note that all results must be added in
 * the order they were retrieved.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ExecuteResult extends AbstractResult implements Serializable
{
  private static final long serialVersionUID = 3303766819336440504L;

  private LinkedList        results;

  /**
   * Creates a new <code>ExecuteResult</code> object
   */
  public ExecuteResult()
  {
    results = new LinkedList();
  }

  /**
   * Add a controller ResultSet to the list of results.
   * 
   * @param crs the ControllerResultSet to add
   */
  public void addResult(ControllerResultSet crs)
  {
    results.addLast(crs);
  }

  /**
   * Add an Integer object corresponding to the given update count to the list
   * of results.
   * 
   * @param updateCount the update count to add
   */
  public void addResult(int updateCount)
  {
    results.addLast(new Integer(updateCount));
  }

  /**
   * Returns the results value.
   * 
   * @return Returns the results.
   */
  public final List getResults()
  {
    return results;
  }

}
