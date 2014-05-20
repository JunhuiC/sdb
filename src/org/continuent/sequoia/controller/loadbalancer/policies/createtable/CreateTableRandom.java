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
 * Contributor(s): Jean-Bernard van Zuylen
 */

package org.continuent.sequoia.controller.loadbalancer.policies.createtable;

import java.util.ArrayList;
import java.util.Random;

import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * Implements a random strategy for <code>CREATE TABLE</code> statements.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public class CreateTableRandom extends CreateTableRule
{
  private Random random = new Random();

  /**
   * Creates a new <code>CreateTableRandom</code>.
   */
  public CreateTableRandom()
  {
    super(CreateTablePolicy.RANDOM);
  }

  /**
   * Creates a new <code>CreateTableRandom</code>.
   * 
   * @param backendList <code>ArrayList</code> of <code>DatabaseBackend</code>
   */
  public CreateTableRandom(ArrayList<String> backendList)
  {
    super(CreateTablePolicy.RANDOM, backendList);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableRule#getBackends(ArrayList)
   */
  public ArrayList<DatabaseBackend> getBackends(ArrayList<?> backends) throws CreateTableException
  {
    if (nbOfNodes == 0)
      return null;
    
    ArrayList<DatabaseBackend> clonedList = super.getBackends(backends);

    int clonedSize = clonedList.size();

    if (nbOfNodes == clonedSize)
      return (ArrayList<DatabaseBackend>) clonedList;
    else if (nbOfNodes > clonedSize)
      throw new CreateTableException(
        "Asking for more backends ("
          + nbOfNodes
          + ") than available ("
          + clonedSize
          + ")");

    ArrayList<DatabaseBackend> result = new ArrayList<DatabaseBackend>(nbOfNodes);

    for (int i = 0; i < nbOfNodes; i++)
      result.add(clonedList.remove(random.nextInt(clonedSize - i)));

    return result;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableRule#getInformation()
   */
  public String getInformation()
  {
    String s;
    if (tableName == null)
      s = "Default rule create table on ";
    else
      s = "Rule for table " + tableName + " create table on ";

    return s + nbOfNodes + " node(s) randomly from " + backendList;
  }
}
