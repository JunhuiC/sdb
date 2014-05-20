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
 * Contributor(s): _______________________
 */

package org.continuent.sequoia.controller.loadbalancer.policies.errorchecking;

import java.util.ArrayList;

import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * Chooses the number of nodes nodes for error checking using a round-robin
 * algorithm.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ErrorCheckingRoundRobin extends ErrorCheckingPolicy
{
  /** Round-robin index. */
  private int index = 0;

  /**
   * Creates a new <code>ErrorCheckingRoundRobin</code> instance.
   * 
   * @param numberOfNodes number of nodes
   */
  public ErrorCheckingRoundRobin(int numberOfNodes)
  {
    super(ErrorCheckingPolicy.ROUND_ROBIN, numberOfNodes);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy#getBackends(ArrayList)
   */
  public ArrayList<?> getBackends(ArrayList<?> backends)
    throws ErrorCheckingException
  {
    int size = backends.size();

    if (nbOfNodes == 0)
      return null;
    else if (nbOfNodes == size)
      return backends;

    ArrayList<DatabaseBackend> result = new ArrayList<DatabaseBackend>(nbOfNodes);
    ArrayList<DatabaseBackend> clonedList = new ArrayList<DatabaseBackend>(size);
    for (int i = 0; i < size; i++)
    { // Take all enabled backends
      DatabaseBackend db = (DatabaseBackend) backends.get(i);
      if (db.isReadEnabled() || db.isWriteEnabled())
        clonedList.add(db);
    }

    int clonedSize = clonedList.size();

    if (nbOfNodes == clonedSize)
      return backends;
    else if (nbOfNodes > clonedSize)
      throw new ErrorCheckingException(
        "Asking for more backends ("
          + nbOfNodes
          + ") than available ("
          + clonedSize
          + ")");

    synchronized (this)
    { // index must be modified in mutual exclusion
      for (int i = 0; i < nbOfNodes; i++)
      {
        index = (index + 1) % clonedSize;
        result.add(clonedList.remove(index));
      }
    }

    return result;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy#getInformation()
   */
  public String getInformation()
  {
    return "Error checking using "
      + nbOfNodes
      + " nodes choosen using a round-robin algorithm";
  }
}
