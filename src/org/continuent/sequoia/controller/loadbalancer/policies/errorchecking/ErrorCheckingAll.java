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

/**
 * Error checking using all backends.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ErrorCheckingAll extends ErrorCheckingPolicy
{

  /**
   * Creates a new <code>ErrorCheckingAll</code> instance.
   */
  public ErrorCheckingAll()
  {
    // We don't care about the number of nodes but the father's constructor
    // needs at least 3 nodes.
    super(ErrorCheckingPolicy.ALL, 3);
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy#getBackends(ArrayList)
   */
  public ArrayList<?> getBackends(ArrayList<?> backends)
    throws ErrorCheckingException
  {
    return backends;
  }

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy#getInformation()
   */
  public String getInformation()
  {
    return "Error checking using all backends";
  }
}
