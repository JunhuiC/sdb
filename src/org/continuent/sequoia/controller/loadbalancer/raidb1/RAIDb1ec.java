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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.loadbalancer.raidb1;

import java.util.ArrayList;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * RAIDb-1 load balancer.
 * <p>
 * This class is an abstract call because the read requests coming from the
 * request manager are NOT treated here but in the subclasses. Transaction
 * management and write requests are broadcasted to all backends.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public abstract class RAIDb1ec extends RAIDb1
{
  /*
   * How the code is organized ? 1. Member variables 2. Constructor(s) 3.
   * Request handling 4. Transaction handling 5. Backend management
   */

  protected ArrayList           backendReadThreads;
  protected int                 nbOfConcurrentReads;
  protected ErrorCheckingPolicy errorCheckingPolicy;

  protected static Trace        logger = Trace
                                           .getLogger("org.continuent.sequoia.controller.loadbalancer.RAIDb1ec");

  /*
   * Constructors
   */

  /**
   * Creates a new RAIDb-1 Round Robin request load balancer. A new backend
   * worker thread is created for each backend.
   * 
   * @param vdb the virtual database this load balancer belongs to
   * @param waitForCompletionPolicy how many backends must complete before
   *          returning the result?
   * @param errorCheckingPolicy policy to apply for error checking.
   * @param nbOfConcurrentReads number of concurrent reads allowed
   * @exception Exception if an error occurs
   */
  public RAIDb1ec(VirtualDatabase vdb,
      WaitForCompletionPolicy waitForCompletionPolicy,
      ErrorCheckingPolicy errorCheckingPolicy, int nbOfConcurrentReads)
      throws Exception
  {
    super(vdb, waitForCompletionPolicy);
    backendReadThreads = new ArrayList();
    this.errorCheckingPolicy = errorCheckingPolicy;
    this.nbOfConcurrentReads = nbOfConcurrentReads;
  }

  /*
   * Backends management
   */

  /**
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#getXmlImpl
   */
  public String getXmlImpl()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_RAIDb_1ec + " "
        + DatabasesXmlTags.ATT_nbOfConcurrentReads + "=\""
        + this.nbOfConcurrentReads + "\">");
    this.getRaidb1Xml();
    if (waitForCompletionPolicy != null)
      info.append(waitForCompletionPolicy.getXml());
    info.append("</" + DatabasesXmlTags.ELT_RAIDb_1ec + ">");
    return info.toString();
  }
}