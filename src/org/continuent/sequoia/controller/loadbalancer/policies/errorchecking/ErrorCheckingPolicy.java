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

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * Defines the policy to adopt for error checking.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public abstract class ErrorCheckingPolicy
{
  /** Pickup backends randomly. */
  public static final int RANDOM      = 0;

  /** Backends are chosen using a round-robin algorithm. */
  public static final int ROUND_ROBIN = 1;

  /** Request is sent to all backends. */
  public static final int ALL         = 2;

  /** Number of nodes that are involved in error-checking per request. */
  protected int           nbOfNodes   = 0;

  protected int           policy;

  /**
   * Creates a new <code>CreateTableRule</code>.
   * 
   * @param policy implemented policy
   * @param numberOfNodes number of nodes to use to check for errors on a query
   */
  public ErrorCheckingPolicy(int policy, int numberOfNodes)
  {
    setPolicy(policy);
    setNumberOfNodes(numberOfNodes);
  }

  /**
   * Returns the number of nodes.
   * 
   * @return an <code>int</code> value
   * @see #setNumberOfNodes
   */
  public int getNumberOfNodes()
  {
    return nbOfNodes;
  }

  /**
   * Sets the number of nodes.
   * 
   * @param numberOfNodes the number of nodes to set
   * @see #getNumberOfNodes
   */
  public void setNumberOfNodes(int numberOfNodes)
  {
    if (numberOfNodes < 3)
      throw new IllegalArgumentException(
          "You must use at least 3 nodes for error checking (" + numberOfNodes
              + " is not acceptable)");
    this.nbOfNodes = numberOfNodes;
  }

  /**
   * Returns the policy.
   * 
   * @return an <code>int</code> value
   * @see #setPolicy
   */
  public int getPolicy()
  {
    return policy;
  }

  /**
   * Sets the policy.
   * 
   * @param policy the policy to set
   * @see #getPolicy
   */
  public void setPolicy(int policy)
  {
    this.policy = policy;
  }

  /**
   * Pickups backends from the given backends arraylist according to the current
   * rule policy.
   * 
   * @param backends backends to choose from
   * @return Arraylist of choosen <code>DatabaseBackend</code>
   * @exception ErrorCheckingException if the rule cannot be applied
   */
  public abstract ArrayList<?> getBackends(ArrayList<?> backends)
      throws ErrorCheckingException;

  /**
   * Gives information about the current policy.
   * 
   * @return a <code>String</code> value
   */
  public abstract String getInformation();

  /**
   * Convert this error checking policy to xml
   * 
   * @return xml formatted string
   */
  public String getXml()

  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_ErrorChecking + " />"
        + DatabasesXmlTags.ATT_numberOfNodes + "=\"" + this.getNumberOfNodes()
        + "\" " + DatabasesXmlTags.ATT_policy + "=\"");
    switch (policy)
    {
      case RANDOM :
        info.append(DatabasesXmlTags.VAL_random);
        break;
      case ROUND_ROBIN :
        info.append(DatabasesXmlTags.VAL_roundRobin);
        break;
      case ALL :
        info.append(DatabasesXmlTags.VAL_all);
        break;
      default :
        break;
    }
    info.append("\"/>");
    return info.toString();
  }
}
