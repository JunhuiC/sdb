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

package org.continuent.sequoia.controller.loadbalancer.policies;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * Defines the policy to adopt before returning a result to the client.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class WaitForCompletionPolicy
{
  /** Return as soon as one node has completed the request. */
  public static final int FIRST    = 0;

  /**
   * Return as soon as a majority (n/2+1) of nodes has completed the request.
   */
  public static final int MAJORITY = 1;

  /** Wait for all nodes to complete the request before returning the result. */
  public static final int ALL      = 2;

  /** Policy (default is {@link #FIRST}). */
  private int             policy   = FIRST;

  /** True if strict table based locking must be enforced */
  private boolean         enforceTableLocking;

  /** Deadlock detection timeout in ms */
  private long            deadlockTimeoutInMs;

  private boolean         enforceTableLockOnAutoIncrementInsert;

  /**
   * Creates a new <code>WaitForCompletionPolicy</code> object
   * 
   * @param policy the default policy to use
   * @param enforceTableLocking true if strict table based locking must be
   *            enforced
   * @param enforceTableLockOnAutoIncrementInsert true if strict table based
   *            locking must be enforced while inserting into a table containing
   *            an auto-incremented column
   * @param deadlockTimeoutInMs deadlock detection timeout in ms
   */
  public WaitForCompletionPolicy(int policy, boolean enforceTableLocking,
      boolean enforceTableLockOnAutoIncrementInsert, long deadlockTimeoutInMs)
  {
    this.policy = policy;
    this.enforceTableLocking = enforceTableLocking;
    this.enforceTableLockOnAutoIncrementInsert = enforceTableLockOnAutoIncrementInsert;
    this.deadlockTimeoutInMs = deadlockTimeoutInMs;
  }

  /**
   * Returns the deadlockTimeoutInMs value.
   * 
   * @return Returns the deadlockTimeoutInMs.
   */
  public final long getDeadlockTimeoutInMs()
  {
    return deadlockTimeoutInMs;
  }

  /**
   * Sets the deadlockTimeoutInMs value.
   * 
   * @param deadlockTimeoutInMs The deadlockTimeoutInMs to set.
   */
  public final void setDeadlockTimeoutInMs(long deadlockTimeoutInMs)
  {
    this.deadlockTimeoutInMs = deadlockTimeoutInMs;
  }

  /**
   * Returns the enforceTableLocking value.
   * 
   * @return Returns the enforceTableLocking.
   */
  public boolean isEnforceTableLocking()
  {
    return enforceTableLocking;
  }

  /**
   * Returns the enforceTableLockOnAutoIncrementInsert value.
   * 
   * @return Returns the enforceTableLockOnAutoIncrementInsert.
   */
  public boolean isEnforceTableLockOnAutoIncrementInsert()
  {
    return enforceTableLockOnAutoIncrementInsert;
  }

  /**
   * Returns the policy.
   * 
   * @return an <code>int</code> value
   */
  public int getPolicy()
  {
    return policy;
  }

  /**
   * Sets the policy.
   * 
   * @param policy the policy to set
   */
  public void setPolicy(int policy)
  {
    this.policy = policy;
  }

  /**
   * Gives information about the current policy.
   * 
   * @return a <code>String</code> value
   */
  public String getInformation()
  {
    switch (policy)
    {
      case FIRST :
        return "return when first node completes";
      case MAJORITY :
        return "return when a majority of nodes completes";
      case ALL :
        return "return when all nodes have completed";
      default :
        return "unknown policy";
    }
  }

  /**
   * Returns this wait policy in xml format.
   * 
   * @return xml formatted string
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_WaitForCompletion + " "
        + DatabasesXmlTags.ATT_policy + "=\"");
    switch (policy)
    {
      case FIRST :
        info.append(DatabasesXmlTags.VAL_first);
        break;
      case ALL :
        info.append(DatabasesXmlTags.VAL_all);
        break;
      case MAJORITY :
        info.append(DatabasesXmlTags.VAL_majority);
        break;
      default :
    }
    info.append("\" " + DatabasesXmlTags.ATT_enforceTableLocking + "=\""
        + enforceTableLocking + "\" "
        + DatabasesXmlTags.ATT_deadlockTimeoutInMs + "=\""
        + deadlockTimeoutInMs + "\"/>");
    return info.toString();
  }
}
