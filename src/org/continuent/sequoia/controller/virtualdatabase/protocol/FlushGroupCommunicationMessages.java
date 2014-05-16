/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.util.LinkedList;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a FlushGroupCommunicationMessages used to flush the
 * messages in the group communication and make sure that controller failover
 * can be started.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class FlushGroupCommunicationMessages
    extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = 468790279514578158L;

  private long              failedControllerId;

  /**
   * Create a new <code>FlushGroupCommunicationMessages</code> instance for
   * the given failed controller id
   * 
   * @param id failed controller id
   */
  public FlushGroupCommunicationMessages(long id)
  {
    failedControllerId = id;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    LinkedList totalOrderQueue = dvdb.getTotalOrderQueue();
    synchronized (totalOrderQueue)
    {
      totalOrderQueue.addLast(this);
    }
    return null;
  }

  /**
   * Wait for our turn in the total order queue to be sure that all previous
   * messages have been processed.
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    AbstractLoadBalancer loadBalancer = dvdb.getRequestManager()
        .getLoadBalancer();
    loadBalancer.waitForTotalOrder(this, true);
    loadBalancer.removeObjectFromAndNotifyTotalOrderQueue(this);
    return Boolean.TRUE;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj)
  {
    if (!(obj instanceof FlushGroupCommunicationMessages))
      return false;

    FlushGroupCommunicationMessages fgcm = (FlushGroupCommunicationMessages) obj;
    return failedControllerId == fgcm.failedControllerId;
  }

}
