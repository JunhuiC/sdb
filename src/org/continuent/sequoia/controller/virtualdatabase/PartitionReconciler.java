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
 */

package org.continuent.sequoia.controller.virtualdatabase;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.continuent.hedera.adapters.MessageListener;
import org.continuent.hedera.adapters.MulticastRequestAdapter;
import org.continuent.hedera.adapters.MulticastRequestListener;
import org.continuent.hedera.adapters.MulticastResponse;
import org.continuent.hedera.channel.AbstractReliableGroupChannel;
import org.continuent.hedera.common.GroupIdentifier;
import org.continuent.hedera.common.Member;
import org.continuent.hedera.factory.AbstractGroupCommunicationFactory;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.virtualdatabase.activity.ActivityService;

/**
 * A PartitionReconciler can be used to "reconcile" members of a vdb after a
 * network partition has been detected. This class creates a reconciliation
 * channel, separate from the vdb channel, to exchange activity information
 * between the reconciling members
 * 
 * @see DistributedVirtualDatabase#networkPartition(GroupIdentifier, List)
 * @see ActivityService
 */
class PartitionReconciler implements MessageListener, MulticastRequestListener
{
  private static final Trace           logger = Trace
                                                  .getLogger("org.continuent.sequoia.controller.virtualdatabase");
  private String                       vdbName;
  private String                       groupName;
  private Member                       member;
  private String                       hederaPropertiesFile;
  private AbstractReliableGroupChannel reconciliationChannel;
  private MulticastRequestAdapter      reconciliationMulticastRequestAdapter;

  PartitionReconciler(String vdbName, String groupName, Member member,
      String hederaPropertiesFile)
  {
    this.vdbName = vdbName;
    this.groupName = groupName;
    this.member = member;
    this.hederaPropertiesFile = hederaPropertiesFile;
  }

  void dispose()
  {
    if (reconciliationChannel != null)
    {
      reconciliationChannel.close();
    }
    if (reconciliationMulticastRequestAdapter != null)
    {
      reconciliationMulticastRequestAdapter.stop();
    }
  }

  synchronized PartitionReconciliationStatus reconcileWith(final Member other)
      throws Exception
  {
    boolean activity = ActivityService.getInstance()
        .hasActivitySinceUnreachable(vdbName, other.getAddress());
    if (logger.isInfoEnabled())
    {
      logger.info("there has been " + (activity ? "some " : "no ")
          + "activity since member has been detected as unreachable (" + other
          + ")");
    }

    Properties p = new Properties();
    InputStream is = this.getClass().getResourceAsStream(hederaPropertiesFile);
    p.load(is);
    is.close();

    AbstractGroupCommunicationFactory groupCommFactory = (AbstractGroupCommunicationFactory) Class
        .forName(p.getProperty("hedera.factory")).newInstance();
    Object[] ret = groupCommFactory.createChannelAndGroupMembershipService(p,
        new GroupIdentifier(groupName + "-reconciliation"));
    reconciliationChannel = (AbstractReliableGroupChannel) ret[0];

    if (logger.isDebugEnabled())
    {
      logger.debug("join channel " + reconciliationChannel);
    }
    reconciliationChannel.join();
    if (logger.isDebugEnabled())
    {
      logger.debug("joined channel " + reconciliationChannel);
    }

    int timeToWaitForOtherMemberToJoinReconciliationChannel = 20 * 1000; // in ms
    long start = System.currentTimeMillis();
    while (reconciliationChannel.getGroup().getMembers().size() < 2)
    {
      logger.debug("waiting to be 2 in the group " + reconciliationChannel);
      Thread.sleep(1000);
      long now = System.currentTimeMillis();
      if ((now - start) > timeToWaitForOtherMemberToJoinReconciliationChannel) {
        break;
      }
    }
    if (reconciliationChannel.getGroup().getMembers().size() < 2)
    {
      logger.error("waited" + timeToWaitForOtherMemberToJoinReconciliationChannel + "ms for other member" +
      " to join the reconciliation group without any success. Spurious network partition is likely.");
      // in that case we can do nothing else than treat it as a split brain
      return PartitionReconciliationStatus.SPLIT_BRAIN;
    }

    Thread.sleep(5 * 1000);

    reconciliationMulticastRequestAdapter = new MulticastRequestAdapter(
        reconciliationChannel, // group channel
        this, // MessageListener
        this // MulticastRequestListener
    );
    reconciliationMulticastRequestAdapter.start();
    if (logger.isDebugEnabled())
    {
      logger.debug("started multicast request adapter for group "
          + reconciliationChannel.getGroup().getGroupIdentifier()
              .getGroupName());
    }

    PartitionReconciliationMessage msg = new PartitionReconciliationMessage(
        activity, other);

    List<?> members = reconciliationChannel.getGroup().getMembers();

    if (logger.isDebugEnabled())
    {
      logger.debug("send " + msg + " to " + members);
    }
    MulticastResponse response = reconciliationMulticastRequestAdapter
        .multicastMessage(members, msg, MulticastRequestAdapter.WAIT_ALL, 30 * 1000);
    Map<?, ?> results = response.getResults();

    if (logger.isDebugEnabled())
    {
      logger.debug("results from " + msg + " : " + results);
    }

    PartitionReconciliationStatus negociationStatus = decide(results);

    return negociationStatus;
  }

  private PartitionReconciliationStatus decide(
      Map/* <Member, Boolean> */<?, ?> results)
  {
    if (results.size() == 0)
    {
      // no other members
      return PartitionReconciliationStatus.OTHER_ALONE_IN_THE_WORLD;
    }
    Iterator<?> iter = results.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<?,?> entry = (Map.Entry<?,?>) iter.next();
      Member m = (Member) entry.getKey();
      if (entry.getValue() == null)
      {
        continue;
      }
      boolean otherActivity = ((Boolean) entry.getValue()).booleanValue();
      boolean ownActivity = ActivityService.getInstance()
          .hasActivitySinceUnreachable(vdbName, m.getAddress());
      if (!otherActivity && !ownActivity)
      {
        return PartitionReconciliationStatus.NO_ACTIVITY;
      }
      if (otherActivity && ownActivity)
      {
        return PartitionReconciliationStatus.SPLIT_BRAIN;
      }
      if (otherActivity && !ownActivity)
      {
        return PartitionReconciliationStatus.ALONE_IN_THE_WORLD;
      }
      if (!otherActivity && ownActivity)
      {
        return PartitionReconciliationStatus.OTHER_ALONE_IN_THE_WORLD;
      }
    }
    throw new IllegalStateException(
        "Unable to decide of a possible reconciliation status");
  }

  /**
   * {@inheritDoc}
   */
  public void receive(Serializable ser)
  {
    if (logger.isWarnEnabled())
    {
      logger.warn("Unexpected message received: " + ser);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Serializable handleMessageMultiThreaded(Serializable obj,
      Member member, Object result)
  {
    return (Serializable) result;
  }

  /**
   * {@inheritDoc}
   */
  public Object handleMessageSingleThreaded(Serializable obj, Member m)
  {
    if (obj instanceof PartitionReconciliationMessage)
    {
      PartitionReconciliationMessage msg = (PartitionReconciliationMessage) obj;
      if (logger.isDebugEnabled())
      {
        logger.debug("received " + msg + " from  " + m);
      }
      if (!msg.other.getAddress().equals(this.member.getAddress()))
      {
        if (logger.isDebugEnabled())
        {
          logger.debug("reconciliation message is not for member "
              + m.getAddress());
        }
        return null;
      }
      boolean hadActivity = ActivityService.getInstance()
          .hasActivitySinceUnreachable(vdbName, m.getAddress());
      if (logger.isInfoEnabled())
      {
        StringBuffer buff = new StringBuffer("own activity = " + hadActivity);
        buff.append(", activity = " + msg.activity + " on " + m.getAddress());
        logger.info(buff);
      }
      return Boolean.valueOf(hadActivity);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  public void cancelMessage(Serializable msg)
  {
    // Does not do anything special.
  }

}

class PartitionReconciliationMessage implements Serializable
{
  private static final long serialVersionUID = 1L;

  final boolean             activity;
  final Member              other;

  PartitionReconciliationMessage(boolean activity, Member other)
  {
    this.activity = activity;
    this.other = other;
  }

  public String toString()
  {
    return "PartitionReconciliationMessage[activity=" + activity + ", other="
        + other + "]";
  }
}