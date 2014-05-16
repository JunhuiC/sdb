/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Olivier Fambon, Damian Arregui, Karl Cassaigne, Stephane Giron.
 */

package org.continuent.sequoia.controller.virtualdatabase;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.continuent.hedera.adapters.MessageListener;
import org.continuent.hedera.adapters.MulticastRequestAdapter;
import org.continuent.hedera.adapters.MulticastRequestListener;
import org.continuent.hedera.adapters.MulticastResponse;
import org.continuent.hedera.channel.AbstractReliableGroupChannel;
import org.continuent.hedera.channel.ChannelException;
import org.continuent.hedera.channel.NotConnectedException;
import org.continuent.hedera.common.Group;
import org.continuent.hedera.common.GroupIdentifier;
import org.continuent.hedera.common.IpAddress;
import org.continuent.hedera.common.Member;
import org.continuent.hedera.factory.AbstractGroupCommunicationFactory;
import org.continuent.hedera.gms.AbstractGroupMembershipService;
import org.continuent.hedera.gms.GroupMembershipListener;
import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.SequoiaException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.jmx.notifications.SequoiaNotificationList;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.metadata.MetadataContainer;
import org.continuent.sequoia.common.sql.metadata.SequoiaParameterMetaData;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backup.DumpTransferInfo;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.recoverylog.RecoverThread;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.events.LogEntry;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requestmanager.distributed.ControllerFailureCleanupThread;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.virtualdatabase.activity.ActivityService;
import org.continuent.sequoia.controller.virtualdatabase.management.RestoreLogOperation;
import org.continuent.sequoia.controller.virtualdatabase.management.TransferBackendOperation;
import org.continuent.sequoia.controller.virtualdatabase.management.TransferDumpOperation;
import org.continuent.sequoia.controller.virtualdatabase.protocol.AddVirtualDatabaseUser;
import org.continuent.sequoia.controller.virtualdatabase.protocol.BackendStatus;
import org.continuent.sequoia.controller.virtualdatabase.protocol.BackendTransfer;
import org.continuent.sequoia.controller.virtualdatabase.protocol.BlockActivity;
import org.continuent.sequoia.controller.virtualdatabase.protocol.CompleteRecoveryLogResync;
import org.continuent.sequoia.controller.virtualdatabase.protocol.ControllerInformation;
import org.continuent.sequoia.controller.virtualdatabase.protocol.CopyLogEntry;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DisableBackendsAndSetCheckpoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedTransactionMarker;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage;
import org.continuent.sequoia.controller.virtualdatabase.protocol.FlushGroupCommunicationMessages;
import org.continuent.sequoia.controller.virtualdatabase.protocol.GetPreparedStatementMetadata;
import org.continuent.sequoia.controller.virtualdatabase.protocol.GetPreparedStatementParameterMetadata;
import org.continuent.sequoia.controller.virtualdatabase.protocol.GetStaticMetadata;
import org.continuent.sequoia.controller.virtualdatabase.protocol.InitiateDumpCopy;
import org.continuent.sequoia.controller.virtualdatabase.protocol.IsValidUserForAllBackends;
import org.continuent.sequoia.controller.virtualdatabase.protocol.MessageTimeouts;
import org.continuent.sequoia.controller.virtualdatabase.protocol.RemoveVirtualDatabaseUser;
import org.continuent.sequoia.controller.virtualdatabase.protocol.ReplicateLogEntries;
import org.continuent.sequoia.controller.virtualdatabase.protocol.ResumeActivity;
import org.continuent.sequoia.controller.virtualdatabase.protocol.ResyncRecoveryLog;
import org.continuent.sequoia.controller.virtualdatabase.protocol.SuspendActivity;
import org.continuent.sequoia.controller.virtualdatabase.protocol.SuspendWritesMessage;
import org.continuent.sequoia.controller.virtualdatabase.protocol.VirtualDatabaseConfiguration;
import org.continuent.sequoia.controller.virtualdatabase.protocol.VirtualDatabaseConfigurationResponse;

/**
 * A <code>DistributedVirtualDatabase</code> is a virtual database hosted by
 * several controllers. Communication between the controllers is achieved with
 * reliable multicast provided by Javagroups.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Damian.Arregui@continuent.com">Damian Arregui</a>
 * @author <a href="mailto:Stephane.Giron@continuent.com>Stephane Giron </a>
 * @version 1.0
 */
public class DistributedVirtualDatabase extends VirtualDatabase
    implements
      MessageListener,
      MulticastRequestListener,
      GroupMembershipListener
{
  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request handling
  // 4. Transaction handling
  // 5. Database backend management
  // 6. Distribution management (multicast)
  // 7. Getter/Setter (possibly in alphabetical order)
  //

  // Distribution

  /** Group name */
  private String                                   groupName                                = null;

  /**
   * Hashtable&lt;Member,String&gt;, the String being the controller JMX name
   * corresponding to the Member
   */
  private Hashtable                                controllerJmxAddress;

  /** Hashtable&lt;Member, Long&lt;DatabaseBackend&gt;&gt; */
  private Hashtable                                controllerIds;

  /** Hashtable&lt;Member, List&lt;DatabaseBackend&gt;&gt; */
  private Hashtable                                backendsPerController;

  /** Hedera channel */
  private AbstractReliableGroupChannel             channel                                  = null;

  /** MessageDispatcher to communicate with the group */
  private MulticastRequestAdapter                  multicastRequestAdapter                  = null;

  private MessageTimeouts                          messageTimeouts;

  private Group                                    currentGroup                             = null;

  private ArrayList                                allMemberButUs                           = null;

  /**
   * Used by VirtualDatabaseConfiguration if a remote controller config is not
   * compatible
   */
  public static final long                         INCOMPATIBLE_CONFIGURATION               = -1;

  private boolean                                  isVirtualDatabaseStarted                 = false;

  /**
   * Our view of the request manager, same as super.requestManager, only just
   * typed properly.
   */
  private DistributedRequestManager                distributedRequestManager;

  private boolean                                  processMacroBeforeBroadcast;

  /**
   * List of threads that are cleaning up resources allocated by a dead remote
   * controller
   */
  private Hashtable                                cleanupThreads;

  /**
   * Stores the "flushed writes" status for each failed controller: true if
   * writes have been flushed by the controller failure clean-up thread or a vdb
   * worker thread clean-up thread, false otherwise.
   */
  private HashMap                                  groupCommunicationMessagesLocallyFlushed;

  /**
   * Maximum time in ms allowed for clients to failover in case of a controller
   * failure
   */
  private long                                     failoverTimeoutInMs;

  /**
   * Cache of request results to allow transparent failover if a failure occurs
   * during a write request.
   */
  private RequestResultFailoverCache               requestResultFailoverCache;

  /** Logger for distributed request execution */
  private Trace                                    distributedRequestLogger;

  private String                                   hederaPropertiesFile;

  private final Object                             MESSAGES_IN_HANDLER_SYNC                 = new Object();

  private int                                      messagesInHandlers                       = 0;

  private boolean                                  channelShuttingDown                      = false;

  private boolean                                  isResynchingFlag;

  private Hashtable                                controllerPersistentConnectionsRecovered = new Hashtable();

  private Hashtable                                controllerTransactionsRecovered          = new Hashtable();
  private PartitionReconciler                      partitionReconciler;

  private List                                     ongoingActivitySuspensions               = new ArrayList();

  /** JVM-wide group communication factory */
  private static AbstractGroupCommunicationFactory groupCommunicationFactory                = null;

  /**
   * Creates a new <code>DistributedVirtualDatabase</code> instance.
   * 
   * @param controller the controller we belong to
   * @param name the virtual database name
   * @param groupName the virtual database group name
   * @param maxConnections maximum number of concurrent connections.
   * @param pool should we use a pool of threads for handling connections?
   * @param minThreads minimum number of threads in the pool
   * @param maxThreads maximum number of threads in the pool
   * @param maxThreadIdleTime maximum time a thread can remain idle before being
   *          removed from the pool.
   * @param idleConnectionTimeout maximum time an established connection can
   *          remain idle before the associated thread is cleaned up and shut
   *          down.
   * @param clientFailoverTimeoutInMs maximum time for clients to failover in
   *          case of a controller failure
   * @param sqlShortFormLength maximum number of characters of an SQL statement
   *          to display in traces or exceptions
   * @param useStaticResultSetMetaData true if DatabaseResultSetMetaData should
   *          use static fields or try to fetch the metadata from the underlying
   *          database
   * @param hederaPropertiesFile Hedera properties file defines the group
   *          communication factory and its parameters
   */
  public DistributedVirtualDatabase(Controller controller, String name,
      String groupName, int maxConnections, boolean pool, int minThreads,
      int maxThreads, long maxThreadIdleTime, long idleConnectionTimeout,
      int sqlShortFormLength, long clientFailoverTimeoutInMs,
      boolean useStaticResultSetMetaData, String hederaPropertiesFile,
      boolean enforceTableExistenceIntoSchema)
  {
    super(controller, name, maxConnections, pool, minThreads, maxThreads,
        maxThreadIdleTime, idleConnectionTimeout, sqlShortFormLength,
        useStaticResultSetMetaData, enforceTableExistenceIntoSchema);

    this.groupName = groupName;
    this.processMacroBeforeBroadcast = true;
    this.failoverTimeoutInMs = clientFailoverTimeoutInMs;
    requestResultFailoverCache = new RequestResultFailoverCache(logger,
        failoverTimeoutInMs);
    backendsPerController = new Hashtable();
    controllerJmxAddress = new Hashtable();
    controllerIds = new Hashtable();
    cleanupThreads = new Hashtable();
    groupCommunicationMessagesLocallyFlushed = new HashMap();
    isVirtualDatabaseStarted = false;
    distributedRequestLogger = Trace
        .getLogger("org.continuent.sequoia.controller.distributedvirtualdatabase.request."
            + name);
    this.hederaPropertiesFile = hederaPropertiesFile;
    this.totalOrderQueue = new LinkedList();
  }

  /**
   * Disconnect the channel and close it.
   * 
   * @see java.lang.Object#finalize()
   */
  protected void finalize() throws Throwable
  {
    quitChannel();
    super.finalize();
  }

  /**
   * This method handle the scheduling part of the queries to be sure that the
   * query is scheduled in total order before letting other queries to execute.
   * 
   * @see org.continuent.hedera.adapters.MulticastRequestListener#handleMessageSingleThreaded(java.io.Serializable,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(Serializable msg, Member sender)
  {
    synchronized (MESSAGES_IN_HANDLER_SYNC)
    {
      if (channelShuttingDown)
        return MESSAGES_IN_HANDLER_SYNC;
      messagesInHandlers++;
    }

    try
    {
      if (msg != null)
      {
        if (logger.isDebugEnabled())
          logger.debug("handleMessageSingleThreaded (" + msg.getClass() + "): "
              + msg);

        if (msg instanceof DistributedVirtualDatabaseMessage)
        {
          return ((DistributedVirtualDatabaseMessage) msg)
              .handleMessageSingleThreaded(this, sender);
        }
        // Other message types will be handled in multithreaded handler
      }
      else
      {
        String errorMsg = "Invalid null message";
        logger.error(errorMsg);
        return new ControllerException(errorMsg);
      }

      return null;
    }
    catch (Exception e)
    {
      if (e instanceof RuntimeException)
        logger.warn("Error while handling group message:" + msg.getClass(), e);
      return e;
    }
  }

  /**
   * @see org.continuent.hedera.adapters.MulticastRequestListener#handleMessageMultiThreaded(java.io.Serializable,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(Serializable msg,
      Member sender, Object handleMessageSingleThreadedResult)
  {
    // Check if we are shutting down first
    if (msg == MESSAGES_IN_HANDLER_SYNC)
      return null;

    try
    {
      if (msg == null)
      {
        String errorMsg = "Invalid null message";
        logger.error(errorMsg);
        return new ControllerException(errorMsg);
      }

      if (logger.isDebugEnabled())
        logger.debug("handleMessageMultiThreaded (" + msg.getClass() + "): "
            + msg);
      if (msg instanceof DistributedVirtualDatabaseMessage)
      {
        return ((DistributedVirtualDatabaseMessage) msg)
            .handleMessageMultiThreaded(this, sender,
                handleMessageSingleThreadedResult);
      }
      else
        logger.warn("Unhandled message type received: " + msg.getClass() + "("
            + msg + ")");

      return null;
    }
    catch (Exception e)
    {
      if (e instanceof RuntimeException)
        logger.warn("Error while handling group message: " + msg.getClass(), e);
      return e;
    }
    finally
    {
      synchronized (MESSAGES_IN_HANDLER_SYNC)
      {
        messagesInHandlers--;
        if (messagesInHandlers <= 0)
          MESSAGES_IN_HANDLER_SYNC.notifyAll();
      }

      if (msg != null)
      {
        // Just in case something bad happen and the request was not properly
        // removed from the queue.
        if (msg instanceof DistributedRequest)
        {
          synchronized (totalOrderQueue)
          {
            if (totalOrderQueue.remove(((DistributedRequest) msg).getRequest()))
            {
              logger.warn("Distributed request "
                  + ((DistributedRequest) msg).getRequest().getSqlShortForm(
                      getSqlShortFormLength())
                  + " did not remove itself from the total order queue");
              totalOrderQueue.notifyAll();
            }
          }
        }
        else if (msg instanceof DistributedTransactionMarker)
        {
          synchronized (totalOrderQueue)
          {
            if (totalOrderQueue.remove(msg))
            {
              logger.warn("Distributed " + msg.toString() + " did not remove "
                  + "itself from the total order queue");
              totalOrderQueue.notifyAll();
            }
          }
        }
      }
    }
  }

  public void cancelMessage(Serializable msg)
  {
    if (msg instanceof DistributedVirtualDatabaseMessage)
    {
      ((DistributedVirtualDatabaseMessage) msg).cancel(this);
    }
    else
      logger.warn("Unhandled message type received: " + msg.getClass() + "("
          + msg + ")");
  }

  /**
   * Flushes all messages in the group communication locally, i.e. ensures that
   * all previously scheduled messages are processed.
   * 
   * @param failedControllerId controller whose suspected failure triggered the
   *          flush operation.
   */
  public void flushGroupCommunicationMessagesLocally(long failedControllerId)
  {
    // Set "writes flushed" flag to false
    synchronized (groupCommunicationMessagesLocallyFlushed)
    {
      if (!groupCommunicationMessagesLocallyFlushed.containsKey(new Long(
          failedControllerId)))
        groupCommunicationMessagesLocallyFlushed.put(new Long(
            failedControllerId), Boolean.FALSE);
    }

    try
    {
      List dest = new ArrayList();

      if ((multicastRequestAdapter != null)
          && (multicastRequestAdapter.getChannel() != null))
      {
        dest.add(multicastRequestAdapter.getChannel().getLocalMembership());

        // Don't care about result
        multicastRequestAdapter.multicastMessage(dest,
            new FlushGroupCommunicationMessages(failedControllerId),
            MulticastRequestAdapter.WAIT_ALL, 0);
      }
    }
    catch (Throwable e)
    {
      logger.error("Failed to flush group communication messages locally", e);
    }
    finally
    {
      // Set "writes flushed" flag to true and notify blocked vdb worker
      // threads
      synchronized (groupCommunicationMessagesLocallyFlushed)
      {
        groupCommunicationMessagesLocallyFlushed.put(new Long(
            failedControllerId), Boolean.TRUE);
        groupCommunicationMessagesLocallyFlushed.notifyAll();
      }
    }
  }

  /**
   * Waits for all messages in the group communication to be flushed locally.
   * 
   * @param failedControllerId controller whose suspected failure triggered the
   *          wait operation.
   */
  public void waitForGroupCommunicationMessagesLocallyFlushed(
      long failedControllerId)
  {
    Long controllerIdKey = new Long(failedControllerId);

    // Set "writes flushed" flag to false and wait for writes being flushed by
    // the controller failure clean-up thread or the vdb worker thread
    synchronized (groupCommunicationMessagesLocallyFlushed)
    {
      if (!groupCommunicationMessagesLocallyFlushed
          .containsKey(controllerIdKey))
        groupCommunicationMessagesLocallyFlushed.put(controllerIdKey,
            Boolean.FALSE);

      while (!((Boolean) groupCommunicationMessagesLocallyFlushed
          .get(controllerIdKey)).booleanValue())
      {
        try
        {
          if (logger.isDebugEnabled())
          {
            logger
                .debug("Will wait for writes to be flushed for failed controller "
                    + controllerIdKey);
          }
          groupCommunicationMessagesLocallyFlushed.wait();
        }
        catch (InterruptedException e)
        {
          // Ignore exception
        }
      }
    }
  }

  /**
   * @see org.continuent.hedera.gms.GroupMembershipListener#failedMember(org.continuent.hedera.common.Member,
   *      org.continuent.hedera.common.GroupIdentifier,
   *      org.continuent.hedera.common.Member)
   */
  public void failedMember(Member failed, GroupIdentifier gid, Member sender)
  {
    quitMember(failed, gid);
    sendJmxNotification(SequoiaNotificationList.DISTRIBUTED_CONTROLLER_FAILED,
        Translate.get("notification.distributed.controller.removed", failed,
            name));
  }

  /**
   * @see org.continuent.hedera.gms.GroupMembershipListener#groupComposition(org.continuent.hedera.common.Group,
   *      org.continuent.hedera.common.IpAddress, int)
   */
  public void groupComposition(Group g, IpAddress sender, int gmsStatus)
  {
    // Just ignore
  }

  /**
   * @see org.continuent.hedera.gms.GroupMembershipListener#networkPartition(org.continuent.hedera.common.GroupIdentifier,
   *      java.util.List)
   */
  public void networkPartition(GroupIdentifier gid,
      final List mergedGroupCompositions)
  {
    // We already left the group. Late notification. Ignore.
    if ((channel == null) || channelShuttingDown)
      return;

    if (gid.equals(currentGroup.getGroupIdentifier()))
    {
      String msg = "Network partition detected in group " + gid + ".";
      logger.error(msg);
      endUserLogger.error(msg);

      if (shuttingDown)
      {
        logger.warn("Do not start network reconciliation process since " + name
            + " is shutting down");
        return;
      }

      sendJmxNotification(
          SequoiaNotificationList.VDB_NETWORK_PARTITION_DETECTION,
          "A network partition has been detected for the virtual database "
              + name + " (gid=" + gid + ").");
      Thread t = new Thread()
      {
        public void run()
        {
          reconcile(mergedGroupCompositions);
        }
      };
      t.start();
    }
  }

  /**
   * Makes this virtual database join a virtual database group. Those groups are
   * mapped to JavaGroups groups.
   * 
   * @exception Exception if an error occurs
   */
  public void joinGroup(boolean resyncRecoveryLog) throws Exception
  {
    RecoveryLog recoveryLog = distributedRequestManager.getRecoveryLog();
    if (recoveryLog == null)
    {
      String msg = "Distributed virtual database cannot be used without a recovery log defined.";
      if (logger.isFatalEnabled())
        logger.fatal(msg);
      throw new SequoiaException(msg);
    }

    logger.info("Recovery log size: " + recoveryLog.getRecoveryLogSize());

    try
    {
      Properties p = new Properties();
      InputStream is = this.getClass()
          .getResourceAsStream(hederaPropertiesFile);
      if (is == null)
      {
        if (logger.isFatalEnabled())
          logger.fatal(Translate.get(
              "fatal.distributed.no.group.communication.properties",
              hederaPropertiesFile));
        endUserLogger.fatal(Translate.get(
            "fatal.distributed.no.group.communication.properties",
            hederaPropertiesFile));
        throw new SequoiaException(
            "Join group failed because Hedera properties file was not found.");
      }
      if (logger.isInfoEnabled())
        logger.info("Using Hedera properties file: " + hederaPropertiesFile);
      p.load(is);
      is.close();

      if (groupCommunicationFactory == null)
      {
        groupCommunicationFactory = (AbstractGroupCommunicationFactory) Class
            .forName(p.getProperty("hedera.factory")).newInstance();
      }
      Object[] ret = groupCommunicationFactory
          .createChannelAndGroupMembershipService(p, new GroupIdentifier(
              groupName));
      AbstractGroupMembershipService gms = (AbstractGroupMembershipService) ret[1];
      gms.registerGroupMembershipListener(this);
      channel = (AbstractReliableGroupChannel) ret[0];

      if (logger.isDebugEnabled())
        logger.debug("Group communication channel is configured as follows: "
            + channel);

      // Join the group
      channel.join();
      currentGroup = channel.getGroup();
      multicastRequestAdapter = new MulticastRequestAdapter(channel // group
          // channel
          , this /* MessageListener */
          , this /* MulticastRequestListener */
      );
      multicastRequestAdapter.start();

      // Wait to let the MulticastRequestAdapter thread pump the membership out
      // of the channel.
      long waitTime;
      try
      {
        waitTime = Long.parseLong(p.getProperty("hedera.gms.wait"));
      }
      catch (NumberFormatException ignore)
      {
        waitTime = 2000; // Default is 2 seconds
      }

      logger.info("Waiting " + waitTime
          + " ms for group membership to be stable");

      Thread.sleep(waitTime);

      logger.info("Group " + groupName + " connected to "
          + channel.getLocalMembership());

      // Add ourselves to the list of controllers
      controllerJmxAddress.put(channel.getLocalMembership(), controller
          .getJmxName());

      long controllerId;

      // Check if we are alone or not
      List currentGroupMembers = currentGroup.getMembers();
      int groupSize = currentGroupMembers.size();
      if (groupSize == 1)
      {
        logger.info(Translate.get(
            "virtualdatabase.distributed.configuration.first.in.group",
            groupName));

        // Refuse to start if we are first but see no last-man-down marker
        if (!recoveryLog.isLastManDown())
          throw new VirtualDatabaseException(
              "NOT STARTING VDB (not the last man down).");

        allMemberButUs = new ArrayList();
        controllerId = 0;
        distributedRequestManager.setControllerId(controllerId);
        if (resyncRecoveryLog)
        {
          // Init backends states from persistence base
          distributedRequestManager
              .initBackendsLastKnownCheckpointFromRecoveryLog();
          recoveryLog.checkRecoveryLogConsistency();
        }
      }
      else
      {
        logger.info("Group now contains " + groupSize + " controllers.");
        if (logger.isDebugEnabled())
        {
          logger.debug("Current list of controllers is as follows:");
          for (Iterator iter = currentGroupMembers.iterator(); iter.hasNext();)
            logger.debug("Controller " + iter.next());
        }

        refreshGroupMembership(); // also updates allMemberButUs

        // Check with the other controller that our config is compatible
        controllerId = checkConfigurationCompatibilityAndReturnControllerId(getAllMemberButUs());
        if (controllerId == INCOMPATIBLE_CONFIGURATION)
        {
          String msg = Translate
              .get("virtualdatabase.distributed.configuration.not.compatible");
          logger.error(msg);
          throw new ControllerException(msg);
        }
        else
        {
          // In case several controllers join at the same time they would get
          // the
          // same highest controller id value and here we discriminate them by
          // adding their position in the membership. This assumes that the
          // membership is ordered the same way at all nodes.
          controllerId += currentGroupMembers.indexOf(channel
              .getLocalMembership());

          if (logger.isInfoEnabled())
          {
            logger.info(Translate
                .get("virtualdatabase.distributed.configuration.compatible"));
            logger.info("Controller identifier is set to: " + controllerId);
          }
          // Set the controller Id
          distributedRequestManager.setControllerId(controllerId);
        }

        if (resyncRecoveryLog)
        {
          // Init backends states from persistence base
          distributedRequestManager
              .initBackendsLastKnownCheckpointFromRecoveryLog();
        }

        // Distribute backends among controllers knowing that at this point
        // there is no conflict on the backend distribution policies.
        broadcastBackendInformation(getAllMemberButUs());
      }

      // Now let group comm messages flow in
      isVirtualDatabaseStarted = true;

      // Resync the recovery log, if any
      if (resyncRecoveryLog && (groupSize > 1) && hasRecoveryLog())
      {
        logger.info("Resyncing recovery log ...");
        resyncRecoveryLog();
        logger.info("Resyncing recovery log done");
      }

      initGlobalCounters(getControllerId());

      recoveryLog.clearLastManDown();

    }
    catch (Exception e)
    {
      if (channel != null)
      {
        quitChannel();
      }
      String msg = Translate.get("virtualdatabase.distributed.joingroup.error",
          groupName);
      if (e instanceof RuntimeException)
        logger.error(msg, e);
      throw new Exception(msg + " (" + e + ")", e);
    }

  }

  private void reconcile(List suspectedMembers)
  {
    try
    {
      distributedRequestManager.blockActivity();
    }
    catch (SQLException e)
    {
      logger.warn(e.getMessage(), e);
    }

    boolean shutdownLocally = false;

    Iterator iter = suspectedMembers.iterator();
    while (iter.hasNext())
    {
      Member other = (Member) iter.next();
      if (isLocalSender(other))
      {
        continue;
      }
      partitionReconciler = new PartitionReconciler(name, groupName, channel
          .getLocalMembership(), hederaPropertiesFile);

      PartitionReconciliationStatus reconciliationStatus;
      try
      {
        reconciliationStatus = partitionReconciler.reconcileWith(other);
        partitionReconciler.dispose();
      }
      catch (Exception e)
      {
        logger.error("Exception while reconciling with " + other, e);
        // in doubt, shutdown arbitrarily one of the vdb parts
        shutdownUnlessFirstMember(suspectedMembers);
        return;
      }
      String msg = "reconciliation status = " + reconciliationStatus
          + ", other = " + other;
      logger.info(msg);
      endUserLogger.info(msg);

      if (reconciliationStatus == PartitionReconciliationStatus.NO_ACTIVITY)
      {
        try
        {
          Collections.sort(suspectedMembers);
          Member firstMember = (Member) suspectedMembers.get(0);
          if (!(firstMember.equals(channel.getLocalMembership())))
          {
            logger.info(channel.getLocalMembership() + " is rejoining group "
                + currentGroup + "...");
            quitChannel();
            channelShuttingDown = false;
            joinGroup(false);
            logger.info(channel.getLocalMembership() + " has rejoined group "
                + currentGroup + "...");
          }
          distributedRequestManager.resumeActivity(false);
        }
        catch (Exception e)
        {
          logger.error("Exception while reconciling with " + other, e);
          // in doubt, shutdown arbitrarily one of the vdb parts
          shutdownUnlessFirstMember(suspectedMembers);
        }
        return;
      }
      if (reconciliationStatus == PartitionReconciliationStatus.ALONE_IN_THE_WORLD)
      {
        shutdown(Constants.SHUTDOWN_FORCE);
        return;
      }
      if (reconciliationStatus == PartitionReconciliationStatus.OTHER_ALONE_IN_THE_WORLD)
      {
        // do nothing
        continue;
      }
      // in all other cases, decide arbitrarily which vdb part will survive
      shutdownLocally = shutdownUnlessFirstMember(suspectedMembers);
      if (shutdownLocally)
      {
        return;
      }
    }
    if (logger.isInfoEnabled())
    {
      logger.info("End of reconciliation process for " + name
          + ". Resume activity normally.");
    }
    distributedRequestManager.resumeActivity(false);
  }

  /*
   * Shutdown local member unless it is the first in the members list (sorted
   * alphabetically).
   */
  private boolean shutdownUnlessFirstMember(List members)
  {
    Collections.sort(members);
    Member firstMember = (Member) members.get(0);
    logger.fatal(firstMember + " will remain as master.");
    if (!(firstMember.equals(channel.getLocalMembership())))
    {
      logger.fatal("Forcing virtual database shutdown here at "
          + channel.getLocalMembership() + ".");
      distributedRequestManager.resumeActivity(false);
      shutdown(Constants.SHUTDOWN_FORCE);
      return true;
    }
    else
    {
      logger.fatal("Virtual database here at " + channel.getLocalMembership()
          + " remaining as master.");
      return false;
    }
  }

  /**
   * Checks if re-synchronizing the recovery log is necessary, and if so,
   * initiates the recovery log recovery process and waits until it is complete.
   * This is called as part of the vdb startup process, before backends are
   * ready to be enabled.
   * 
   * @throws VirtualDatabaseException in case of error
   * @throws SQLException rethrown from recoverylog operations
   */
  private void resyncRecoveryLog() throws VirtualDatabaseException,
      SQLException
  {
    if (getAllMembers().size() == 1)
    {
      logger.info("First controller in vdb, no recovery log resync.");
      return;
    }

    String lastShutdownCheckpointName = getLastShutdownCheckpointName();
    if (lastShutdownCheckpointName == null)
    {
      // Reset recovery log cleanup if JVM property
      // "reset.dirty.recoverylog" is set to false (default is true)
      if ("true".equals(System.getProperty("reset.dirty.recoverylog", "true")))
      {
        logger
            .info("No shutdown checkpoint found in recovery log. Clearing recovery log (dirty).");
        logger.info("Please resync manually using 'restore log'.");
        getRecoveryLog().resetRecoveryLog(false);
      }
      else
      {
        logger
            .info("No shutdown checkpoint found in recovery log: please resync manually using 'restore log'.");
        logger
            .warn("Keeping recovery log for debug purpose only because reset.dirty.recoverylog property is set to false. \n"
                + "DO NOT PERFORM ADMIN COMMANDS ON THIS VIRTUAL DATABASE ON THIS NODE BEFORE RESTORING THE RECOVERY LOG.");
      }
      return;
    }

    // try to resync from the last shutdown checkpoint
    isResynchingFlag = true;

    try
    {
      logger.info("Resyncing from " + lastShutdownCheckpointName);
      resyncFromCheckpoint(lastShutdownCheckpointName);
    }
    catch (VirtualDatabaseException e)
    {
      // Reset recovery log cleanup if JVM property
      // "reset.dirty.recoverylog" is set to false (default is true)
      if ("true".equals(System.getProperty("reset.dirty.recoverylog", "true")))
      {
        logger
            .error("Failed to resync recovery log from last clean shutdown checkpoint. Clearing recovery log (dirty).");
        logger.info("Please resync manually using 'restore log'.");
        getRecoveryLog().resetRecoveryLog(false);
      }
      else
      {
        logger
            .error("Failed to resync recovery log from last clean shutdown checkpoint.");
        logger
            .warn("Keeping recovery log for debug purpose only because reset.dirty.recoverylog property is set to false. \n"
                + "DO NOT PERFORM ADMIN COMMANDS ON THIS VIRTUAL DATABASE ON THIS NODE BEFORE RESTORING THE RECOVERY LOG.");
      }
      isResynchingFlag = false;
    }
  }

  /**
   * Initializes global counters based on recovery log.
   * 
   * @param controllerId this controller id, as allocated by hte group
   *          communication. Base of all global counters numbering: counters are
   *          layed out as [ controllerId | <local count> ]
   * @throws SQLException if an error occurs accessing the recovery log
   */
  public void initGlobalCounters(long controllerId) throws SQLException
  {
    // Counters update should be properly synchronized since this it may happen
    // concurrently with a vdb worker thread delivering unique IDs. This
    // scenario occurs when using the "restore log" admin command following a
    // vdb crash.
    if (!hasRecoveryLog())
      return; // no recovery log: no init. Stick to default values (zero)
    RecoveryLog recoveryLog = requestManager.getRecoveryLog();
    requestManager.initializeRequestId(recoveryLog
        .getLastRequestId(controllerId) + 1);
    requestManager.getScheduler().initializeTransactionId(
        recoveryLog.getLastTransactionId(controllerId) + 1);
    synchronized (CONNECTION_ID_SYNC_OBJECT)
    {
      // Use the max operator as a safeguard: IDs may have been delivered but
      // not logged yet.
      this.connectionId = Math.max(this.connectionId, recoveryLog
          .getLastConnectionId(controllerId)) + 1;
    }
  }

  private void resyncFromCheckpoint(String checkpointName)
      throws VirtualDatabaseException
  {
    // talk to first other member in group
    Member remoteControllerMember = (Member) getAllMemberButUs().get(0);

    // send message, no return expected, just block until it's done.
    sendMessageToController(remoteControllerMember, new ResyncRecoveryLog(
        checkpointName), messageTimeouts.getDefaultTimeout());

    // If a remote error occured, or if the resync failed for any reason
    // whatsoever, a VirtualDatabaseException has been thrown.

    isResynchingFlag = false;
  }

  protected boolean isResyncing()
  {
    return isResynchingFlag;
  }

  /**
   * Return the last ShutdownCheckpointName, or null if none was found.
   * 
   * @return the last ShutdownCheckpointName, or null if none was found.
   * @throws VirtualDatabaseException in case getting the cp names list from the
   *           recovery log failed
   */
  private String getLastShutdownCheckpointName()
      throws VirtualDatabaseException
  {
    // get checkpoint names and see which is the last shutdown checkpoint. This
    // list is expected to be ordered, newest first.
    ArrayList checkpointNames;
    try
    {
      checkpointNames = getRecoveryLog().getCheckpointNames();
    }
    catch (SQLException e)
    {
      logger.error(e.getMessage());
      throw new VirtualDatabaseException(e);
    }

    Iterator iter = checkpointNames.iterator();
    while (iter.hasNext())
    {
      String cpName = (String) iter.next();
      if (cpName.startsWith("shutdown-" + getControllerName()))
        return cpName;
    }
    return null;
  }

  /**
   * @see org.continuent.hedera.gms.GroupMembershipListener#joinMember(org.continuent.hedera.common.Member,
   *      org.continuent.hedera.common.GroupIdentifier)
   */
  public void joinMember(Member m, GroupIdentifier gid)
  {
    if (hasRecoveryLog())
    {
      try
      {
        requestManager.getRecoveryLog().storeCheckpoint(
            buildCheckpointName(m + " joined group " + gid));
      }
      catch (SQLException ignore)
      {
        logger.warn("Failed to log checkpoint for joining member " + m);
      }
    }
    sendJmxNotification(SequoiaNotificationList.DISTRIBUTED_CONTROLLER_JOINED,
        Translate.get("notification.distributed.controller.removed", m, name));
  }

  //
  // Message dispatcher request handling
  //

  /**
   * Terminate the multicast request adapter and quit the Hedera channel.
   */
  public void quitChannel()
  {
    quitChannel(Constants.SHUTDOWN_SAFE);
  }

  /**
   * Terminate the multicast request adapter and quit the Hedera channel.
   * 
   * @param level of the vdb shutdown operation being executed
   */
  public void quitChannel(int level)
  {
    if (level == Constants.SHUTDOWN_FORCE)
    {
      multicastRequestAdapter.cancelRequests();
    }
    synchronized (MESSAGES_IN_HANDLER_SYNC)
    {
      channelShuttingDown = true;
      if (messagesInHandlers > 0)
        try
        {
          MESSAGES_IN_HANDLER_SYNC.wait();
        }
        catch (InterruptedException ignore)
        {
        }
    }

    if (multicastRequestAdapter != null)
    {
      multicastRequestAdapter.stop();
      multicastRequestAdapter = null;
    }
    if (channel != null)
    {
      channel.close();
      try
      {
        channel.quit();
      }
      catch (ChannelException e)
      {
        if (logger.isWarnEnabled())
        {
          logger.warn("Problem when quitting channel " + channel, e);
        }
      }
      catch (NotConnectedException e)
      {
        if (logger.isWarnEnabled())
        {
          logger.warn("Problem when quitting channel " + channel, e);
        }
      }
      channel = null;
    }
    if (groupCommunicationFactory != null)
    {
      // If we were not able to successfully dispose the factory we must keep a
      // reference to it in order to dispose it later
      if (groupCommunicationFactory.dispose())
        groupCommunicationFactory = null;
    }
  }

  /**
   * @see org.continuent.hedera.gms.GroupMembershipListener#quitMember(org.continuent.hedera.common.Member,
   *      org.continuent.hedera.common.GroupIdentifier)
   */
  public void quitMember(Member m, GroupIdentifier gid)
  {
    synchronized (MESSAGES_IN_HANDLER_SYNC)
    {
      // Ignore if channel has been closed (i.e. vdb shutdown)
      if ((channel == null) || (channelShuttingDown))
        return;
      messagesInHandlers++;
    }

    try
    {
      // Ignore our own quit message
      if (isLocalSender(m))
        return;

      if (hasRecoveryLog())
      {
        try
        {
          requestManager.getRecoveryLog().storeCheckpoint(
              buildCheckpointName(m + " quit group " + gid));
        }
        catch (SQLException ignore)
        {
          logger.warn("Failed to log checkpoint for quitting member " + m);
        }
      }

      ActivityService.getInstance().addUnreachableMember(name, m.getAddress());

      // Remove controller from list and notify JMX listeners
      String remoteControllerName = removeRemoteControllerAndStartCleanupThread(m);
      if (remoteControllerName != null)
      {
        endUserLogger.warn(Translate.get(
            "notification.distributed.controller.removed", new String[]{
                m.toString(), name}));
        logger.warn("Controller " + m + " has left the cluster.");
        sendJmxNotification(
            SequoiaNotificationList.DISTRIBUTED_CONTROLLER_REMOVED, Translate
                .get("notification.distributed.controller.removed", m, name));
      }

      // Notify adapter that we do not expect responses anymore from this member
      synchronized (MESSAGES_IN_HANDLER_SYNC)
      {
        // Ignore if channel is being closed, i.e. vdb shutdown.
        if (!channelShuttingDown)
        {
          int failures = multicastRequestAdapter.memberFailsOnAllReplies(m);
          logger.info(failures + " requests were waiting responses from " + m);
        }
      }
    }
    finally
    {
      synchronized (MESSAGES_IN_HANDLER_SYNC)
      {
        messagesInHandlers--;
      }
    }
  }

  /**
   * @see org.continuent.hedera.adapters.MessageListener#receive(java.io.Serializable)
   */
  public void receive(Serializable msg)
  {
    logger.error("Distributed virtual database received unhandled message: "
        + msg);
  }

  /**
   * Refresh the current group membership when someone has joined or left the
   * group.
   */
  private void refreshGroupMembership()
  {
    if (logger.isDebugEnabled())
      logger.debug("Refreshing members list:" + currentGroup.getMembers());

    synchronized (controllerJmxAddress)
    {
      allMemberButUs = (ArrayList) (((ArrayList) currentGroup.getMembers())
          .clone());
      allMemberButUs.remove(channel.getLocalMembership());
    }
  }

  //
  // Getter/Setter and tools (equals, ...)
  //

  /**
   * Two virtual databases are equal if they have the same name, login and
   * password.
   * 
   * @param other an object
   * @return a <code>boolean</code> value
   */
  public boolean equals(Object other)
  {
    if ((other == null)
        || (!(other instanceof org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase)))
      return false;
    else
    {
      DistributedVirtualDatabase db = (org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase) other;
      return name.equals(db.getDatabaseName())
          && groupName.equals(db.getGroupName());
    }
  }

  /**
   * Synchronized access to current group members.
   * 
   * @return a clone of the list of all members (never null).
   */
  public ArrayList getAllMembers()
  {
    // FIXME Should not return ArrayList but rather List
    synchronized (controllerJmxAddress)
    {
      if (currentGroup == null) // this happens if we did not #joinGroup()
        return new ArrayList();
      ArrayList members = (ArrayList) currentGroup.getMembers();
      if (members == null) // SEQUOIA-745 fix
        return new ArrayList();
      return (ArrayList) members.clone();
    }
  }

  /**
   * Returns the list of all members in the group except us. Consider the value
   * read-only (do not alter).
   * 
   * @return the allMembersButUs field (never null).
   */
  public ArrayList getAllMemberButUs()
  {
    synchronized (controllerJmxAddress)
    {
      if (allMemberButUs == null) // this happens if we did not #joinGroup()
        return new ArrayList();

      /**
       * This synchronized block might seem loussy, but actually it's enough, as
       * long as no caller alters the returned value: field allMembersButUs is
       * replaced (as opposed to updated) by refreshGroupMembership(). So
       * someone who has called this lives with a (possibly) outdated list, but,
       * still, with a safe list (never updated concurently by vdb threads). If
       * clients/callers are not trusted to leave the returned value un-touched,
       * use a clone
       */
      return allMemberButUs;
    }
  }

  /**
   * Get the group channel used for group communications
   * 
   * @return a <code>JChannel</code>
   */
  public AbstractReliableGroupChannel getChannel()
  {
    return channel;
  }

  /**
   * Returns the cleanupThreads value.
   * 
   * @return Returns the cleanupThreads.
   */
  public Hashtable getCleanupThreads()
  {
    return cleanupThreads;
  }

  /**
   * Used by the ControllerFailureCleanupThread to cleanup following a
   * controller failure. This returned the list of recovered transactions and
   * removes the list.
   * 
   * @param controllerId the id of the failed controller
   * @return List of recovered transactions for the given controller id (null if
   *         none)
   */
  public List getTransactionsRecovered(Long controllerId)
  {
    return (List) controllerTransactionsRecovered.remove(controllerId);
  }

  /**
   * Used by the ControllerFailureCleanupThread to cleanup following a
   * controller failure. This returned the list of recovered persistent
   * connections and removes the list.
   * 
   * @param controllerId the id of the failed controller
   * @return List of recovered persistent connections for the given controller
   *         id (null if none)
   */
  public List getControllerPersistentConnectionsRecovered(Long controllerId)
  {
    return (List) controllerPersistentConnectionsRecovered.remove(controllerId);
  }

  /**
   * Called by FailoverForPersistentConnection when a client reconnects
   * following a controller failure.
   * 
   * @param controllerId the id of the failed controller
   * @param connectionId the id of the persistent connection
   */
  public void notifyPersistentConnectionFailover(Long controllerId,
      Long connectionId)
  {
    synchronized (controllerPersistentConnectionsRecovered)
    {
      LinkedList persistentConnectionsRecovered = (LinkedList) controllerPersistentConnectionsRecovered
          .get(controllerId);
      if (persistentConnectionsRecovered == null)
      {
        persistentConnectionsRecovered = new LinkedList();
        controllerPersistentConnectionsRecovered.put(controllerId,
            persistentConnectionsRecovered);
      }

      persistentConnectionsRecovered.add(connectionId);
      if (logger.isInfoEnabled())
        logger.info("Failover detected for persistent connection "
            + connectionId);
    }
  }

  /**
   * Called by FailoverForTransaction when a client reconnects following a
   * controller failure.
   * 
   * @param controllerId the id of the failed controller
   * @param transactionId the id of the transaction
   */
  public void notifyTransactionFailover(Long controllerId, Long transactionId)
  {
    synchronized (controllerTransactionsRecovered)
    {
      LinkedList transactionsRecovered = (LinkedList) controllerTransactionsRecovered
          .get(controllerId);
      if (transactionsRecovered == null)
      {
        transactionsRecovered = new LinkedList();
        controllerTransactionsRecovered
            .put(controllerId, transactionsRecovered);
      }

      transactionsRecovered.add(transactionId);
      if (logger.isInfoEnabled())
        logger.info("Failover detected for transaction " + transactionId);
    }
  }

  /**
   * Gets a Controller specified by its name as a Member object suitable for
   * group communication.
   * 
   * @param controllerName the name of the target controller
   * @return a Member representing the target controller
   * @throws VirtualDatabaseException
   */
  private Member getControllerByName(String controllerName)
      throws VirtualDatabaseException
  {
    // Get the target controller
    Iterator iter = controllerJmxAddress.entrySet().iterator();
    Member targetMember = null;
    while (iter.hasNext())
    {
      Entry entry = (Entry) iter.next();
      if (entry.getValue().equals(controllerName))
      {
        targetMember = (Member) entry.getKey();
        break;
      }
    }
    if (targetMember == null)
      throw new VirtualDatabaseException("Cannot find controller "
          + controllerName + " in group");
    return targetMember;
  }

  /**
   * Returns the controllerName value.
   * 
   * @return Returns the controllerName.
   */
  public String getControllerName()
  {
    return controller.getControllerName();
  }

  /**
   * Returns the controller ID.
   * 
   * @return Returns the controller ID.
   */
  public long getControllerId()
  {
    return ((DistributedRequestManager) requestManager).getControllerId();
  }

  /**
   * Returns the currentGroup value.
   * 
   * @return Returns the currentGroup.
   */
  public Group getCurrentGroup()
  {
    return currentGroup;
  }

  /**
   * Returns the distributedRequestLogger value.
   * 
   * @return Returns the distributedRequestLogger.
   */
  public final Trace getDistributedRequestLogger()
  {
    return distributedRequestLogger;
  }

  /**
   * Get the XML dump of the Distribution element.
   * 
   * @return XML dump of the Distribution element
   */
  protected String getDistributionXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_Distribution + " "
        + DatabasesXmlTags.ATT_groupName + "=\"" + groupName + "\" "
        + DatabasesXmlTags.ATT_hederaPropertiesFile + "=\""
        + hederaPropertiesFile + "\" "
        + DatabasesXmlTags.ATT_clientFailoverTimeout + "=\""
        + failoverTimeoutInMs + "\">");

    getMessageTimeouts().generateXml(info);

    info.append("</" + DatabasesXmlTags.ELT_Distribution + ">");
    return info.toString();
  }

  /**
   * Returns the group name this virtual database belongs to.
   * 
   * @return a <code>String</code> value. Returns <code>null</code> if this
   *         virtual database is standalone
   */
  public String getGroupName()
  {
    return groupName;
  }

  /**
   * Sets the group name used by the controllers hosting this virtual database.
   * 
   * @param groupName the group name to set
   */
  public void setGroupName(String groupName)
  {
    this.groupName = groupName;
  }

  /**
   * Returns the messageTimeouts value.
   * 
   * @return Returns the messageTimeouts.
   */
  public MessageTimeouts getMessageTimeouts()
  {
    return messageTimeouts;
  }

  /**
   * Sets the messageTimeouts value.
   * 
   * @param messageTimeouts The messageTimeouts to set.
   */
  public void setMessageTimeouts(MessageTimeouts messageTimeouts)
  {
    this.messageTimeouts = messageTimeouts;
  }

  /**
   * Return the group communication multicast request adapter.
   * 
   * @return the group communication multicast request adapter
   */
  public MulticastRequestAdapter getMulticastRequestAdapter()
  {
    return multicastRequestAdapter;
  }

  //
  // Getter/Setter and tools (equals, ...)
  //

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#getNextConnectionId()
   */
  public long getNextConnectionId()
  {
    long id = super.getNextConnectionId();
    return distributedRequestManager.getNextConnectionId(id);
  }

  //
  // Getter/Setter and tools (equals, ...)
  //

  //
  // Getter/Setter and tools (equals, ...)
  //

  protected int getNumberOfEnabledBackends() throws VirtualDatabaseException
  {
    // 1/ get number of local active backends
    int nbActive = super.getNumberOfEnabledBackends();

    // 2/ add remote active backends

    // TODO: synchronize this access to backendsPerController (and others)
    DatabaseBackend b;
    Iterator iter = backendsPerController.keySet().iterator();
    while (iter.hasNext())
    {
      Member member = (Member) iter.next();

      List remoteBackends = (List) backendsPerController.get(member);
      int size = remoteBackends.size();
      b = null;
      for (int i = 0; i < size; i++)
      {
        b = (DatabaseBackend) remoteBackends.get(i);
        if (b.isReadEnabled() || b.isWriteEnabled())
          // test symetrical to RequestManager.backupBackend()
          nbActive++;
      }
    }
    /*
     * temporary, until backendsPerController is really updated (not yet done),
     * make as is force=true in backupBackend().
     */
    nbActive = -1;
    return nbActive;
  }

  /**
   * Tries to getPreparedStatement(Parameter)MetaData on other controllers
   * 
   * @param request Message containing metadata request to send to other
   *          controllers
   */
  private Serializable tryPreparedStatementMetaDataRequestOnRemoteControllers(
      DistributedVirtualDatabaseMessage request) throws SQLException
  {
    try
    {
      MulticastResponse rspList = getMulticastRequestAdapter()
          .multicastMessage(getAllMemberButUs(), request,
              MulticastRequestAdapter.WAIT_ALL,
              getMessageTimeouts().getVirtualDatabaseConfigurationTimeout());

      Map results = rspList.getResults();
      if (results.size() == 0)
      {
        throw new SQLException(
            Translate
                .get("virtualdatabase.distributed.prepared.statement.metadata.remote.no.response"));
      }
      for (Iterator iter = results.values().iterator(); iter.hasNext();)
      {
        Object response = iter.next();
        // Any SQLException will be wrapped into a ControllerException
        if (response instanceof ControllerException)
        {
          SQLException wrappingExcpt = new SQLException(
              Translate
                  .get("virtualdatabase.distributed.prepared.statement.metadata.remote.error"));
          wrappingExcpt.initCause((ControllerException) response);
          throw wrappingExcpt;
        }
        else
        {
          // Here we succeeded in getting prepared statement metadata from a
          // remote controller
          return (Serializable) response;
        }
      }
    }
    catch (NotConnectedException e2)
    {
      SQLException wrappingExcpt = new SQLException(
          Translate
              .get("virtualdatabase.distributed.prepared.statement.metadata.remote.channel.unavailable"));
      wrappingExcpt.initCause(e2);
      throw wrappingExcpt;
    }
    // Should not happen, but make the compiler happy
    throw new SQLException(
        Translate
            .get("virtualdatabase.distributed.prepared.statement.metadata.remote.no.response"));
  }

  /**
   * Return a ControllerResultSet containing the PreparedStatement metaData of
   * the given sql template
   * 
   * @param request the request containing the sql template
   * @return an empty ControllerResultSet with the metadata
   * @throws SQLException if a database error occurs
   */
  public ControllerResultSet getPreparedStatementGetMetaData(
      AbstractRequest request) throws SQLException
  {
    try
    {
      return requestManager.getPreparedStatementGetMetaData(request);
    }
    catch (NoMoreBackendException e)
    {
      return (ControllerResultSet) tryPreparedStatementMetaDataRequestOnRemoteControllers(new GetPreparedStatementMetadata(
          request));
    }
  }

  /**
   * Returns a <code>ParameterMetaData</code> containing the metadata of the
   * prepared statement parameters for the given request
   * 
   * @param request the request containing the sql template
   * @return the metadata of the given prepared statement parameters
   * @throws SQLException if a database error occurs
   */
  public ParameterMetaData getPreparedStatementGetParameterMetaData(
      AbstractRequest request) throws SQLException
  {
    try
    {
      return requestManager.getPreparedStatementGetParameterMetaData(request);
    }
    catch (NoMoreBackendException e)
    {
      return (SequoiaParameterMetaData) tryPreparedStatementMetaDataRequestOnRemoteControllers(new GetPreparedStatementParameterMetadata(
          request));
    }
  }

  /**
   * Returns the processMacroBeforeBroadcast value.
   * 
   * @return Returns the processMacroBeforeBroadcast.
   */
  public boolean isProcessMacroBeforeBroadcast()
  {
    return processMacroBeforeBroadcast;
  }

  /**
   * Sets the processMacroBeforeBroadcast value.
   * 
   * @param processMacros true if macros must be processed before broadcast.
   */
  public void setProcessMacroBeforeBroadcast(boolean processMacros)
  {
    this.processMacroBeforeBroadcast = processMacros;
  }

  /**
   * Returns the request result failover cache associated to this distributed
   * virtual database.
   * 
   * @return a <code>RequestResultFailoverCache</code> object.
   */
  public RequestResultFailoverCache getRequestResultFailoverCache()
  {
    return requestResultFailoverCache;
  }

  /**
   * Sets a new distributed request manager for this database.
   * 
   * @param requestManager the new request manager.
   */
  public void setRequestManager(RequestManager requestManager)
  {
    if (!(requestManager instanceof DistributedRequestManager))
      throw new RuntimeException(
          "A distributed virtual database can only work with a distributed request manager.");

    distributedRequestManager = (DistributedRequestManager) requestManager;
    // really, this is super.requestManager
    this.requestManager = distributedRequestManager;
  }

  /**
   * Get the whole static metadata for this virtual database. A new empty
   * metadata object is created if there was none yet. It will be filled later
   * by gatherStaticMetadata() when the backend is enabled.
   * 
   * @return Virtual database static metadata
   */
  public VirtualDatabaseStaticMetaData getStaticMetaData()
  {
    staticMetadata = doGetStaticMetaData();

    // If no backends enabled and vdb is distributed try remote controllers
    if ((staticMetadata == null)
        || (staticMetadata.getMetadataContainer() == null))
    {
      try
      {
        MulticastResponse rspList = getMulticastRequestAdapter()
            .multicastMessage(getAllMemberButUs(), new GetStaticMetadata(),
                MulticastRequestAdapter.WAIT_ALL,
                getMessageTimeouts().getVirtualDatabaseConfigurationTimeout());

        Map results = rspList.getResults();
        if (results.size() == 0)
          if (logger.isWarnEnabled())
            logger
                .warn("No response while getting static metadata from remote controller");
        for (Iterator iter = results.values().iterator(); iter.hasNext();)
        {
          Object response = iter.next();
          if (response instanceof ControllerException)
          {
            if (logger.isErrorEnabled())
            {
              logger
                  .error("Error while getting static metadata from remote controller");
            }
          }
          else
          {
            // Here we succeded in getting static metadata from a remote
            // controller
            staticMetadata.setMetadataContainer((MetadataContainer) response);
          }
        }
      }
      catch (NotConnectedException e2)
      {
        if (logger.isErrorEnabled())
          logger
              .error(
                  "Channel unavailable while getting static metadata from remote controller",
                  e2);
      }
    }

    return staticMetadata;
  }

  /**
   * Check if the given backend definition is compatible with the backend
   * definitions of this distributed virtual database. Not that if the given
   * backend does not exist in the current configuration, it is considered as
   * compatible. Incompatibility results from 2 backends with the same JDBC URL
   * or same logical name.
   * 
   * @param backend the backend to check
   * @return true if the backend is compatible with the local definition
   * @throws VirtualDatabaseException if locking the local backend list fails
   */
  public boolean isCompatibleBackend(BackendInfo backend)
      throws VirtualDatabaseException
  {
    try
    {
      acquireReadLockBackendLists();
    }
    catch (InterruptedException e)
    {
      String msg = "Unable to acquire read lock on backend list in isCompatibleBackend ("
          + e + ")";
      logger.error(msg);
      throw new VirtualDatabaseException(msg);
    }

    try
    {
      // Find the backend
      String backendURL = backend.getUrl();
      String backendName = backend.getName();
      int size = backends.size();
      DatabaseBackend b = null;
      for (int i = 0; i < size; i++)
      {
        b = (DatabaseBackend) backends.get(i);
        if (b.getURL().equals(backendURL) || b.getName().equals(backendName))
          return false;
      }
    }
    catch (RuntimeException re)
    {
      throw new VirtualDatabaseException(re);
    }
    finally
    {
      releaseReadLockBackendLists();
    }
    // This backend does not exist here
    return true;
  }

  /**
   * Return true if the provided schema is compatible with the existing schema
   * of this distributed virtual database. Note that if the given schema is
   * null, this function returns true.
   * 
   * @param dbs the database schema to compare with
   * @return true if dbs is compatible with the current schema (according to
   *         RAIDb level)
   */
  public boolean isCompatibleDatabaseSchema(DatabaseSchema dbs)
  {
    // Database schema checking (if any)
    if (dbs == null)
    {
      logger.warn(Translate
          .get("virtualdatabase.distributed.configuration.checking.noschema"));
    }
    else
    {
      // Check database schemas compatibility
      switch (getRequestManager().getLoadBalancer().getRAIDbLevel())
      {
        case RAIDbLevels.RAIDb0 :
          // There must be no overlap between schemas
          if (dbs.equals(getRequestManager().getDatabaseSchema()))
          {
            logger
                .warn(Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.databaseschema"));
            return false;
          }
          break;
        case RAIDbLevels.RAIDb1 :
          // Schemas must be identical
          if (!dbs.equals(getRequestManager().getDatabaseSchema()))
          {
            logger
                .warn(Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.databaseschema"));
            return false;
          }
          break;
        case RAIDbLevels.RAIDb2 :
          // Common parts of the schema must be identical
          if (!dbs.isCompatibleWith(getRequestManager().getDatabaseSchema()))
          {
            logger
                .warn(Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.databaseschema"));
            return false;
          }
          break;
        case RAIDbLevels.SingleDB :
        default :
          logger.error("Unsupported RAIDb level: "
              + getRequestManager().getLoadBalancer().getRAIDbLevel());
          return false;
      }
    }
    return true;
  }

  /**
   * Is this virtual database distributed ?
   * 
   * @return true
   */
  public boolean isDistributed()
  {
    return true;
  }

  /**
   * Returns the isVirtualDatabaseStarted value.
   * 
   * @return Returns the isVirtualDatabaseStarted.
   */
  public final boolean isVirtualDatabaseStarted()
  {
    return isVirtualDatabaseStarted;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getControllers()
   */
  public String[] viewControllerList()
  {
    if (logger.isInfoEnabled())
    {
      logger.info(channel.getLocalMembership() + " see members:"
          + currentGroup.getMembers() + " and has mapping:"
          + controllerJmxAddress);
    }
    Collection controllerJmxNames = controllerJmxAddress.values();
    return (String[]) controllerJmxNames.toArray(new String[controllerJmxNames
        .size()]);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#viewGroupBackends()
   */
  public Hashtable viewGroupBackends() throws VirtualDatabaseException
  {
    Hashtable map = super.viewGroupBackends();
    synchronized (backendsPerController)
    {
      Iterator iter = backendsPerController.keySet().iterator();
      while (iter.hasNext())
      {
        Member member = (Member) iter.next();

        // Create an List<BackendInfo> from the member backend list
        List backends = (List) backendsPerController.get(member);
        List backendInfos = DatabaseBackend.toBackendInfos(backends);
        map.put(controllerJmxAddress.get(member), backendInfos);
      }
    }
    return map;
  }

  //
  // Distributed virtual database management functions
  //

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#addBackend(org.continuent.sequoia.controller.backend.DatabaseBackend)
   */
  public void addBackend(DatabaseBackend db) throws VirtualDatabaseException
  {
    // Add the backend to the virtual database.
    super.addBackend(db);

    // Send a group message if already joined group
    try
    {
      broadcastBackendInformation(getAllMemberButUs());
    }
    catch (Exception e)
    {
      String msg = "Error while broadcasting backend information when adding backend";
      logger.error(msg, e);
      throw new VirtualDatabaseException(msg, e);
    }
  }

  /**
   * Add a controller id to the controllerIds list.
   * 
   * @param remoteControllerMembership the membership identifying the remote
   *          controller
   * @param remoteControllerId remote controller identifier
   */
  public void addRemoteControllerId(Member remoteControllerMembership,
      long remoteControllerId)
  {
    controllerIds.put(remoteControllerMembership, new Long(remoteControllerId));

    if (logger.isDebugEnabled())
      logger.debug("Adding new controller id:" + remoteControllerId
          + " for member " + remoteControllerMembership);
  }

  /**
   * Add a list of remote backends to the backendsPerController map.
   * 
   * @param sender the membership identifying the remote controller
   * @param remoteBackends remote controller backends
   */
  public void addBackendPerController(Member sender, List remoteBackends)
  {
    backendsPerController.put(sender, remoteBackends);

    if (logger.isInfoEnabled())
      logger.info(Translate.get(
          "virtualdatabase.distributed.configuration.updating.backend.list",
          sender));
  }

  /**
   * Returns the local view of the backends in this virtual database across all
   * <em>remote</em> controllers.
   * 
   * @return a Hashtable&lt;Member, List&lt;DatabaseBackend&gt;&gt;
   */
  public Hashtable getBackendsPerController()
  {
    return backendsPerController;
  }

  /**
   * Add a new controller name to the controllerJmxAddress list and refresh the
   * group membership.
   * 
   * @param remoteControllerMembership the membership identifying the remote
   *          controller
   * @param remoteControllerJmxName the JMX name of the remote controller
   */
  public void addRemoteControllerJmxName(Member remoteControllerMembership,
      String remoteControllerJmxName)
  {
    controllerJmxAddress.put(remoteControllerMembership,
        remoteControllerJmxName);
    if (logger.isDebugEnabled())
      logger.debug("Adding new controller " + remoteControllerJmxName
          + " for member " + remoteControllerMembership);

    sendJmxNotification(SequoiaNotificationList.DISTRIBUTED_CONTROLLER_ADDED,
        Translate.get("notification.distributed.controller.added",
            new String[]{remoteControllerJmxName, name}));

    refreshGroupMembership();
  }

  /**
   * Broadcast backend information among controllers.
   * 
   * @param dest List of <code>Address</code> to send the message to
   * @throws NotConnectedException if the channel is not connected
   */
  private void broadcastBackendInformation(ArrayList dest)
      throws NotConnectedException
  {
    logger
        .debug(Translate
            .get("virtualdatabase.distributed.configuration.querying.remote.status"));

    // Send our backend status using serializable BackendInfo
    List backendInfos = DatabaseBackend.toBackendInfos(backends);
    MulticastResponse rspList = multicastRequestAdapter.multicastMessage(dest,
        new BackendStatus(backendInfos, distributedRequestManager
            .getControllerId()), MulticastRequestAdapter.WAIT_ALL,
        messageTimeouts.getBackendStatusTimeout());

    int size = dest.size();
    for (int i = 0; i < size; i++)
    {
      // Add the backend configuration of every remote controller
      Member m = (Member) dest.get(i);
      if (rspList.getResult(m) != null)
      {
        BackendStatus bs = (BackendStatus) rspList.getResult(m);
        // Update backend list from sender
        List remoteBackendInfos = bs.getBackendInfos();
        // convert the BackendInfos to DatabaseBackends
        List remoteBackends = BackendInfo
            .toDatabaseBackends(remoteBackendInfos);
        backendsPerController.put(m, remoteBackends);
        if (logger.isDebugEnabled())
          logger
              .debug(Translate
                  .get(
                      "virtualdatabase.distributed.configuration.updating.backend.list",
                      m.toString()));
      }
      else
        logger.warn(Translate.get(
            "virtualdatabase.distributed.unable.get.remote.status", m
                .toString()));
    }
  }

  /**
   * Send the configuration of this controller to remote controller. All remote
   * controllers must agree on the compatibility of the local controller
   * configuration with their own configuration. Compatibility checking include
   * Authentication Manager, Scheduler and Load Balancer settings.
   * 
   * @param dest List of <code>Address</code> to send the message to
   * @return INCOMPATIBLE_CONFIGURATION if the configuration is not compatible
   *         with other controllers or the controller id to use otherwise.
   */
  private long checkConfigurationCompatibilityAndReturnControllerId(
      ArrayList dest)
  {
    if (logger.isInfoEnabled())
      logger.info(Translate
          .get("virtualdatabase.distributed.configuration.checking"));

    // Send our configuration
    MulticastResponse rspList;
    try
    {
      rspList = multicastRequestAdapter.multicastMessage(dest,
          new VirtualDatabaseConfiguration(this),
          MulticastRequestAdapter.WAIT_ALL, messageTimeouts
              .getVirtualDatabaseConfigurationTimeout());
    }
    catch (NotConnectedException e)
    {
      logger.error(
          "Channel unavailable while checking configuration compatibility", e);
      return INCOMPATIBLE_CONFIGURATION;
    }

    // Check that everybody agreed
    Map results = rspList.getResults();
    int size = results.size();
    if (size == 0)
      logger.warn(Translate
          .get("virtualdatabase.distributed.configuration.checking.noanswer"));

    long highestRemoteControllerId = 0;
    for (Iterator iter = results.values().iterator(); iter.hasNext();)
    {
      Object response = iter.next();
      if (response instanceof VirtualDatabaseConfigurationResponse)
      {
        // These highestRemotecontrollerId and remoteControllerId are returned
        // directly by the remote controller, and are 'thus' of 'shifted
        // nature': effective bits = upper 16 bits. See
        // DistributedRequestManager.CONTROLLER_ID_BITS
        VirtualDatabaseConfigurationResponse vdbcr = (VirtualDatabaseConfigurationResponse) response;
        long remoteControllerId = vdbcr.getControllerId();
        if (remoteControllerId == INCOMPATIBLE_CONFIGURATION)
        {
          return INCOMPATIBLE_CONFIGURATION;
        }

        // Check if there still is a problem of missing vdb users.
        // If it is the case try to add them dynamically.
        if (getAuthenticationManager().isTransparentLoginEnabled()
            && (vdbcr.getAdditionalVdbUsers() != null))
        {
          if (logger.isWarnEnabled())
            logger
                .warn("Some virtual database users are missing from this configuration, trying to create them transparently...");
          for (Iterator iter2 = vdbcr.getAdditionalVdbUsers().iterator(); iter2
              .hasNext();)
          {
            VirtualDatabaseUser vdbUser = (VirtualDatabaseUser) iter2.next();

            // Using the "super" trick here probably means bad design.
            // The intent is to create the vdb users just locally, hence we use
            // the method in
            // VirtualDatabase rather than the overridden method in
            // DistributedVirtual Database.
            super.checkAndAddVirtualDatabaseUser(vdbUser);
            if (!getAuthenticationManager().isValidVirtualUser(vdbUser))
            {
              return INCOMPATIBLE_CONFIGURATION;
            }
          }
        }

        if (highestRemoteControllerId < remoteControllerId)
          highestRemoteControllerId = remoteControllerId;
      }
      else
      {
        logger
            .error("Unexpected response while checking configuration compatibility: "
                + response);
        return INCOMPATIBLE_CONFIGURATION;
      }
    }

    // Ok, everybody agreed that our configuration is compatible.
    // Take the highest controller id + 1 as our id. (non-shifted, this is used
    // to pass in setControllerId which expects 16 bits)
    return ((highestRemoteControllerId >> DistributedRequestManager.CONTROLLER_ID_SHIFT_BITS) & DistributedRequestManager.CONTROLLER_ID_BITS) + 1;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#checkAndAddVirtualDatabaseUser(org.continuent.sequoia.common.users.VirtualDatabaseUser)
   */
  public void checkAndAddVirtualDatabaseUser(VirtualDatabaseUser vdbUser)
  {
    // Is vdb user valid?
    MulticastResponse rspList;
    try
    {
      rspList = multicastRequestAdapter.multicastMessage(getAllMembers(),
          new IsValidUserForAllBackends(vdbUser),
          MulticastRequestAdapter.WAIT_ALL, messageTimeouts
              .getVirtualDatabaseConfigurationTimeout());
    }
    catch (NotConnectedException e)
    {
      logger.error("Channel unavailable while checking validity of vdb user "
          + vdbUser.getLogin(), e);
      return;
    }

    // Check that everybody agreed
    Map results = rspList.getResults();
    int size = results.size();
    if (size == 0)
      logger.warn("No response while checking validity of vdb user "
          + vdbUser.getLogin());
    for (Iterator iter = results.values().iterator(); iter.hasNext();)
    {
      Object response = iter.next();
      if (response instanceof Boolean)
      {
        if (!((Boolean) response).booleanValue())
        {
          if (logger.isWarnEnabled())
          {
            logger.warn("Could not create new vdb user " + vdbUser.getLogin()
                + " because it does not exist on all backends");
          }
          return;
        }
      }
      else
      {
        logger.error("Unexpected response while checking validity of vdb user "
            + vdbUser.getLogin() + " : " + response);
        return;
      }
    }

    // Add user
    try
    {
      rspList = multicastRequestAdapter.multicastMessage(getAllMembers(),
          new AddVirtualDatabaseUser(vdbUser),
          MulticastRequestAdapter.WAIT_ALL, messageTimeouts
              .getVirtualDatabaseConfigurationTimeout());
    }
    catch (NotConnectedException e)
    {
      logger.error("Channel unavailable while adding vdb user "
          + vdbUser.getLogin() + ", trying to clean-up...", e);
      removeVirtualDatabaseUser(vdbUser);
    }

    // Check for exceptions
    results = rspList.getResults();
    size = results.size();
    if (size == 0)
      logger.warn("No response while adding vdb user " + vdbUser.getLogin());
    for (Iterator iter = results.values().iterator(); iter.hasNext();)
    {
      Object response = iter.next();
      if (response instanceof ControllerException)
      {
        if (logger.isErrorEnabled())
        {
          logger.error("Error while adding vdb user " + vdbUser.getLogin()
              + ", trying to clean-up...");
        }
        removeVirtualDatabaseUser(vdbUser);
        return;
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#closePersistentConnection(java.lang.String,
   *      long)
   */
  public void closePersistentConnection(String login,
      long persistentConnectionId)
  {
    distributedRequestManager.distributedClosePersistentConnection(login,
        persistentConnectionId);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#openPersistentConnection(java.lang.String,
   *      long)
   */
  public void openPersistentConnection(String login, long persistentConnectionId)
      throws SQLException
  {
    distributedRequestManager.distributedOpenPersistentConnection(login,
        persistentConnectionId);
  }

  /**
   * What this method does is really initiating the copy. It is the remote
   * controller's vdb that performs the actual copy, fetching the dump from this
   * vdb's local backuper.
   * 
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#copyDump(java.lang.String,
   *      java.lang.String)
   * @param dumpName the name of the dump to copy. Should exist locally, and not
   *          remotely.
   * @param remoteControllerName the remote controller to talk to.
   * @throws VirtualDatabaseException in case of error.
   */
  public void copyDump(String dumpName, String remoteControllerName)
      throws VirtualDatabaseException
  {
    transferDump(dumpName, remoteControllerName, false);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#copyLogFromCheckpoint(java.lang.String,
   *      java.lang.String)
   */
  public void copyLogFromCheckpoint(String dumpName, String controllerName)
      throws VirtualDatabaseException
  {
    // perform basic error checks (in particular, on success, we have a
    // recovery log)
    super.copyLogFromCheckpoint(dumpName, controllerName);

    Member controllerByName = getControllerByName(controllerName);
    if (isLocalSender(controllerByName))
      throw new VirtualDatabaseException(
          "A restore log command must be applied to a remote controller");

    // get the checkpoint name from the dump info, or die
    String dumpCheckpointName;
    DumpInfo dumpInfo;

    try
    {
      dumpInfo = getRecoveryLog().getDumpInfo(dumpName);
    }
    catch (SQLException e)
    {
      throw new VirtualDatabaseException(
          "Recovery log error access occured while checking for dump"
              + dumpName, e);
    }

    if (dumpInfo == null)
      throw new VirtualDatabaseException(
          "No information was found in the dump table for dump " + dumpName);

    RestoreLogOperation restoreLogOperation = new RestoreLogOperation(dumpName,
        controllerName);
    addAdminOperation(restoreLogOperation);
    try
    {
      dumpCheckpointName = dumpInfo.getCheckpointName();

      // set a global 'now' checkpoint (temporary) and suspend all activities
      String nowCheckpointName = setLogReplicationCheckpoint(controllerName);

      // AT THIS POINT, ALL ACTIVITIES ARE SUSPENDED

      // get its id (ewerk) so that we can replicate it on the other side
      long nowCheckpointId;
      RecoveryLog recoveryLog = getRequestManager().getRecoveryLog();
      try
      {
        try
        {
          nowCheckpointId = recoveryLog.getCheckpointLogId(nowCheckpointName);
        }
        catch (SQLException e)
        {
          String errorMessage = "Cannot find 'now checkpoint' log entry";
          logger.error(errorMessage);
          throw new VirtualDatabaseException(errorMessage);
        }

        // initiate the replication - clears the remote recovery log.
        sendMessageToController(controllerByName, new ReplicateLogEntries(
            nowCheckpointName, null, dumpName, nowCheckpointId),
            messageTimeouts.getReplicateLogEntriesTimeout());
      }
      finally
      {
        getRequestManager().resumeActivity(false);
      }

      // SCHEDULER ACTIVITIES ARE RESUMES AT THIS POINT

      // protect from concurrent log updates: fake a recovery (increments
      // semaphore)
      recoveryLog.beginRecovery();

      // copy the entries over to the remote controller.
      // Send them one by one over to the remote controller, coz each LogEntry
      // can
      // potentially be huge (e.g. if it contains a blob)
      try
      {
        ArrayList dest = new ArrayList();
        dest.add(controllerByName);
        long copyLogEntryTimeout = getMessageTimeouts()
            .getCopyLogEntryTimeout();
        long dumpId = recoveryLog.getCheckpointLogId(dumpCheckpointName);

        if (logger.isDebugEnabled())
        {
          logger.debug("Resynchronizing from checkpoint " + dumpCheckpointName
              + " (" + dumpId + ") to checkpoint " + nowCheckpointName + " ("
              + nowCheckpointId + ")");
        }

        for (long id = dumpId; id < nowCheckpointId; id++)
        {
          LogEntry entry = recoveryLog.getNextLogEntry(id);
          if (entry == null)
          {
            // No more entries available, stop here
            break;
          }

          // Because 'getNextLogEntry()' will hunt for the next valid log entry,
          // we need to update the iterator with the new id value - 1
          id = entry.getLogId() - 1;

          MulticastResponse resp = getMulticastRequestAdapter()
              .multicastMessage(dest, new CopyLogEntry(entry),
                  MulticastRequestAdapter.WAIT_NONE, copyLogEntryTimeout);
          if (resp.getFailedMembers() != null)
            throw new IOException("Failed to deliver log entry " + id
                + " to remote controller " + controllerName);
        }

        // Now check that no entry was missed by the other controller since we
        // shipped all entries asynchronously without getting any individual ack
        // (much faster to address SEQUOIA-504)
        long localNbOfLogEntries = recoveryLog.getNumberOfLogEntries(dumpId,
            nowCheckpointId);

        if (logger.isDebugEnabled())
        {
          logger.debug("Checking that " + localNbOfLogEntries
              + " entries were resynchronized in remote log");
        }

        Serializable replyValue = sendMessageToController(controllerByName,
            new CompleteRecoveryLogResync(dumpId, nowCheckpointName,
                localNbOfLogEntries), getMessageTimeouts()
                .getReplicateLogEntriesTimeout());
        if (replyValue instanceof Long)
        {
          long diff = ((Long) replyValue).longValue();
          if (diff != 0)
            throw new VirtualDatabaseException(
                "Recovery log resynchronization reports a difference of "
                    + diff + " entries");
        }
        else
          throw new RuntimeException(
              "Invalid answer from remote controller on CompleteRecoveryLogResync ("
                  + replyValue + ")");

        // terminate the replication - sets the remote dump checkpoint name.
        sendMessageToController(controllerName, new ReplicateLogEntries(null,
            dumpCheckpointName, dumpName, dumpId), messageTimeouts
            .getReplicateLogEntriesTimeout());
      }
      catch (Exception e)
      {
        String errorMessage = "Failed to send log entries";
        logger.error(errorMessage, e);
        throw new VirtualDatabaseException(errorMessage);
      }
      finally
      {
        recoveryLog.endRecovery(); // release semaphore
      }
    }
    finally
    {
      removeAdminOperation(restoreLogOperation);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#failoverForPersistentConnection(long)
   */
  public void failoverForPersistentConnection(long persistentConnectionId)
  {
    distributedRequestManager
        .distributedFailoverForPersistentConnection(persistentConnectionId);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#failoverForTransaction(long)
   */
  public void failoverForTransaction(long currentTid)
  {
    distributedRequestManager.distributedFailoverForTransaction(currentTid);
  }

  /**
   * Returns the recovery log associated with this controller.
   * 
   * @return the recovery log associated with this controller.
   * @throws VirtualDatabaseException if the database has not recovery log
   */
  public RecoveryLog getRecoveryLog() throws VirtualDatabaseException
  {
    if (!hasRecoveryLog())
      throw new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.recovery.log"));

    return getRequestManager().getRecoveryLog();
  }

  /**
   * Update remote backends list after a backend disable notification has been
   * received.
   * 
   * @param disabledBackend backend that is disabled
   * @param sender the message sender
   */
  public void handleRemoteDisableBackendNotification(
      DatabaseBackend disabledBackend, Member sender)
  {
    synchronized (backendsPerController)
    {
      List remoteBackends = (List) backendsPerController.get(sender);
      if (remoteBackends == null)
      { // This case was reported by Alessandro Gamboz on April 1, 2005.
        // It looks like the EnableBackend message arrives before membership
        // has been properly updated.
        logger.warn("No information has been found for remote controller "
            + sender);
        remoteBackends = new ArrayList();
        backendsPerController.put(sender, remoteBackends);
      }
      int size = remoteBackends.size();
      boolean backendFound = false;
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend remoteBackend = (DatabaseBackend) remoteBackends.get(i);
        if (remoteBackend.equals(disabledBackend))
        {
          logger.info("Backend " + remoteBackend.getName()
              + " disabled on controller " + sender);
          remoteBackends.set(i, disabledBackend);
          backendFound = true;
          break;
        }
      }
      if (!backendFound)
      {
        logger.warn("Updating backend list with unknown backend "
            + disabledBackend.getName() + " disabled on controller " + sender);
        remoteBackends.add(disabledBackend);
      }
    }
  }

  /**
   * Update remote backends list after a backend disable notification has been
   * received.
   * 
   * @param disabledBackendInfos List of BackendInfo objects that are disabled
   * @param sender the message sender
   */
  public void handleRemoteDisableBackendsNotification(
      ArrayList disabledBackendInfos, Member sender)
  {
    synchronized (backendsPerController)
    {
      List remoteBackends = (List) backendsPerController.get(sender);
      if (remoteBackends == null)
      { // This case was reported by Alessandro Gamboz on April 1, 2005.
        // It looks like the EnableBackend message arrives before membership
        // has been properly updated.
        logger.warn("No information has been found for remote controller "
            + sender);
        remoteBackends = new ArrayList();
        backendsPerController.put(sender, remoteBackends);
      }
      Iterator iter = disabledBackendInfos.iterator();
      while (iter.hasNext())
      {
        BackendInfo backendInfo = (BackendInfo) iter.next();
        DatabaseBackend backend = backendInfo.getDatabaseBackend();

        if (remoteBackends.contains(backend))
        {
          logger.info("Backend " + backend.getName()
              + " disabled on controller " + sender);
          remoteBackends.set(remoteBackends.indexOf(backend), backend);
        }
        else
        {
          remoteBackends.add(backend);
          logger.warn("Updating backend list with unknown backend "
              + backendInfo.getName() + " disabled on controller " + sender);
        }
      }
    }
  }

  /**
   * Sent the local controller configuration to a remote controller
   * 
   * @param dest the membership of the controller so send the information to
   * @throws NotConnectedException if the group communication channel is not
   *           connected
   */
  public void sendLocalConfiguration(Member dest) throws NotConnectedException
  {
    // Send controller name to new comer
    if (logger.isDebugEnabled())
      logger.debug("Sending local controller name to joining controller ("
          + dest + ")");

    List target = new ArrayList();
    target.add(dest);
    multicastRequestAdapter.multicastMessage(target, new ControllerInformation(
        controller.getControllerName(), controller.getJmxName(),
        distributedRequestManager.getControllerId()),
        MulticastRequestAdapter.WAIT_ALL, messageTimeouts
            .getControllerNameTimeout());

    // Send backend status
    if (logger.isDebugEnabled())
    {
      logger.debug("Sending backend status name to joining controller (" + dest
          + ")");
    }
    List backendInfos = DatabaseBackend.toBackendInfos(backends);
    multicastRequestAdapter.multicastMessage(target, new BackendStatus(
        backendInfos, distributedRequestManager.getControllerId()),
        MulticastRequestAdapter.WAIT_ALL, messageTimeouts
            .getBackendStatusTimeout());
  }

  /**
   * Returns true if the corresponding controller is alive
   * 
   * @param controllerId Id of the controller to check
   * @return true if controller is in controllerIds list
   */
  public boolean isAliveController(Long controllerId)
  {
    return controllerIds.containsValue(controllerId);
  }

  /**
   * Returns true if the given member is ourselves.
   * 
   * @param sender the sender
   * @return true if we are the sender, false otherwise
   */
  public boolean isLocalSender(Member sender)
  {
    return channel.getLocalMembership().equals(sender);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#removeBackend(java.lang.String)
   */
  public void removeBackend(String backend) throws VirtualDatabaseException
  {
    super.removeBackend(backend);

    try
    {
      // Send a group message to update backend list
      broadcastBackendInformation(getAllMemberButUs());
    }
    catch (Exception e)
    {
      String msg = "An error occured while multicasting new backedn information";
      logger.error(msg, e);
      throw new VirtualDatabaseException(msg, e);
    }
  }

  /**
   * Remove a remote controller (usually because it has failed) from the
   * controllerMap list and refresh the group membership. This also start a
   * ControllerFailureCleanupThread.
   * 
   * @param remoteControllerMembership the membership identifying the remote
   *          controller
   * @return the JMX name of the removed controller (or null if this controller
   *         was not in the list)
   */
  private String removeRemoteControllerAndStartCleanupThread(
      Member remoteControllerMembership)
  {
    String remoteControllerJmxName = (String) controllerJmxAddress
        .remove(remoteControllerMembership);
    if (logger.isDebugEnabled())
      logger.debug("Removing controller " + remoteControllerJmxName);

    // Remove the list of remote backends since they are no more reachable
    backendsPerController.remove(remoteControllerMembership);
    refreshGroupMembership();

    // Resume ongoing activity suspensions originated from the failed controller
    resumeOngoingActivitySuspensions(remoteControllerMembership);

    // Retrieve id of controller that failed and start a cleanup thread to
    // eliminate any remaining transactions/remaining persistent connections if
    // no client failover occurs in the defined timeframe
    Long failedControllerId = (Long) controllerIds
        .remove(remoteControllerMembership);

    if (failedControllerId != null)
    {
      ControllerFailureCleanupThread controllerFailureCleanupThread = new ControllerFailureCleanupThread(
          this, failedControllerId.longValue(), failoverTimeoutInMs,
          cleanupThreads, groupCommunicationMessagesLocallyFlushed);
      cleanupThreads.put(failedControllerId, controllerFailureCleanupThread);
      controllerFailureCleanupThread.start();
    }

    return remoteControllerJmxName;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#removeVirtualDatabaseUser(org.continuent.sequoia.common.users.VirtualDatabaseUser)
   */
  private void removeVirtualDatabaseUser(VirtualDatabaseUser vdbUser)
  {
    try
    {
      multicastRequestAdapter.multicastMessage(getAllMembers(),
          new RemoveVirtualDatabaseUser(vdbUser),
          MulticastRequestAdapter.WAIT_NONE, messageTimeouts
              .getVirtualDatabaseConfigurationTimeout());
    }
    catch (NotConnectedException e)
    {
      logger.error("Channel unavailable while removing vdb user "
          + vdbUser.getLogin(), e);
      return;
    }
  }

  /**
   * Set a cluster-wide checkpoint relying on the implementation of
   * SetCheckpoint to atomically set the checkpoint on all controllers. This
   * method leaves writes, transactions, and persistent connections suspended.
   * The caller must call RequestManager.resumeActivity() when the processing
   * associated with this checkpoint is complete.
   * 
   * @param checkpointName the name of the (transfer) checkpoint to create
   * @param groupMembers an ArrayList of target Members
   * @throws VirtualDatabaseException in case of scheduler or recoveryLog
   *           exceptions
   * @see SetCheckpointAndResumeTransactions
   */
  public void setGroupCheckpoint(String checkpointName, ArrayList groupMembers)
      throws VirtualDatabaseException
  {
    try
    {
      // First suspend transactions
      distributedRequestManager.suspendActivity();
      getMulticastRequestAdapter().multicastMessage(groupMembers,
          new DisableBackendsAndSetCheckpoint(new ArrayList(), checkpointName),
          MulticastRequestAdapter.WAIT_ALL,
          messageTimeouts.getSetCheckpointTimeout());
    }
    catch (Exception e)
    {
      String msg = "Set group checkpoint failed: checkpointName="
          + checkpointName;
      logger.error(msg, e);
      throw new VirtualDatabaseException(msg);
    }
  }

  /**
   * Sets an atomic (group-wide) checkpoint on local & target controllers
   * (referenced by Name).
   * 
   * @param controllerName the target remote controller
   * @param suspendActivity Indicates whether or not system activity should
   *          remain suspended when this request completes.
   * @return the 'now' checkpoint name.
   * @throws VirtualDatabaseException in case of error (whatever error, wraps
   *           the underlying error)
   */
  private String setLogReplicationCheckpoint(String controllerName)
      throws VirtualDatabaseException
  {
    return setLogReplicationCheckpoint(getControllerByName(controllerName));
  }

  /**
   * Sets an atomic (group-wide) checkpoint on local & target controllers
   * (refrenced by Name). When this call completes sucessfully, all activity on
   * the system is suspend. RequestManager.resumeActivity() must be called to
   * resume processing.
   * 
   * @param controller the target remote controller
   * @return the 'now' checkpoint name.
   * @throws VirtualDatabaseException in case of error (whatever error, wraps
   *           the underlying error)
   */
  public String setLogReplicationCheckpoint(Member controller)
      throws VirtualDatabaseException
  {
    String checkpointName = buildCheckpointName("now");

    // Apply checkpoint to remote controllers
    ArrayList dest = new ArrayList();
    dest.add(controller);
    dest.add(channel.getLocalMembership());
    setGroupCheckpoint(checkpointName, dest);
    return checkpointName;
  }

  /**
   * Sets an atomic (group-wide) checkpoint on all controllers, indicating that
   * this vdb has shutdown.
   */
  public void setShutdownCheckpoint()
  {
    // Set a cluster-wide checkpoint
    try
    {
      setGroupCheckpoint(buildCheckpointName("shutdown"), getAllMembers());
    }
    catch (VirtualDatabaseException e)
    {
      logger.warn("Error while setting shutdown checkpoint", e);
    }
    finally
    {
      if (isShuttingDown())
        setRejectingNewTransaction(true);
      distributedRequestManager.resumeActivity(false);
    }
  }

  /**
   * Send a Message to a remote controller, referenced by name. This sends a
   * point-to-point message, fifo. No total order is specifically required.
   * 
   * @param controllerName name of the remote controller
   * @param message the message to send (should be Serializable)
   * @param timeout message timeout in ms
   * @throws VirtualDatabaseException (wrapping error) in case of communication
   *           failure
   */
  private void sendMessageToController(String controllerName,
      Serializable message, long timeout) throws VirtualDatabaseException
  {
    sendMessageToController(getControllerByName(controllerName), message,
        timeout);
  }

  /**
   * Send a Message to a remote controller, referenced by its Member. This sends
   * a point-to-point message, fifo. No total order is specifically required
   * (but enforced anyway).
   * 
   * @param controllerMember Member object refering to the remote controller
   * @param message the message to send (should be Serializable)
   * @param timeout message timeout in ms
   * @return the result returned by the remote controller (except if this an
   *         exception in which case it is automatically thrown)
   * @throws VirtualDatabaseException (wrapping error) in case of communication
   *           failure
   */
  public Serializable sendMessageToController(Member controllerMember,
      Serializable message, long timeout) throws VirtualDatabaseException
  {
    try
    {
      ArrayList dest = new ArrayList();
      dest.add(controllerMember);
      MulticastResponse resp = getMulticastRequestAdapter().multicastMessage(
          dest, message, MulticastRequestAdapter.WAIT_ALL, timeout);
      Object o = resp.getResult(controllerMember);
      if (o instanceof Exception)
        throw (Exception) o;
      return (Serializable) o;
    }
    catch (Exception e)
    {
      logger.error(e);
      throw new VirtualDatabaseException(e);
    }
  }

  /**
   * Send a Message to a set of controllers. This sends a multicast message,
   * fifo and checks for returned exceptions. No total order is specifically
   * required (but enforced anyway). If an exception is returned, it is
   * rethrown.
   * 
   * @param members ArrayList of Member object refering to controllers
   * @param message the message to send (should be Serializable)
   * @param timeout message timeout in ms
   * @return the result returned (except if one controller returned an exception
   *         in which case it is automatically thrown)
   * @throws VirtualDatabaseException (wrapping error) in case of communication
   *           failure
   */
  public MulticastResponse sendMessageToControllers(ArrayList members,
      Serializable message, long timeout) throws VirtualDatabaseException
  {
    try
    {
      MulticastResponse resp = getMulticastRequestAdapter().multicastMessage(
          members, message, MulticastRequestAdapter.WAIT_ALL, timeout);
      Iterator it = resp.getResults().keySet().iterator();
      while (it.hasNext())
      {
        Object o = resp.getResults().get(it.next());
        // TODO: return compound exception instead of the first one
        if (o instanceof Exception)
          throw (Exception) o;
      }
      return resp;
    }
    catch (Exception e)
    {
      logger.error(e);
      throw new VirtualDatabaseException(e);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#shutdown(int)
   */
  public void shutdown(int level)
  {
    ActivityService.getInstance().reset(name);
    if (partitionReconciler != null)
    {
      partitionReconciler.dispose();
    }

    // Shutdown cleanup threads
    for (Iterator iter = cleanupThreads.values().iterator(); iter.hasNext();)
    {
      ControllerFailureCleanupThread thread = (ControllerFailureCleanupThread) iter
          .next();
      thread.shutdown();
    }

    // Shutdown request result failover cache clean-up thread
    requestResultFailoverCache.shutdown();

    super.shutdown(level);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#transferBackend(java.lang.String,
   *      java.lang.String)
   */
  public void transferBackend(String backend, String controllerDestination)
      throws VirtualDatabaseException
  {
    TransferBackendOperation transferOperation = new TransferBackendOperation(
        backend, controllerDestination);
    addAdminOperation(transferOperation);
    try
    {
      Member targetMember = getControllerByName(controllerDestination);

      // Get reference on backend
      DatabaseBackend db = getAndCheckBackend(backend, CHECK_BACKEND_DISABLE);
      String transfertCheckpointName = buildCheckpointName("transfer backend: "
          + db.getName() + " from " + controller.getControllerName() + " to "
          + targetMember.getUid());

      if (logger.isDebugEnabled())
        logger.debug("**** Disabling backend for transfer");

      // Disable local backend
      try
      {
        if (!hasRecoveryLog())
          throw new VirtualDatabaseException(
              "Transfer is not supported on virtual databases without a recovery log");

        distributedRequestManager.disableBackendWithCheckpoint(db,
            transfertCheckpointName);
      }
      catch (SQLException e)
      {
        throw new VirtualDatabaseException(e.getMessage());
      }

      // Enable remote transfered backend.
      try
      {
        if (logger.isDebugEnabled())
          logger.debug("**** Sending transfer message to:" + targetMember);

        ArrayList dest = new ArrayList(1);
        dest.add(targetMember);

        sendMessageToController(targetMember,
            new BackendTransfer(controllerDestination, transfertCheckpointName,
                new BackendInfo(db)), messageTimeouts
                .getBackendTransferTimeout());

        if (logger.isDebugEnabled())
          logger.debug("**** Removing local backend");

        // Remove backend from this controller
        removeBackend(db);

        // Broadcast updated backend list
        broadcastBackendInformation(getAllMemberButUs());
      }
      catch (Exception e)
      {
        String msg = "An error occured while transfering the backend";
        logger.error(msg, e);
        throw new VirtualDatabaseException(msg, e);
      }
    }
    finally
    {
      removeAdminOperation(transferOperation);
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#transferDump(java.lang.String,
   *      java.lang.String, boolean)
   */
  public void transferDump(String dumpName, String remoteControllerName,
      boolean noCopy) throws VirtualDatabaseException
  {
    TransferDumpOperation transferOperation = new TransferDumpOperation(
        dumpName, remoteControllerName);
    addAdminOperation(transferOperation);
    try
    {
      // get the info from the backuper
      DumpInfo dumpInfo = null;
      try
      {
        dumpInfo = getRecoveryLog().getDumpInfo(dumpName);
        /*
         * getDumpInfo() is the one that throws SQLException (should it be a
         * VirtualDatabaseException instead ???)
         */
      }
      catch (SQLException e)
      {
        String msg = "getting dump info from backup manager failed";
        throw new VirtualDatabaseException(msg, e);
      }

      if (dumpInfo == null)
        throw new VirtualDatabaseException("no dump info for dump '" + dumpName
            + "'");

      if (remoteControllerName.equals(controller.getJmxName()))
        throw new VirtualDatabaseException("Not transfering dump to myself");

      // if a copy is needed, hand-off copy to backuper: setup server side of
      // the
      // copy
      DumpTransferInfo dumpTransferInfo = null;
      if (!noCopy)
      {
        try
        {
          dumpTransferInfo = getRequestManager().getBackupManager()
              .getBackuperByFormat(dumpInfo.getDumpFormat()).setupDumpServer();
        }
        catch (IOException e)
        {
          throw new VirtualDatabaseException(e);
        }
      }

      // send message to remote vdb instance, to act as a client
      // (see handleInitiateDumpCopy)
      sendMessageToController(remoteControllerName, new InitiateDumpCopy(
          dumpInfo, dumpTransferInfo), messageTimeouts
          .getInitiateDumpCopyTimeout());
    }
    finally
    {
      removeAdminOperation(transferOperation);
    }
  }

  /**
   * If we are executing in a distributed virtual database, we have to make sure
   * that we post the query in the queue following the total order. This method
   * does not remove the request from the total order queue. You have to call
   * removeHeadFromAndNotifyTotalOrderQueue() to do so.
   * 
   * @param request the request to wait for (can be any object but usually a
   *          DistributedRequest, Commit or Rollback)
   * @param errorIfNotFound true if an error message should be logged if the
   *          request is not found in the total order queue
   * @return true if the element was found and wait has succeeded, false
   *         otherwise
   */
  public boolean waitForTotalOrder(Object request, boolean errorIfNotFound)
  {
    synchronized (totalOrderQueue)
    {
      int index = totalOrderQueue.indexOf(request);
      while (index > 0)
      {
        if (logger.isDebugEnabled())
          logger.debug("Waiting for " + index
              + " queries to execute (current is " + totalOrderQueue.get(0)
              + ")");
        try
        {
          totalOrderQueue.wait();
        }
        catch (InterruptedException ignore)
        {
        }
        index = totalOrderQueue.indexOf(request);
      }
      if (index == -1)
      {
        if (errorIfNotFound)
          logger
              .error("Request was not found in total order queue, posting out of order ("
                  + request + ")");
        return false;
      }
      else
        return true;
    }
  }

  /**
   * Variant of the waitForTotalOrder() method, please refer to its
   * documentation. The difference here is that we only wait for request of type
   * SuspendWritesMessage which are in front of the given request. All other
   * request are by-passed.
   * 
   * @see ResumeActivity
   * @see BlockActivity
   * @see SuspendActivity
   * @param request the request to wait for (expected to be a
   *          ResumeActivityMessage)
   * @param errorIfNotFound true if an error message should be logged if the
   *          request is not found in the total order queue
   * @return true if the element was found and wait has succeeded, false
   *         otherwise
   */
  public boolean waitForBlockAndSuspendInTotalOrder(Object request,
      boolean errorIfNotFound)
  {
    synchronized (totalOrderQueue)
    {
      // Init
      int index = totalOrderQueue.indexOf(request);
      boolean shouldWait = false;
      for (int i = 0; i < index; i++)
        if ((totalOrderQueue.get(i) instanceof SuspendWritesMessage))
          shouldWait = true;

      // Main loop
      while ((index > 0) && shouldWait)
      {
        System.out.println("index=" + index + " shouldWait=" + shouldWait);
        if (logger.isDebugEnabled())
          logger.debug("Waiting for " + index
              + " queries to execute (current is " + totalOrderQueue.get(0)
              + ")");
        try
        {
          totalOrderQueue.wait();
        }
        catch (InterruptedException ignore)
        {
        }
        index = totalOrderQueue.indexOf(request);
        shouldWait = false;
        for (int i = 0; i < index; i++)
          if ((totalOrderQueue.get(i) instanceof SuspendWritesMessage))
            shouldWait = true;
      }

      if (index == -1)
      {
        if (errorIfNotFound)
          logger
              .error("Request was not found in total order queue, posting out of order ("
                  + request + ")");
        return false;
      }
      else
        return true;
    }
  }

  /**
   * Adds an ongoing activity suspension marker to the current list only if
   * suspension was not triggered by the local member.
   * 
   * @param controllerMember member which triggered the activity suspension
   */
  public void addOngoingActivitySuspension(Member controllerMember)
  {
    if (controllerMember.equals(multicastRequestAdapter.getChannel()
        .getLocalMembership()))
      return;
    ongoingActivitySuspensions.add(controllerMember);
  }

  /**
   * Removes an ongoing activity suspension marker from the current list only if
   * resuming was not triggered by the local member.
   * 
   * @param controllerMember member which triggered the activity resuming
   */
  public void removeOngoingActivitySuspension(Member controllerMember)
  {
    if (controllerMember.equals(multicastRequestAdapter.getChannel()
        .getLocalMembership()))
      return;
    ongoingActivitySuspensions.remove(controllerMember);
  }

  // Resume ongoing activity suspensions originated from the failed controller
  private void resumeOngoingActivitySuspensions(Member controllerMember)
  {
    List dest = new ArrayList();
    dest.add(multicastRequestAdapter.getChannel().getLocalMembership());

    for (Iterator iter = ongoingActivitySuspensions.iterator(); iter.hasNext();)
    {
      Member m = (Member) iter.next();
      if (m.equals(controllerMember))
        try
        {
          multicastRequestAdapter.multicastMessage(dest, new ResumeActivity(),
              MulticastRequestAdapter.WAIT_ALL, messageTimeouts
                  .getDisableBackendTimeout());
          // Need to remove the marker here because the sender of the
          // ResumeActivity message will be the local member
          iter.remove();
        }
        catch (NotConnectedException e)
        {
          if (logger.isWarnEnabled())
            logger.warn(
                "Problem when trying to resume ongoing activity suspensions triggered by "
                    + controllerMember, e);
        }
    }
  }

  /**
   * Interrupts the recover thread in order to interactively resume activity for
   * this virtual database
   */
  public void interactiveResumeActivity()
  {
    synchronized (this)
    {
      if (!recoverThreads.isEmpty())
      {
        for (Iterator iterator = recoverThreads.iterator(); iterator.hasNext();)
        {
          RecoverThread thread = (RecoverThread) iterator.next();
          if (thread != null)
          {
            if (thread.interruptRecoveryProcess())
            {
              // Wait for the recover thread to complete
              try
              {
                thread.join();
              }
              catch (InterruptedException e)
              {
              }
            }
          }
        }
      }
    }
  }
}