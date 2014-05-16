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
import java.sql.SQLException;
import java.util.LinkedList;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseStartingException;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a DistributedOpenPersistentConnection
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DistributedOpenPersistentConnection
    extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = -693544521730643721L;
  private String            login;
  private long              persistentConnectionId;

  /**
   * Creates a new <code>DistributedOpenPersistentConnection</code> object
   * 
   * @param login login to retrieve the connection manager
   * @param persistentConnectionId persistent connection id
   */
  public DistributedOpenPersistentConnection(String login,
      long persistentConnectionId)
  {
    this.login = login;
    this.persistentConnectionId = persistentConnectionId;
  }

  /**
   * Returns the login value.
   * 
   * @return Returns the login.
   */
  public final String getLogin()
  {
    return login;
  }

  /**
   * Returns the persistentConnectionId value.
   * 
   * @return Returns the persistentConnectionId.
   */
  public final long getPersistentConnectionId()
  {
    return persistentConnectionId;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    if (!dvdb.isVirtualDatabaseStarted())
      return new VirtualDatabaseStartingException();

    LinkedList totalOrderQueue = dvdb.getTotalOrderQueue();
    synchronized (totalOrderQueue)
    {
      totalOrderQueue.addLast(this);
    }
    return this;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    if (handleMessageSingleThreadedResult instanceof Exception)
      return (Serializable) handleMessageSingleThreadedResult;

    dvdb.getRequestManager().getLoadBalancer().waitForTotalOrder(this, true);

    DistributedRequestManager drm = ((DistributedRequestManager) dvdb
        .getRequestManager());
    AbstractScheduler scheduler = drm.getScheduler();
    RecoveryLog recoveryLog = drm.getRecoveryLog();
    long entryId = -1;
    try
    {
      boolean success = false;
      try
      {
        scheduler.scheduleOpenPersistentConnection(this);

        entryId = recoveryLog.logOpenPersistentConnection(login,
            persistentConnectionId);

        drm.getLoadBalancer().openPersistentConnection(login,
            persistentConnectionId);
        success = true;
        recoveryLog.logRequestCompletion(entryId, success, 0);
      }
      catch (NoMoreBackendException e)
      {
        throw e;
      }
      catch (SQLException e)
      {
        throw e;
      }
      finally
      {
        scheduler.openPersistentConnectionCompleted(persistentConnectionId,
            success);
      }
      return Boolean.TRUE;
    }
    catch (SQLException e)
    {
      UnknownWriteRequest notifRequest = new UnknownWriteRequest("open "
          + persistentConnectionId, false, 0, null);
      notifRequest.setLogId(entryId);
      notifRequest.setPersistentConnection(true);
      notifRequest.setPersistentConnectionId(persistentConnectionId);
      drm.addFailedOnAllBackends(notifRequest, false);
      return e;
    }
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj)
  {
    if (obj instanceof DistributedOpenPersistentConnection)
    {
      DistributedOpenPersistentConnection other = (DistributedOpenPersistentConnection) obj;
      return persistentConnectionId == other.persistentConnectionId;
    }
    return false;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return (int) persistentConnectionId;
  }
}
