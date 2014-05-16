/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks
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

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseStartingException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a DistributedTransactionMarker which is used to transport
 * commit/rollback/savepoint type of commands.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public abstract class DistributedTransactionMarker
    extends DistributedVirtualDatabaseMessage
{

  protected long transactionId;

  /**
   * Creates a new <code>DistributedTransactionMarker</code> object
   * 
   * @param transactionId the transaction identifier
   */
  public DistributedTransactionMarker(long transactionId)
  {
    this.transactionId = transactionId;
  }

  /**
   * Schedule the command (i.e. commit or rollback). This method blocks until
   * the command is scheduled.
   * 
   * @param drm a distributed request manager
   * @return the object inserted in the total order queue
   * @throws SQLException if an error occurs.
   */
  public abstract Object scheduleCommand(DistributedRequestManager drm)
      throws SQLException;

  /**
   * Code to be executed by the distributed request manager receiving the
   * command.
   * 
   * @param drm a distributed request manager
   * @return a Serializable object to be sent back to the caller
   * @throws SQLException if an error occurs.
   */
  public abstract Serializable executeCommand(DistributedRequestManager drm)
      throws SQLException;

  /**
   * Returns the transactionId value.
   * 
   * @return Returns the transactionId.
   */
  public long getTransactionId()
  {
    return transactionId;
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

    Trace distributedRequestLogger = dvdb.getDistributedRequestLogger();
    if (distributedRequestLogger.isInfoEnabled())
      distributedRequestLogger.info(toString());

    try
    {
      return scheduleCommand((DistributedRequestManager) dvdb
          .getRequestManager());
    }
    catch (SQLException e)
    {
      return e;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    if (handleMessageSingleThreadedResult != null)
    {
      if (handleMessageSingleThreadedResult instanceof Exception)
        return (Serializable) handleMessageSingleThreadedResult;
    }

    try
    {
      return executeCommand((DistributedRequestManager) dvdb
          .getRequestManager());
    }
    catch (SQLException e)
    {
      return e;
    }
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return (int) transactionId;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj)
  {
    if (obj == null)
      return false;
    if (obj.getClass().equals(this.getClass()))
      return transactionId == ((DistributedTransactionMarker) obj)
          .getTransactionId();
    else
      return false;
  }
}
