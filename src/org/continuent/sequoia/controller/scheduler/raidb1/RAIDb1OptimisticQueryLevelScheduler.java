/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): Jean-Bernard van Zuylen.
 */

package org.continuent.sequoia.controller.scheduler.raidb1;

import java.sql.SQLException;
import java.util.HashSet;

import org.continuent.sequoia.common.exceptions.RollbackException;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;

/**
 * This scheduler provides optimistic query level scheduling for RAIDb-1
 * controllers. Reads can execute in parallel of any request. Writes are flagged
 * as blocking or not based on the completion of a previous write inside the
 * same transaction.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 * @deprecated since Sequoia 2.2
 */
public class RAIDb1OptimisticQueryLevelScheduler extends AbstractScheduler
{

  //
  // How the code is organized ?
  //
  // 1. Member variables
  // 2. Constructor
  // 3. Request handling
  // 4. Transaction management
  // 5. Debug/Monitoring
  //

  private HashSet completedWrites = new HashSet(); // set of tids

  //
  // Constructor
  //

  /**
   * Creates a new Query Level Scheduler
   */
  public RAIDb1OptimisticQueryLevelScheduler()
  {
    super(RAIDbLevels.RAIDb1, ParsingGranularities.NO_PARSING);
  }

  //
  // Request Handling
  //

  /**
   * Additionally to scheduling the request, this method replaces the SQL Date
   * macros such as now() with the current date.
   * 
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedReadRequest(SelectRequest)
   */
  public void scheduleNonSuspendedReadRequest(SelectRequest request)
      throws SQLException
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#readCompletedNotify(SelectRequest)
   */
  public final void readCompletedNotify(SelectRequest request)
  {
  }

  /**
   * Additionally to scheduling the request, this method replaces the SQL Date
   * macros such as now() with the current date.
   * 
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleWriteRequest(AbstractWriteRequest)
   */
  public synchronized void scheduleNonSuspendedWriteRequest(
      AbstractWriteRequest request) throws SQLException
  {
    // if (request.isAutoCommit())
    // request.setBlocking(true);
    // else
    request.setBlocking(completedWrites.contains(new Long(request
        .getTransactionId())));
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyWriteCompleted(AbstractWriteRequest)
   */
  public final synchronized void notifyWriteCompleted(
      AbstractWriteRequest request)
  {
    if (!request.isAutoCommit())
      completedWrites.add(new Long(request.getTransactionId()));
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedStoredProcedure(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final synchronized void scheduleNonSuspendedStoredProcedure(
      StoredProcedure proc) throws SQLException, RollbackException
  {
    proc.setBlocking(completedWrites
        .contains(new Long(proc.getTransactionId())));
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyStoredProcedureCompleted(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final synchronized void notifyStoredProcedureCompleted(
      StoredProcedure proc)
  {
    if (!proc.isAutoCommit())
      completedWrites.add(new Long(proc.getTransactionId()));
  }

  //
  // Transaction Management
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#commitTransaction(long)
   */
  protected final synchronized void commitTransaction(long transactionId)
  {
    completedWrites.remove(new Long(transactionId));
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#rollbackTransaction(long)
   */
  protected final synchronized void rollbackTransaction(long transactionId)
  {
    completedWrites.remove(new Long(transactionId));
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#rollbackTransaction(long,
   *      String)
   */
  protected final void rollbackTransaction(long transactionId,
      String savepointName)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#setSavepointTransaction(long,
   *      String)
   */
  protected final void setSavepointTransaction(long transactionId, String name)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#releaseSavepointTransaction(long,
   *      String)
   */
  protected final void releaseSavepointTransaction(long transactionId,
      String name)
  {
  }

  //
  // Debug/Monitoring
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#getXmlImpl()
   */
  public String getXmlImpl()
  {
    return "<" + DatabasesXmlTags.ELT_RAIDb1Scheduler + " "
        + DatabasesXmlTags.ATT_level + "=\""
        + DatabasesXmlTags.VAL_optimisticQuery + "\"/>";
  }
}
