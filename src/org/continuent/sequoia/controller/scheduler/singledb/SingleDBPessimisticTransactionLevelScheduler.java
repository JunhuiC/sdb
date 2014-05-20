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

package org.continuent.sequoia.controller.scheduler.singledb;

import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.RollbackException;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.scheduler.schema.TransactionExclusiveLock;

/**
 * This scheduler provides transaction level scheduling for a SingleDB. Each
 * write takes a lock on the whole database. All following writes are blocked
 * until the transaction of the first write completes.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class SingleDBPessimisticTransactionLevelScheduler
    extends AbstractScheduler
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

  TransactionExclusiveLock lock = new TransactionExclusiveLock();

  //
  // Constructor
  //

  /**
   * Creates a new Pessimistic Transaction Level Scheduler
   */
  @SuppressWarnings("deprecation")
public SingleDBPessimisticTransactionLevelScheduler()
  {
    super(RAIDbLevels.SingleDB, ParsingGranularities.NO_PARSING);
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
  public final void scheduleNonSuspendedReadRequest(SelectRequest request)
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
  public void scheduleNonSuspendedWriteRequest(AbstractWriteRequest request)
      throws SQLException
  {
    if (lock.acquire(request))
    {
      if (logger.isDebugEnabled())
        logger.debug("Request " + request.getId() + " scheduled for write ("
            + getPendingWrites() + " pending writes)");
    }
    else
    {
      if (logger.isWarnEnabled())
        logger.warn("Request " + request.getId() + " timed out ("
            + request.getTimeout() + " s)");
      throw new SQLException("Timeout (" + request.getTimeout()
          + ") for request: " + request.getId());
    }
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyWriteCompleted(AbstractWriteRequest)
   */
  public final void notifyWriteCompleted(AbstractWriteRequest request)
  {
    // Requests outside transaction delimiters must release the lock
    // as soon as they have executed
    if (request.isAutoCommit())
      releaseLock(request.getTransactionId());
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedStoredProcedure(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final void scheduleNonSuspendedStoredProcedure(StoredProcedure proc)
      throws SQLException, RollbackException
  {
    if (lock.acquire(proc))
    {
      if (logger.isDebugEnabled())
        logger.debug("Stored procedure " + proc.getId()
            + " scheduled for write (" + getPendingWrites()
            + " pending writes)");
    }
    else
    {
      if (logger.isWarnEnabled())
        logger.warn("Stored procedure " + proc.getId() + " timed out ("
            + proc.getTimeout() + " s)");
      throw new SQLException("Timeout (" + proc.getTimeout()
          + ") for request: "
          + proc.getSqlShortForm(ControllerConstants.SQL_SHORT_FORM_LENGTH));
    }
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyStoredProcedureCompleted(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final void notifyStoredProcedureCompleted(StoredProcedure proc)
  {
    // Requests outside transaction delimiters must release the lock
    // as soon as they have executed
    if (proc.isAutoCommit() && (!proc.isCreate()))
      releaseLock(proc.getTransactionId());
  }

  //
  // Transaction Management
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#commitTransaction(long)
   */
  protected final void commitTransaction(long transactionId)
  {
    releaseLock(transactionId);
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#rollbackTransaction(long)
   */
  protected final void rollbackTransaction(long transactionId)
  {
    releaseLock(transactionId);
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

  /**
   * Release the locks we may own on the schema.
   * 
   * @param transactionId id of the transaction that releases the lock
   */
  private void releaseLock(long transactionId)
  {
    // Are we the lock owner ?
    if (lock.isLocked())
    {
      if (lock.getLocker() == transactionId)
        lock.release();

      // Note that the following warnings could be safely ignored if the
      // transaction
      // commiting/rolllbacking (releasing the lock) has not done any
      // conflicting write
      else if (logger.isDebugEnabled())
        logger.debug("Transaction " + transactionId
            + " wants to release the lock held by transaction "
            + lock.getLocker());
    }
    else if (logger.isDebugEnabled())
      logger.warn("Transaction " + transactionId
          + " tries to release a lock that has not been acquired.");
  }

  //
  // Debug/Monitoring
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#getXmlImpl()
   */
  public String getXmlImpl()
  {
    return "<" + DatabasesXmlTags.ELT_SingleDBScheduler + " "
        + DatabasesXmlTags.ATT_level + "=\""
        + DatabasesXmlTags.VAL_pessimisticTransaction + "\"/>";
  }
}
