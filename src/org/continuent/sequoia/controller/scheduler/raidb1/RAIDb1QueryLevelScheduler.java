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

import org.continuent.sequoia.common.exceptions.RollbackException;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;

/**
 * This scheduler provides query level scheduling for RAIDb-1 controllers. Reads
 * can execute in parallel until a write comes in. Then the write waits for the
 * completion of the reads. Any new read is stacked after the write and they are
 * released together when the write has completed its execution.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 * @deprecated since Sequoia 2.2
 */
public class RAIDb1QueryLevelScheduler extends AbstractScheduler
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

  private int    pendingReads;

  // We have to distinguish read and write to wake up only
  // waiting reads or writes according to the situation
  private Object readSync;    // to synchronize on reads completion
  private Object writeSync;   // to synchronize on writes completion

  //
  // Constructor
  //

  /**
   * Creates a new Query Level Scheduler
   */
  public RAIDb1QueryLevelScheduler()
  {
    super(RAIDbLevels.RAIDb1, ParsingGranularities.NO_PARSING);
    pendingReads = 0;
    readSync = new Object();
    writeSync = new Object();
  }

  //
  // Request Handling
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedReadRequest(SelectRequest)
   */
  public void scheduleNonSuspendedReadRequest(SelectRequest request)
      throws SQLException
  {
    // Now deal with synchronization
    synchronized (this.writeSync)
    {
      if (getPendingWrites() == 0)
      { // No writes pending, go ahead !
        synchronized (this.readSync)
        {
          pendingReads++;
          if (logger.isDebugEnabled())
            logger.debug("Request "
                + request.getId()
                + (request.isAutoCommit() ? "" : " transaction "
                    + request.getTransactionId()) + " scheduled for read ("
                + pendingReads + " pending reads)");
          return;
        }
      }

      // Wait for the writes completion
      try
      {
        if (logger.isDebugEnabled())
          logger.debug("Request " + request.getId() + " waiting for "
              + getPendingWrites() + " pending writes)");

        int timeout = request.getTimeout();
        if (timeout > 0)
        {
          long start = System.currentTimeMillis();
          // Convert seconds to milliseconds for wait call
          long lTimeout = timeout * 1000L;
          this.writeSync.wait(lTimeout);
          long end = System.currentTimeMillis();
          int remaining = (int) (lTimeout - (end - start));
          if (remaining > 0)
            request.setTimeout(remaining);
          else
          {
            String msg = "Timeout (" + request.getTimeout() + ") for request: "
                + request.getId();
            logger.warn(msg);
            throw new SQLException(msg);
          }
        }
        else
          this.writeSync.wait();

        synchronized (this.readSync)
        {
          pendingReads++;
          if (logger.isDebugEnabled())
            logger.debug("Request " + request.getId() + " scheduled for read ("
                + pendingReads + " pending reads)");
          return; // Ok, write completed before timeout
        }
      }
      catch (InterruptedException e)
      {
        // Timeout
        if (logger.isWarnEnabled())
          logger.warn("Request " + request.getId() + " timed out ("
              + request.getTimeout() + " s)");
        throw new SQLException("Timeout (" + request.getTimeout()
            + ") for request: " + request.getId());
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#readCompletedNotify(SelectRequest)
   */
  public final void readCompletedNotify(SelectRequest request)
  {
    synchronized (this.readSync)
    {
      pendingReads--;
      if (logger.isDebugEnabled())
        logger.debug("Read request " + request.getId() + " completed - "
            + pendingReads + " pending reads");
      if (pendingReads == 0)
      {
        if (logger.isDebugEnabled())
          logger.debug("Last read completed, notifying writes");
        readSync.notifyAll(); // Wakes up any waiting write query
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleWriteRequest(AbstractWriteRequest)
   */
  public void scheduleNonSuspendedWriteRequest(AbstractWriteRequest request)
      throws SQLException
  {
    // We have to take the locks in the same order as reads else
    // we could have a deadlock
    synchronized (this.writeSync)
    {
      synchronized (this.readSync)
      {
        if (pendingReads == 0)
        { // No read pending, go ahead
          if (logger.isDebugEnabled())
            logger.debug("Request "
                + request.getId()
                + (request.isAutoCommit() ? "" : " transaction "
                    + request.getTransactionId()) + " scheduled for write ("
                + getPendingWrites() + " pending writes)");
          return;
        }
      }
    }

    waitForReadCompletion(request);
    scheduleNonSuspendedWriteRequest(request);
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyWriteCompleted(AbstractWriteRequest)
   */
  public final synchronized void notifyWriteCompleted(
      AbstractWriteRequest request)
  {
    synchronized (this.writeSync)
    {
      if (logger.isDebugEnabled())
        logger.debug("Request " + request.getId() + " completed - "
            + getPendingWrites() + " pending writes");
      if (getPendingWrites() == 0)
      {
        if (logger.isDebugEnabled())
          logger.debug("Last write completed, notifying reads");
        writeSync.notifyAll(); // Wakes up all waiting read queries
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedStoredProcedure(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final synchronized void scheduleNonSuspendedStoredProcedure(
      StoredProcedure proc) throws SQLException, RollbackException
  {
    // We have to take the locks in the same order as reads else
    // we could have a deadlock
    synchronized (this.writeSync)
    {
      synchronized (this.readSync)
      {
        if (pendingReads == 0)
        { // No read pending, go ahead
          if (logger.isDebugEnabled())
            logger.debug("Stored procedure "
                + proc.getId()
                + (proc.isAutoCommit() ? "" : " transaction "
                    + proc.getTransactionId()) + " scheduled for write ("
                + getPendingWrites() + " pending writes)");
          return;
        }
      }
    }

    waitForReadCompletion(proc);
    scheduleNonSuspendedStoredProcedure(proc);
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyStoredProcedureCompleted(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final void notifyStoredProcedureCompleted(StoredProcedure proc)
  {
    synchronized (this.writeSync)
    {
      if (logger.isDebugEnabled())
        logger.debug("Stored procedure " + proc.getId() + " completed - "
            + getPendingWrites() + " pending writes");
      if (getPendingWrites() == 0)
      {
        if (logger.isDebugEnabled())
          logger.debug("Last write completed, notifying reads");
        writeSync.notifyAll(); // Wakes up all waiting read queries
      }
    }
  }

  /**
   * Wait for the reads completion. Synchronizes on this.readSync.
   * 
   * @param request the request that is being scheduled
   * @throws SQLException if an error occurs
   */
  private void waitForReadCompletion(AbstractRequest request)
      throws SQLException
  {
    synchronized (this.readSync)
    {
      // Wait for the reads completion
      try
      {
        if (logger.isDebugEnabled())
          logger.debug("Request " + request.getId() + " waiting for "
              + pendingReads + " pending reads)");

        int timeout = request.getTimeout();
        if (timeout > 0)
        {
          long start = System.currentTimeMillis();
          // Convert seconds to milliseconds for wait call
          long lTimeout = timeout * 1000L;
          this.readSync.wait(lTimeout);
          long end = System.currentTimeMillis();
          int remaining = (int) (lTimeout - (end - start));
          if (remaining > 0)
            request.setTimeout(remaining);
          else
          {
            String msg = "Timeout (" + request.getTimeout() + ") for request: "
                + request.getId();
            logger.warn(msg);
            throw new SQLException(msg);
          }
        }
        else
          this.readSync.wait();
      }
      catch (InterruptedException e)
      {
        // Timeout
        if (logger.isWarnEnabled())
          logger.warn("Request " + request.getId() + " timed out ("
              + request.getTimeout() + " s)");
        throw new SQLException("Timeout (" + request.getTimeout()
            + ") for request: " + request.getId());
      }
    }
  }

  //
  // Transaction Management
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#commitTransaction(long)
   */
  protected final void commitTransaction(long transactionId)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#rollbackTransaction(long)
   */
  protected final void rollbackTransaction(long transactionId)
  {
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
        + DatabasesXmlTags.ATT_level + "=\"" + DatabasesXmlTags.VAL_query
        + "\"/>";
  }
}
