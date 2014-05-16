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
import java.util.ArrayList;

import org.continuent.sequoia.common.exceptions.RollbackException;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.scheduler.schema.SchedulerDatabaseSchema;
import org.continuent.sequoia.controller.scheduler.schema.SchedulerDatabaseTable;
import org.continuent.sequoia.controller.scheduler.schema.TransactionExclusiveLock;

/**
 * This scheduler provides transaction level scheduling for RAIDb-1 controllers.
 * Each write takes a lock on the table it affects. All following writes are
 * blocked until the transaction of the first write completes. This scheduler
 * automatically detects simple deadlocks and rollbacks the transaction inducing
 * the deadlock. Note that transitive deadlocks (involving more than 2 tables
 * are not detected).
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 * @deprecated since Sequoia 2.2
 */
public class RAIDb1OptimisticTransactionLevelScheduler
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

  private SchedulerDatabaseSchema schedulerDatabaseSchema = null;

  //
  // Constructor
  //

  /**
   * Creates a new Optimistic Transaction Level Scheduler
   */
  public RAIDb1OptimisticTransactionLevelScheduler()
  {
    super(RAIDbLevels.RAIDb1, ParsingGranularities.TABLE);
  }

  //
  // Request Handling
  //

  /**
   * Sets the <code>DatabaseSchema</code> of the current virtual database.
   * This is only needed by some schedulers that will have to define their own
   * scheduler schema
   * 
   * @param dbs a <code>DatabaseSchema</code> value
   * @see org.continuent.sequoia.controller.scheduler.schema.SchedulerDatabaseSchema
   */
  public synchronized void setDatabaseSchema(DatabaseSchema dbs)
  {
    if (schedulerDatabaseSchema == null)
    {
      logger.info("Setting new database schema");
      schedulerDatabaseSchema = new SchedulerDatabaseSchema(dbs);
    }
    else
    { // Schema is updated, compute the diff !
      SchedulerDatabaseSchema newSchema = new SchedulerDatabaseSchema(dbs);
      ArrayList tables = schedulerDatabaseSchema.getTables();
      ArrayList newTables = newSchema.getTables();
      if (newTables == null)
      { // New schema is empty (no backend is active anymore)
        logger.info("Removing all tables.");
        schedulerDatabaseSchema = null;
        return;
      }

      // Remove extra-tables
      for (int i = 0; i < tables.size(); i++)
      {
        SchedulerDatabaseTable t = (SchedulerDatabaseTable) tables.get(i);
        if (!newSchema.hasTable(t.getName()))
        {
          schedulerDatabaseSchema.removeTable(t);
          if (logger.isInfoEnabled())
            logger.info("Removing table " + t.getName());
        }
      }

      // Add missing tables
      int size = newTables.size();
      for (int i = 0; i < size; i++)
      {
        SchedulerDatabaseTable t = (SchedulerDatabaseTable) newTables.get(i);
        if (!schedulerDatabaseSchema.hasTable(t.getName()))
        {
          schedulerDatabaseSchema.addTable(t);
          if (logger.isInfoEnabled())
            logger.info("Adding table " + t.getName());
        }
      }
    }
  }

  /**
   * Merge the given <code>DatabaseSchema</code> with the current one.
   * 
   * @param dbs a <code>DatabaseSchema</code> value
   * @see org.continuent.sequoia.controller.scheduler.schema.SchedulerDatabaseSchema
   */
  public void mergeDatabaseSchema(DatabaseSchema dbs)
  {
    try
    {
      logger.info("Merging new database schema");
      schedulerDatabaseSchema.mergeSchema(new SchedulerDatabaseSchema(dbs));
    }
    catch (Exception e)
    {
      logger.error("Error while merging new database schema", e);
    }
  }

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
      throws SQLException, RollbackException
  {
    if (request.isCreate())
    {
      return;
    }

    SchedulerDatabaseTable t = schedulerDatabaseSchema.getTable(request
        .getTableName());
    if (t == null)
    {
      String msg = "No table found for request " + request.getId();
      logger.error(msg);
      throw new SQLException(msg);
    }

    // Deadlock detection
    TransactionExclusiveLock tableLock = t.getLock();
    if (!request.isAutoCommit())
    {
      synchronized (this)
      {
        if (tableLock.isLocked())
        { // Is the lock owner blocked by a lock we already own?
          long owner = tableLock.getLocker();
          long us = request.getTransactionId();
          if (owner != us)
          { // Parse all tables
            ArrayList tables = schedulerDatabaseSchema.getTables();
            ArrayList weAreblocking = new ArrayList();
            int size = tables.size();
            for (int i = 0; i < size; i++)
            {
              SchedulerDatabaseTable table = (SchedulerDatabaseTable) tables
                  .get(i);
              if (table == null)
                continue;
              TransactionExclusiveLock lock = table.getLock();
              // Are we the lock owner ?
              if (lock.isLocked())
              {
                if (lock.getLocker() == us)
                {
                  // Is 'owner' in the list of the blocked transactions?
                  if (lock.isWaiting(owner))
                  { // Deadlock detected, we must rollback
                    releaseLocks(us);
                    throw new RollbackException(
                        "Deadlock detected, rollbacking transaction " + us);
                  }
                  else
                    weAreblocking.addAll(lock.getWaitingList());
                }
              }
            }
          }
          else
          { // We are the lock owner
            return;
          }
        }
        else
        { // Lock is free, take it in the synchronized block
          acquireLockAndSetRequestId(request, tableLock);
          return;
        }
      }
    }

    acquireLockAndSetRequestId(request, tableLock);
  }

  private void acquireLockAndSetRequestId(AbstractWriteRequest request,
      TransactionExclusiveLock tableLock) throws SQLException
  {
    // Acquire the lock
    if (tableLock.acquire(request))
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
    if (request.isCreate())
    { // Add table to schema
      if (logger.isDebugEnabled())
        logger.debug("Adding table '" + request.getTableName()
            + "' to scheduler schema");
      synchronized (this)
      {
        schedulerDatabaseSchema.addTable(new SchedulerDatabaseTable(
            new DatabaseTable(request.getTableName())));
      }
    }
    else if (request.isDrop())
    { // Drop table from schema
      if (logger.isDebugEnabled())
        logger.debug("Removing table '" + request.getTableName()
            + "' to scheduler schema");
      synchronized (this)
      {
        schedulerDatabaseSchema.removeTable(schedulerDatabaseSchema
            .getTable(request.getTableName()));
      }
      return;
    }

    // Requests outside transaction delimiters must release the lock
    // as soon as they have executed
    if (request.isAutoCommit())
    {
      SchedulerDatabaseTable t = schedulerDatabaseSchema.getTable(request
          .getTableName());
      if (t == null)
      {
        String msg = "No table found to release lock for request "
            + request.getId();
        logger.error(msg);
      }
      else
        t.getLock().release();
    }
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedStoredProcedure(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public void scheduleNonSuspendedStoredProcedure(StoredProcedure proc)
      throws SQLException, RollbackException
  {
    throw new SQLException(
        "Stored procedures are not supported by the RAIDb-1 optimistic transaction level scheduler.");
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyStoredProcedureCompleted(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public void notifyStoredProcedureCompleted(StoredProcedure proc)
  {
    // We should never execute here since scheduleNonSuspendedStoredProcedure
    // should have failed prior calling us
    throw new RuntimeException(
        "Stored procedures are not supported by the RAIDb-1 optimistic transaction level scheduler.");
  }

  //
  // Transaction Management
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#commitTransaction(long)
   */
  protected final void commitTransaction(long transactionId)
  {
    releaseLocks(transactionId);
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#rollbackTransaction(long)
   */
  protected final void rollbackTransaction(long transactionId)
  {
    releaseLocks(transactionId);
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
   * Release all locks we may own on tables.
   * 
   * @param transactionId id of the transaction that releases the locks
   */
  private synchronized void releaseLocks(long transactionId)
  {
    ArrayList tables = schedulerDatabaseSchema.getTables();
    int size = tables.size();
    for (int i = 0; i < size; i++)
    {
      SchedulerDatabaseTable t = (SchedulerDatabaseTable) tables.get(i);
      if (t == null)
        continue;
      TransactionExclusiveLock lock = t.getLock();
      // Are we the lock owner ?
      if (lock.isLocked())
        if (lock.getLocker() == transactionId)
          lock.release();
    }
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
        + DatabasesXmlTags.VAL_optimisticTransaction + "\"/>";
  }
}
