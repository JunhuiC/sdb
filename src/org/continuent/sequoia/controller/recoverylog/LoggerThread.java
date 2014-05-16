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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.recoverylog;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.recoverylog.events.LogEvent;
import org.continuent.sequoia.controller.recoverylog.events.LogRequestEvent;

/**
 * Logger thread for the RecoveryLog.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class LoggerThread extends Thread
{
  private boolean           killed       = false; // Control thread death
  /*
   * the only place where we must remove the first element of the queue is in
   * the run() method. do note remove its first element anywhere else!
   */
  private LinkedList        logQueue;
  private Trace             logger;
  private RecoveryLog       recoveryLog;
  private LogEvent          currentEvent = null;
  private LogEvent          lastFailed;

  /**
   * Creates a new <code>LoggerThread</code> object
   * 
   * @param log the RecoveryLog that instanciates this thread
   */
  public LoggerThread(RecoveryLog log)
  {
    super("LoggerThread");
    this.recoveryLog = log;
    this.logger = RecoveryLog.logger;
    logQueue = new LinkedList();
  }

  /**
   * Returns the logger value.
   * 
   * @return Returns the logger.
   */
  public Trace getLogger()
  {
    return logger;
  }

  /**
   * Tells whether there are pending logs
   * 
   * @return true if no more jobs in the log queue
   */
  public synchronized boolean getLogQueueIsEmpty()
  {
    if (logQueue.isEmpty())
    {
      // Notifies the Recovery log that the queue is empty.
      notify();
      return true;
    }
    else
    {
      return false;
    }
  }

  /**
   * Returns the recoveryLog value.
   * 
   * @return Returns the recoveryLog.
   */
  public RecoveryLog getRecoveryLog()
  {
    return recoveryLog;
  }

  /**
   * Returns true if there is any log event in the queue that belongs to the
   * given transaction.
   * 
   * @param tid transaction id to look for
   * @return true if a log entry belongs to this transaction
   */
  public synchronized boolean hasLogEntryForTransaction(long tid)
  {
    for (Iterator iter = logQueue.iterator(); iter.hasNext();)
    {
      LogEvent logEvent = (LogEvent) iter.next();
      if (logEvent.belongToTransaction(tid))
        return true;
    }
    return false;
  }

  /**
   * Log a write-query into the recovery log. This posts the specified logObject
   * (query) into this loggerThread queue. The actual write to the recoverly-log
   * db is performed asynchronously by the thread.
   * 
   * @param logObject the log event to be processed
   */
  public synchronized void log(LogEvent logObject)
  {
    logQueue.addLast(logObject);
    notify();
  }

  /**
   * Put back a log entry at the head of the queue in case a problem happened
   * with this entry and we need to retry it right away.
   * 
   * @param event the event to be used next by the logger thread.
   * @param e exception causing the event to fail and to be retried
   */
  public synchronized void putBackAtHeadOfQueue(LogEvent event, Exception e)
  {
    if (lastFailed != event)
    {
      logQueue.addFirst(event);
      notify();
      lastFailed = event;
    }
    else
    {
      if (event instanceof LogRequestEvent)
      {
        logger
            .error("WARNING! Your recovery log is probably corrupted, you should perform a restore log operation");
        getRecoveryLog().isDirty = true;
      }
      logger.error("Logger thread was unable to log " + event.toString()
          + " because of " + e, e);
    }
  }

  /**
   * Remove all queries that have not been logged yet and belonging to the
   * specified transaction.
   * 
   * @param tid transaction id to rollback
   */
  public synchronized void removeQueriesOfTransactionFromQueue(long tid)
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("recovery.jdbc.loggerthread.removing", tid));
    Iterator iter = logQueue.iterator();
    // do not remove the first element of the queue
    // (must only be done by the run() method)
    if (iter.hasNext())
    {
      iter.next();
    }
    while (iter.hasNext())
    {
      LogEvent event = (LogEvent) iter.next();
      if (event.belongToTransaction(tid))
      {
        iter.remove();
      }
    }
  }

  /**
   * Remove a possibly empty transaction from the recovery log. This method
   * returns true if no entry or just a begin is found for that transaction. If
   * a begin was found it will be removed from the log.
   * 
   * @param transactionId the id of the transaction
   * @return true if the transaction was empty
   * @throws SQLException if an error occurs
   */
  public boolean removeEmptyTransaction(RecoveryLogConnectionManager manager, 
      long transactionId) throws SQLException
  {
    if (hasLogEntryForTransaction(transactionId))
      return false;

    PreparedStatement stmt = null;
    ResultSet rs = null;
    try
    {
      stmt = manager.getConnection().prepareStatement(
          "SELECT * FROM " + recoveryLog.getLogTableName()
              + " WHERE transaction_id=?");
      stmt.setLong(1, transactionId);
      rs = stmt.executeQuery();
      if (!rs.next())
        return true; // no entry for that transaction

      // Check if the first entry found is a begin
      String sql = rs.getString(recoveryLog.getLogTableSqlColumnName());
      if ((sql == null) || !sql.startsWith(RecoveryLog.BEGIN))
        return false;

      if (rs.next())
        return false; // multiple entries in this transaction

      rs.close();
      stmt.close();

      // There is a single BEGIN in the log for that transaction, remove it.
      stmt = manager.getConnection().prepareStatement(
          "DELETE FROM " + recoveryLog.getLogTableName()
              + " WHERE transaction_id=?");
      stmt.setLong(1, transactionId);
      stmt.executeUpdate();
      return true;
    }
    catch (SQLException e)
    {
      throw new SQLException(Translate.get(
          "recovery.jdbc.transaction.remove.failed", new String[]{
              String.valueOf(transactionId), e.getMessage()}));
    }
    finally
    {
      try
      {
        if (rs != null)
          rs.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Delete all entries from the CheckpointTable.
   * 
   * @throws SQLException if an error occurs
   */
  public void deleteCheckpointTable(RecoveryLogConnectionManager manager) 
      throws SQLException
  {
    // First delete from the checkpoint table
    PreparedStatement stmt = null;
    try
    {
      stmt = manager.getConnection().prepareStatement(
          "DELETE FROM " + recoveryLog.getCheckpointTableName());
      stmt.executeUpdate();
    }
    catch (SQLException e)
    {
      String msg = "Failed to delete checkpoint table";
      logger.warn(msg, e);
      throw new SQLException(msg);
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Store a checkpoint in the recovery log using the provided local log id.<br>
   * Moreover, in case of error, additionally closes and invalidates log and
   * unlog statements (internal) before calling
   * RecoveryLog#invalidateInternalConnection().
   * 
   * @param connection live recovery log connection
   * @param checkpointName checkpoint name to insert
   * @param checkpointLogId checkpoint log identifier
   * @throws SQLException if a database access error occurs
   * @see RecoveryLog#storeCheckpointWithLogEntry(String, CheckpointLogEntry)
   * @see #invalidateLogStatements()
   */
  public void storeCheckpointWithLogId(RecoveryLogConnectionManager manager, 
      String checkpointName, long checkpointLogId) throws SQLException
  {
    PreparedStatement stmt = null;
    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Storing checkpoint " + checkpointName + " at request id "
            + checkpointLogId);
      stmt = manager.getConnection().prepareStatement(
          "INSERT INTO " + recoveryLog.getCheckpointTableName()
              + " VALUES(?,?)");
      stmt.setString(1, checkpointName);
      stmt.setLong(2, checkpointLogId);
      stmt.executeUpdate();
      logger.warn(Translate.get("recovery.checkpoint.stored", checkpointName));
    }
    catch (SQLException e)
    {
      manager.invalidate();
      logger.error(Translate.get(
          "recovery.jdbc.checkpoint.store.failed", new String[]{checkpointName,
              e.getMessage()}) + " : " + e);
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.store.failed", new String[]{checkpointName,
              e.getMessage()}));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Remove a checkpoint in the recovery log.<br />
   * In case of error, additionely close and invalidates log and unlog
   * statements (internal) before calling
   * RecoveryLog#invalidateInternalConnection().
   * 
   * @param checkpointName name of the checkpoint to remove
   * @throws SQLException if a database access error occurs
   * @see org.continuent.sequoia.controller.recoverylog.events.RemoveCheckpointEvent
   */
  public void removeCheckpoint(RecoveryLogConnectionManager manager, 
      String checkpointName) throws SQLException
  {
    PreparedStatement stmt = null;

    try
    {
      stmt = manager.getConnection().prepareStatement(
          "DELETE FROM " + recoveryLog.getCheckpointTableName()
              + " WHERE name like ?");
      stmt.setString(1, checkpointName);
      stmt.executeUpdate();
      stmt.close();
    }
    catch (SQLException e)
    {
      throw new SQLException(Translate.get(
          "recovery.jdbc.checkpoint.remove.failed", new String[]{
              checkpointName, e.getMessage()}));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Delete all LogEntries with an identifier lower than oldId (inclusive).
   * oldId is normally derived from a checkpoint name, which marks the last
   * request before the checkpoint.
   * 
   * @param oldId the id up to which entries should be removed.
   * @throws SQLException if an error occurs
   */
  public void deleteLogEntriesBeforeId(RecoveryLogConnectionManager manager, 
      long oldId) throws SQLException
  {
    PreparedStatement stmt = null;
    try
    {
      stmt = manager.getConnection().prepareStatement(
          "DELETE FROM " + recoveryLog.getLogTableName() + " WHERE log_id<=?");
      stmt.setLong(1, oldId);
      stmt.executeUpdate();
    }
    catch (SQLException e)
    {
      // TODO: Check error message below
      throw new SQLException(Translate.get(
          "recovery.jdbc.transaction.remove.failed", new String[]{
              String.valueOf(oldId), e.getMessage()}));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Return the real number of log entries between 2 log ids (usually matching
   * checkpoint indices). The SELECT excludes both boundaries.
   * 
   * @param lowerLogId the lower log id
   * @param upperLogId the upper log id
   * @return the number of entries between the 2 ids
   * @throws SQLException if an error occurs querying the recovery log
   */
  public long getNumberOfLogEntries(RecoveryLogConnectionManager manager, 
      long lowerLogId, long upperLogId) throws SQLException
  {
    ResultSet rs = null;
    PreparedStatement stmt = null;
    try
    {
      stmt = manager.getConnection().prepareStatement(
          "SELECT COUNT(*) FROM " + recoveryLog.getLogTableName()
              + " WHERE log_id>? AND log_id<?");
      // Note that the statement is closed in the finally block
      stmt.setLong(1, lowerLogId);
      stmt.setLong(2, upperLogId);
      rs = stmt.executeQuery();
      if (!rs.next())
        throw new SQLException(
            "Failed to retrieve number of log entries (no rows returned)");

      return rs.getLong(1);
    }
    catch (SQLException e)
    {
      throw e;
    }
    finally
    {
      try
      {
        if (rs != null)
          rs.close();
      }
      catch (Exception ignore)
      {
      }
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Shift LogEntries identifiers from the specified value (value is added to
   * existing identifiers).
   * 
   * @param shiftValue the value to shift
   * @throws SQLException if an error occurs
   */
  public void shiftLogEntriesIds(RecoveryLogConnectionManager manager, 
      long shiftValue) throws SQLException
  {
    PreparedStatement stmt = null;
    try
    {
      stmt = manager.getConnection().prepareStatement(
          "UPDATE " + recoveryLog.getLogTableName() + " SET log_id=log_id+?");
      stmt.setLong(1, shiftValue);
      stmt.executeUpdate();
    }
    catch (SQLException e)
    {
      throw new SQLException(Translate.get(
          "recovery.jdbc.loggerthread.shift.failed", e.getMessage()));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Shift LogEntries identifiers from the specified shiftValue (value is added
   * to existing identifiers) starting with identifier with a value strictly
   * greater than the given id.
   * 
   * @param fromId id to start shifting from
   * @param shiftValue the value to shift
   * @throws SQLException if an error occurs
   */
  public void shiftLogEntriesAfterId(RecoveryLogConnectionManager manager, 
      long fromId, long shiftValue) throws SQLException
  {
    PreparedStatement stmt = null;
    try
    {
      stmt = manager.getConnection().prepareStatement(
          "UPDATE " + recoveryLog.getLogTableName()
              + " SET log_id=log_id+? WHERE log_id>?");
      stmt.setLong(1, shiftValue);
      stmt.setLong(2, fromId);
      stmt.executeUpdate();
    }
    catch (SQLException e)
    {
      throw new SQLException(Translate.get(
          "recovery.jdbc.loggerthread.shift.failed", e.getMessage()));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Delete all log entries that have an id strictly between the 2 given
   * boundaries (commonCheckpointId<id<nowCheckpointId). All checkpoints
   * pointing to an id in the wiped zone will be deleted as well.
   * 
   * @param commonCheckpointId lower id bound
   * @param nowCheckpointId upper id bound
   * @throws SQLException if an error occurs accessing the log
   */
  public void deleteLogEntriesAndCheckpointBetween(RecoveryLogConnectionManager manager, 
      long commonCheckpointId, long nowCheckpointId) throws SQLException
  {
    PreparedStatement stmt = null;
    try
    {
      // Delete log entries first
      stmt = manager.getConnection().prepareStatement(
          "DELETE FROM " + recoveryLog.getLogTableName()
              + " WHERE ?<log_id AND log_id<?");
      stmt.setLong(1, commonCheckpointId);
      stmt.setLong(2, nowCheckpointId);
      int rows = stmt.executeUpdate();
      stmt.close();

      if (logger.isInfoEnabled())
      {
        logger.info(rows
            + " outdated log entries have been removed from the recovery log");

        // Print checkpoints that will be deleted
        stmt = manager.getConnection().prepareStatement(
            "SELECT * FROM " + recoveryLog.getCheckpointTableName()
                + " WHERE ?<log_id AND log_id<?");
        stmt.setLong(1, commonCheckpointId);
        stmt.setLong(2, nowCheckpointId);
        ResultSet rs = stmt.executeQuery();
        while (rs.next())
        {
          logger.info("Checkpoint " + rs.getString(1) + " (" + rs.getLong(2)
              + ") will be deleted.");
        }
        if (rs != null)
          rs.close();
        stmt.close();
      }

      // Now delete checkpoints
      stmt = manager.getConnection().prepareStatement(
          "DELETE FROM " + recoveryLog.getCheckpointTableName()
              + " WHERE ?<log_id AND log_id<?");
      stmt.setLong(1, commonCheckpointId);
      stmt.setLong(2, nowCheckpointId);
      rows = stmt.executeUpdate();

      if (logger.isInfoEnabled())
        logger
            .info(rows
                + " out of sync checkpoints have been removed from the recovery log");

    }
    catch (SQLException e)
    {
      throw new SQLException(Translate.get(
          "recovery.jdbc.entries.remove.failed", e.getMessage()));
    }
    finally
    {
      try
      {
        if (stmt != null)
          stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Log the requests from queue until the thread is explicitly killed. The
   * logger used is the one of the RecoveryLog.
   */
  public void run()
  {
    // Create a connection manager for all recovery log entries. 
    RecoveryLogConnectionManager manager = recoveryLog.getRecoveryLogConnectionManager();
    
    while (!killed)
    {
      synchronized (this)
      {
        while (getLogQueueIsEmpty() && !killed)
        {
          try
          {
            wait();
          }
          catch (InterruptedException e)
          {
            logger.warn(Translate.get("recovery.jdbc.loggerthread.awaken"), e);
          }
        }
        if (killed)
          break;
        // Pump first log entry from the queue but leave it in the queue to show
        // that we are processing it
        currentEvent = (LogEvent) logQueue.getFirst();
      }
      try
      {
        // Execute event, passing in connection. 
        currentEvent.execute(this, manager);
      }
      finally
      { // Remove from the queue anyway
        synchronized (this)
        {
          logQueue.removeFirst();
        }
      }
    }

    // Ensure that the log is empty. 
    int finalLogSize = logQueue.size();
    if (finalLogSize > 0)
    {
      logger.warn("Log queue contains requests following shutdown: " 
          + finalLogSize);
    }
    
    // Close the connection manager. 
    manager.invalidate();
    logger.info("Logger thread ending: " + this.getName());
  }

  /**
   * Shutdown the current thread. This will cause the log to terminate as soon
   * as the current event is finished processing. Any remaining events in the
   * log queue will be discarded.
   */
  public synchronized void shutdown()
  {
    killed = true;
    logger.info("Log shutdown method has been invoked");
    notify();
  }

}
