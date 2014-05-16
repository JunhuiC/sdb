/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Initial developer(s): Nicolas Modrzyk. 
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.core.shutdown;

import java.sql.SQLException;
import java.util.ArrayList;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;

/**
 * Abstract class for all implementations of virtual database shutdown
 * strategies.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public abstract class VirtualDatabaseShutdownThread extends ShutdownThread
{
  protected VirtualDatabase virtualDatabase;

  /**
   * Prepare the thread for shutting down.
   * 
   * @param vdb the database to shutdown
   * @param level Constants.SHUTDOWN_WAIT, Constants.SHUTDOWN_SAFE or
   *          Constants.SHUTDOWN_FORCE
   */
  public VirtualDatabaseShutdownThread(VirtualDatabase vdb, int level)
  {
    super(level);
    this.virtualDatabase = vdb;
  }

  /**
   * Shutdown the result cache, recovery log and close the distributed virtual
   * database group communication channel (if the virtual database is
   * distributed).
   */
  protected void shutdownCacheRecoveryLogAndGroupCommunication()
  {
    // Shutdown the result cache
    AbstractResultCache resultCache = virtualDatabase.getRequestManager()
        .getResultCache();
    if (resultCache != null)
      resultCache.shutdown();

    boolean lastManDown = true;
    if (virtualDatabase.isDistributed())
    {
      logger.info("Shutting down group communication");
      DistributedVirtualDatabase dvdb = (DistributedVirtualDatabase) virtualDatabase;
      try
      {
        lastManDown = dvdb.getCurrentGroup().getMembers().size() == 1;
        dvdb.quitChannel(shutdownLevel);
      }
      catch (Exception e)
      {
        logger
            .warn(
                "An error occured while shutting down the group communication channel",
                e);
      }
    }

    // Shutdown the recovery log
    RecoveryLog recoveryLog = virtualDatabase.getRequestManager()
        .getRecoveryLog();
    if (recoveryLog != null)
    {
      if (lastManDown)
      {        
        try
        {
          recoveryLog.setLastManDown();
        }
        catch (SQLException e)
        {
          logger.error("Failed to store last-man-down flag.");
        }
      }
      // Make the recovery log not accessible so that new messages are not sent
      // to it anymore.
      virtualDatabase.getRequestManager().setRecoveryLog(null);
      // Shut it down
      recoveryLog.shutdown();
    }
  }

  /**
   * Disable all database backends with a checkpoint named after the current
   * time if a recovery log is available.
   */
  protected void disableAllBackendsWithCheckpoint()
  {
    if (virtualDatabase.getRequestManager().getRecoveryLog() != null)
    {
      try
      { // disable and checkpoint for recovery log
        virtualDatabase.disableAllBackendsWithCheckpoint(virtualDatabase
            .buildCheckpointName("disable all backends"));
      }
      catch (Exception ve)
      {
        logger.error(Translate
            .get("controller.shutdown.backends.exception", ve));
      }
      finally
      {
        virtualDatabase.storeBackendsInfo();
      }
    }
    else
    { // no recovery log, so just disable backends
      disableAllBackendsWithoutCheckpoint();
    }
  }

  /**
   * Disable all backends in force mode without setting a checkpoint.
   */
  protected void disableAllBackendsWithoutCheckpoint()
  {
    try
    {
      virtualDatabase.disableAllBackends(true);
    }
    catch (Exception vde)
    {
      logger
          .error(Translate.get("controller.shutdown.backends.exception", vde));
    }
  }

  /**
   * Terminate the VirtualDatabaseWorkerThreads. We actually wait for the
   * threads to be terminated.
   */
  protected void terminateVirtualDatabaseWorkerThreads()
  {
    ArrayList idleThreads = virtualDatabase.getPendingConnections();

    // Kill inactive threads first. As virtual database is shutting down, no new
    // thread will be created
    synchronized (idleThreads)
    {
      virtualDatabase.setPoolConnectionThreads(false);
      idleThreads.notifyAll();
    }

    ArrayList threads = virtualDatabase.getActiveThreads();
    logger.info(Translate.get("controller.shutdown.active.threads", threads
        .size()));

    boolean retry;
    do
    {
      synchronized (threads)
      {
        // Kill active threads
        for (int i = 0; i < threads.size(); i++)
        {
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("controller.shutdown.database.thread",
                new String[]{virtualDatabase.getVirtualDatabaseName(),
                    String.valueOf(i)}));
          ((VirtualDatabaseWorkerThread) threads.get(i)).shutdown();
        }
      }
      synchronized (threads)
      {
        retry = threads.size() > 0;
        if (retry)
        {
          try
          {
            threads.wait(100);
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
          }
        }
      }
    }
    while (retry);
  }

  /**
   * Wait for all VirtualDatabaseWorkerThreads to terminate when all clients
   * have disconnected.
   */
  protected void waitForClientsToDisconnect()
  {
    boolean wait = true;
    ArrayList threads = virtualDatabase.getActiveThreads();
    while (wait)
    {
      synchronized (threads)
      {
        int nbThreads = threads.size();
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("controller.shutdown.active.threads",
              nbThreads));
        if (nbThreads == 0)
          wait = false;
      }
      if (wait)
      {
        synchronized (this)
        {
          try
          {
            wait(1000);
          }
          catch (InterruptedException e)
          {
            // Ignore
          }
        }
      }
    }
  }

}