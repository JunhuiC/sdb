/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2007 Continuent, Inc.
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
 * Contributor(s): Nicolas Modrzyk, Jean-Bernard van Zuylen, Damian Arregui.
 * Refactored by Marc Herbert to remove the use of Java serialization.
 */

package org.continuent.sequoia.controller.virtualdatabase;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.net.SocketException;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.continuent.sequoia.common.exceptions.BadJDBCApiUsageException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoMoreControllerException;
import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.exceptions.ProtocolException;
import org.continuent.sequoia.common.exceptions.VDBisShuttingDownException;
import org.continuent.sequoia.common.exceptions.driver.protocol.BackendDriverException;
import org.continuent.sequoia.common.exceptions.driver.protocol.ControllerCoreException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.protocol.Commands;
import org.continuent.sequoia.common.protocol.SQLDataSerialization;
import org.continuent.sequoia.common.protocol.TypeTag;
import org.continuent.sequoia.common.protocol.SQLDataSerialization.Serializer;
import org.continuent.sequoia.common.sql.Request;
import org.continuent.sequoia.common.sql.RequestWithResultSetParameters;
import org.continuent.sequoia.common.sql.metadata.MetadataContainer;
import org.continuent.sequoia.common.sql.metadata.MetadataDescription;
import org.continuent.sequoia.common.sql.metadata.SequoiaParameterMetaData;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.controller.backend.result.AbstractResult;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.interceptors.impl.InterceptorManagerAdapter;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.events.LogEntry;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.RequestFactory;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.requests.StoredProcedureCallResult;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.driver.Connection;

/**
 * This class handles a connection with a Sequoia driver.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui
 * @author <a href="mailto:Gilles.Rayrat@continuent.com">Gilles Rayrat </a>
 * @version 2.0
 */
public class VirtualDatabaseWorkerThread extends Thread
{
  //
  // How the code is organized?
  //
  // 1. Member variables
  // 2. Constructor(s)
  // 3. Request management
  // 4. Getter/Setters

  /** <code>true</code> if this has been killed. */
  private boolean                    isKilled                = false;

  private long                       lastCommandTime         = System
                                                                 .currentTimeMillis();                         ;

  /** Virtual database instantiating this thread. */
  private VirtualDatabase            vdb;

  /** Logger instance. */
  private Trace                      logger                  = null;

  private DriverBufferedInputStream  in                      = null;
  private DriverBufferedOutputStream out                     = null;

  private VirtualDatabaseUser        user;

  private Controller                 controller;

  private boolean                    waitForCommand;

  private HashMap<String, AbstractResult>                    streamedResultSets;

  private RequestFactory             requestFactory          = ControllerConstants.CONTROLLER_FACTORY
                                                                 .getRequestFactory();
  /**
   * The following variables represent the state of the connection with the
   * client
   */
  private boolean                    persistentConnection;
  private long                       persistentConnectionId;
  private boolean                    connectionHasClosed;
  private boolean                    retrieveSQLWarnings;
  private long                       currentTid;
  private boolean                    transactionStarted;
  private boolean                    transactionHasAborted;
  private boolean                    transactionCompleted    = false;
  private boolean                    queryExecutedInThisTransaction;
  private boolean                    writeQueryExecutedInThisTransaction;
  // Number of savepoints in the current transaction
  private int                        hasSavepoint;
  private String                     clientIpAddress;
  private String                     login;
  private boolean                    closed;
  private int                        transactionIsolation    = Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL;
  private boolean                    isReadOnly              = false;
  private String                     connectionLineSeparator = null;

  /* end user logger */
  static Trace                       endUserLogger           = Trace
                                                                 .getLogger("org.continuent.sequoia.enduser");

  /*
   * Constructor
   */

  /**
   * Creates a new <code>VirtualDatabaseWorkerThread</code> instance.
   * 
   * @param controller the thread was originated from
   * @param vdb the virtual database instantiating this thread.
   */
  public VirtualDatabaseWorkerThread(Controller controller, VirtualDatabase vdb)
  {
    super("VirtualDatabaseWorkerThread-" + vdb.getVirtualDatabaseName());
    this.vdb = vdb;
    this.controller = controller;
    try
    {
      this.logger = Trace
          .getLogger("org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread."
              + vdb.getVirtualDatabaseName());
    }
    catch (Exception e)
    {
      this.logger = vdb.logger;
    }
  }

  //
  // Decoding commands from the stream
  //

  /**
   * Gets a connection from the connection queue and process it.
   */
  @SuppressWarnings("deprecation")
public void run()
  {
    ArrayList<VirtualDatabaseWorkerThread> vdbActiveThreads = vdb.getActiveThreads();
    ArrayList<?> vdbPendingQueue = vdb.getPendingConnections();
    // List of open ResultSets for streaming. This is not synchronized since the
    // connection does only handle one request at a time
    streamedResultSets = new HashMap<String, AbstractResult>();
    boolean isActive = true;

    if (vdbActiveThreads == null)
    {
      logger
          .error("Got null active threads queue in VirtualDatabaseWorkerThread");
      isKilled = true;
    }
    if (vdbPendingQueue == null)
    {
      logger.error("Got null connection queue in VirtualDatabaseWorkerThread");
      isKilled = true;
    }

    // Main loop
    while (!isKilled)
    {
      // Get a connection from the pending queue
      synchronized (vdbPendingQueue)
      {
        while (vdbPendingQueue.isEmpty())
        {
          if (!vdb.poolConnectionThreads)
          { // User does not want thread pooling, kill this thread!
            isKilled = true;
            break;
          }
          boolean timeout = false;
          try
          {
            if (isActive)
            {
              isActive = false;
              // Remove ourselves from the active thread list
              synchronized (vdbActiveThreads)
              {
                vdbActiveThreads.remove(this);
                vdb.incrementIdleThreadCount();
              }
            }
            long before = System.currentTimeMillis();
            vdbPendingQueue.wait(vdb.getMaxThreadIdleTime());
            long now = System.currentTimeMillis();
            // Check if timeout has expired
            timeout = now - before >= vdb.getMaxThreadIdleTime();
          }
          catch (InterruptedException e)
          {
            logger.warn("VirtualDatabaseWorkerThread wait() interrupted");
          }
          if (timeout && vdbPendingQueue.isEmpty())
          {
            if (vdb.currentNbOfThreads > vdb.minNbOfThreads)
            { // We have enough threads, kill this one
              isKilled = true;
              break;
            }
          }
        }

        if (isKilled)
        { // Cleaning up
          synchronized (vdbActiveThreads)
          { // Remove ourselves from the appropriate thread list
            if (isActive)
            {
              vdbActiveThreads.remove(this);
              vdb.decreaseCurrentNbOfThread();
            }
            else
              vdb.decreaseIdleThread();
          }
          // Get out of the while loop
          continue;
        }

        // Get a connection
        try
        {
          in = (DriverBufferedInputStream) vdbPendingQueue.remove(0);
          out = (DriverBufferedOutputStream) vdbPendingQueue.remove(0);
        }
        catch (Exception e)
        {
          logger.error("Error while getting streams from connection");
          continue;
        }

        synchronized (vdbActiveThreads)
        {
          if (!isActive)
          {
            vdb.decreaseIdleThread();
            isActive = true;
            // Add this thread to the active thread list
            vdbActiveThreads.add(this);
          }
        }
      }

      closed = false;

      // Handle connection
      // Read the user information and check authentication
      /**
       * @see org.continuent.sequoia.driver.Driver#connectToController(Properties,
       *      SequoiaUrl, ControllerInfo)
       */
      boolean success = false;
      try
      {
        login = in.readLongUTF();
        String password = in.readLongUTF();
        user = new VirtualDatabaseUser(login, password);

        // Pre-check for transparent login
        if (vdb.getAuthenticationManager().isTransparentLoginEnabled())
        {
          if (!vdb.getAuthenticationManager().isValidVirtualUser(user))
          {
            vdb.checkAndAddVirtualDatabaseUser(user);
          }
        }

        if (vdb.getAuthenticationManager().isValidVirtualUser(user))
        { // Authentication ok
          out.writeBoolean(true); // success code
          out.flush();
          success = true;
          try
          {
            clientIpAddress = in.getSocket().getInetAddress().toString();
          }
          catch (NullPointerException e) // no method above throws anything
          {
            clientIpAddress = "Unable to fetch client address";
          }

          if (logger.isDebugEnabled())
            logger.debug("Login accepted for " + login + " from "
                + clientIpAddress);

          connectionLineSeparator = in.readLongUTF();
          persistentConnection = in.readBoolean();
          if (persistentConnection)
          {
            persistentConnectionId = vdb.getNextConnectionId();
            try
            {
              vdb.openPersistentConnection(login, persistentConnectionId);
              out.writeBoolean(true);
              out.writeLong(persistentConnectionId);
              out.flush();
            }
            catch (SQLException e)
            {
              success = false;
              out.writeBoolean(false);
              out.flush();
              continue;
            }
          }
          retrieveSQLWarnings = in.readBoolean();
        }
        else
        { // Authentication failed, close the connection
          String msg = "Authentication failed for user '" + login + "'";
          out.writeBoolean(false); // authentication failed
          out.writeLongUTF(msg); // error message
          if (logger.isDebugEnabled())
            logger.debug(msg);
          endUserLogger.error(Translate.get(
              "virtualdatabase.authentication.failed", login));
          continue;
        }
      }
      catch (IOException e)
      {
        logger.error("I/O error during user authentication (" + e + ")");
        closed = true;
      }
      finally
      {
        if (!success)
        {
          try
          {
            out.close();
            in.close();
          }
          catch (IOException ignore)
          {
          }
        }
      }

      currentTid = 0;
      connectionHasClosed = false;
      transactionStarted = false;
      transactionHasAborted = false;
      transactionIsolation = Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL;
      queryExecutedInThisTransaction = false;
      writeQueryExecutedInThisTransaction = false;
      hasSavepoint = 0;

      int command = -1;
      while (!closed && !isKilled)
      {
        try
        {
          // Get the query
          waitForCommand = true;
          out.writeInt(Commands.ControllerPrompt);
          out.flush();
          if (Commands.CommandPrefix != in.readInt())
          {
            logger.error("Protocol corruption with client " + login
                + ", last command was:" + command + ". Closing.");
            // FIXME: because of the protocol corruption, this has very little
            // chance to actually close the connection. We need something more
            // rude here, like shutting down ourselves.
            command = Commands.Close;
          }
          else
          {
            try
            {
              command = in.readInt();
            }
            catch (NullPointerException e)
            {
              // SEQUOIA-777: this NPE happens when this thread is blocked
              // on in.read() and 'in' gets closed either explicitly
              // by the shutdown thread or by an unexpected client socket death.
              // The NPE is a known issue with jdk < 1.5, see SEQUOIA-777.
              // In the shutdown case, the flag isKilled is set
              // and we can exit the while loop. Otherwize, the client socket
              // died unexpectedly and we consider this is a CLOSE.
              if (isKilled)
                continue;
              logger.warn("Client unexpectedly dropped connection. Closing.");
              command = Commands.Close;
            }
          }

          waitForCommand = false;
          lastCommandTime = System.currentTimeMillis();

          // Process it
          switch (command)
          {
            case Commands.StatementExecuteQuery :
              statementExecuteQuery(null);
              break;
            case Commands.StatementExecuteUpdate :
              statementExecuteUpdate(null);
              break;
            case Commands.StatementExecuteUpdateWithKeys :
              statementExecuteUpdateWithKeys();
              break;
            case Commands.CallableStatementExecuteQuery :
              callableStatementExecuteQuery(null, false);
              break;
            case Commands.CallableStatementExecuteUpdate :
              callableStatementExecuteUpdate(null, false);
              break;
            case Commands.CallableStatementExecute :
              callableStatementExecute(null, false);
              break;
            case Commands.CallableStatementExecuteQueryWithParameters :
              callableStatementExecuteQuery(null, true);
              break;
            case Commands.CallableStatementExecuteUpdateWithParameters :
              callableStatementExecuteUpdate(null, true);
              break;
            case Commands.CallableStatementExecuteWithParameters :
              callableStatementExecute(null, true);
              break;
            case Commands.StatementExecute :
              statementExecute(null);
              break;
            case Commands.Begin :
              begin();
              break;
            case Commands.Commit :
              commit();
              break;
            case Commands.Rollback :
              rollback();
              break;
            case Commands.SetNamedSavepoint :
              setNamedSavepoint();
              break;
            case Commands.SetUnnamedSavepoint :
              setUnnamedSavepoint();
              break;
            case Commands.ReleaseSavepoint :
              releaseSavepoint();
              break;
            case Commands.RollbackToSavepoint :
              rollbackToSavepoint();
              break;
            case Commands.SetTransactionIsolation :
              connectionSetTransactionIsolation();
              break;
            case Commands.SetReadOnly :
              connectionSetReadOnly();
              break;
            case Commands.ConnectionGetWarnings :
              connectionGetWarnings();
              break;
            case Commands.ConnectionClearWarnings :
              connectionClearWarnings();
              break;
            case Commands.GetVirtualDatabaseName :
              getVirtualDatabaseName();
              break;
            case Commands.DatabaseMetaDataGetDatabaseProductName :
              databaseMetaDataGetDatabaseProductName();
              break;
            case Commands.GetControllerVersionNumber :
              getControllerVersionNumber();
              break;
            case Commands.DatabaseMetaDataGetTables :
              databaseMetaDataGetTables();
              break;
            case Commands.DatabaseMetaDataGetColumns :
              databaseMetaDataGetColumns();
              break;
            case Commands.DatabaseMetaDataGetPrimaryKeys :
              databaseMetaDataGetPrimaryKeys();
              break;
            case Commands.DatabaseMetaDataGetProcedures :
              databaseMetaDataGetProcedures();
              break;
            case Commands.DatabaseMetaDataGetProcedureColumns :
              databaseMetaDataGetProcedureColumns();
              break;
            case Commands.ConnectionGetCatalogs :
              connectionGetCatalogs();
              break;
            case Commands.ConnectionGetCatalog :
              connectionGetCatalog();
              break;
            case Commands.DatabaseMetaDataGetTableTypes :
              databaseMetaDataGetTableTypes();
              break;
            case Commands.DatabaseMetaDataGetSchemas :
              databaseMetaDataGetSchemas();
              break;
            case Commands.DatabaseMetaDataGetTablePrivileges :
              databaseMetaDataGetTablePrivileges();
              break;
            case Commands.DatabaseMetaDataGetAttributes :
              databaseMetaDataGetAttributes();
              break;
            case Commands.DatabaseMetaDataGetBestRowIdentifier :
              databaseMetaDataGetBestRowIdentifier();
              break;
            case Commands.DatabaseMetaDataGetColumnPrivileges :
              databaseMetaDataGetColumnPrivileges();
              break;
            case Commands.DatabaseMetaDataGetCrossReference :
              databaseMetaDataGetCrossReference();
              break;
            case Commands.DatabaseMetaDataGetExportedKeys :
              databaseMetaDataGetExportedKeys();
              break;
            case Commands.DatabaseMetaDataGetImportedKeys :
              databaseMetaDataGetImportedKeys();
              break;
            case Commands.DatabaseMetaDataGetIndexInfo :
              databaseMetaDataGetIndexInfo();
              break;
            case Commands.DatabaseMetaDataGetSuperTables :
              databaseMetaDataGetSuperTables();
              break;
            case Commands.DatabaseMetaDataGetSuperTypes :
              databaseMetaDataGetSuperTypes();
              break;
            case Commands.DatabaseMetaDataGetTypeInfo :
              databaseMetaDataGetTypeInfo();
              break;
            case Commands.DatabaseMetaDataGetUDTs :
              databaseMetaDataGetUDTs();
              break;
            case Commands.DatabaseMetaDataGetVersionColumns :
              databaseMetaDataGetVersionColumns();
              break;
            case Commands.PreparedStatementGetMetaData :
              preparedStatementGetMetaData();
              break;
            case Commands.PreparedStatementGetParameterMetaData :
              preparedStatementGetParameterMetaData();
              break;
            case Commands.ConnectionSetCatalog :
              connectionSetCatalog();
              break;
            case Commands.Close :
              close();
              break;
            case Commands.Reset :
              reset();
              break;
            case Commands.FetchNextResultSetRows :
              fetchNextResultSetRows();
              break;
            case Commands.CloseRemoteResultSet :
              closeRemoteResultSet();
              break;
            case Commands.DatabaseStaticMetadata :
              databaseStaticMetadata();
              break;
            case Commands.RestoreConnectionState :
              restoreConnectionState();
              break;
            case Commands.RetrieveExecuteQueryResult :
              retrieveExecuteQueryResult();
              break;
            case Commands.RetrieveExecuteResult :
              retrieveExecuteResult();
              break;
            case Commands.RetrieveExecuteUpdateResult :
              retrieveExecuteUpdateResult();
              break;
            case Commands.RetrieveExecuteUpdateWithKeysResult :
              retrieveExecuteUpdateWithKeysResult();
              break;
            case Commands.RetrieveExecuteQueryResultWithParameters :
              retrieveExecuteQueryResultWithParameters();
              break;
            case Commands.RetrieveExecuteUpdateResultWithParameters :
              retrieveExecuteUpdateResultWithParameters();
              break;
            case Commands.RetrieveExecuteResultWithParameters :
              retrieveExecuteResultWithParameters();
              break;
            case Commands.RetrieveCommitResult :
              retrieveCommitResult();
              break;
            case Commands.RetrieveRollbackResult :
              retrieveRollbackResult();
              break;
            case Commands.RetrieveReleaseSavepoint :
              retrieveReleaseSavepoint();
              break;
            default :
              String errorMsg = "Unsupported protocol command: " + command;
              logger.error(errorMsg);
              sendToDriver(new RuntimeException(errorMsg));
              break;
          }
        }
        catch (EOFException e)
        {
          logger.warn("Client (login:" + login + ",host:"
              + in.getSocket().getInetAddress().getHostName()
              + " closed connection with server)");
          closed = true;
        }
        catch (SocketException e)
        {
          // shutting down
          closed = true;
        }
        catch (IOException e)
        {
          closed = true;
          logger.warn("Closing connection with client " + login
              + " because of IOException.(" + e + ")");
        }
        catch (VDBisShuttingDownException e)
        {
          isKilled = true;
        }
        catch (SQLException e)
        {
          logger
              .info("Error during command execution (" + e.getMessage() + ")");
          if (transactionStarted && !transactionHasAborted)
          { // Failure of a query within a transaction automatically aborts the
            // transaction
            transactionHasAborted = (hasSavepoint == 0)
                && ((command == Commands.StatementExecuteUpdate)
                    || (command == Commands.StatementExecuteUpdateWithKeys)
                    || (command == Commands.StatementExecute)
                    || (command == Commands.CallableStatementExecuteWithParameters)
                    || (command == Commands.CallableStatementExecuteQueryWithParameters)
                    || (command == Commands.CallableStatementExecuteUpdateWithParameters)
                    || (command == Commands.CallableStatementExecuteQuery) || (command == Commands.CallableStatementExecuteUpdate));
          }
          try
          {
            sendToDriver(e);
          }
          catch (IOException ignore)
          {
          }
        }
        catch (BadJDBCApiUsageException e)
        {
          logger
              .warn("Error during command execution (" + e.getMessage() + ")");
          try
          {
            sendToDriver(e);
          }
          catch (IOException ignore)
          {
          }
        }
        catch (Throwable e)
        {
          logger.warn("Runtime error during command execution ("
              + e.getMessage() + ")", e);
          if (transactionStarted)
          { // Failure of a query within a transaction automatically aborts the
            // transaction
            transactionHasAborted = (hasSavepoint == 0)
                && ((command == Commands.StatementExecuteQuery)
                    || (command == Commands.StatementExecuteUpdate)
                    || (command == Commands.StatementExecuteUpdateWithKeys)
                    || (command == Commands.StatementExecute)
                    || (command == Commands.CallableStatementExecute)
                    || (command == Commands.CallableStatementExecuteQuery) || (command == Commands.CallableStatementExecuteUpdate));
          }
          try
          {
            sendToDriver((SQLException) new SQLException(e
                .getLocalizedMessage()).initCause(e));
          }
          catch (IOException ignore)
          {
          }
        }
      } // while (!closed && !isKilled) get and process command from driver

      // Do the cleanup
      if (!streamedResultSets.isEmpty())
      {
        for (Iterator<AbstractResult> iter = streamedResultSets.values().iterator(); iter
            .hasNext();)
        {
          ControllerResultSet crs = (ControllerResultSet) iter.next();
          crs.closeResultSet();
        }
        streamedResultSets.clear();
      }

      if (!isKilled)
      {
        // cleanup();
      }
      else
      {
        // FIXME: debug message for safe mode parallel shutdown of controllers
        // (note that parallel shutdown should be avoided)
        if (logger.isInfoEnabled())
        {
          logger
              .info("VirtualDatabaseWorkerThread killed by shutdown, no clean-up"
                  + " done. Number of pending transaction in scheduler: "
                  + vdb.getRequestManager().getScheduler()
                      .getPendingTransactions());
        }
      }

      // Close streams and underlying socket
      try
      {
        in.close();
      }
      catch (IOException ignore)
      {
      }
      try
      {
        out.close();
      }
      catch (IOException ignore)
      {
      }
    }

    synchronized (vdbActiveThreads)
    { // Remove ourselves from the appropriate thread list
      if (vdbActiveThreads.remove(this))
        vdb.decreaseCurrentNbOfThread();
    }

    if (logger.isDebugEnabled())
      logger.debug("VirtualDatabaseWorkerThread associated to login: "
          + this.getUser() + " terminating.");
  }

  private void close() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Close command");

    cleanup();

    sendToDriver(true);

    closed = true;
  }

  private void closeRemoteResultSet() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("CloseRemoteResultSet command");

    String cursor = in.readLongUTF();
    ControllerResultSet crsToClose = (ControllerResultSet) streamedResultSets
        .remove(cursor);
    if (crsToClose == null)
    {
      sendToDriver(new SQLException("No valid RemoteResultSet to close."));
    }
    else
    {
      crsToClose.closeResultSet();
      sendToDriver(true);
    }
  }

  private void reset() throws IOException
  {
    // The client application has closed the connection but it is kept
    // open in case the transparent connection pooling reuses it.
    if (logger.isDebugEnabled())
      logger.debug("Reset command");

    cleanup();

    connectionHasClosed = false;
    currentTid = 0;
    transactionStarted = false;
    transactionHasAborted = false;
    transactionIsolation = Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL;
    queryExecutedInThisTransaction = false;
    hasSavepoint = 0;
    sendToDriver(true);
  }

  public void cleanup()
  {
    // Abort transaction
    if (transactionStarted && !transactionHasAborted)
    {
      /*
       * We need to abort the begin to cleanup the metadata associated with the
       * started transaction.
       */
      if (logger.isDebugEnabled())
        logger.debug("Aborting transaction " + currentTid);
      try
      {
        vdb.abort(currentTid, writeQueryExecutedInThisTransaction, true);
      }
      catch (Exception e)
      {
        if (logger.isDebugEnabled())
          logger.debug("Error while aborting transaction " + currentTid + "("
              + e + ")", e);
      }
    }

    // Close persistent connection
    if (persistentConnection)
    {
      vdb.closePersistentConnection(login, persistentConnectionId);
    }
  }

  private void restoreConnectionState() throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("RestoreConnectionState command");

    // Re-connect has opened a new persistent connection that will not be used
    if (persistentConnection)
    {
      vdb.closePersistentConnection(login, persistentConnectionId);
    }

    writeQueryExecutedInThisTransaction = in.readBoolean();
    // We receive autocommit from driver
    transactionStarted = !in.readBoolean();
    if (transactionStarted)
      currentTid = in.readLong();
    persistentConnection = in.readBoolean();
    if (persistentConnection)
      persistentConnectionId = in.readLong();

    // Restore the persistent connection first (if any) before trying to perform
    // any operation on the transaction
    if (persistentConnection)
    {
      if (!vdb.hasPersistentConnection(persistentConnectionId))
      {
        RecoveryLog recoveryLog = vdb.getRequestManager().getRecoveryLog();
        if (!recoveryLog
            .findCloseForPersistentConnection(persistentConnectionId))
        {
          vdb.failoverForPersistentConnection(persistentConnectionId);
        }
        else
        {
          connectionHasClosed = true;
        }
      }
    }

    retrieveSQLWarnings = in.readBoolean();

    // Acknowledge driver
    out.writeBoolean(true);
    out.flush();

    if (transactionStarted)
    {
      try
      {
        // Check if the transaction exists here
        vdb.requestManager.getTransactionMetaData(new Long(currentTid));
        // Only notify failover if we have the transaction in our context
        vdb.failoverForTransaction(currentTid);
        /*
         * Transaction is started on this controller... it was either a
         * transaction that contained write statements, either a read-only
         * transaction with broadcasted statements, so we force
         * writeQueryExecutedInThisTransaction to true.
         */
        writeQueryExecutedInThisTransaction = true;
      }
      catch (SQLException e)
      {
        /*
         * Transaction has not been found because it either already
         * committed/rollbacked or it was not started (no request played so far
         * in the transaction or just read queries on the controller that has
         * failed). Check first if we can find a trace of commit/rollback in the
         * recovery log and if not start the transaction now. This is needed
         * only if it was a write transaction or a transaction with broadcasted
         * read requests.
         */
        transactionCompleted = false;
        RecoveryLog recoveryLog = vdb.getRequestManager().getRecoveryLog();
        if (writeQueryExecutedInThisTransaction)
        {
          if (!recoveryLog.findCommitForTransaction(currentTid)
              && !recoveryLog.findRollbackForTransaction(currentTid))
          {
            vdb.requestManager.doBegin(login, currentTid, persistentConnection,
                persistentConnectionId);
          }
          else
          {
            transactionCompleted = true;
          }
        }
        else
        {
          vdb.requestManager.doBegin(login, currentTid, persistentConnection,
              persistentConnectionId);
          writeQueryExecutedInThisTransaction = true;
        }
      }
    }
  }

  //
  // Catalog
  //

  private void connectionSetCatalog() throws IOException
  {
    // Warning! This could bypass the security checkings based on client IP
    // address. If a user has access to a virtual database, through setCatalog()
    // is will be able to access all other virtual databases where his
    // login/password is valid regardless of the IP filtering settings.
    if (logger.isDebugEnabled())
      logger.debug("ConnectionSetCatalog command");
    String catalog = in.readLongUTF();
    boolean change = controller.hasVirtualDatabase(catalog);
    if (change)
    {
      VirtualDatabase tempvdb = controller.getVirtualDatabase(catalog);
      if (!tempvdb.getAuthenticationManager().isValidVirtualUser(user))
        sendToDriver(new SQLException(
            "User authentication has failed for asked catalog. No change"));
      else
      {
        this.vdb = tempvdb;
        sendToDriver(true);
      }
    }
    else
      sendToDriver(false);

  }

  private void connectionGetCatalog() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("ConnectionGetCatalog command");

    sendToDriver(vdb.getVirtualDatabaseName());
  }

  private void connectionGetCatalogs() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("ConnectionGetCatalogs command");
    ArrayList<?> list = controller.getVirtualDatabaseNames();
    sendToDriver(vdb.getDynamicMetaData().getCatalogs(list));
  }

  private void connectionSetTransactionIsolation() throws IOException
  {
    int level = in.readInt();
    if (logger.isDebugEnabled())
      logger.debug("SetTransactionIsolation command (level=" + level + ")");

    // Check that we are not in a running transaction
    if (transactionStarted && queryExecutedInThisTransaction)
    {
      sendToDriver(new SQLException(
          "Cannot change the transaction isolation in a running transaction"));
      return;
    }

    MetadataContainer metadataContainer = vdb.getStaticMetaData()
        .getMetadataContainer();
    if (metadataContainer != null)
    {
      Object value = metadataContainer.get(MetadataContainer.getContainerKey(
          MetadataDescription.SUPPORTS_TRANSACTION_ISOLATION_LEVEL,
          new Class[]{Integer.TYPE}, new Object[]{new Integer(level)}));

      if (value != null)
      {
        if (!((Boolean) value).booleanValue())
        {
          sendToDriver(new SQLException("Transaction isolation level " + level
              + " is not supported by the database"));
          return;
        }
      }
      else
        logger.warn("Unable to check validity of transaction isolation level "
            + level);
    }
    else
      logger.warn("Unable to check validity of transaction isolation level "
          + level);
    transactionIsolation = level;
    sendToDriver(true);
  }

  private void connectionSetReadOnly() throws IOException
  {
    isReadOnly = in.readBoolean();
    if (logger.isDebugEnabled())
      logger.debug("SetReadOnly command (value=" + true + ")");

    sendToDriver(true);
  }

  private void connectionGetWarnings() throws IOException
  {
    long persistentConnId = in.readLong();
    try
    {
      sendToDriver(vdb.getConnectionWarnings(persistentConnId));
    }
    catch (SQLException e)
    {
      sendToDriver(e);
    }
  }

  private void connectionClearWarnings() throws IOException
  {
    long persistentConnId = in.readLong();
    try
    {
      vdb.clearConnectionWarnings(persistentConnId);
      sendToDriver(true);
    }
    catch (SQLException e)
    {
      sendToDriver(e);
    }
  }

  //
  // Database MetaData
  //

  /**
   * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetAttributes() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetAttributes command");
    String catalog = in.readLongUTF();
    String schemaPattern = in.readLongUTF();
    String typeNamePattern = in.readLongUTF();
    String attributeNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getAttributes(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog,
          schemaPattern, typeNamePattern, attributeNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetAttributes", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String,
   *      java.lang.String, java.lang.String, int, boolean)
   */
  private void databaseMetaDataGetBestRowIdentifier() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetBestRowIdentifier command");

    String catalog = in.readLongUTF();
    String schema = in.readLongUTF();
    String table = in.readLongUTF();
    int scope = in.readInt();
    boolean nullable = in.readBoolean();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getBestRowIdentifier(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog, schema,
          table, scope, nullable));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetBestRowIdentifier",
            e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetColumnPrivileges() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetColumnPrivileges command");

    String catalog = in.readLongUTF();
    String schema = in.readLongUTF();
    String table = in.readLongUTF();
    String columnNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getColumnPrivileges(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog, schema,
          table, columnNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetColumnPrivileges",
            e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getColumns(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetColumns() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetColumns command");
    String ccatalog = in.readLongUTF();
    String cschemaPattern = in.readLongUTF();
    String ctableNamePattern = in.readLongUTF();
    String ccolumnNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getColumns(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), ccatalog,
          cschemaPattern, ctableNamePattern, ccolumnNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetColumns", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetCrossReference() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetCrossReference command");

    String primaryCatalog = in.readLongUTF();
    String primarySchema = in.readLongUTF();
    String primaryTable = in.readLongUTF();
    String foreignCatalog = in.readLongUTF();
    String foreignSchema = in.readLongUTF();
    String foreignTable = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getCrossReference(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), primaryCatalog,
          primarySchema, primaryTable, foreignCatalog, foreignSchema,
          foreignTable));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetCrossReference", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getDatabaseProductName()
   */
  private void databaseMetaDataGetDatabaseProductName() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("GetDatabaseProductName command");

    sendToDriver(vdb.getDatabaseProductName());
  }

  /**
   * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetExportedKeys() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetExportedKeys command");

    String catalog = in.readLongUTF();
    String schema = in.readLongUTF();
    String table = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getExportedKeys(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog, schema,
          table));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetExportedKeys", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetImportedKeys() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetImportedKeys command");

    String catalog = in.readLongUTF();
    String schema = in.readLongUTF();
    String table = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getImportedKeys(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog, schema,
          table));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetImportedKeys", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String,
   *      java.lang.String, java.lang.String, boolean, boolean)
   */
  private void databaseMetaDataGetIndexInfo() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("databaseMetaDataGetIndexInfo command");

    String catalog = in.readLongUTF();
    String schema = in.readLongUTF();
    String table = in.readLongUTF();
    boolean unique = in.readBoolean();
    boolean approximate = in.readBoolean();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getIndexInfo(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog, schema,
          table, unique, approximate));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetIndexInfo", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetPrimaryKeys() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetPrimaryKeys command");

    String pcatalog = in.readLongUTF();
    String pschemaPattern = in.readLongUTF();
    String ptableNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getPrimaryKeys(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), pcatalog,
          pschemaPattern, ptableNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetPrimaryKeys", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getProcedureColumns(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetProcedureColumns() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetProcedureColumns command");

    String pccatalog = in.readLongUTF();
    String pcschemaPattern = in.readLongUTF();
    String pcprocedureNamePattern = in.readLongUTF();
    String pccolumnNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getProcedureColumns(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), pccatalog,
          pcschemaPattern, pcprocedureNamePattern, pccolumnNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetProcedureColumns",
            e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getProcedures(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetProcedures() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetProcedures command");

    String rcatalog = in.readLongUTF();
    String rschemaPattern = in.readLongUTF();
    String procedureNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getProcedures(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), rcatalog,
          rschemaPattern, procedureNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetProcedures", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSchemas()
   */
  private void databaseMetaDataGetSchemas() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetSchemas Types command");

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getSchemas(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId)));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetSchemas", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetSuperTables() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetSuperTables command");

    String catalog = in.readLongUTF();
    String schemaPattern = in.readLongUTF();
    String tableNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getSuperTables(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog,
          schemaPattern, tableNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetSuperTables", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetSuperTypes() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetSuperTables command");

    String catalog = in.readLongUTF();
    String schemaPattern = in.readLongUTF();
    String tableNamePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getSuperTypes(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog,
          schemaPattern, tableNamePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetSuperTypes", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getTablePrivileges(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetTablePrivileges() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetTablePrivileges command");

    String tpcatalog = in.readLongUTF();
    String tpschemaPattern = in.readLongUTF();
    String tptablePattern = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getTablePrivileges(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), tpcatalog,
          tpschemaPattern, tptablePattern));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger
            .warn("Error while calling databaseMetaDataGetTablePrivileges", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getTables(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String[])
   */
  private void databaseMetaDataGetTables() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetTables command");

    String tcatalog = in.readLongUTF();
    String tschemaPattern = in.readLongUTF();
    String ttableNamePattern = in.readLongUTF();

    String[] ttypes = null;
    if (in.readBoolean())
    {
      int size = in.readInt();
      ttypes = new String[size];
      for (int i = 0; i < size; i++)
        ttypes[i] = in.readLongUTF();
    }

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getTables(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), tcatalog,
          tschemaPattern, ttableNamePattern, ttypes));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetTables", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getTableTypes()
   */
  private void databaseMetaDataGetTableTypes() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetTableTypes command");

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getTableTypes(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId)));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetTableTypes", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getTypeInfo()
   */
  private void databaseMetaDataGetTypeInfo() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetTypeInfo command");

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getTypeInfo(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId)));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetTypeInfo", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String,
   *      java.lang.String, int[])
   */
  private void databaseMetaDataGetUDTs() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetUDTs command");

    String catalog = in.readLongUTF();
    String schemaPattern = in.readLongUTF();
    String tableNamePattern = in.readLongUTF();

    int[] types = null;
    if (in.readBoolean())
    {
      int size = in.readInt();
      types = new int[size];
      for (int i = 0; i < size; i++)
        types[i] = in.readInt();
    }

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getUDTs(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog,
          schemaPattern, tableNamePattern, types));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetUDTs", e);
      sendToDriver(e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  private void databaseMetaDataGetVersionColumns() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("DatabaseMetaDataGetVersionColumns command");

    String catalog = in.readLongUTF();
    String schema = in.readLongUTF();
    String table = in.readLongUTF();

    try
    {
      sendToDriver(vdb.getDynamicMetaData().getVersionColumns(
          new ConnectionContext(login, transactionStarted, currentTid,
              persistentConnection, persistentConnectionId), catalog, schema,
          table));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling databaseMetaDataGetVersionColumns", e);
      sendToDriver(e);
    }
  }

  /**
   * Get the static metadata key from the socket and return the corresponding
   * metadata.
   * 
   * @throws IOException if an IO error occurs
   * @throws NotImplementedException if the underlying metadata access method is
   *             not implemented
   */
  private void databaseStaticMetadata() throws IOException,
      NotImplementedException
  {
    // the "getXXX(Y,Z,...)" hash key of the metadata
    // query called by the client using the driver.
    String key = in.readLongUTF();
    if (logger.isDebugEnabled())
      logger.debug("DatabaseStaticMetadata command for " + key);
    MetadataContainer container = vdb.getStaticMetaData()
        .getMetadataContainer();
    if (container == null) // no metadata has been gathered yet from backends
    {
      String msg = "No metadata is available probably because no backend is enabled on that controller.";
      logger.info(msg);
      sendToDriver(new SQLException(msg));
    }
    else
    {
      /**
       * To get an exhaustive list of all the types of java objects stored in
       * this hash table, search for all callers of
       * {@link org.continuent.sequoia.driver.DatabaseMetaData#getMetadata(String, Class[], Object[], boolean)}
       * and see also
       * {@link org.continuent.sequoia.controller.backend.DatabaseBackendMetaData#retrieveDatabaseMetadata()}
       * At this time it's limited to the following types: String, int and
       * boolean. boolean is the most frequent.
       */
      /*
       * Since we don't expect that any of these metadata methods will ever
       * return a non- java.sql.Types, we re-use here the serialization
       * implemented for SQL Data/ResultSets elements.
       */

      SQLDataSerialization.Serializer serializer;
      Object result = container.get(key);

      try
      {
        serializer = SQLDataSerialization.getSerializer(result);
        // TODO: clean-up this.
        if (serializer.isUndefined()) // <=> result == null
          throw new NotImplementedException();
      }
      catch (NotImplementedException innerEx)
      { // Should we just print a warning in case result == null ?
        // This should never happen with decent drivers.
        String msg;
        if (null == result)
          msg = " returned a null object.";
        else
          msg = " returned an object of an unsupported java type:"
              + result.getClass().getName() + ".";

        NotImplementedException outerEx = new NotImplementedException(
            "Backend driver method " + key + msg);
        outerEx.initCause(innerEx);
        throw outerEx;
      }

      TypeTag.NOT_EXCEPTION.sendToStream(out);
      serializer.getTypeTag().sendToStream(out);
      serializer.sendToStream(result, out);
    }

    out.flush();
  }

  /**
   * Reads a SQL template from the stream and creates an
   * <code>AbstractRequest</code> from it<br>
   * 
   * @return an <code>UnknownWriteRequest</code> wrapped into an
   *         <code>AbstractRequest</code> containing the SQL template sent by
   *         the driver
   * @throws IOException upon driver communication failure
   * @throws SQLException if the current context does not allow request
   *             execution
   */
  private AbstractRequest createFakeRequestFromStream() throws IOException,
      SQLException
  {
    String sqlTemplate = in.readLongUTF();
    AbstractRequest request = new UnknownWriteRequest(sqlTemplate, false, 0, "");
    request.setIsAutoCommit(!transactionStarted);
    setRequestParametersAndTransactionStarted(request);
    return request;
  }

  private void preparedStatementGetMetaData() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("PreparedStatementGetMetaData command");

    try
    {
      AbstractRequest request = createFakeRequestFromStream();
      sendToDriver(vdb.getPreparedStatementGetMetaData(request));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn("Error while calling getPreparedStatementGetMetaData", e);
      sendToDriver(e);
    }
  }

  private void preparedStatementGetParameterMetaData() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("PreparedStatementGetParameterMetaData command");

    try
    {
      AbstractRequest request = createFakeRequestFromStream();
      sendToDriver(vdb.getPreparedStatementGetParameterMetaData(request));
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
        logger.warn(
            "Error while calling preparedStatementGetParameterMetaData", e);
      sendToDriver(e);
    }
  }

  private void getControllerVersionNumber() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("GetControllerVersionNumber command");

    sendToDriver(Constants.VERSION);
  }

  private void getVirtualDatabaseName() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("GetVirtualDatabaseName command");

    sendToDriver(vdb.getDatabaseName());
  }

  //
  // Transaction management
  //

  /**
   * Check that we did not get a concurrent abort due to deadlock detection.
   * 
   * @param request request that was executing
   * @throws SQLException if a concurrent abort has been detected
   */
  private void checkForConcurrentAbort(AbstractRequest request)
      throws SQLException
  {
    if (transactionStarted)
    {
      // 
      synchronized (this)
      {
        if (transactionHasAborted)
        {
          /*
           * If the transaction was aborted before we execute we would never
           * have reached this point and vdb.execWriteRequest(write) would have
           * thrown a SQLException. Now we have to force a rollback because we
           * have probably lazily re-started the transaction and that has to be
           * cleaned up.
           */
          vdb.rollback(currentTid, writeQueryExecutedInThisTransaction);
          throw new SQLException("Transaction " + currentTid
              + " aborted, request " + request + "failed.");
        }
      }
    }
  }

  /**
   * Commit the current transaction and reset the transaction state. If
   * sendTransactionId is true, the current transaction id is send back to the
   * driver else 'true' is sent back. See SEQUOIA-703.
   * 
   * @throws SQLException if an error occurs at commit time
   * @throws IOException if an error occurs when sending the value to the driver
   */
  private void commit() throws SQLException, IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Commit command");

    if (!transactionHasAborted)
      vdb.commit(currentTid, writeQueryExecutedInThisTransaction,
          !queryExecutedInThisTransaction);
    else if (logger.isWarnEnabled())
    {
      logger.warn("Transaction " + currentTid + " was aborted by database");
    }

    // acknowledged the commit (even if transaction is aborted)
    sendToDriver(currentTid);

    resetTransactionState();
  }

  private void begin() throws SQLException, IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Begin command");

    currentTid = vdb.begin(login, persistentConnection, persistentConnectionId);
    sendToDriver(currentTid);

    transactionStarted = true;
    transactionHasAborted = false;
    queryExecutedInThisTransaction = false;
    writeQueryExecutedInThisTransaction = false;
    hasSavepoint = 0;
  }

  /*
   * reset transaction State, begin will be initiated by driver
   */
  private void resetTransactionState()
  {
    currentTid = 0;
    transactionStarted = false;
    transactionHasAborted = false;
    queryExecutedInThisTransaction = false;
    writeQueryExecutedInThisTransaction = false;
    hasSavepoint = 0;
  }

  private void rollback() throws SQLException, IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Rollback command");

    if (!transactionHasAborted)
      vdb.rollback(currentTid, writeQueryExecutedInThisTransaction);
    else if (logger.isWarnEnabled())
    {
      logger.warn("Transaction " + currentTid + " was aborted by database");
    }

    // acknowledged the rollback (even if transaction is aborted)
    sendToDriver(currentTid);

    resetTransactionState();
  }

  private void setNamedSavepoint() throws SQLException, IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Set named savepoint command");

    String savepointName = in.readLongUTF();

    // Check if this is not a duplicate savepoints
    if (vdb.getRequestManager().hasSavepoint(new Long(currentTid),
        savepointName))
      throw new SQLException("A savepoint named " + savepointName
          + " already exists for transaction " + currentTid);

    vdb.setSavepoint(currentTid, savepointName);
    writeQueryExecutedInThisTransaction = true;
    hasSavepoint++;
    sendToDriver(true);
  }

  private void setUnnamedSavepoint() throws SQLException, IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Set unnamed savepoint command");

    int savepointId = vdb.setSavepoint(currentTid);
    writeQueryExecutedInThisTransaction = true;
    hasSavepoint++;
    sendToDriver(savepointId);
  }

  private void releaseSavepoint() throws SQLException, IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Release savepoint command");
    String savepointName = in.readLongUTF();
    vdb.releaseSavepoint(currentTid, savepointName);
    hasSavepoint--;
    sendToDriver(true);
  }

  private void rollbackToSavepoint() throws SQLException, IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Rollback to savepoint command");
    String savepointName = in.readLongUTF();
    vdb.rollback(currentTid, savepointName);
    hasSavepoint = vdb.getNumberOfSavepointsInTransaction(currentTid);
    sendToDriver(true);
  }

  private void retrieveReleaseSavepoint() throws IOException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve release savepoint command");

    // Wait for failover to be authorized
    waitForWritesFlushed(currentTid);

    String savepointName = in.readLongUTF();
    sendToDriver(!vdb.getRequestManager().hasSavepoint(new Long(currentTid),
        savepointName));
  }

  //
  // Decoding commands from the stream
  //

  /**
   * Read a request (without ResultSet parameters) send by the
   * <code>Connection</code> object.
   * 
   * @return an instance of <code>AbstractRequest</code>
   * @throws IOException if an error occurs in the procotol
   * @throws BadJDBCApiUsageException if the decoded request does not match
   *             anything we can handle
   * @see Request#Request(DriverBufferedInputStream)
   */
  private AbstractRequest decodeRequestFromStream() throws IOException,
      BadJDBCApiUsageException
  {
    // Get request from the socket
    Request driverRequest = new Request(in);

    String sqlQuery = driverRequest.getSqlQueryOrTemplate();

    if (!requestFactory.isAuthorizedRequest(sqlQuery))
      throw new BadJDBCApiUsageException(
          "The following statement is not authorized to execute on the cluster (check your user documentation): "
              + sqlQuery);

    AbstractRequest decodedRequest = requestFactory.requestFromString(sqlQuery,
        false, driverRequest.isEscapeProcessing(), driverRequest
            .getTimeoutInSeconds(), connectionLineSeparator);
    if (decodedRequest == null)
      throw new BadJDBCApiUsageException(
          "SQL statement does not match a query returning an update count ("
              + sqlQuery + ")");

    decodedRequest.setPreparedStatementParameters(driverRequest
        .getPreparedStatementParameters());
    decodedRequest.setIsAutoCommit(driverRequest.isAutoCommit());
    return decodedRequest;
  }

  /**
   * Read a request with ResultSet parameters send by the
   * <code>Connection</code> object.
   * 
   * @param isExecuteQuery set to true if the received query is probably a read
   *            statement (i.e. called by an executeQuery-like statement). This
   *            will give priority to the parsing of read requests.
   * @return an instance of <code>AbstractRequest</code>
   * @throws IOException if an error occurs in the procotol
   * @throws BadJDBCApiUsageException if the request is not authorized to
   *             execute
   * @see RequestWithResultSetParameters#RequestWithResultSetParameters(DriverBufferedInputStream)
   */
  private AbstractRequest decodeRequestWithResultSetParametersFromStream(
      boolean isExecuteQuery) throws IOException, BadJDBCApiUsageException
  {
    RequestWithResultSetParameters driverRequest = new RequestWithResultSetParameters(
        in);

    String sqlQuery = driverRequest.getSqlQueryOrTemplate();

    if (!requestFactory.isAuthorizedRequest(sqlQuery))
      throw new BadJDBCApiUsageException(
          "The following statement is not authorized to execute on the cluster (check your user documentation): "
              + sqlQuery);

    AbstractRequest decodedRequest = requestFactory.requestFromString(sqlQuery,
        isExecuteQuery, driverRequest.isEscapeProcessing(), driverRequest
            .getTimeoutInSeconds(), connectionLineSeparator);
    if (decodedRequest == null)
    {
      decodedRequest = new UnknownWriteRequest(sqlQuery, driverRequest
          .isEscapeProcessing(), driverRequest.getTimeoutInSeconds(),
          connectionLineSeparator);
    }
    if (decodedRequest instanceof SelectRequest)
    {
      if (requestFactory.checkIfSelectRequiresBroadcast(sqlQuery, vdb
          .getFunctionsToBroadcastList()))
        ((SelectRequest) decodedRequest).setMustBroadcast(true);
    }
    decodedRequest.setPreparedStatementParameters(driverRequest
        .getPreparedStatementParameters());
    decodedRequest.setIsAutoCommit(driverRequest.isAutoCommit());
    decodedRequest.setMaxRows(driverRequest.getMaxRows());
    decodedRequest.setFetchSize(driverRequest.getFetchSize());
    decodedRequest.setCursorName(driverRequest.getCursorName());
    return decodedRequest;
  }

  /**
   * Log a transaction begin if needed for the AbstractRequest.<br />
   * The transaction is started only if needed (if the request is the first
   * write request for the current transaction)
   * 
   * @param request a request
   * @throws SQLException if the transaction has aborted
   */
  private synchronized void logTransactionBegin(AbstractRequest request)
      throws SQLException
  {
    transactionStarted = setRequestParameters(request, login, currentTid,
        transactionStarted);

    if (connectionHasClosed)
      throw new SQLException(
          "Persistent connection is closed, cannot execute query " + request);

    if (transactionHasAborted)
      throw new SQLException("Transaction is aborted, cannot execute query "
          + request);

    if (!transactionStarted)
      currentTid = 0;
    else
    {
      // Transaction not started, check if we should do a lazy start
      queryExecutedInThisTransaction = true;
      writeQueryExecutedInThisTransaction = true;
    }
  }

  /**
   * Set the login and transaction id on the given request. If the request is
   * autocommit and a transaction was started, the transaction is first commited
   * to return in autocommit mode.
   * 
   * @param request The request to set
   * @param login user login to set
   * @param tid the transaction id to set
   * @return new value of transaction started
   */
  private boolean setRequestParameters(AbstractRequest request, String login,
      long tid, boolean transactionStarted) throws SQLException
  {
    request.setClientIpAddress(clientIpAddress);
    request.setLogin(login);
    request.setTransactionIsolation(transactionIsolation);
    request.setLineSeparator(connectionLineSeparator);
    request.setPersistentConnection(persistentConnection);
    request.setPersistentConnectionId(persistentConnectionId);
    request.setRetrieveSQLWarnings(retrieveSQLWarnings);
    request.setIsReadOnly(isReadOnly);
    request.setId(vdb.getNextRequestId());
    if (request.isAutoCommit() && transactionStarted)
    {
      vdb.commit(tid, writeQueryExecutedInThisTransaction,
          !queryExecutedInThisTransaction);
      return false;
    }
    else
      request.setTransactionId(tid);
    return transactionStarted;
  }

  private void setRequestParametersAndTransactionStarted(AbstractRequest request)
      throws SQLException
  {
    synchronized (this)
    {
      transactionStarted = setRequestParameters(request, login, currentTid,
          transactionStarted);

      if (connectionHasClosed)
        throw new SQLException(
            "Persistent connection is closed, cannot execute query " + request);

      if (transactionHasAborted)
        throw new SQLException("Transaction is aborted, cannot execute query "
            + request);

      if (!transactionStarted)
        currentTid = 0;
      else
        queryExecutedInThisTransaction = true;
    }
  }

  //
  // Request execution
  //

  private void statementExecuteQuery(SelectRequest decodedRequest)
      throws IOException, SQLException, BadJDBCApiUsageException
  {
    if (logger.isDebugEnabled())
      logger.debug("StatementExecuteQuery command");
    AbstractRequest request = decodedRequest;
    if (decodedRequest == null)
      request = decodeRequestWithResultSetParametersFromStream(true);

    if (request instanceof SelectRequest)
    {
      SelectRequest select = (SelectRequest) request;
      setRequestParametersAndTransactionStarted(select);

      // Here, if the transaction isolation level was set to SERIALIZABLE, we
      // need to broadcast the request to all controllers
      if (!request.isAutoCommit()
          && requestFactory.isBroadcastRequired(transactionIsolation))
      {
        select.setMustBroadcast(true);
        writeQueryExecutedInThisTransaction = true;
      }

      // Invoke request interceptors.
      InterceptorManagerAdapter.getInstance()
          .invokeFrontendRequestInterceptors(vdb, select);

      // send the resultset
      ControllerResultSet crs = vdb.statementExecuteQuery(select);

      checkForConcurrentAbort(select);

      // If this is a remapping of the call, we have to send the id back
      if (decodedRequest != null)
        sendToDriver(select.getId());

      // send statement warnings
      sendToDriver(crs.getStatementWarnings());

      sendToDriver(crs);

      // streaming
      if (crs.hasMoreData())
        streamedResultSets.put(crs.getCursorName(), crs);
    }
    else if (request instanceof StoredProcedure)
    { // This is a stored procedure
      if (logger.isInfoEnabled())
        logger.info("Statement.executeQuery() detected a stored procedure ("
            + request
            + ") remapping the call to CallableStatement.executeQuery()");
      callableStatementExecuteQuery((StoredProcedure) request, false);
      return;
    }
    else
      throw new BadJDBCApiUsageException(
          "Statement.executeQuery() not allowed for requests returning an update count ("
              + request + ")");
  }

  /**
   * Execute a write request that returns an int.
   * 
   * @param decodedRequest an already decoded request or null
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while executing the request
   * @throws BadJDBCApiUsageException if a query returning a ResultSet is called
   */
  private void statementExecuteUpdate(AbstractWriteRequest decodedRequest)
      throws IOException, SQLException, BadJDBCApiUsageException
  {
    if (logger.isDebugEnabled())
      logger.debug("StatementExecuteUpdate command");

    AbstractRequest request = decodedRequest;
    if (request == null)
    {
      try
      {
        request = decodeRequestFromStream();
      }
      catch (BadJDBCApiUsageException e)
      {
        throw new BadJDBCApiUsageException(
            "Statement.executeUpdate() not allowed for requests returning a ResultSet",
            e);
      }
      logTransactionBegin(request);
    }

    // System.err.println("mustThrowError = " + mustThrowError);
    // System.err.println("controller jmx name = " +
    // vdb.controller.getJmxName());
    // if (mustThrowError && vdb.controller.getJmxName().equals("eight:1090"))
    // {
    // System.err.println("Simulating IOException...");
    // mustThrowError = false;
    // throw new IOException("Fake error during VDWT.statementExecuteUpdate()");
    // }
    // else
    // {
    // System.err.println("NOT Simulating IOException...");
    // }

    try
    {
      AbstractWriteRequest write = (AbstractWriteRequest) request;

      // At this point we don't have a stored procedure
      // Send query id to driver for failover
      sendToDriver(request.getId());

      // Invoke request interceptors.
      InterceptorManagerAdapter.getInstance()
          .invokeFrontendRequestInterceptors(vdb, write);

      // Execute the request
      ExecuteUpdateResult result = vdb.statementExecuteUpdate(write);
      // Check if there was an issue with deadlock detection
      checkForConcurrentAbort(write);
      // Send SQL Warnings if any
      sendToDriver(result.getStatementWarnings());
      // Send result back
      sendToDriver(result.getUpdateCount());
    }
    catch (ClassCastException e)
    {
      if (request instanceof StoredProcedure)
      {
        if (logger.isInfoEnabled())
          logger.info("Statement.executeUpdate() detected a stored procedure ("
              + request
              + ") remapping the call to CallableStatement.executeUpdate()");
        callableStatementExecuteUpdate((StoredProcedure) request, false);
        return;
      }
      else
        throw new BadJDBCApiUsageException(
            "Statement.executeUpdate() not allowed for requests returning a ResultSet ("
                + request + ")");
    }
  }

  private void statementExecuteUpdateWithKeys() throws IOException,
      SQLException, BadJDBCApiUsageException
  {
    if (logger.isDebugEnabled())
      logger.debug("StatementExecuteUpdateWithKeys command");
    try
    {
      // Get the request from the socket
      AbstractWriteRequest writeWithKeys;
      try
      {
        writeWithKeys = (AbstractWriteRequest) decodeRequestFromStream();
      }
      catch (BadJDBCApiUsageException e)
      {
        throw new BadJDBCApiUsageException(
            "Statement.executeUpdate() not allowed for requests returning a ResultSet",
            e);
      }
      logTransactionBegin(writeWithKeys);

      // Send query id to driver for failover
      sendToDriver(writeWithKeys.getId());

      // Invoke request interceptors.
      InterceptorManagerAdapter.getInstance()
          .invokeFrontendRequestInterceptors(vdb, writeWithKeys);

      // Execute the request
      GeneratedKeysResult updateCountWithKeys = vdb
          .statementExecuteUpdateWithKeys(writeWithKeys);
      // Check if there was an issue with deadlock detection
      checkForConcurrentAbort(writeWithKeys);
      // Send SQL Warnings if any
      sendToDriver(updateCountWithKeys.getStatementWarnings());
      // Send result back
      sendToDriver(updateCountWithKeys.getUpdateCount());
      ControllerResultSet rs = updateCountWithKeys.getControllerResultSet();
      sendToDriver(rs);

      // streaming
      if (rs.hasMoreData())
        streamedResultSets.put(rs.getCursorName(), updateCountWithKeys);
    }
    catch (ClassCastException e)
    {
      throw new BadJDBCApiUsageException(
          "RETURN_GENERATED_KEYS is not supported for stored procedures");
    }

  }

  private void statementExecute(AbstractRequest decodedRequest)
      throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("statementExecute command");

    AbstractRequest request = decodedRequest;
    if (decodedRequest == null)
      try
      {
        request = decodeRequestWithResultSetParametersFromStream(false);
      }
      catch (BadJDBCApiUsageException e)
      {
        throw new SQLException(e.getMessage());
      }

    synchronized (this)
    {
      transactionStarted = setRequestParameters(request, login, currentTid,
          transactionStarted);

      if (connectionHasClosed)
        throw new SQLException(
            "Persistent connection is closed, cannot execute query " + request);

      if (transactionHasAborted)
        throw new SQLException("Transaction is aborted, cannot execute query "
            + request);

      if (!transactionStarted)
        currentTid = 0;
      else
        queryExecutedInThisTransaction = true;
    }

    ExecuteResult result;
    // Direct to Statement.execute() if this is an inline batch
    // or if matching some statements (such as EXPLAIN ANALYZE in postgres)
    if (requestFactory.requestNeedsExecute(request))
    {
      // Send query id to driver for failover
      sendToDriver(request.getId());

      writeQueryExecutedInThisTransaction = true;

      if (request instanceof SelectRequest)
      { // Convert to an unknown write request as expected by underlying
        // components (relates to SEQUOIA-674)
        UnknownWriteRequest writeRequest = new UnknownWriteRequest(request
            .getSqlOrTemplate(), request.getEscapeProcessing(), request
            .getTimeout(), request.getLineSeparator());
        writeRequest.setIsAutoCommit(request.isAutoCommit());
        writeRequest.setTransactionId(request.getTransactionId());
        writeRequest.setTransactionIsolation(request.getTransactionIsolation());
        writeRequest.setId(request.getId());
        writeRequest.setLogin(request.getLogin());
        writeRequest.setPreparedStatementParameters(request
            .getPreparedStatementParameters());
        writeRequest.setTimeout(request.getTimeout());
        writeRequest.setMaxRows(request.getMaxRows());
        writeRequest.setPersistentConnection(request.isPersistentConnection());
        writeRequest.setPersistentConnectionId(request
            .getPersistentConnectionId());
        request = writeRequest;
      }

      // Invoke request interceptors.
      InterceptorManagerAdapter.getInstance()
          .invokeFrontendRequestInterceptors(vdb, request);

      result = vdb.statementExecute(request);
    }
    // Route to CallableStatement.execute() if this is a stored procedure
    else if (request instanceof StoredProcedure)
    {
      if (logger.isInfoEnabled())
        logger.info("Statement.execute() did detect a stored procedure ("
            + request + ") remapping the call to CallableStatement.execute()");

      writeQueryExecutedInThisTransaction = true;

      callableStatementExecute((StoredProcedure) request, false);
      return;
    }
    else
    { // Route SELECT to Statement.executeQuery() and others to
      // Statement.executeUpdate()

      // Send query id to driver for failover (driver still expect a
      // Statement.execute() protocol)
      sendToDriver(request.getId());

      result = new ExecuteResult();
      if (request instanceof SelectRequest)
      {
        // Here, if the transaction isolation level was set to SERIALIZABLE, we
        // need to broadcast the select request to all controllers
        if (!request.isAutoCommit()
            && requestFactory.isBroadcastRequired(transactionIsolation))
        {
          ((SelectRequest) request).setMustBroadcast(true);
          writeQueryExecutedInThisTransaction = true;
        }

        // Invoke request interceptors.
        InterceptorManagerAdapter.getInstance()
            .invokeFrontendRequestInterceptors(vdb, request);

        ControllerResultSet crs = vdb
            .statementExecuteQuery((SelectRequest) request);
        // call remapping: construct a ExecuteResult from a ExecuteUpdateResult
        result.addResult(crs);
        result.setStatementWarnings(crs.getStatementWarnings());
        result.addResult(-1);
        // streaming
        if (crs.hasMoreData())
          streamedResultSets.put(crs.getCursorName(), crs);
      }
      else
      {
        writeQueryExecutedInThisTransaction = true;

        // Invoke request interceptors.
        InterceptorManagerAdapter.getInstance()
            .invokeFrontendRequestInterceptors(vdb, request);

        ExecuteUpdateResult updateCount = vdb
            .statementExecuteUpdate((AbstractWriteRequest) request);
        // call remapping: construct a ExecuteResult from a ExecuteUpdateResult
        result.setStatementWarnings(updateCount.getStatementWarnings());
        result.addResult(updateCount.getUpdateCount());
        // end of result list marker
        if (updateCount.getUpdateCount() != -1)
          result.addResult(-1);
      }
    }

    checkForConcurrentAbort(request);

    // Send SQL Warnings if any
    sendToDriver(result.getStatementWarnings());

    for (Iterator<?> iter = result.getResults().iterator(); iter.hasNext();)
    {
      Object r = iter.next();
      if (r instanceof Integer)
      {
        sendToDriver(false);
        sendToDriver(((Integer) r).intValue());
      }
      else if (r instanceof ControllerResultSet)
      {
        sendToDriver(true);
        sendToDriver((ControllerResultSet) r);
      }
      else
        logger.error("Unexpected result " + r
            + " in statementExecute for request " + request);
    }
  }

  /**
   * @param decodedProc Stored procedure if called from statementExecuteQuery(),
   *            otherwise null
   * @param returnsOutParameters true if the call must return out/named
   *            parameters
   * @throws BadJDBCApiUsageException
   */
  private void callableStatementExecuteQuery(StoredProcedure decodedProc,
      boolean returnsOutParameters) throws IOException, SQLException,
      BadJDBCApiUsageException
  {
    if (logger.isDebugEnabled())
      logger.debug("CallableStatementExecuteQuery command");

    StoredProcedure proc = decodedProc;
    if (proc == null)
    {
      AbstractRequest request = decodeRequestWithResultSetParametersFromStream(true);
      if (request == null)
        throw new ProtocolException("Failed to decode stored procedure");

      try
      {
        // Fetch the query from the socket
        proc = (StoredProcedure) request;

        // Parse the query first to update the semantic information
        vdb.getRequestManager().getParsingFromCacheOrParse(proc);

        // If procedure is read-only, we don't log lazy begin
        DatabaseProcedureSemantic semantic = proc.getSemantic();
        if ((semantic == null) || !semantic.isReadOnly())
          logTransactionBegin(proc);
      }
      catch (ClassCastException e)
      {
        if (request instanceof SelectRequest)
        {
          if (logger.isInfoEnabled())
            logger
                .info("CallableStatement.executeQuery() did not detect a stored procedure ("
                    + request
                    + ") remapping the call to Statement.executeQuery()");
          statementExecuteQuery((SelectRequest) request);
          if (returnsOutParameters)
            sendNamedAndOutParametersToDriver(request);
          return;
        }
        throw new BadJDBCApiUsageException(
            "Unhandled stored procedure call in " + request);
      }
    }

    setRequestParametersAndTransactionStarted(proc);

    if (decodedProc == null)
    { // Send query id to driver for failover
      sendToDriver(proc.getId());
    }
    // else we come from statement.executeQuery and we should not send the id

    // Invoke request interceptors.
    InterceptorManagerAdapter.getInstance().invokeFrontendRequestInterceptors(
        vdb, proc);

    // Execute the stored procedure
    ControllerResultSet sprs = vdb.callableStatementExecuteQuery(proc);
    checkForConcurrentAbort(proc);

    // Send SQL Warnings if any
    sendToDriver(sprs.getStatementWarnings());

    sendToDriver(sprs);

    // streaming
    if (sprs.hasMoreData())
      streamedResultSets.put(sprs.getCursorName(), sprs);

    if (returnsOutParameters)
      sendNamedAndOutParametersToDriver(proc);
  }

  /**
   * @param sp Stored procedure if called from statementExecuteUpdate(),
   *            otherwise null
   * @param returnsOutParameters true if the call must return out/named
   *            parameters
   */
  private void callableStatementExecuteUpdate(StoredProcedure sp,
      boolean returnsOutParameters) throws IOException, SQLException,
      BadJDBCApiUsageException
  {
    if (logger.isDebugEnabled())
      logger.debug("CallableStatementExecuteUpdate command");

    if (sp == null)
    {
      AbstractRequest request;
      try
      {
        request = decodeRequestFromStream();
      }
      catch (BadJDBCApiUsageException e)
      {
        throw new BadJDBCApiUsageException(
            "CallableStatement.executeUpdate() not allowed for requests returning a ResultSet ",
            e);
      }
      logTransactionBegin(request);

      try
      {
        // Fetch the query from the socket
        sp = (StoredProcedure) request;
      }
      catch (ClassCastException e)
      {
        if (request instanceof AbstractWriteRequest)
        {
          if (logger.isInfoEnabled())
            logger
                .info("CallableStatement.executeUpdate() did not detect a stored procedure ("
                    + request
                    + ") remapping the call to Statement.executeUpdate()");
          statementExecuteUpdate((AbstractWriteRequest) request);
          if (returnsOutParameters)
            sendNamedAndOutParametersToDriver(request);
          return;
        }
        throw new BadJDBCApiUsageException(
            "Unhandled stored procedure call in " + request);
      }
    }

    // Send query id to driver for failover
    sendToDriver(sp.getId());

    // Invoke request interceptors.
    InterceptorManagerAdapter.getInstance().invokeFrontendRequestInterceptors(
        vdb, sp);

    // Execute the query
    ExecuteUpdateResult result = vdb.callableStatementExecuteUpdate(sp);
    checkForConcurrentAbort(sp);
    // Send SQL Warnings if any
    sendToDriver(result.getStatementWarnings());
    // Send result back
    sendToDriver(result.getUpdateCount());

    if (returnsOutParameters)
      sendNamedAndOutParametersToDriver(sp);
  }

  /**
   * @param sp Stored procedure if called from statementExecute(), otherwise
   *            null.
   * @param returnsOutParameters true if the call must return out/named
   *            parameters
   */
  private void callableStatementExecute(StoredProcedure sp,
      boolean returnsOutParameters) throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("CallableStatementExecute command");

    if (sp == null)
    {
      AbstractRequest request;
      try
      {
        request = decodeRequestWithResultSetParametersFromStream(false);
      }
      catch (BadJDBCApiUsageException e)
      {
        throw new SQLException(e.getMessage());
      }
      if (request == null)
        throw new ProtocolException("Failed to decode stored procedure");
      try
      {
        // Fetch the query from the socket
        sp = (StoredProcedure) request;

        // Parse the query first to update the semantic information
        vdb.getRequestManager().getParsingFromCacheOrParse(sp);

        // If procedure is read-only, we don't log lazy begin
        DatabaseProcedureSemantic semantic = sp.getSemantic();
        if ((semantic == null) || !semantic.isReadOnly())
          logTransactionBegin(sp);
      }
      catch (ClassCastException e)
      {
        if (logger.isInfoEnabled())
          logger
              .info("CallableStatement.execute() did not detect a stored procedure ("
                  + request + ") remapping the call to Statement.execute()");
        statementExecute(request);
        if (returnsOutParameters)
          sendNamedAndOutParametersToDriver(request);
        return;
      }
    }

    setRequestParametersAndTransactionStarted(sp);

    // Send query id to driver for failover
    sendToDriver(sp.getId());

    // Invoke request interceptors.
    InterceptorManagerAdapter.getInstance().invokeFrontendRequestInterceptors(
        vdb, sp);

    // Execute the query
    ExecuteResult result = vdb.callableStatementExecute(sp);
    checkForConcurrentAbort(sp);

    // Send SQL Warnings if any
    sendToDriver(result.getStatementWarnings());

    for (Iterator<?> iter = result.getResults().iterator(); iter.hasNext();)
    {
      Object r = iter.next();

      if (r instanceof Integer)
      {
        sendToDriver(false);
        sendToDriver(((Integer) r).intValue());
      }
      else if (r instanceof ControllerResultSet)
      {
        sendToDriver(true);
        sendToDriver((ControllerResultSet) r);
      }
      else
        logger.error("Unexepected result " + r
            + " in callableStatementExecute for request " + sp);
    }
    if (returnsOutParameters)
      sendNamedAndOutParametersToDriver(sp);
  }

  private void sendNamedAndOutParametersToDriver(AbstractRequest request)
      throws IOException, ProtocolException
  {
    if (request instanceof StoredProcedure)
    {
      try
      {
        StoredProcedure proc = (StoredProcedure) request;
        // First send the out parameters
        List<?> outParamIndexes = proc.getOutParameterIndexes();
        if (outParamIndexes != null)
        {
          // Now send each param (index, then serializer and serialized object)
          for (Iterator<?> iter = outParamIndexes.iterator(); iter.hasNext();)
          {
            Integer index = (Integer) iter.next();
            sendToDriver(index.intValue());
            Object object = proc.getOutParameterValue(index);
            sendObjectToDriver(object);
          }
        }
        sendToDriver(0);

        // Fetch the named parameters
        List<?> namedParamNames = proc.getNamedParameterNames();
        if (namedParamNames != null)
        {
          for (Iterator<?> iter = namedParamNames.iterator(); iter.hasNext();)
          {
            // Send param name first
            String paramName = (String) iter.next();
            sendToDriver(paramName);
            // Now send value (serializer first then serialized object)
            Object object = proc.getNamedParameterValue(paramName);
            sendObjectToDriver(object);
          }
        }
        sendToDriver("0");
      }
      catch (NotImplementedException e)
      {
        String msg = "Unable to serialize parameter result for request "
            + request;
        logger.error(msg, e);
        throw new ProtocolException(msg);
      }
    }
    else
    // Not a stored procedure (remapped call)
    {
      // No out parameter
      sendToDriver(0);
      // No named parameter
      sendToDriver("0");
    }
  }

  /**
   * Send an object to the driver (first tag then serialized object).
   * 
   * @param object object to send
   * @throws IOException if an error occurs with the socket
   * @throws NotImplementedException if the object cannot be serialized
   */
  private void sendObjectToDriver(Object object) throws IOException,
      NotImplementedException
  {
    if (object == null)
    { // Special tag for null objects (nothing to send)
      TypeTag.JAVA_NULL.sendToStream(out);
    }
    else
    { // Regular object
      Serializer s = SQLDataSerialization.getSerializer(object);
      s.getTypeTag().sendToStream(out);
      s.sendToStream(object, out);
    }
  }

  /**
   * Retrieve the result from the request result failover cache for the given
   * request id. If the result is not found, the scheduler is checked in case
   * the query is currently executing. If the query is executing, we wait until
   * it has completed and then return the result. If no result is found, null is
   * returned.
   * 
   * @param requestId the request unique identifier
   * @return the request result or null if not found
   */
  private Serializable getResultForRequestId(long requestId)
  {
    waitForWritesFlushed(requestId);

    Serializable result = ((DistributedVirtualDatabase) vdb)
        .getRequestResultFailoverCache().retrieve(requestId);

    if (result == null)
    { // Check if query is not currently executing
      AbstractScheduler scheduler = vdb.getRequestManager().getScheduler();
      if (scheduler.isActiveRequest(requestId))
      {
        // Wait for request completion and then retrieve the result from the
        // failover cache.
        scheduler.waitForRequestCompletion(requestId);
        result = ((DistributedVirtualDatabase) vdb)
            .getRequestResultFailoverCache().retrieve(requestId);
      }
    }
    return result;
  }

  /**
   * Retrieve the result of a stored procedure that returns multiple results,
   * out parameters and named parameters. Returns null to the driver if the
   * result has not been found.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveExecuteResultWithParameters() throws IOException,
      SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve execute result with parameters command");

    long requestId = in.readLong();

    if (vdb.isDistributed())
    {
      Serializable result = getResultForRequestId(requestId);

      if (result != null)
      {
        // Cache hit
        if (result instanceof StoredProcedureCallResult)
        {
          StoredProcedureCallResult spResult = (StoredProcedureCallResult) result;

          // re-send statement warnings
          sendToDriver(((ExecuteResult) spResult.getResult())
              .getStatementWarnings());
          // Send results first
          for (Iterator<?> iter = ((ExecuteResult) spResult.getResult())
              .getResults().iterator(); iter.hasNext();)
          {
            Object element = iter.next();
            if (element instanceof Integer)
            {
              sendToDriver(false);
              sendToDriver(((Integer) element).intValue());
            }
            else if (element instanceof ControllerResultSet)
            {
              sendToDriver(true);
              sendToDriver((ControllerResultSet) element);
            }
            else
              logger.error("Unexpected result " + element
                  + " in statementExecute for request " + requestId);
          }

          // Send parameters
          sendNamedAndOutParametersToDriver(spResult.getStoredProcedure());
        }
        else
          throw new SQLException(
              "Expected StoredProcedureCallResult for request " + requestId
                  + " failover but got " + result);
      }
      else
      {
        // No cache hit
        sendToDriver((SQLWarning) null);
        sendToDriver(true);
        sendToDriver((ControllerResultSet) null);
      }
    }
    else
    {
      throw new SQLException(
          "Transparent failover for statements that potentially return multiple results is only supported in distributed configurations.");
    }
  }

  /**
   * Retrieve the result of a stored procedure that returns an int, out
   * parameters and named parameters. Returns -1 to the driver if the result has
   * not been found.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveExecuteUpdateResultWithParameters() throws IOException,
      SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve execute update with parameters command");

    long requestId = in.readLong();

    if (vdb.isDistributed())
    {
      Serializable result = getResultForRequestId(requestId);

      if (result != null)
      {
        // Cache hit
        if (result instanceof StoredProcedureCallResult)
        {
          StoredProcedureCallResult spResult = (StoredProcedureCallResult) result;
          // Send warnings
          ExecuteUpdateResult r = (ExecuteUpdateResult) spResult.getResult();
          // re-send statement warnings
          sendToDriver(r.getStatementWarnings());
          // Send udpate count
          sendToDriver(r.getUpdateCount());
          // Send parameters
          sendNamedAndOutParametersToDriver(spResult.getStoredProcedure());
        }
        else
          throw new SQLException(
              "Expected StoredProcedureCallResult for request " + requestId
                  + " failover but got " + result);
      }
      else
      {
        // No cache hit
        sendToDriver((SQLWarning) null);
        sendToDriver(-1);
      }
    }
    else
    {
      throw new SQLException(
          "Transparent failover for statements that potentially return multiple results is only supported in distributed configurations.");
    }
  }

  /**
   * Retrieve the result of a stored procedure that returns a ResultSet, out
   * parameters and named parameters. Returns null to the driver if the result
   * has not been found.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveExecuteQueryResultWithParameters() throws IOException,
      SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve execute update with parameters command");

    long requestId = in.readLong();

    if (vdb.isDistributed())
    {
      Serializable result = getResultForRequestId(requestId);

      if (result != null)
      {
        // Cache hit
        if (result instanceof StoredProcedureCallResult)
        {
          StoredProcedureCallResult spResult = (StoredProcedureCallResult) result;
          // re-send statement warnings
          sendToDriver(((ControllerResultSet) spResult.getResult())
              .getStatementWarnings());
          // Send ResultSet
          sendToDriver((ControllerResultSet) spResult.getResult());
          // Send parameters
          sendNamedAndOutParametersToDriver(spResult.getStoredProcedure());
        }
        else
          throw new SQLException(
              "Expected StoredProcedureCallResult for request " + requestId
                  + " failover but got " + result);
      }
      else
      {
        // No cache hit
        sendToDriver((SQLWarning) null);
        sendToDriver((ControllerResultSet) null);
      }
    }
    else
    {
      throw new SQLException(
          "Transparent failover for statements that potentially return multiple results is only supported in distributed configurations.");
    }
  }

  /**
   * Retrieve the result of a write request that returns an int. Returns -1 to
   * the driver if the result has not been found.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveExecuteUpdateResult() throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve execute update result command");

    long requestId = in.readLong();

    if (vdb.isDistributed())
    {
      // Result will always be null since result is only stored in the recovery
      // log but this call ensures that the query execution has completed.
      getResultForRequestId(requestId);

      // We don't cache the warnings for write queries
      sendToDriver(new SQLWarning(
          Translate
              .get(
                  "virtualdatabase.distributed.write.failover.lost.warnings", requestId))); //$NON-NLS-1$
      sendToDriver(vdb.getRequestManager().getRecoveryLog()
          .getUpdateCountResultForQuery(requestId));
    }
    else
    {
      throw new SQLException(
          "Transparent failover for statements that return an update count is only supported in distributed configurations.");
    }
  }

  /**
   * Retrieve the result of a request that returns multiple results. Returns
   * null to the driver if the result has not been found.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveExecuteResult() throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve execute result command");

    long requestId = in.readLong();

    if (vdb.isDistributed())
    {
      Serializable result = getResultForRequestId(requestId);

      if (result != null)
      {
        // Cache hit
        // re-send warnings
        SQLWarning cachedWarns = ((ExecuteResult) result)
            .getStatementWarnings();
        sendToDriver(cachedWarns);
        // and result
        for (Iterator<?> iter = ((ExecuteResult) result).getResults().iterator(); iter
            .hasNext();)
        {
          Object element = iter.next();
          if (element instanceof Integer)
          {
            sendToDriver(false);
            sendToDriver(((Integer) element).intValue());
          }
          else if (element instanceof ControllerResultSet)
          {
            sendToDriver(true);
            sendToDriver((ControllerResultSet) element);
          }
          else
            logger.error("Unexpected result " + element
                + " in statementExecute for request " + requestId);
        }
      }
      else
      {
        try
        {
          // The query may have been remapped
          int updateCount = vdb.getRequestManager().getRecoveryLog()
              .getUpdateCountResultForQuery(requestId);
          sendToDriver((SQLWarning) null);
          if (updateCount != -1)
          { // Build a response with the update count and add 'no more result'
            sendToDriver(false);
            sendToDriver(updateCount);
            sendToDriver(false);
            sendToDriver(-1);
          }
          else
          { // Not found
            sendToDriver(true);
            sendToDriver((ControllerResultSet) null);
          }
        }
        catch (SQLException ex)
        { // No cache hit
          sendToDriver((SQLWarning) null);
          sendToDriver(true);
          sendToDriver((ControllerResultSet) null);
        }
      }
    }
    else
    {
      throw new SQLException(
          "Transparent failover for statements that potentially return multiple results is only supported in distributed configurations.");
    }
  }

  /**
   * Retrieve the result of a write request that returns an int and generated
   * keys. Returns -1 to the driver if the result has not been found.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveExecuteUpdateWithKeysResult() throws IOException,
      SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve execute update with keys result command");

    long requestId = in.readLong();

    if (vdb.isDistributed())
    {
      Serializable result = getResultForRequestId(requestId);

      if (result != null)
      {
        // Cache hit
        sendToDriver(((GeneratedKeysResult) result).getStatementWarnings());
        sendToDriver(((GeneratedKeysResult) result).getUpdateCount());
        sendToDriver(((GeneratedKeysResult) result).getControllerResultSet());
      }
      else
      {
        // No cache hit
        sendToDriver((SQLWarning) null);
        sendToDriver(-1);
      }
    }
    else
    {
      throw new SQLException(
          "Transparent failover for statements that return generated keys is only supported in distributed configurations.");
    }
  }

  /**
   * Retrieve the result of a request that returns a ResultSet. Returns null to
   * the driver if the result has not been found.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveExecuteQueryResult() throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve execute query result command");

    long requestId = in.readLong();

    if (vdb.isDistributed())
    {
      Serializable result = getResultForRequestId(requestId);

      if (result != null)
      {
        // Cache hit
        sendToDriver(((ControllerResultSet) result).getStatementWarnings());
        sendToDriver((ControllerResultSet) result);
      }
      else
      {
        // No cache hit
        sendToDriver((SQLWarning) null);
        sendToDriver((ControllerResultSet) null);
      }
    }
    else
    {
      throw new SQLException(
          "Transparent failover for statements that return a ResultSet is only supported in distributed configurations.");
    }
  }

  /**
   * Retrieve the result of a transaction commit. If the transaction was not
   * commited, we commit it and acknowledge the driver back in all cases.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveCommitResult() throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve commit command");

    waitForWritesFlushed(currentTid);

    if (transactionHasAborted || transactionCompleted)
    {
      if (transactionHasAborted && logger.isWarnEnabled())
      {
        logger.warn("Transaction " + currentTid + " was aborted by database");
      }
      // mimic the behavior of the commit() method
      sendToDriver(currentTid);
      resetTransactionState();
      return;
    }

    boolean retry;
    do
    {
      retry = false;
      String commitStatus = vdb.getRequestManager().getRecoveryLog()
          .getCommitStatusForTransaction(currentTid);

      if (LogEntry.MISSING.equals(commitStatus))
      {
        if (writeQueryExecutedInThisTransaction)
        {
          // Transaction was not commited yet, let's commit
          commit();
        }
        else
        {
          // If this was a read-only transaction, it was never started on this
          // controller and therefore there is no need to commit. It is ok to
          // let the user believe that the transaction successfully commited
          // since it was read-only and had no effect on the database anyway.
          // We have to tell the client that the transaction was committed
          // successfully anyway, otherwise the application will hang waiting
          // for this answer.
          sendToDriver(currentTid);
          resetTransactionState();
          return;
        }
      }
      else if (LogEntry.SUCCESS.equals(commitStatus))
      {
        // Transaction was already commited, acknowledge the transaction id
        sendToDriver(currentTid);

        resetTransactionState();
      }
      else if (LogEntry.FAILED.equals(commitStatus))
      {
        logger.warn("Commit of transaction " + currentTid + " failed");
        // commit failed
        throw new SQLException("Commit of transaction " + currentTid
            + " failed");
      }
      else
      {
        /*
         * Status is executing or unknown, if status is executing and we have
         * enabled backends locally, we have to wait for the final status to be
         * updated in the recovery log
         */

        retry = LogEntry.EXECUTING.equals(commitStatus)
            && (vdb.getRequestManager().getLoadBalancer()
                .getNumberOfEnabledBackends() > 0);
        if (!retry)
          throw new SQLException("Commit of transaction " + currentTid
              + " is in unknown or executing state");
      }
    }
    while (retry);

  }

  /**
   * Retrieve the result of a transaction rollback. If the transaction was not
   * rollbacked, we rollback and acknowledge the driver back in all cases.
   * 
   * @throws IOException if an error occurs with the socket
   * @throws SQLException if an error occurs while retrieving the result
   */
  private void retrieveRollbackResult() throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Retrieve rollback command");

    waitForWritesFlushed(currentTid);

    if (transactionHasAborted || transactionCompleted)
    {
      if (transactionHasAborted && logger.isWarnEnabled())
      {
        logger.warn("Transaction " + currentTid + " was aborted by database");
      }
      // mimic the behavior of the rollback() method
      sendToDriver(currentTid);
      resetTransactionState();
    }
    else
    {
      String rollbackStatus = vdb.getRequestManager().getRecoveryLog()
          .getRollbackStatusForTransaction(currentTid);

      if (LogEntry.MISSING.equals(rollbackStatus))
        // Transaction was not rollbacked yet, let's do it
        rollback();
      else if (LogEntry.SUCCESS.equals(rollbackStatus)
          || LogEntry.FAILED.equals(rollbackStatus))
      {
        // Transaction was already rollbacked, acknowledge the transaction id
        sendToDriver(currentTid);
        resetTransactionState();
      }
      else
      { // UNKOWN OR EXECUTING state
        if (vdb.isDistributed())
        {
          ((DistributedRequestManager) vdb.getRequestManager())
              .cleanupRollbackFromOtherController(currentTid);
          sendToDriver(currentTid);
          resetTransactionState();
        }
        else
        {
          // rollback cannot fail locally, so notify right away even if the
          // rollback has not fully completed
          sendToDriver(currentTid);
          resetTransactionState();
        }
      }
    }
  }

  private void waitForWritesFlushed(long requestIdOrTransactionId)
  {
    // In non-distributed configuration, there is no failover cache, so there
    // is no need to wait
    if (!vdb.isDistributed())
      return;

    DistributedVirtualDatabase dvdb = (DistributedVirtualDatabase) vdb;
    dvdb.flushGroupCommunicationMessagesLocally(requestIdOrTransactionId
        & DistributedRequestManager.CONTROLLER_ID_BIT_MASK);
    dvdb
        .waitForGroupCommunicationMessagesLocallyFlushed(requestIdOrTransactionId
            & DistributedRequestManager.CONTROLLER_ID_BIT_MASK);
  }

  /**
   * Serialize a ControllerResultSet answer, prefixed with the appropriate
   * TypeTag. Note that this will be deserialized in a DriverResultSet.
   * 
   * @param crs the resultset to send
   * @throws IOException stream error
   */
  private void sendToDriver(ControllerResultSet crs) throws IOException
  {
    /**
     * If a (buggy) backend was returning a null ResultSet, we would have failed
     * much earlier in
     * {@link ControllerResultSet#ControllerResultSet(AbstractRequest, java.sql.ResultSet, MetadataCache, Statement, boolean)
     */
    /*
     * So we can safely use "null" as a special value during transparent
     * failover when the controller's request cache hasn't found a result for a
     * given request.
     */
    if (null == crs)
    {
      TypeTag.NULL_RESULTSET.sendToStream(out);
      out.flush();
      return;
    }

    try
    {
      crs.initSerializers();
    }
    catch (NotImplementedException nie)
    { // we don't know how to serialize something
      sendToDriver(nie);
      return;
    }

    TypeTag.RESULTSET.sendToStream(out);
    crs.sendToStream(out);
  }

  /**
   * Send a protocol String, prefixed with the appropriate TypeTag
   */
  private void sendToDriver(String str) throws IOException
  {
    TypeTag.NOT_EXCEPTION.sendToStream(out);
    out.writeLongUTF(str);
    out.flush();
  }

  /**
   * Send a protocol boolean, prefixed with the appropriate TypeTag
   */
  private void sendToDriver(boolean b) throws IOException
  {
    TypeTag.NOT_EXCEPTION.sendToStream(out);
    out.writeBoolean(b);
    out.flush();
  }

  /**
   * Send a protocol int, prefixed with the appropriate TypeTag
   */
  private void sendToDriver(int i) throws IOException
  {
    TypeTag.NOT_EXCEPTION.sendToStream(out);
    out.writeInt(i);
    out.flush();
  }

  /**
   * Send a protocol long, prefixed with the appropriate TypeTag
   */
  private void sendToDriver(long l) throws IOException
  {
    TypeTag.NOT_EXCEPTION.sendToStream(out);
    out.writeLong(l);
    out.flush();
  }

  private void sendToDriver(SQLWarning s) throws IOException
  {
    if (s != null)
    {
      sendToDriver(true);
      TypeTag.BACKEND_EXCEPTION.sendToStream(out);
      new BackendDriverException(s).sendToStream(out);
    }
    else
      sendToDriver(false);
  }

  private void sendToDriver(Exception e) throws IOException
  {
    TypeTag.EXCEPTION.sendToStream(out);
    // This is the place where we convert Exceptions to something
    // serializable and that the driver can understand
    // So this is the place where it's possible to trap all unknown exceptions

    if (e instanceof SQLException)
    { // we assume that an SQLexception comes from the backend

      // since this is currently false because some ControllerCoreExceptions
      // subclass SQLException, here are a few workarounds
      if (e instanceof NoMoreBackendException
          || e instanceof NoMoreControllerException
          || e instanceof NotImplementedException)
      {
        TypeTag.CORE_EXCEPTION.sendToStream(out);
        new ControllerCoreException(e).sendToStream(out);
        return;
      }

      // non-workaround, regular SQLException from backend
      TypeTag.BACKEND_EXCEPTION.sendToStream(out);
      new BackendDriverException(e).sendToStream(out);
      return;
    }

    // else we assume this is an exception from the core (currently...?)
    TypeTag.CORE_EXCEPTION.sendToStream(out);
    new ControllerCoreException(e).sendToStream(out);
    return;

  }

  /**
   * Serializes a ParameterMetaData, answer to
   * PreparedStatementGetParameterMetaData command prefixed by the appropriate
   * TypeTag
   * 
   * @param pmd the prepared statement parameter metadata to send
   * @throws IOException stream error
   */
  private void sendToDriver(ParameterMetaData pmd) throws IOException
  {
    TypeTag.PP_PARAM_METADATA.sendToStream(out);
    if (pmd == null)
      out.writeBoolean(false);
    else
    {
      out.writeBoolean(true);
      ((SequoiaParameterMetaData) pmd).sendToStream(out);
    }
    out.flush();
  }

  /**
   * Implements streaming: send the next ResultSet chunk to driver, pulling it
   * from ControllerResultSet. The driver decides of the chunk size at each
   * call. Note that virtualdatabase streaming is independent from backend
   * streaming (which may not be supported). They even could be configured with
   * two different fetchSize -s (it's not currently the case).
   * <p>
   * This is a real issue: in case of a low fetchsize hint ignored by the driver
   * of the backend, then the whole backend resultset stays in the memory on the
   * controller. And we probably cannot know how many rows did it pulled out.
   * 
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#executeStatementExecuteQueryOnBackend(SelectRequest,
   *      org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      org.continuent.sequoia.controller.backend.BackendWorkerThread,
   *      Connection, org.continuent.sequoia.controller.backend.MetadataCache)
   * @see ControllerResultSet#fetchData(int)
   */
  private void fetchNextResultSetRows() throws IOException, SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("FetchNextResultSetRows command");

    String cursorName = in.readLongUTF();
    int fetchSize = in.readInt();
    ControllerResultSet crs = (ControllerResultSet) streamedResultSets
        .get(cursorName);
    if (crs == null)
    {
      sendToDriver(new SQLException(
          "No valid ControllerResultSet to fetch data from"));
    }
    else
    {
      // refresh ControllerResultSet with a new chunk of rows
      crs.fetchData(fetchSize);

      // send it
      TypeTag.NOT_EXCEPTION.sendToStream(out);
      crs.sendRowsToStream(out);

      // At this point we could probably data.clear() already sent as a memory
      // optimization, but still in doubt about others using it we leave it as
      // is.

      if (!crs.hasMoreData())
        streamedResultSets.remove(cursorName);
    }
  }

  //
  // Public API
  //

  /**
   * Return the current transaction id (should be 0 if not in a transaction).
   * 
   * @return the current transaction id
   */
  public long getCurrentTransactionId()
  {
    return currentTid;
  }

  /**
   * Return the persistent connection id.
   * 
   * @return the persistent connection id
   */
  public long getPersistentConnectionId()
  {
    return persistentConnectionId;
  }

  /**
   * Get time active
   * 
   * @return time active since started
   */
  public long getTimeActive()
  {
    return ((System.currentTimeMillis() - in.getDateCreated()) / 1000);
  }

  /**
   * @return Returns the login of the current user.
   */
  public String getUser()
  {
    if (user == null)
    {
      return "No user connected";
    }
    return user.getLogin();
  }

  //
  // Public API
  //

  /**
   * Notify the abort of the given transaction which should match the current
   * transaction id of this thread else an exception will be thrown.
   * 
   * @param tid the transaction identifier to abort
   * @throws SQLException if the tid does not correspond to the current
   *             transaction id of this thread or if the abort throws a
   *             SQLException
   */
  public void notifyAbort(long tid) throws SQLException
  {
    synchronized (this)
    {
      if ((!transactionStarted) || (currentTid != tid))
        throw new SQLException("Cannot abort transaction " + tid
            + " since current worker thread is assigned to transaction "
            + currentTid);

      transactionHasAborted = true;
    }
  }

  /**
   * Notify the closing of the given persistent connection which should match
   * the current persistent connection id of this thread else an exception will
   * be thrown.
   * 
   * @param tid the persistent connection identifier to abort
   */
  public void notifyClose(long persistentConnectionId)
  {
    synchronized (this)
    {
      if ((!persistentConnection)
          || (this.persistentConnectionId != persistentConnectionId))
        logger
            .warn("Cannot notify closing of persistent connection "
                + persistentConnectionId
                + " since current worker thread is assigned to persistent connection "
                + this.persistentConnectionId);

      connectionHasClosed = true;
    }
  }

  /**
   * Retrieve general information on this client
   * 
   * @return an array of string
   */
  public String[] retrieveClientData()
  {
    String[] data = new String[4];
    data[0] = in.getSocket().getInetAddress().getHostName();
    data[1] = in.getSocket().getInetAddress().getHostAddress();
    data[2] = String
        .valueOf(((System.currentTimeMillis() - in.getDateCreated()) / 1000));
    return data;
  }

  /**
   * Shutdown this thread by setting <code>isKilled</code> value to true. This
   * gives time to check for needed rollback transactions
   */
  public void shutdown()
  {
    // Tell this thread to stop working gently.
    this.isKilled = true;
    try
    {
      if (waitForCommand)
      {
        // close only the streams if we're not in the middle of a request
        in.close();
        out.close();
      }
    }
    catch (IOException e)
    {
      // ignore, only the input stream should be close
      // for this thread to end
    }
  }

  /**
   * Returns the time since last command was executed on this connection (in
   * ms).
   * 
   * @return idle time in ms.
   */
  public long getIdleTime()
  {
    return System.currentTimeMillis() - lastCommandTime;
  }

}