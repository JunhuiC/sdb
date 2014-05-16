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
 * Contributor(s): Nicolas Modrzyk, Marc Herbert, Jean-Bernard van Zuylen
 */

package org.continuent.sequoia.common.protocol;

/*
 * TODO: replace <br>~argument by @argument etc. and implement the
 * corresponding taglet. An interesting alternative is to dedicate a new "RPC"
 * class with exactly one method per command. This class would be a subset of
 * today's Connection class. We could then use standard @param and @return tags
 * instead of custom tags.
 */

/**
 * Protocol commands between Sequoia driver (client) and controller (server).
 * All communications follow a classic RPC scheme: the driver sends a Command
 * code, followed by argument(s) for some of the commands, and expects some
 * answer(s), at the very least an error code or an exception. The server is
 * event-driven; communications are inited by the client which is the one
 * sending Protocol commands, so the verbs <cite>send</cite> and <cite>receive</cite>
 * must be understood as from driver point of view. Almost all these commands
 * are put on the wire by client class
 * {@link org.continuent.sequoia.driver.Connection} and read (and answered) by
 * class {@link
 * org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#run()}.
 * The only exceptions are the following commands: <br>- ProtocolVersion<br>
 * read only by {@link
 * org.continuent.sequoia.controller.core.ControllerWorkerThread#run()}, which
 * then constructs the connection and pass it to VirtualDatabaseWorkerThread for
 * the rest of the execution of this command. <br>- Close<br>
 * in addition to Connection, also sent by
 * {@link org.continuent.sequoia.driver.ConnectionClosingThread#closeConnection(org.continuent.sequoia.driver.Connection)}
 * <br>- Ping <br>
 * no more used, was sent on a dedicated connection and silently received only
 * by (again)
 * {@link org.continuent.sequoia.controller.core.ControllerWorkerThread#run()}
 * <h2>Protocol Data Types</h2>
 * <strong>optUTF</strong> is a custom type defined like this:
 * 
 * <pre>
 * (boolean false) | (boolean true; UTF somestring)
 * </pre>
 * 
 * <h3>Sent types</h3>
 * <p>
 * Several commands send a SQL query. All SQL queries sent on the wire use the
 * same starting pattern, a <em>requestStub</em> defined below and in
 * {@link org.continuent.sequoia.controller.requests.AbstractRequest#AbstractRequest(String, boolean, int, String, int)}
 * <br>
 * <strong>requestStub</strong>
 * 
 * <pre>
 * UTF     request           : SQL query
 * boolean EscapeProcessing
 * UTF     LINE_SEPARATOR
 * Int     timeout
 * boolean autoCommit
 * boolean isDriverProcessed
 * </pre>
 * 
 * <p>
 * Queries that expect a result set (read commands mostly) send immediately
 * after the requestStub a <em>subsetLengths</em> parameter, of type: <br>
 * <strong>subsetLengths</strong>. See
 * {@link org.continuent.sequoia.controller.requests.AbstractRequest#receiveResultSetParams(org.continuent.sequoia.common.stream.DriverBufferedInputStream)}
 * 
 * <pre>
 * Int    maxRows
 * Int    fetchSize
 * </pre>
 * 
 * <p>
 * Depending on some configuration flag/state (shared by driver and controller),
 * most query commands add an optional <em>skeleton</em> parameter of type
 * optUTF.
 * <h3>Received types</h3>
 * <p>
 * Several commands receive a ResultSet of type: <br>
 * <strong>ResultSet</strong>
 * {@link org.continuent.sequoia.driver.DriverResultSet#DriverResultSet(org.continuent.sequoia.driver.Connection)}
 * 
 * <pre>
 *  {@link org.continuent.sequoia.common.protocol.Field Field}[]   fields
 *  {@link org.continuent.sequoia.common.protocol.TypeTag}[] java column types
 *  ArrayList   data
 *  optUTF      hasMoreData: cursor name
 * </pre> - <em>fields</em> is the description of the ResultSet columns. <br>-
 * <em>data</em> is the actual data of the ResultSet. Each element of this
 * list is an Object array holding one row of the ResultSet. The whole arraylist
 * is serialized using standard Java serialization/readUnshared().
 * <h3>Exceptions</h3>
 * For almost every command sent, the driver checks if the reply is an exception
 * serialized by the controller instead of the regular reply type. Most
 * exceptions are put on the wire by
 * {@link org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#run()}
 * TODO: finish the classification below. <br>
 * Exception reply recognized by the driver in: <br>
 * FetchNextResultSetRows, closeRemoteResultSet, RestoreConnectionState,
 * setAutoCommit, getCatalog, getCatalogs, setCatalog,
 * getControllerVersionNumber, DatabaseMetaDataGetTables,
 * DatabaseMetaDataGetColumns, DatabaseMetaDataGetPrimaryKeys,
 * DatabaseMetaDataGetProcedureColumns, DatabaseMetaDataGetTableTypes,
 * DatabaseMetaDataGetTablePrivileges, DatabaseMetaDataGetSchemas,
 * DatabaseMetaDataGetDatabaseProductName, DatabaseStaticMetadata <br>
 * Exception reply ignored by the driver in: Close, Reset <br>
 * Exceptions catched by instanceof(catch-all) default clause in: <br>
 * setAutoCommit, <br>
 * Commands not implemented at all by the driver: <br>
 * GetVirtualDatabaseName, <br>
 * TODO: <br>- exceptions and the server side (VirtualDatabaseWorkerThread)
 * <br>- double-check arguments and replies <br>- better topic ordering
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat </a>
 * @version 1.0
 */

public class Commands
{
  /** "Controller Ready" magic number. */
  public static final int ControllerPrompt = 0x1FA1501F;
  /** Command prefix sent before each command */
  public static final int CommandPrefix    = 0xB015CA;

  /**
   * ALL commands are used ONLY by classes:
   * {@link org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread}
   * and {@link org.continuent.sequoia.driver.Connection}
   * <p>
   * EXCEPT constant ProtocolVersion which is used ONLY by
   * {@link org.continuent.sequoia.controller.core.ControllerWorkerThread} and
   * constant Ping which is not used anymore. These constants do NOT collide
   * with the rest.
   */

  /**
   * Command used to create a new connection, while checking that driver and
   * controller are compatible with each other.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF dataBaseName<br>
   * ~argument UTF user <br>
   * ~argument UTF password
   * <p>
   * ~reply true | (false + failure message (vdb not found)) <br>
   * ~reply true | (false + failure message (authentication error)) <br>
   * <p>
   * ~argument UTF lineSeparator<br>
   * ~argument boolean persistentConnection<br>
   * <p>
   * ~if persistentConnection true<br>
   * ~reply false if no connection is available on the cluster | (true +
   * persistentConnectionId) <br>
   * 
   * @see org.continuent.sequoia.controller.core.ControllerWorkerThread#run()
   * @see org.continuent.sequoia.driver.ConnectionClosingThread#closeConnection(org.continuent.sequoia.driver.Connection)
   */
  public static final int ProtocolVersion  = (8 /* major */<< 16) + 13 /* minor */;

  /**
   * Return the major version number of a protocol
   * 
   * @param protocolVersion the protocol version number to extract the major
   *          from
   * @return the protocol major version number
   */
  public static final int getProtocolMajorVersion(int protocolVersion)
  {
    return protocolVersion >>> 16;
  }

  /**
   * Return the minor version number of a protocol
   * 
   * @param protocolVersion the protocol version number to extract the minor
   *          from
   * @return the protocol minor version number
   */
  public static final int getProtocolMinorVersion(int protocolVersion)
  {
    return protocolVersion & 0xFFFF;
  }

  /**
   * Ping was used by the ControllerPingThread to check if a controller is back
   * online after a failure. Since the new ping mechanism, this command is not
   * used anymore. It is kept for backward compatibility and for future needs
   */
  public static final int Ping                                         = -1;

  /**
   * Performs a read request and returns the reply.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument Int
   * {@link org.continuent.sequoia.controller.requests.RequestType} <br>
   * ~argument requestStub <br>
   * ~argument subsetLengths <br>
   * ~argument optUTF cursorname <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply ResultSet
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#statementExecuteQuery(org.continuent.sequoia.controller.requests.SelectRequest)
   */
  public static final int StatementExecuteQuery                        = 0;

  /**
   * Performs a write request and returns the number of rows affected.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument Int
   * {@link org.continuent.sequoia.controller.requests.RequestType} <br>
   * ~argument requestStub <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply request id (long) <br>
   * ~reply nbRows (int)
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#statementExecuteUpdate(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public static final int StatementExecuteUpdate                       = 1;

  /**
   * Performs a write request and returns the auto generated keys.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument Int
   * {@link org.continuent.sequoia.controller.requests.RequestType} <br>
   * ~argument requestStub <br>
   * ~argument subsetLengths <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply request id (long) <br>
   * ~reply nbRows <br>
   * ~reply ResultSet
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#statementExecuteUpdateWithKeys()
   */
  public static final int StatementExecuteUpdateWithKeys               = 2;

  /**
   * Calls a stored procedure and returns the reply (ResultSet).
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument requestStub <br>
   * ~argument subsetLengths <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply request id (long)<br>
   * ~reply ResultSet
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#callableStatementExecuteQuery(org.continuent.sequoia.controller.requests.StoredProcedure)
   * @deprecated since 2.7
   */
  public static final int CallableStatementExecuteQuery                = 3;

  /**
   * Calls a stored procedure and returns the number of rows affected (write
   * query).
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument requestStub <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply long request id <br>
   * ~reply int nbRows
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#callableStatementExecuteQuery(org.continuent.sequoia.controller.requests.StoredProcedure)
   * @deprecated since 2.7
   */
  public static final int CallableStatementExecuteUpdate               = 4;

  /**
   * Calls a stored procedure using CallableStatement.execute(). This returns an
   * arbitrary number of update counts and ResultSets
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument requestStub <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply long request id <br>
   * ~reply boolean hasResultSet (true means ResultSet follows, false means Int
   * follows)<br>
   * ~reply Int nbRows (-1 if no more results)<br>
   * ~reply ResultSet
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#callableStatementExecute(org.continuent.sequoia.controller.requests.StoredProcedure)
   * @deprecated since 2.7
   */
  public static final int CallableStatementExecute                     = 5;

  /**
   * Execute a request using Statement.execute(). This returns an arbitrary
   * number of update counts and ResultSets
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument requestStub <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply long request id <br>
   * ~reply boolean hasResultSet (true means ResultSet follows, false means Int
   * follows) <br>
   * ~reply Int nbRows (-1 if no more results) <br>
   * ~reply ResultSet
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#statementExecute(org.continuent.sequoia.controller.requests.AbstractRequest)
   */
  public static final int StatementExecute                             = 6;

  /**
   * Calls a stored procedure with IN/OUT and/or named parameters and returns
   * the reply (ResultSet, OUT parameters, named parameters objects).
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument requestStub <br>
   * ~argument subsetLengths <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply request id (long)<br>
   * ~reply ResultSet<br>
   * ~reply int OUT parameter index (0 if no more OUT parameter to be sent)<br>
   * ~reply Object OUT parameter value (if index != 0)<br>
   * ~reply String parameter name ("0" if no more OUT parameter to be sent)<br>
   * ~reply Object named parameter value (if name != "0")<br>
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#callableStatementExecuteQuery(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public static final int CallableStatementExecuteQueryWithParameters  = 7;

  /**
   * Calls a stored procedure and returns the number of rows affected (write
   * query).
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument requestStub <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply long request id <br>
   * ~reply int nbRows (update count) <br>
   * ~reply int OUT parameter index (0 if no more OUT parameter to be sent)<br>
   * ~reply Object OUT parameter value (if index != 0)<br>
   * ~reply String parameter name ("0" if no more OUT parameter to be sent)<br>
   * ~reply Object named parameter value (if name != "0")<br>
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#callableStatementExecuteQuery(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public static final int CallableStatementExecuteUpdateWithParameters = 8;

  /**
   * Calls a stored procedure using CallableStatement.execute(). This returns an
   * arbitrary number of update counts and ResultSets
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument requestStub <br>
   * ~argument optUTF skeleton
   * <p>
   * <br>
   * ~reply long request id <br>
   * ~reply boolean hasResultSet (true means ResultSet follows, false means Int
   * follows) <br>
   * ~reply Int nbRows (-1 if no more results)<br>
   * ~reply ResultSet<br>
   * Once all results have been fetched, we fetch OUT and named parameters as
   * follows:<br>
   * ~reply int OUT parameter index (0 if no more OUT parameter to be sent)<br>
   * ~reply Object OUT parameter value (if index != 0)<br>
   * ~reply String parameter name ("0" if no more OUT parameter to be sent)<br>
   * ~reply Object named parameter value (if name != "0")<br>
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#callableStatementExecute(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public static final int CallableStatementExecuteWithParameters       = 9;

  /*
   * Failover commands
   */

  /**
   * Try to retrieve the result of a previouly executed query using
   * executeQuery().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument long request id
   * <p>
   * <br>
   * ~reply ResultSet (or null)
   */
  public static final int RetrieveExecuteQueryResult                   = 10;

  /**
   * Try to retrieve the result of a previouly executed query.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument long request id
   * <p>
   * <br>
   * ~reply int nbRows (-1 if no result has been found)
   */
  public static final int RetrieveExecuteUpdateResult                  = 11;

  /**
   * Try to retrieve the result of a previouly executed query.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument long request id
   * <p>
   * <br>
   * ~reply null if no result was found <br>
   * ~reply long request id <br>
   * ~reply boolean hasResultSet (true means ResultSet follows, false means Int
   * follows) <br>
   * ~reply Int nbRows (-1 if no more results) <br>
   * ~reply ResultSet
   */
  public static final int RetrieveExecuteResult                        = 12;

  /**
   * Try to retrieve the result of a previouly executed query.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument long request id
   * <p>
   * <br>
   * ~reply int nbRows (-1 if no result has been found) <br>
   * ~reply ResultSet (if nbRows was != -1)
   */
  public static final int RetrieveExecuteUpdateWithKeysResult          = 13;

  /**
   * Try to retrieve the result of a previouly executed commit. If the commit
   * was not executed before, the controller will execute it and return the
   * result which is the transaction id acknowledgement.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply Long transactionId: id of commited transaction
   */
  public static final int RetrieveCommitResult                         = 14;

  /**
   * Try to retrieve the result of a previouly executed rollback. If the
   * rollback was not executed before, the controller will execute it and return
   * the result which is the transaction id acknowledgement.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply Long transactionId: id of rollbacked transaction
   */
  public static final int RetrieveRollbackResult                       = 15;

  /**
   * Try to retrieve the result of a previouly executed stored procedure with
   * IN/OUT and/or named parameters using executeQuery().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument long request id
   * <p>
   * <br>
   * ~reply ResultSet (or null) ~reply int number of OUT parameters<br>
   * ~reply Object OUT parameter values (nb of times indicated above)<br>
   * ~reply int number of named parameters<br>
   * ~reply String parameter name<br>
   * ~reply Object named parameter value (nb of times indicated above)<br>
   */
  public static final int RetrieveExecuteQueryResultWithParameters     = 16;

  /**
   * Try to retrieve the result of a previouly executed stored procedure with
   * IN/OUT and/or named parameters.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument long request id
   * <p>
   * <br>
   * ~reply int nbRows (-1 if no result has been found) ~reply int number of OUT
   * parameters<br>
   * ~reply Object OUT parameter values (nb of times indicated above)<br>
   * ~reply int number of named parameters<br>
   * ~reply String parameter name<br>
   * ~reply Object named parameter value (nb of times indicated above)<br>
   */
  public static final int RetrieveExecuteUpdateResultWithParameters    = 17;

  /**
   * Try to retrieve the result of a previouly executed stored procedure with
   * IN/OUT and/or named parameters.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument long request id
   * <p>
   * <br>
   * ~reply null if no result was found <br>
   * ~reply long request id <br>
   * ~reply boolean hasResultSet (true means ResultSet follows, false means Int
   * follows) <br>
   * ~reply Int nbRows (-1 if no more results) <br>
   * ~reply ResultSet<br>
   * Once all results have been fetched, we fetch OUT and named parameters as
   * follows:<br>
   * ~reply int number of OUT parameters<br>
   * ~reply Object OUT parameter values (nb of times indicated above)<br>
   * ~reply int number of named parameters<br>
   * ~reply String parameter name<br>
   * ~reply Object named parameter value (nb of times indicated above)<br>
   */
  public static final int RetrieveExecuteResultWithParameters          = 18;

  /**
   * Begins a new transaction and returns the corresponding transaction
   * identifier. This method is called from the driver when
   * {@link org.continuent.sequoia.driver.Connection#setAutoCommit(boolean)}is
   * called with <code>false</code> argument.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply Long transactionId
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#begin(String,
   *      boolean, long)
   */
  public static final int Begin                                        = 20;

  /**
   * Commits the current transaction.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply Long transactionId: id of commited transaction
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#commit(long,
   *      boolean, boolean)
   */
  public static final int Commit                                       = 21;

  /**
   * Rollbacks the current transaction.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply Long transactionId: id of rollbacked transaction
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#rollback(long,
   *      boolean)
   */
  public static final int Rollback                                     = 22;

  /**
   * Sets a named savepoint to a transaction given its id
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#setSavepoint(long,
   *      String)
   */
  public static final int SetNamedSavepoint                            = 23;

  /**
   * Sets a unnamed savepoint to a transaction given its id
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#setSavepoint(long)
   */
  public static final int SetUnnamedSavepoint                          = 24;

  /**
   * Releases a savepoint from a transaction given its id
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#releaseSavepoint(long,
   *      String)
   */
  public static final int ReleaseSavepoint                             = 25;

  /**
   * Rollbacks the current transaction back to the given savepoint
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#rollbackToSavepoint()
   */
  public static final int RollbackToSavepoint                          = 26;

  /*
   * Connection management
   */

  /**
   * Close the connection. The controller replies a CommandCompleted
   * SequoiaException, ignored by the driver.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply &lt;anything&gt;
   */
  public static final int Close                                        = 30;

  /**
   * Reset the connection.
   * <p>
   * ~commandcode {@value}
   */
  public static final int Reset                                        = 31;

  /**
   * Fetch next rows of data for ResultSet streaming.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF cursorName <br>
   * ~argument Int fetchSize
   * <p>
   * <br>
   * ~reply ArrayList data <br>
   * ~reply boolean hasMoreData
   */
  public static final int FetchNextResultSetRows                       = 32;

  /**
   * Closes a remote ResultSet that was opened for streaming.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF cursorName
   * <p>
   * <br>
   * ~reply SequoiaException CommandCompleted
   */
  public static final int CloseRemoteResultSet                         = 33;

  /**
   * Restore a connection state after an automatic reconnection. Tell the
   * controller if we were in autoCommit mode (i.e.: a transaction was
   * <em>not</em> started), and if we were not then give the current
   * transactionId. Warning: this is not an optUTF type at all.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument boolean writeExecutedInTransaction<br>
   * ~argument (boolean true) | (boolean false; Long transactionId) <br>
   * ~argument (boolean false) | (boolean true; persistentConnectionId) <br>
   */
  public static final int RestoreConnectionState                       = 34;

  /**
   * Retrieve the catalog (database) we are connected to.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply String vdbName
   * 
   * @see org.continuent.sequoia.driver.Connection#getCatalog()
   */
  public static final int ConnectionGetCatalog                         = 36;

  /**
   * Retrieve the list of available catalogs (databases).
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply ResultSet virtualDatabasesList
   * 
   * @see org.continuent.sequoia.driver.Connection#getCatalogs()
   */
  public static final int ConnectionGetCatalogs                        = 37;

  /**
   * Connect to another catalog/database (as the same user).
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF catalog
   * <p>
   * <br>
   * ~reply boolean isValidCatalog
   * 
   * @see org.continuent.sequoia.driver.Connection#setCatalog(String)
   */
  public static final int ConnectionSetCatalog                         = 38;

  /**
   * Set the new transaction isolation level to use for this connection.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument int transaction isolation level
   * <p>
   * <br>
   * ~reply boolean true (meaning: not an exception)
   * 
   * @see org.continuent.sequoia.driver.Connection#setTransactionIsolation(int)
   */
  public static final int SetTransactionIsolation                      = 39;

  /**
   * Retrieves the SQLWarning chain associated to a persistent connection to a
   * backend.
   * <p>
   * ~commandcode {@value}<br>
   * ~argument int persistent connection id
   * <p>
   * <br>
   * ~reply BackendDriverException SQLWarning chain wrapped into a bde
   * 
   * @see org.continuent.sequoia.driver.Connection#getWarnings()
   */
  public static final int ConnectionGetWarnings                        = 40;

  /**
   * Clears the SQLWarning chain associated to a persistent connection
   * <p>
   * ~commandcode {@value}<br>
   * ~argument int persistent connection id
   * <p>
   * <br>
   * ~reply boolean true (meaning: not an exception)
   * 
   * @see org.continuent.sequoia.driver.Connection#clearWarnings()
   */
  public static final int ConnectionClearWarnings                      = 41;

  /**
   * Set the connection readonly status.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument boolean readOnly value
   * <p>
   * <br>
   * ~reply boolean true (meaning: not an exception)
   * 
   * @see org.continuent.sequoia.driver.Connection#setReadOnly(boolean)
   */
  public static final int SetReadOnly                                  = 42;

  /*
   * MetaData functions
   */

  /**
   * Gets the virtual database name to be used by the client (Sequoia driver).
   * It currently returns the same result as ConnectionGetCatalog(). It is
   * currently never used by the driver.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply String dbName
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#getVirtualDatabaseName()
   */
  public static final int GetVirtualDatabaseName                       = 50;

  /**
   * Gets the controller version number.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply String controllerVersion
   * 
   * @see org.continuent.sequoia.controller.core.Controller#getVersionNumber()
   */
  public static final int GetControllerVersionNumber                   = 51;

  /**
   * Used to get the schema tables by calling DatabaseMetaData.getTables().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF catalog <br>
   * ~argument UTF schemaPattern <br>
   * ~argument UTF tableNamePattern <br>
   * ~argument String[] types
   * <p>
   * <br>
   * ~reply ResultSet tables
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetTables()
   */
  public static final int DatabaseMetaDataGetTables                    = 52;

  /**
   * Used to get the schema columns by calling DatabaseMetaData.getColumns().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF catalog <br>
   * ~argument UTF schemaPattern <br>
   * ~argument UTF tableNamePattern <br>
   * ~argument UTF columnNamePattern
   * <p>
   * <br>
   * ~reply ResultSet schemaColumns
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetColumns()
   */
  public static final int DatabaseMetaDataGetColumns                   = 53;

  /**
   * Used to get the schema primary keys by calling
   * DatabaseMetaData.getColumns().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF catalog <br>
   * ~argument UTF schemaPattern <br>
   * ~argument UTF tableNamePattern
   * <p>
   * <br>
   * ~reply ResultSet pKeys
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetPrimaryKeys()
   */
  public static final int DatabaseMetaDataGetPrimaryKeys               = 54;

  /**
   * Used to get the schema procedures by calling
   * DatabaseMetaData.getProcedures().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF catalog <br>
   * ~argument UTF schemaPattern <br>
   * ~argument UTF procedureNamePattern
   * <p>
   * <br>
   * ~reply ResultSet procedures
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetProcedures()
   */
  public static final int DatabaseMetaDataGetProcedures                = 55;

  /**
   * Used to get the schema procedure columns by calling
   * DatabaseMetaData.getProcedureColumns().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF catalog <br>
   * ~argument UTF schemaPattern <br>
   * ~argument UTF procedureNamePattern <br>
   * ~argument UTF columnNamePattern
   * <p>
   * <br>
   * ~reply ResultSet procColumns
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetProcedureColumns()
   */
  public static final int DatabaseMetaDataGetProcedureColumns          = 56;

  /**
   * Retrieve the database table types.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply ResultSet tableTypes
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetTableTypes()
   */
  public static final int DatabaseMetaDataGetTableTypes                = 58;

  /**
   * Retrieve the table privileges.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF catalog <br>
   * ~argument UTF schemaPattern <br>
   * ~argument UTF tableNamePattern
   * <p>
   * <br>
   * ~reply ResultSet accessRights
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetTablePrivileges()
   */
  public static final int DatabaseMetaDataGetTablePrivileges           = 59;

  /**
   * Retrieve the schemas.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply ResultSet schemas
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetSchemas()
   */
  public static final int DatabaseMetaDataGetSchemas                   = 60;

  /**
   * Retrieve the database product name.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~reply String productName
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetDatabaseProductName()
   */
  public static final int DatabaseMetaDataGetDatabaseProductName       = 61;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetAttributes()
   */
  public static final int DatabaseMetaDataGetAttributes                = 62;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetBestRowIdentifier()
   */
  public static final int DatabaseMetaDataGetBestRowIdentifier         = 63;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetColumnPrivileges()
   */
  public static final int DatabaseMetaDataGetColumnPrivileges          = 64;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetCrossReference()
   */
  public static final int DatabaseMetaDataGetCrossReference            = 65;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetExportedKeys()
   */
  public static final int DatabaseMetaDataGetExportedKeys              = 66;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetImportedKeys()
   */
  public static final int DatabaseMetaDataGetImportedKeys              = 67;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetIndexInfo()
   */
  public static final int DatabaseMetaDataGetIndexInfo                 = 68;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetSuperTables()
   */
  public static final int DatabaseMetaDataGetSuperTables               = 69;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetSuperTypes()
   */
  public static final int DatabaseMetaDataGetSuperTypes                = 70;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetTypeInfo()
   */
  public static final int DatabaseMetaDataGetTypeInfo                  = 71;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseMetaDataGetUDTs()
   */
  public static final int DatabaseMetaDataGetUDTs                      = 72;

  /**
   * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public static final int DatabaseMetaDataGetVersionColumns            = 73;

  /**
   * Retrieve one value from the virtual database metadata.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument UTF: serialized DatabaseMetaData method call.
   * <p>
   * <br>
   * ~reply Integer|Boolean|String|other ? value
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseStaticMetadata()
   */
  public static final int DatabaseStaticMetadata                       = 80;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#preparedStatementGetMetaData()
   */
  public static final int PreparedStatementGetMetaData                 = 81;

  /**
   * Try to retrieve the result of a previouly executed release savepoint using
   * releaseSavepoint().
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument String savepoint name
   * <p>
   * <br>
   * ~reply boolean: true if the savepoint was removed, false if it is still
   * there.
   */
  public static final int RetrieveReleaseSavepoint                     = 82;

  /**
   * Retrieve MetaData for PreparedStatement parameters.
   * <p>
   * ~commandcode {@value}
   * <p>
   * <br>
   * ~argument String sql template
   * <p>
   * <br>
   * ~reply (boolean false) | (boolean true; ParameterMetaData metadata)
   */
  public static final int PreparedStatementGetParameterMetaData        = 83;
}
