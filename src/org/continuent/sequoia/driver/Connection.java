/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Contributor(s): Julie Marguerite, Guillaume Bort, Duncan Smith, Vadim Kassin,
 * Nicolas Modrzyk, Jaco Swart,  Jean-Bernard van Zuylen
 * Completely refactored by Marc Herbert to remove the use of Java serialization.
 */

package org.continuent.sequoia.driver;

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.continuent.sequoia.common.exceptions.AuthenticationException;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoMoreControllerException;
import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.exceptions.ProtocolException;
import org.continuent.sequoia.common.exceptions.driver.DriverIOException;
import org.continuent.sequoia.common.exceptions.driver.DriverSQLException;
import org.continuent.sequoia.common.exceptions.driver.VirtualDatabaseUnavailableException;
import org.continuent.sequoia.common.exceptions.driver.protocol.BackendDriverException;
import org.continuent.sequoia.common.exceptions.driver.protocol.ControllerCoreException;
import org.continuent.sequoia.common.exceptions.driver.protocol.SerializableException;
import org.continuent.sequoia.common.protocol.Commands;
import org.continuent.sequoia.common.protocol.SQLDataSerialization;
import org.continuent.sequoia.common.protocol.TypeTag;
import org.continuent.sequoia.common.protocol.SQLDataSerialization.Serializer;
import org.continuent.sequoia.common.sql.Request;
import org.continuent.sequoia.common.sql.RequestWithResultSetParameters;
import org.continuent.sequoia.common.sql.metadata.SequoiaParameterMetaData;
import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;

/**
 * This class implements the communication protocol to the Controller.
 * <p>
 * Connection.java was inspired from the PostgreSQL JDBC driver by Peter T.
 * Mount.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:vadim@kase.kz">Vadim Kassin </a>
 * @author <a href="mailto:duncan@mightybot.com">Duncan Smith </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:jaco.swart@iblocks.co.uk">Jaco Swart </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:Gilles.Rayrat@continuent.com">Gilles Rayrat </a>
 * @version 2.0
 */
public class Connection implements java.sql.Connection
{
  /** Status of the connection. */
  protected boolean                    isClosed                            = false;

  protected String                     escapeChar;

  /** Sequoia controller we are connected to */
  protected ControllerInfo             controllerInfo                      = null;

  // ConnectionClosingThread
  /** Driver that created us. */
  protected Driver                     driver                              = null;

  /** Connection with the controller. */
  protected Socket                     socket;
  /** Socket input stream. */
  protected DriverBufferedInputStream  socketInput;
  /** Socket output stream. */
  protected DriverBufferedOutputStream socketOutput;

  /** @see org.continuent.sequoia.controller.core.ControllerConstants#SQL_SHORT_FORM_LENGTH */
  public static final int              ABBREV_REQUEST_LENGTH               = 40;

  // used by Statement (and maybe also by some others _below_)
  static final String                  LINE_SEPARATOR                      = System
                                                                               .getProperty("line.separator");

  // Member variables describing the state of the connection

  /** Commit mode of the connection (<code>true</code>= automatic). */
  protected boolean                    autoCommit                          = true;

  /** Is the connection in read-only mode ? */
  protected boolean                    readOnly                            = false;

  /** Has a write request been executed in the current transaction? */
  boolean                              writeExecutedInTransaction          = false;

  /** Default transaction isolation level if the user has not enforced one */
  public static final int              DEFAULT_TRANSACTION_ISOLATION_LEVEL = -1;

  /** Current transaction isolation level. */
  protected int                        isolationLevel                      = DEFAULT_TRANSACTION_ISOLATION_LEVEL;

  /** transaction identifier. */
  protected long                       transactionId                       = 0;

  /** List of <code>Warnings</code> for this connection. */
  protected SQLWarning                 firstWarning                        = null;

  /** ResultSet holdability (JDBC 3) */
  protected int                        holdability                         = HOLDABILITY_NOT_SET;
  private static final int             HOLDABILITY_NOT_SET                 = -1;

  /** Meta-data of Sequoia connections. */
  protected DatabaseMetaData           metaData                            = null;

  /** Parsed URL to the database. */
  private final SequoiaUrl             sequoiaUrl;

  /** Virtual database user used for this connection. */
  protected String                     vdbUser                             = null;
  protected String                     vdbPassword                         = null;

  private boolean                      connectionPooling;

  // Escape processing tuning
  protected boolean                    escapeBackslash;
  protected boolean                    escapeSingleQuote;

  // flag to check if a new transaction must be
  // started before executing any statement
  private boolean                      mustBeginTransaction                = false;

  // True if the connection must be persisted on all cluster backends even when
  // autoCommit=true
  private boolean                      persistentConnection;

  // Persistent connection identifier if persistentConnection is true
  private long                         persistentConnectionId;

  // Do we want SQL Warnings ?
  private boolean                      retrieveSQLWarnings                 = false;

  // Do we force generated keys to be retrieved?
  private boolean                      alwaysGetGeneratedKeys              = false;
  
  // Default fetch size.  If client specifies a non-zero value, we use that instead
  // of defaulting to 0. 
  private int                          defaultFetchSize                    = 0;

  /*****************************************************************************
   * *************** * Constructor and get/set methods * ***********************
   * ****************************************************************************
   */

  /**
   * Creates a new <code>Connection</code> instance.
   * 
   * @param driver calling driver
   * @param socket connection with the controller
   * @param in socket input stream
   * @param out socket output stream
   * @param sequoiaUrl Sequoia URL of the database
   * @param controller controller we are connected to
   * @param userName user login
   * @param password user password
   * @throws AuthenticationException login error
   * @throws IOException stream error
   * @throws SQLException if the virtual database is not available on the
   *           controller
   */
  Connection(Driver driver, Socket socket, DriverBufferedInputStream in,
      DriverBufferedOutputStream out, SequoiaUrl sequoiaUrl,
      ControllerInfo controller, String userName, String password)
      throws AuthenticationException, IOException,
      VirtualDatabaseUnavailableException
  {
    this.driver = driver;
    this.socket = socket;
    this.socketInput = in;
    this.socketOutput = out;
    this.sequoiaUrl = sequoiaUrl;
    this.controllerInfo = controller;
    this.vdbUser = userName;
    this.vdbPassword = password;

    escapeBackslash = driver.getEscapeBackslash();
    escapeChar = driver.getEscapeChar();
    escapeSingleQuote = driver.getEscapeSingleQuote();
    connectionPooling = driver.getConnectionPooling();
    persistentConnection = driver.getPersistentConnection();
    retrieveSQLWarnings = driver.getRetrieveSQLWarnings();
    alwaysGetGeneratedKeys = driver.getRetrieveGeneratedKeys();

    // Is virtual database available?
    if (!in.readBoolean()) // failed
      throw new VirtualDatabaseUnavailableException(in.readLongUTF());

    // Is authentication successful?
    if (!in.readBoolean()) // failed
      throw new AuthenticationException(in.readLongUTF());

    setUrlParametersOptionsOnConnection(sequoiaUrl);

    out.writeLongUTF(LINE_SEPARATOR);
    out.writeBoolean(persistentConnection);
    out.flush();

    if (persistentConnection)
    {
      if (in.readBoolean())
        persistentConnectionId = in.readLong();
      else
        throw new AuthenticationException(
            "No more persistent connections available for virtual database "
                + sequoiaUrl.getDatabaseName() + "[url=" + sequoiaUrl + "]");
    }

    out.writeBoolean(retrieveSQLWarnings);
    out.flush();

    if (sequoiaUrl.isDebugEnabled())
      System.out.println("New connection:" + this.toString());
  }

  /**
   * Set SequoiaUrl parameters options on connection.
   * 
   * @param sequoiaUrl the Sequoia URL to use
   */
  private void setUrlParametersOptionsOnConnection(SequoiaUrl sequoiaUrl)
  {
    HashMap sequoiaUrlParameters = sequoiaUrl.getParameters();

    String escapeBaskslash = (String) sequoiaUrlParameters
        .get(Driver.ESCAPE_BACKSLASH_PROPERTY);
    if (escapeBaskslash != null)
      setEscapeBackslash(new Boolean(escapeBaskslash).booleanValue());

    String escapeQuote = (String) sequoiaUrlParameters
        .get(Driver.ESCAPE_SINGLE_QUOTE_PROPERTY);
    if (escapeQuote != null)
      setEscapeSingleQuote(new Boolean(escapeQuote).booleanValue());

    String escapeCharacter = (String) sequoiaUrlParameters
        .get(Driver.ESCAPE_CHARACTER_PROPERTY);
    if (escapeCharacter != null)
      setEscapeChar(escapeCharacter);

    // true if transparent connection pooling must be used
    String connPool = (String) sequoiaUrlParameters
        .get(Driver.CONNECTION_POOLING_PROPERTY);
    if (connPool != null)
      this.connectionPooling = "true".equals(connPool);

    String persistentConn = (String) sequoiaUrlParameters
        .get(Driver.PERSISTENT_CONNECTION_PROPERTY);
    if (persistentConn != null)
      this.persistentConnection = "true".equals(persistentConn);

    String retSQLWarns = (String) sequoiaUrlParameters
        .get(Driver.RETRIEVE_SQL_WARNINGS_PROPERTY);
    if (retSQLWarns != null)
      this.retrieveSQLWarnings = "true".equals(retSQLWarns);

    String retGeneratedKeys = (String) sequoiaUrlParameters
        .get(Driver.ALWAYS_RETRIEVE_GENERATED_KEYS_PROPERTY);
    if (retGeneratedKeys != null)
      this.alwaysGetGeneratedKeys = "true".equals(retGeneratedKeys);

    String retDefaultFetchSize = (String) sequoiaUrlParameters
        .get(Driver.DEFAULT_FETCH_SIZE_PROPERTY);
    if (retDefaultFetchSize != null)
    {
      try
      {
        this.defaultFetchSize = Integer.parseInt(retDefaultFetchSize);
      }
      catch (NumberFormatException e)
      {
        // Invalid or unparsable settings are ignored. 
      }
    }

    if (sequoiaUrl.isDebugEnabled())
    {
      // Give a warning for unrecognized driver options in the URL
      // (only in the URL: unknown properties have been filtered out)
      for (Iterator iter = sequoiaUrlParameters.entrySet().iterator(); iter
          .hasNext();)
      {
        Map.Entry e = (Map.Entry) iter.next();
        String param = (String) e.getKey();
        if (!Driver.driverPropertiesNames.contains(param))
          System.out.println("Unrecognized driver parameter: " + param + " = "
              + (String) e.getValue());
      }
    }
  }

  /**
   * Get the information about the controller we are connected to
   * 
   * @return <code>ControllerInfo</code> object of the controller
   */
  public ControllerInfo getControllerInfo()
  {
    return controllerInfo;
  }

  /**
   * Gets the password used to login to the database.
   * 
   * @return password
   */
  public String getPassword()
  {
    return vdbPassword;
  }

  /**
   * Gets the untouched String URL that was passed by the client application
   * 
   * @return value of url.
   */
  public String getUrl()
  {
    return sequoiaUrl.getUrl();
  }

  /**
   * Gets the parsed Sequoia URL, including merged properties
   * 
   * @return the parsed Sequoia URL
   */
  SequoiaUrl getSequoiaUrl()
  {
    return sequoiaUrl;
  }

  /**
   * Gets the user name used to login to the database.
   * 
   * @return login name
   */
  public String getUserName()
  {
    return vdbUser;
  }

  /**
   * Returns the escapeBackslash value.
   * 
   * @return Returns the escapeBackslash.
   */
  public boolean isEscapeBackslash()
  {
    return escapeBackslash;
  }

  /**
   * Sets the escapeBackslash value.
   * 
   * @param escapeBackslash The escapeBackslash to set.
   */
  public void setEscapeBackslash(boolean escapeBackslash)
  {
    this.escapeBackslash = escapeBackslash;
  }

  /**
   * Returns the escapeSingleQuote value.
   * 
   * @return Returns the escapeSingleQuote.
   */
  public boolean isEscapeSingleQuote()
  {
    return escapeSingleQuote;
  }

  /**
   * Sets the escapeSingleQuote value.
   * 
   * @param escapeSingleQuote The escapeSingleQuote to set.
   */
  public void setEscapeSingleQuote(boolean escapeSingleQuote)
  {
    this.escapeSingleQuote = escapeSingleQuote;
  }

  /**
   * Sets the escapeCharacter value
   * 
   * @param escapeChar the escapeChar value to set
   */
  public void setEscapeChar(String escapeChar)
  {
    this.escapeChar = escapeChar;
  }

  /**
   * @return Returns the escapeChar.
   */
  public String getEscapeChar()
  {
    return escapeChar;
  }

  /**
   * Returns the connectionPooling value.
   * 
   * @return Returns the connectionPooling.
   */
  public boolean isConnectionPooling()
  {
    return connectionPooling;
  }

  /**
   * Sets the connectionPooling value.
   * 
   * @param connectionPooling The connectionPooling to set.
   */
  public void setConnectionPooling(boolean connectionPooling)
  {
    this.connectionPooling = connectionPooling;
  }

  /**
   * Returns the alwaysGetGeneratedKeys value.
   * 
   * @return Returns the alwaysGetGeneratedKeys.
   */
  boolean isAlwaysGettingGeneratedKeys()
  {
    return alwaysGetGeneratedKeys;
  }

  /**
   * Returns the default fetch size.  0 means there is no 
   * default. 
   */
  int getDefaultFetchSize()
  {
    return defaultFetchSize; 
  }
  
  //
  // java.sql.Connection implementation
  //

  /**
   * After this call, <code>getWarnings()</code> returns <code>null</code>
   * until a new call to getWarnings() on this connection.
   * 
   * @exception SQLException if a database access error occurs
   */
  public void clearWarnings() throws SQLException
  {
    if (!persistentConnection || !retrieveSQLWarnings)
      // nop
      return;
    if (isClosed)
    {
      // on a closed connection, just reset the warnings
      // jdbc spec doesn't ask to throw SQLException
      firstWarning = null;
      return;
    }

    try
    {
      sendCommand(Commands.ConnectionClearWarnings);
      socketOutput.writeLong(persistentConnectionId);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName());
      // forget the ack, we just need to know if an exception occured
      receiveBooleanOrException();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      { // Connection failed, try to reconnect and re-send command
        reconnect();
        clearWarnings();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while clearWarnings() and automatic reconnect failed("
                + e1 + ")", e1);
      }
    }

  }

  /**
   * Releases the connection. In fact, the connection is marked to be released
   * but will be effectively closed by the <code>ConnectionClosingThread</code>
   * if the connection has not been reused before.
   * 
   * @exception DriverSQLException if an error occurs
   */
  public void close() throws DriverSQLException
  {
    synchronized (this) // Wait until other methods/Commands are done
    {
      // Spec says:
      // Calling the method close on a Connection object that is already closed
      // is a no-op.
      if (isClosed)
        return;
      isClosed = true;
      /*
       * All JDBC entry points (methods) of this Connection have to
       * throwSQLExceptionIfClosed(). Relaxed: at least every JDBC method _with
       * some side-effect_ has to throwSQLExceptionIfClosed(). So now we are
       * safe and can leave the lock, since they will fail anyway.
       */
    }

    if (connectionPooling && !persistentConnection)
    { // Try to pool the connection for later reuse
      // Persistent connections are not pooled to free resources right away
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Resetting connection and adding it to the pool");
      autoCommit = true;
      mustBeginTransaction = false;
      readOnly = false;
      writeExecutedInTransaction = false;
      isolationLevel = DEFAULT_TRANSACTION_ISOLATION_LEVEL;
      try
      {
        sendCommand(Commands.Reset);
        socketOutput.flush();
        if (socketInput != null)
        {
          // Wait for the controller to receive the reset, in order to have
          // an exception thrown if there are no more controllers
          receiveBooleanOrException();
        }
      }
      catch (Exception ignored)
      {
        // Try to reconnect to inform other controllers that we are reseting
        // this connection. Do it only once to avoid endless loops
        reconnect();
        try
        {
          sendCommand(Commands.Reset);
          socketOutput.flush();
          if (socketInput != null)
          {
            // Wait for the controller to receive the reset, in order to have
            // an exception thrown if there are no more controllers
            receiveBooleanOrException();
          }
        }
        catch (Exception e)
        {
          // Ok, no way to reset connection on the controller side, let's throw
          // the exception
          throw new DriverSQLException("Error while closing the connection\n"
              + e.getLocalizedMessage(), e);
        }
      }

      // only one (Connection) accessing the pool at a time
      synchronized (driver.pendingConnectionClosing)
      {
        if (!driver.connectionClosingThreadisAlive)
        { // First connection to close, start a new closing thread
          if (sequoiaUrl.isDebugEnabled())
            System.out.println("Starting a new connection closing thread");
          ConnectionClosingThread t = new ConnectionClosingThread(driver);
          t.start();
        }
        // Add to the list
        driver.pendingConnectionClosing.add(this);
      }
    }
    else
    { // Close connection
      try
      {
        // driver = null; // probably useless since we use now
        // throwSQLExceptionIfClosed(), but
        // harmless anyway
        if (socketOutput != null)
        {
          if (sequoiaUrl.isDebugEnabled())
            System.out.println("Closing connection");
          sendCommand(Commands.Close);
          socketOutput.flush();
          if (socketInput != null)
          { // Wait for the controller to receive the connection and close the
            // stream. If we do not wait for the controller ack, the connection
            // is closed on the controller before the closing is handled which
            // results in an ugly warning message on the controller side. We are
            // not in a hurry when closing the connection so let do the things
            // nicely!
            receiveBooleanOrException();
            socketInput.close();
          }
          socketOutput.close();
        }

      }
      catch (Exception ignore)
      {
        // we attempt to reconnect to another controller:
        // since the connection was persistent, controllers
        // must be informed of the close operation so that
        // they can clean up their resources properly
        reconnect();
        close();
      }
    }
  }

  /**
   * Makes all changes made since the previous commit/rollback permanent and
   * releases any database locks currently held by the <code>Connection</code>.
   * This method should only be used when auto-commit has been disabled. (If
   * <code>autoCommit</code>== <code>true</code>, then we throw a
   * DriverSQLException).
   * 
   * @exception DriverSQLException if a database access error occurs or the
   *              connection is in autocommit mode
   * @see Connection#setAutoCommit(boolean)
   */
  public synchronized void commit() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    if (autoCommit)
    {
      if (isCommitInAutoCommitAllowed())
      {
        return; // nothing to do
      }
      else
      {
        throw new DriverSQLException(
            "Trying to commit a connection in autocommit mode");
      }
    }
    // Check if we are not committing an empty transaction (not started yet)
    if (mustBeginTransaction)
      return;

    doCommit();
  }

  private void doCommit() throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.Commit);
      socketOutput.flush();

      // Commit acknowledgement
      long acknowledgedTransactionId = receiveLongOrException();
      // sanity check
      if (acknowledgedTransactionId != transactionId)
      {
        throw new DriverSQLException(
            "Protocol error during commit (acknowledge transaction ID = "
                + acknowledgedTransactionId + ", expected transaction ID = "
                + transactionId + ")");
      }
      mustBeginTransaction = true; // lazy begin
      writeExecutedInTransaction = false;
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the commit
      try
      {
        // this should resend transactionId (among others)
        reconnect();

        // get an ack
        long acknowledgedTransactionId = retrieveCommitResult();
        // sanity check
        if (acknowledgedTransactionId != transactionId)
        {
          throw new DriverSQLException(
              "Protocol error during commit (acknowledge transaction ID = "
                  + acknowledgedTransactionId + ", expected transaction ID = "
                  + transactionId + ")");
        }
        mustBeginTransaction = true;
        writeExecutedInTransaction = false;

        // The controller will automatically redo the commit if it was not done
        // earlier so we can safely return here, this is a success.
        return;
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost during commit of transaction '" + transactionId

            + "' and automatic reconnect failed(" + e1 + ")", e1);
      }
    }
  }

  /**
   * Determines whether or not commit should be allowed when autocommit is
   * enabled.
   * 
   * @return true when commit is allowed with autocommit
   */
  private boolean isCommitInAutoCommitAllowed()
  {
    String allowed = (String) sequoiaUrl.getParameters().get(
        Driver.ALLOW_COMMIT_WITH_AUTOCOMMIT_PROPERTY);

    return allowed != null && allowed.toLowerCase().equals("true");
  }

  /**
   * SQL statements without parameters are normally executed using
   * <code>Statement</code> objects. If the same SQL statement is executed
   * many times, it is more efficient to use a <code>PreparedStatement</code>.
   * The <code>ResultSet</code> will be
   * <code>TYPE_FORWARD_ONLY</cde>/<code>CONCUR_READ_ONLY</code>.
   *    *
   * @return a new <code>Statement</code> object
   * @exception DriverSQLException passed through from the constructor
   */
  public java.sql.Statement createStatement() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    return new Statement(this, driver);
  }

  /**
   * SQL statements without parameters are normally executed using
   * <code>Statement</code> objects. If the same SQL statement is executed
   * many times, it is more efficient to use a <code>PreparedStatement</code>.
   * 
   * @param resultSetType resultSetType to use
   * @param resultSetConcurrency resultSetConcurrency to use
   * @return a new <code>Statement</code> object
   * @exception SQLException passed through from the constructor
   */
  public java.sql.Statement createStatement(int resultSetType,
      int resultSetConcurrency) throws SQLException
  {
    throwSQLExceptionIfClosed();
    Statement s = new Statement(this, driver);
    s.setResultSetType(resultSetType);
    s.setResultSetConcurrency(resultSetConcurrency);
    return s;
  }

  /**
   * Creates a <code>Statement</code> object that will generate
   * <code>ResultSet</code> objects with the given type, concurrency, and
   * holdability.
   * <p>
   * This method is the same as the <code>createStatement</code> method above,
   * but it allows the default result set type, concurrency, and holdability to
   * be overridden.
   * 
   * @param resultSetType one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
   *          <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
   *          <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   * @param resultSetConcurrency one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
   *          <code>ResultSet.CONCUR_UPDATABLE</code>
   * @param resultSetHoldability one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
   *          <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
   * @return a new <code>Statement</code> object that will generate
   *         <code>ResultSet</code> objects with the given type, concurrency,
   *         and holdability
   * @exception SQLException if a database access error occurs or the given
   *              parameters are not <code>ResultSet</code> constants
   *              indicating type, concurrency, and holdability
   * @see ResultSet
   * @since JDK 1.4
   */
  public java.sql.Statement createStatement(int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    throw new NotImplementedException(getCurrentMethodName());
  }

  /**
   * Gets the current auto-commit state.
   * 
   * @return current state of the auto-commit mode
   * @exception DriverSQLException is connection is closed
   * @see Connection#setAutoCommit
   */
  public boolean getAutoCommit() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    return this.autoCommit;
  }

  /**
   * A connection's database is able to provide information describing its
   * tables, its supported SQL grammar, its stored procedures, the capabilities
   * of this connection, etc. This information is made available through a
   * DatabaseMetaData object.
   * 
   * @return a <code>DatabaseMetaData</code> object for this connection
   * @exception DriverSQLException if connection is closed
   */
  public java.sql.DatabaseMetaData getMetaData() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    if (metaData == null)
    {
      metaData = new DatabaseMetaData(this);
    }
    return metaData;
  }

  /**
   * Return current catalog name.
   * 
   * @return name of the current <code>VirtualDatabase</code>
   * @throws DriverSQLException if any error occurs
   * @see Connection#getCatalog()
   */
  public synchronized String getCatalog() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.ConnectionGetCatalog);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName());

      return receiveStringOrException();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getCatalog();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * getCatalogs definition.
   * 
   * @return instace of <code>ResultSet<code>
   * @throws DriverSQLException if fails (include ANY exception that can be thrown in the code)
   */
  public synchronized ResultSet getCatalogs() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.ConnectionGetCatalogs);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName());

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getCatalogs();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost and automatic reconnect failed while executing "
                + getCurrentMethodName(), e1);
      }
    }
  }

  /**
   * Retrieves the current holdability of <code>ResultSet</code> objects
   * created using this <code>Connection</code> object. If the value was not
   * set using setHoldability(), the default value of the database is returned.
   * 
   * @return the holdability, one of
   *         <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
   *         <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
   * @throws SQLException if a database access occurs
   * @see #setHoldability
   * @see ResultSet
   * @since JDK 1.4
   */
  public int getHoldability() throws SQLException
  {
    if (holdability == HOLDABILITY_NOT_SET)
      holdability = getMetaData().getResultSetHoldability();
    return holdability;
  }

  /**
   * Gets this Connection's current transaction isolation mode. If the
   * transaction isolation has not been set using setTransactionIsolation, this
   * method will return by default
   * java.sql.Connection.TRANSACTION_READ_UNCOMMITTED whatever transaction
   * isolation is really used by the cluster nodes. If you want to enfore
   * TRANSACTION_READ_UNCOMMITTED, you have to explicitely call
   * setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
   * 
   * @return the current <code>TRANSACTION_*</code> mode value
   * @exception DriverSQLException if a database access error occurs
   * @see #setTransactionIsolation(int)
   */
  public int getTransactionIsolation() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    // Warning, here we assume that if no transaction isolation is set the
    // database will provide READ_UNCOMMITED.
    if (isolationLevel == DEFAULT_TRANSACTION_ISOLATION_LEVEL)
      return driver.getDefaultTransactionIsolationLevel();
    return isolationLevel;
  }

  /**
   * Sequoia does NOT support type map.
   * 
   * @return an exception
   * @exception SQLException not supported
   */
  public java.util.Map getTypeMap() throws SQLException
  {
    throw new NotImplementedException(getCurrentMethodName());
  }

  /**
   * Returns the first warning reported by calls on this connection. Subsequent
   * warnings will be chained to this SQLWarning<br>
   * <B>Note: </B> If the 'persistent connections' option is set to false, this
   * function will always return null.
   * 
   * @return the first SQLWarning or null
   * @exception DriverSQLException if a database access error occurs or this
   *              method is called on a closed connection
   */
  public SQLWarning getWarnings() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    if (!persistentConnection || !retrieveSQLWarnings)
      return firstWarning;

    try
    {
      sendCommand(Commands.ConnectionGetWarnings);
      socketOutput.writeLong(persistentConnectionId);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName());
      return receiveSQLWarnings();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      { // Connection failed, try to reconnect and re-send command
        reconnect();
        return getWarnings();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while getting SQL Warnings and automatic reconnect failed("
                + e1 + ")", e1);
      }
    }
  }

  /**
   * Returns <code>true</code> if the connection has been closed by the user
   * (but Sequoia may leave it open underneath, unknown to the user).
   * 
   * @return <code>true</code> if connection has never been opened or
   *         <code>close()</code> has been called
   */
  public boolean isClosed()
  {
    return isClosed;
  }

  /**
   * Tests to see if the connection is in read only Mode. Note that we cannot
   * really put the database in read only mode, but we pretend we can by
   * returning the value of the <code>readOnly</code> flag.
   * 
   * @return <code>true</code> if the connection is read only
   */
  public boolean isReadOnly()
  {
    return readOnly;
  }

  /**
   * As we can't know for sure which database will execute this request (now or
   * later), we can't translate it in the native query language of the
   * underlying DBMS. Therefore the query is returned unchanged.
   * 
   * @param query the query to change
   * @return the original query
   */
  public String nativeSQL(String query)
  {
    return query;
  }

  /**
   * A SQL statement with or without <code>IN</code> parameters can be
   * pre-compiled and stored in a PreparedStatement object. This object can then
   * be used to efficiently execute this statement multiple times.
   * 
   * @param sql a SQL statement that may contain one or more '?' IN * parameter
   *          placeholders
   * @return a new <code>PreparedStatement</code> object containing the
   *         pre-compiled statement.
   * @exception SQLException if a database access error occurs.
   */
  public java.sql.PreparedStatement prepareStatement(String sql)
      throws SQLException
  {
    throwSQLExceptionIfClosed();
    return new PreparedStatement(this, sql, driver);
  }

  /**
   * Creates a default <code>PreparedStatement</code> object that has the
   * capability to retrieve auto-generated keys. The given constant tells the
   * driver whether it should make auto-generated keys available for retrieval.
   * This parameter is ignored if the SQL statement is not an
   * <code>INSERT</code> statement.
   * <p>
   * <b>Note: </b> This method is optimized for handling parametric SQL
   * statements that benefit from precompilation. If the driver supports
   * precompilation, the method <code>prepareStatement</code> will send the
   * statement to the database for precompilation. Some drivers may not support
   * precompilation. In this case, the statement may not be sent to the database
   * until the <code>PreparedStatement</code> object is executed. This has no
   * direct effect on users; however, it does affect which methods throw certain
   * SQLExceptions.
   * <p>
   * Result sets created using the returned <code>PreparedStatement</code>
   * object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
   * concurrency level of <code>CONCUR_READ_ONLY</code>.
   * 
   * @param sql an SQL statement that may contain one or more '?' IN parameter
   *          placeholders
   * @param autoGeneratedKeys a flag indicating whether auto-generated keys
   *          should be returned; one of
   *          <code>Statement.RETURN_GENERATED_KEYS</code> or
   *          <code>Statement.NO_GENERATED_KEYS</code>
   * @return a new <code>PreparedStatement</code> object, containing the
   *         pre-compiled SQL statement, that will have the capability of
   *         returning auto-generated keys
   * @exception SQLException if a database access error occurs or the given
   *              parameter is not a <code>Statement</code> constant
   *              indicating whether auto-generated keys should be returned
   * @since JDK 1.4
   */
  public java.sql.PreparedStatement prepareStatement(String sql,
      int autoGeneratedKeys) throws SQLException
  {
    throwSQLExceptionIfClosed();
    PreparedStatement ps = new PreparedStatement(this, sql, driver,
        autoGeneratedKeys);
    return ps;
  }

  /**
   * Creates a default <code>PreparedStatement</code> object capable of
   * returning the auto-generated keys designated by the given array. This array
   * contains the indexes of the columns in the target table that contain the
   * auto-generated keys that should be made available. This array is ignored if
   * the SQL statement is not an <code>INSERT</code> statement.
   * <p>
   * An SQL statement with or without IN parameters can be pre-compiled and
   * stored in a <code>PreparedStatement</code> object. This object can then
   * be used to efficiently execute this statement multiple times.
   * <p>
   * <b>Note: </b> This method is optimized for handling parametric SQL
   * statements that benefit from precompilation. If the driver supports
   * precompilation, the method <code>prepareStatement</code> will send the
   * statement to the database for precompilation. Some drivers may not support
   * precompilation. In this case, the statement may not be sent to the database
   * until the <code>PreparedStatement</code> object is executed. This has no
   * direct effect on users; however, it does affect which methods throw certain
   * SQLExceptions.
   * <p>
   * Result sets created using the returned <code>PreparedStatement</code>
   * object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
   * concurrency level of <code>CONCUR_READ_ONLY</code>.
   * 
   * @param sql an SQL statement that may contain one or more '?' IN parameter
   *          placeholders
   * @param columnIndexes an array of column indexes indicating the columns that
   *          should be returned from the inserted row or rows
   * @return a new <code>PreparedStatement</code> object, containing the
   *         pre-compiled statement, that is capable of returning the
   *         auto-generated keys designated by the given array of column indexes
   * @exception SQLException if a database access error occurs
   * @since JDK 1.4
   */
  public java.sql.PreparedStatement prepareStatement(String sql,
      int[] columnIndexes) throws SQLException
  {
    throwSQLExceptionIfClosed();
    PreparedStatement ps = new PreparedStatement(this, sql, driver,
        PreparedStatement.RETURN_GENERATED_KEYS);
    return ps;
  }

  /**
   * Creates a default <code>PreparedStatement</code> object capable of
   * returning the auto-generated keys designated by the given array. This array
   * contains the names of the columns in the target table that contain the
   * auto-generated keys that should be returned. This array is ignored if the
   * SQL statement is not an <code>INSERT</code> statement.
   * <p>
   * An SQL statement with or without IN parameters can be pre-compiled and
   * stored in a <code>PreparedStatement</code> object. This object can then
   * be used to efficiently execute this statement multiple times.
   * <p>
   * <b>Note: </b> This method is optimized for handling parametric SQL
   * statements that benefit from precompilation. If the driver supports
   * precompilation, the method <code>prepareStatement</code> will send the
   * statement to the database for precompilation. Some drivers may not support
   * precompilation. In this case, the statement may not be sent to the database
   * until the <code>PreparedStatement</code> object is executed. This has no
   * direct effect on users; however, it does affect which methods throw certain
   * <code>SQLExceptions</code>.
   * <p>
   * Result sets created using the returned <code>PreparedStatement</code>
   * object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
   * concurrency level of <code>CONCUR_READ_ONLY</code>.
   * 
   * @param sql an SQL statement that may contain one or more '?' IN parameter
   *          placeholders
   * @param columnNames an array of column names indicating the columns that
   *          should be returned from the inserted row or rows
   * @return a new <code>PreparedStatement</code> object, containing the
   *         pre-compiled statement, that is capable of returning the
   *         auto-generated keys designated by the given array of column names
   * @exception SQLException if a database access error occurs
   * @since JDK 1.4
   */
  public java.sql.PreparedStatement prepareStatement(String sql,
      String[] columnNames) throws SQLException
  {
    throwSQLExceptionIfClosed();
    PreparedStatement ps = new PreparedStatement(this, sql, driver,
        PreparedStatement.RETURN_GENERATED_KEYS);
    return ps;
  }

  /**
   * A SQL statement with or without IN parameters can be pre-compiled and
   * stored in a <code>PreparedStatement</code> object. This object can then
   * be used to efficiently execute this statement multiple times.
   * 
   * @param sql a SQL statement that may contain one or more '?' IN
   * @param resultSetType <code>ResultSetType</code> to use
   * @param resultSetConcurrency <code>ResultSetConcurrency</code> to use
   * @return a new <code>PreparedStatement</code> object
   * @exception SQLException passed through from the constructor
   */
  public java.sql.PreparedStatement prepareStatement(String sql,
      int resultSetType, int resultSetConcurrency) throws SQLException
  {
    throwSQLExceptionIfClosed();
    PreparedStatement s = new PreparedStatement(this, sql, driver);
    s.setResultSetType(resultSetType);
    s.setResultSetConcurrency(resultSetConcurrency);
    return s;
  }

  /**
   * Creates a <code>PreparedStatement</code> object that will generate
   * <code>ResultSet</code> objects with the given type, concurrency, and
   * holdability.
   * <p>
   * This method is the same as the <code>prepareStatement</code> method
   * above, but it allows the default result set type, concurrency, and
   * holdability to be overridden.
   * 
   * @param sql a <code>String</code> object that is the SQL statement to be
   *          sent to the database; may contain one or more ? IN parameters
   * @param resultSetType one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
   *          <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
   *          <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   * @param resultSetConcurrency one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
   *          <code>ResultSet.CONCUR_UPDATABLE</code>
   * @param resultSetHoldability one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
   *          <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
   * @return a new <code>PreparedStatement</code> object, containing the
   *         pre-compiled SQL statement, that will generate
   *         <code>ResultSet</code> objects with the given type, concurrency,
   *         and holdability
   * @exception SQLException if a database access error occurs or the given
   *              parameters are not <code>ResultSet</code> constants
   *              indicating type, concurrency, and holdability
   * @see ResultSet
   * @since JDK 1.4
   */
  public java.sql.PreparedStatement prepareStatement(String sql,
      int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException
  {
    throwSQLExceptionIfClosed();
    PreparedStatement ps = new PreparedStatement(this, sql, driver);
    ps.setResultSetType(resultSetType);
    ps.setResultSetConcurrency(resultSetConcurrency);
    setHoldability(resultSetHoldability);
    return ps;
  }

  /**
   * Creates a CallableStatement that contains sql and produces a ResultSet that
   * is TYPE_SCROLL_INSENSITIVE and CONCUR_READ_ONLY.
   * 
   * @param sql SQL request
   * @return a CallableStatement
   * @exception SQLException not supported
   */
  public java.sql.CallableStatement prepareCall(String sql) throws SQLException
  {
    throwSQLExceptionIfClosed();
    return prepareCall(sql, java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
        java.sql.ResultSet.CONCUR_READ_ONLY);
  }

  /**
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
   */
  public java.sql.CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException
  {
    throwSQLExceptionIfClosed();
    CallableStatement c = new CallableStatement(this, sql, driver);
    c.setResultSetType(resultSetType);
    c.setResultSetConcurrency(resultSetConcurrency);
    return c;
  }

  /**
   * Creates a <code>CallableStatement</code> object that will generate
   * <code>ResultSet</code> objects with the given type and concurrency. This
   * method is the same as the <code>prepareCall</code> method above, but it
   * allows the default result set type, result set concurrency type and
   * holdability to be overridden.
   * 
   * @param sql a <code>String</code> object that is the SQL statement to be
   *          sent to the database; may contain on or more ? parameters
   * @param resultSetType one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
   *          <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
   *          <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   * @param resultSetConcurrency one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
   *          <code>ResultSet.CONCUR_UPDATABLE</code>
   * @param resultSetHoldability one of the following <code>ResultSet</code>
   *          constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
   *          <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
   * @return a new <code>CallableStatement</code> object, containing the
   *         pre-compiled SQL statement, that will generate
   *         <code>ResultSet</code> objects with the given type, concurrency,
   *         and holdability
   * @exception SQLException if a database access error occurs or the given
   *              parameters are not <code>ResultSet</code> constants
   *              indicating type, concurrency, and holdability
   * @see ResultSet
   * @since JDK 1.4
   */
  public java.sql.CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    throwSQLExceptionIfClosed();
    CallableStatement cs = new CallableStatement(this, sql, driver);
    cs.setResultSetType(resultSetType);
    cs.setResultSetConcurrency(resultSetConcurrency);
    setHoldability(resultSetHoldability);
    return cs;
  }

  /**
   * Removes the given <code>Savepoint</code> object from the current
   * transaction. Any reference to the savepoint after it have been removed will
   * cause an <code>SQLException</code> to be thrown.
   * 
   * @param savepoint the <code>Savepoint</code> object to be removed
   * @exception DriverSQLException if a database access error occurs or the
   *              given <code>Savepoint</code> object is not a valid savepoint
   *              in the current transaction
   * @since JDK 1.4
   */
  public void releaseSavepoint(Savepoint savepoint) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    if (savepoint == null)
      throw new DriverSQLException("Savepoint cannot be null");

    if (autoCommit)
      throw new DriverSQLException(
          "Trying to release a savepoint in autocommit mode");

    if (driver == null)
      throw new DriverSQLException("No driver to release a savepoint");

    try
    {
      sendCommand(Commands.ReleaseSavepoint);
      savepointOnStream(savepoint);
      socketOutput.flush();

      this.receiveBooleanOrException();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        // Connection failed, try to reconnect and release the savepoint again
        reconnect();
        boolean done = retrieveReleaseSavepoint(savepoint);
        if (!done)
        {
          releaseSavepoint(savepoint);
        }
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while releasing savepoint '" + savepoint
                + "' and automatic reconnect failed(" + e1 + ")", e1);
      }
    }
  }

  /**
   * Drops all changes made since the previous commit/rollback and releases any
   * database locks currently held by this connection. If the connection was in
   * autocommit mode, we throw a DriverSQLException.
   * 
   * @exception DriverSQLException if a database access error occurs or the
   *              connection is in autocommit mode
   * @see Connection#commit()
   */
  public synchronized void rollback() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    if (autoCommit)
      throw new DriverSQLException(
          "Trying to rollback a connection in autocommit mode");

    // Check if we are not rollbacking an empty transaction (not started yet)
    if (mustBeginTransaction)
      return;

    try
    {
      sendCommand(Commands.Rollback);
      socketOutput.flush();

      // rollback acknowledgement
      long acknowledgedTransactionId = receiveLongOrException();
      if (acknowledgedTransactionId != transactionId)
      {
        throw new DriverSQLException(
            "Protocol error during rollback (acknowledge transaction ID = "
                + acknowledgedTransactionId + ", expected transaction ID = "
                + transactionId + ")");
      }
      mustBeginTransaction = true;
      writeExecutedInTransaction = false;
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the rollback
      try
      {
        reconnect();

        long acknowledgedTransactionId = retrieveRollbackResult();
        if (acknowledgedTransactionId != transactionId)
        {
          throw new DriverSQLException(
              "Protocol error during rollback failover (acknowledge transaction ID = "
                  + acknowledgedTransactionId + ", expected transaction ID = "
                  + transactionId + ")");
        }
        mustBeginTransaction = true;

        // The controller will automatically redo the rollback if it was not
        // done earlier so we can safely return here, this is a success.
        return;
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost during rollback of transaction '" + transactionId

            + "' and automatic reconnect failed(" + e1 + ")", e1);
      }
    }
  }

  /**
   * Undoes all changes made after the given <code>Savepoint</code> object was
   * set.
   * <p>
   * This method should be used only when auto-commit has been disabled.
   * 
   * @param savepoint the <code>Savepoint</code> object to roll back to
   * @exception DriverSQLException if a database access error occurs, the
   *              <code>Savepoint</code> object is no longer valid, or this
   *              <code>Connection</code> object is currently in auto-commit
   *              mode
   * @see Savepoint
   * @see #rollback()
   * @since JDK 1.4
   */
  public void rollback(Savepoint savepoint) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    if (savepoint == null)
      throw new DriverSQLException("Savepoint cannot be null");

    if (autoCommit)
      throw new DriverSQLException(
          "Trying to rollback to a savepoint in autocommit mode");

    if (driver == null)
      throw new DriverSQLException("No driver to rollback to savepoint");

    try
    {
      sendCommand(Commands.RollbackToSavepoint);
      savepointOnStream(savepoint);
      socketOutput.flush();

      this.receiveBooleanOrException();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        // Connection failed, try to reconnect and rollback again if checkpoint
        // still exists
        reconnect();
        boolean isCheckpointRemoved = retrieveReleaseSavepoint(savepoint);
        if (!isCheckpointRemoved)
          rollback(savepoint);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while rollbacking to savepoint '" + savepoint
                + "' and automatic reconnect failed(" + e1 + ")", e1);
      }
    }
  }

  private void begin() throws DriverSQLException, ProtocolException
  {
    try
    {
      sendCommand(Commands.Begin);
      socketOutput.flush();

      transactionId = receiveLongOrException();

      if (sequoiaUrl.isDebugEnabled())
        System.out
            .println("Transaction " + transactionId + " has been started");
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      // Connection failed, try to reconnect and re-exec the query
      if (sequoiaUrl.isInfoEnabled())
        System.out.println("I/O Error while trying to disable autocommit\n"
            + e.getLocalizedMessage());
      reconnect();
      begin();
    }
  }

  /**
   * To keep track of active transactions, sequoia uses the old BEGIN modeless
   * model. Backend's connections on the controller are always maintained/reset
   * in autocommit mode = true, except of course when in the middle of an active
   * transaction. So part of our job here is to translate the new ODBC/JDBC
   * autoCommit mode concept to the old BEGIN model.
   * 
   * @see Commands#SetAutoCommit
   * @see java.sql.Connection#setAutoCommit(boolean)
   * @param autoCommitArg <code>true</code> enables auto-commit;
   *          <code>false</code> disables it
   * @exception DriverSQLException if a database access error occurs
   * @throws DriverIOException if an IO error occured with the controller
   */
  public synchronized void setAutoCommit(boolean autoCommitArg)
      throws DriverSQLException, DriverIOException
  {
    throwSQLExceptionIfClosed();

    // Do nothing if already in the right state
    if (this.autoCommit == autoCommitArg)
      return;

    // true -> false (send nothing, lazy begin)
    if (this.autoCommit)
    {
      this.autoCommit = false;
      this.mustBeginTransaction = true;
      return;
    }

    // false -> true
    if (mustBeginTransaction)
    { // Transaction has NOT yet begun
      // Just cancel the (lazy, not yet done) begin
      mustBeginTransaction = false;
      this.autoCommit = true;
      return;
    }
    else
    {
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Setting connection in autocommit mode");
      doCommit();
      this.autoCommit = true;
    }
  }

  /**
   * Change the current catalog
   * 
   * @param catalog a <code>String</code> value
   * @exception SQLException if fails or if catalog name is invalid
   */
  public synchronized void setCatalog(String catalog) throws SQLException
  {
    throwSQLExceptionIfClosed();
    if (catalog == null)
      throw new DriverSQLException("Invalid Catalog");
    sequoiaUrl.setUrl(driver.changeDatabaseName(sequoiaUrl.getUrl(), catalog));

    try
    {
      sendCommand(Commands.ConnectionSetCatalog);
      socketOutput.writeLongUTF(catalog);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with catalog '" + catalog + "'");

      if (!receiveBooleanOrException())
        throw new DriverSQLException("Invalid Catalog");

    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        // Connection failed, try to reconnect and re-set the catalog
        reconnect();
        setCatalog(catalog);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while setting the catalog '" + catalog
                + "' and automatic reconnect failed(" + e1 + ")", e1);
      }
    }
  }

  /**
   * Changes the holdability of <code>ResultSet</code> objects created using
   * this <code>Connection</code> object to the given holdability.
   * 
   * @param holdability a <code>ResultSet</code> holdability constant; one of
   *          <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
   *          <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
   * @throws SQLException if a database access occurs, the given parameter is
   *           not a <code>ResultSet</code> constant indicating holdability,
   *           or the given holdability is not supported
   * @see #getHoldability
   * @see ResultSet
   * @since JDK 1.4
   */
  public void setHoldability(int holdability) throws SQLException
  {
    if ((holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
        && (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT))
      throw new SQLException("Invalid holdaibility value " + holdability);
    this.holdability = holdability;
  }

  /**
   * You can put a connection in read-only mode as a hint to enable database
   * optimizations
   * 
   * @param readOnly <code>true</code> enables read-only mode;
   *          <code>false</code> disables it
   * @exception DriverSQLException if a database access error occurs
   */
  public void setReadOnly(boolean readOnly) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();

    try
    {
      sendCommand(Commands.SetReadOnly);
      socketOutput.writeBoolean(readOnly);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Setting connection to read-only=" + readOnly);

      receiveBooleanOrException();
      // Success
      this.readOnly = readOnly;
      return;

    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException ioe)
    {
      try
      {
        // Connection failed, try to reconnect and re-set the transaction
        // isolation level
        reconnect();
        setReadOnly(readOnly);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while setting the connection to read-only="
                + readOnly + " and automatic reconnect failed(" + e1 + ")", e1);
      }
    }
  }

  private static final int SAVEPOINT_NOT_SET = -1;

  /**
   * Creates an unnamed savepoint in the current transaction and returns the new
   * <code>Savepoint</code> object that represents it.
   * 
   * @return the new <code>Savepoint</code> object
   * @exception DriverSQLException if a database access error occurs or this
   *              <code>Connection</code> object is currently in auto-commit
   *              mode
   * @see Savepoint
   * @since JDK 1.4
   */
  public Savepoint setSavepoint() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    beginTransactionIfNeeded();
    if (autoCommit)
      throw new DriverSQLException(
          "Trying to set a savepoint in autocommit mode");

    if (driver == null)
      throw new DriverSQLException("No driver to set a savepoint");

    int savepointId = SAVEPOINT_NOT_SET;
    try
    {
      sendCommand(Commands.SetUnnamedSavepoint);
      socketOutput.flush();

      savepointId = receiveIntOrException();
      return new org.continuent.sequoia.driver.Savepoint(savepointId);
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException ioe)
    {
      try
      {
        // Connection failed, try to reconnect and re-set the savepoint if it
        // was not set before the failure
        reconnect();
        org.continuent.sequoia.driver.Savepoint savepoint = new org.continuent.sequoia.driver.Savepoint(
            savepointId);
        boolean checkpointDoesNotExit = (savepointId == SAVEPOINT_NOT_SET)
            || retrieveReleaseSavepoint(savepoint);
        if (checkpointDoesNotExit)
          return this.setSavepoint(); // retry, did not work the first time
        else
          return savepoint; // ok, already set
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while setting an unnamed savepoint and automatic reconnect failed("
                + e1 + ")", e1);
      }
    }
  }

  /**
   * Creates a savepoint with the given name in the current transaction and
   * returns the new <code>Savepoint</code> object that represents it.
   * 
   * @param name a <code>String</code> containing the name of the savepoint
   * @return the new <code>Savepoint</code> object
   * @exception DriverSQLException if a database access error occurs or this
   *              <code>Connection</code> object is currently in auto-commit
   *              mode
   * @see Savepoint
   * @since JDK 1.4
   */
  public Savepoint setSavepoint(String name) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    beginTransactionIfNeeded();
    if (name == null)
      throw new IllegalArgumentException("Savepoint name cannot be null");

    if (autoCommit)
      throw new DriverSQLException(
          "Trying to set a savepoint in autocommit mode");

    if (driver == null)
      throw new DriverSQLException("No driver to set a savepoint");

    try
    {
      sendCommand(Commands.SetNamedSavepoint);
      socketOutput.writeLongUTF(name);
      socketOutput.flush();

      this.receiveBooleanOrException();
      return new org.continuent.sequoia.driver.Savepoint(name);
    }
    catch (SerializableException se)
    {
      throw new DriverSQLException(se);
    }
    catch (IOException e)
    {
      try
      {
        // Connection failed, try to reconnect and re-set the savepoint if it
        // was not set before the failure
        reconnect();
        org.continuent.sequoia.driver.Savepoint savepoint = new org.continuent.sequoia.driver.Savepoint(
            name);
        boolean checkpointDoesNotExit = retrieveReleaseSavepoint(savepoint);
        if (checkpointDoesNotExit)
          return setSavepoint(name);
        else
          return savepoint;
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while setting the savepoint '" + name
                + "' and automatic reconnect failed(" + e1 + ")", e1);
      }
    }
  }

  /**
   * You can call this method to try to change the transaction isolation level
   * using one of the TRANSACTION_* values.
   * <p>
   * <B>Note: </B> this method cannot be called while in the middle of a
   * transaction. The JDBC spec says it should trigger a commit. We should
   * probably let the backend handle this, not trying to add our own complexity.
   * 
   * @param level one of the TRANSACTION_* isolation values with * the exception
   *          of TRANSACTION_NONE; some databases may * not support other values
   * @exception DriverSQLException if a database access error occurs
   * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel
   */
  public synchronized void setTransactionIsolation(int level)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    // Check if we are in a transaction or not. We have no trace on the driver
    // side if a read query has already been executed or not in the current
    // transaction (if any). We let the controller check for this (we only check
    // for writes here) as well as if the underlying databases support the
    // transaction isolation level. If this is not supported, the driver will
    // send back an exception.
    if ((autoCommit == false) && writeExecutedInTransaction)
      throw new DriverSQLException(
          getCurrentMethodName()
              + " cannot be called in a transaction that has executed write requests.");

    if (level != isolationLevel)
    { // Only try to change if there is a new value
      if ((level == TRANSACTION_READ_COMMITTED)
          || (level == TRANSACTION_READ_UNCOMMITTED)
          || (level == TRANSACTION_REPEATABLE_READ)
          || (level == TRANSACTION_SERIALIZABLE))
      {
        try
        {
          sendCommand(Commands.SetTransactionIsolation);
          socketOutput.writeInt(level);
          socketOutput.flush();

          if (sequoiaUrl.isDebugEnabled())
            System.out.println("Setting transaction isolation level to "
                + level);

          receiveBooleanOrException();
          // Success
          isolationLevel = level;
          return;

        }
        catch (SerializableException e)
        {
          throw new DriverSQLException(e);
        }
        catch (IOException ioe)
        {
          try
          {
            // Connection failed, try to reconnect and re-set the transaction
            // isolation level
            reconnect();
            setTransactionIsolation(level);
          }
          catch (DriverSQLException e1)
          {
            throw new DriverSQLException(
                "Connection lost while setting the transaction isolation level '"
                    + level + "' and automatic reconnect failed(" + e1 + ")",
                e1);
          }
        }
      }
      else
        throw new DriverSQLException("Invalid transaction isolation level "
            + level);
    } // we were already in that level; do nothing.
  }

  /**
   * Sequoia does NOT support type map.
   * 
   * @param map ignored
   * @exception SQLException not supported
   */
//  public void setTypeMap(java.util.Map map) throws SQLException
//  {
//    throw new NotImplementedException(getCurrentMethodName());
//  }

  /*
   * Connection Sequoia internals
   */

  /**
   * Begins a new transaction if needed (<code>mustBeginTransaction</code> is
   * set to <code>true</code>).
   * 
   * @throws DriverSQLException if begin fails
   */
  private void beginTransactionIfNeeded() throws DriverSQLException
  {
    if (!mustBeginTransaction)
      return;

    begin();
    this.mustBeginTransaction = false;
  }

  /**
   * Fetch multiple results from a query executed with Statement.execute()
   * 
   * @return the list of results
   */
  private List fetchMultipleResultsFromStream(String callerName)
      throws IOException, SerializableException, ProtocolException
  {
    boolean hasResult;
    int updateCount = 0;
    LinkedList results = new LinkedList();
    do
    {
      hasResult = receiveBooleanOrException();
      if (hasResult)
      {
        DriverResultSet rs = receiveResultSet(callerName);
        if (rs == null)
        {
          // This happens during transparent failover when the controller's
          // request cache hasn't found a result for a given request.
          return null;
        }
        else
        {
          results.addLast(rs);
        }
      }
      else
      {
        updateCount = receiveIntOrException();
        results.addLast(new Integer(updateCount));
      }
    }
    while (hasResult || updateCount != -1);
    return results;
  }

  /**
   * Fetch named parameters from the stream.
   * 
   * @return HashMap of <ParameterName,ParameterValue> or null
   * @throws ProtocolException if a protocol error occured
   * @throws IOException if an error with the socket occured
   * @throws SerializableException if a problem occured deserializing an object
   */
  private HashMap fetchNamedParameters() throws ProtocolException, IOException,
      SerializableException
  {
    if (sequoiaUrl.isDebugEnabled())
      System.out.println("Retrieving named parameters");
    String paramName = receiveStringOrException();
    if ("0".equals(paramName))
      return null;
    HashMap params = new HashMap();
    while (!"0".equals(paramName))
    {
      Object value = receiveObject();
      params.put(paramName, value);
      paramName = receiveStringOrException();
    }
    return params;
  }

  /**
   * Fetch OUT parameters from the stream.
   * 
   * @return HashMap of <ParameterName,ParameterValue> or null
   * @throws ProtocolException if a protocol error occured
   * @throws IOException if an error with the socket occured
   * @throws SerializableException if a problem occured deserializing an object
   */
  private HashMap fetchOutParameters() throws ProtocolException, IOException,
      SerializableException
  {
    if (sequoiaUrl.isDebugEnabled())
      System.out.println("Retrieving out parameters");

    int index = receiveIntOrException();
    if (index == 0)
      return null;
    HashMap params = new HashMap();
    while (index != 0)
    {
      Object value = receiveObject();
      params.put(new Integer(index), value);
      index = receiveIntOrException();
    }
    return params;
  }

  /**
   * Set the autocommit mode and read-only status on this request.
   * 
   * @param request The request to set
   */
  private void setConnectionParametersOnRequest(Request request)
  {
    request.setIsAutoCommit(autoCommit);
  }

  /**
   * Receive an object from the stream by fetching a tag first and then the
   * value from the proper serializer.
   * 
   * @return the deserialized object (can be null)
   * @throws IOException if a socket error occurs
   * @throws ProtocolException if a protocol corruption is detected
   */
  private Object receiveObject() throws IOException, ProtocolException
  {
    TypeTag tag = new TypeTag(socketInput);

    // Handle the null specific case
    if (TypeTag.JAVA_NULL.equals(tag))
      return null;

    // We have a real object to de-serialize
    try
    {
      Serializer serializer = SQLDataSerialization.getSerializer(tag);
      return serializer.receiveFromStream(socketInput);
    }
    catch (IllegalArgumentException iae)
    {
      ProtocolException pe = new ProtocolException(
          "Protocol corruption: received unknown TypeTag " + tag
              + " when receiving object.");
      pe.initCause(iae);
      throw pe;
    }
  }

  /**
   * Returns a DriverResultSet read from the stream or throws the
   * SerializableException that came instead
   * 
   * @param callerName used for error messages. Is this really useful?
   * @return received ResultSet
   * @throws IOException stream or protocol error
   * @throws SerializableException received from the controller
   */
  private DriverResultSet receiveResultSet(String callerName)
      throws IOException, ProtocolException, SerializableException
  {
    TypeTag tag = new TypeTag(socketInput);

    if (TypeTag.NULL_RESULTSET.equals(tag))
      return null;

    if (TypeTag.RESULTSET.equals(tag))
    {
      DriverResultSet drs = new DriverResultSet(this);
      return drs;
    }

    if (TypeTag.EXCEPTION.equals(tag))
      throw receiveException();

    throw new ProtocolException(callerName
        + ": expected a resultset, received unexpected tag: " + tag);
  }

  /**
   * Deserialize SQL warnings from the stream: converts BackendDriverException
   * to an SQLWarning chain
   * 
   * @return the deserialized warning chain
   * @throws IOException stream error
   * @throws ProtocolException protocol error
   * @throws SerializableException
   */
  private SQLWarning receiveSQLWarnings() throws IOException,
      ProtocolException, SerializableException
  {
    if (!receiveBooleanOrException())
      // no warning
      return null;
    // Receive the warning as a BackendDriverException
    SerializableException e = receiveException();
    if (!(e instanceof BackendDriverException))
      throw new ProtocolException(
          "Unknown exception received instead of SQLWarning");
    return convertToSQLWarnings(e);
  }

  /**
   * Deserialize an exception from the stream: converts explicit protocol typing
   * into java types.
   * 
   * @return the deserialized exception read from the stream
   * @throws IOException stream error
   * @throws ProtocolException protocol error
   */
  private SerializableException receiveException() throws IOException,
      ProtocolException
  {
    TypeTag exceptionType = new TypeTag(socketInput);

    if (TypeTag.BACKEND_EXCEPTION.equals(exceptionType))
      return new BackendDriverException(socketInput);
    if (TypeTag.CORE_EXCEPTION.equals(exceptionType))
      return new ControllerCoreException(socketInput);

    throw new ProtocolException("received unknown exception type");
  }

  /**
   * Returns a String read from the stream or throws the SerializableException
   * that came instead.
   * 
   * @throws IOException stream or protocol error
   * @throws SerializableException coming from the controller
   * @throws ProtocolException protocol error
   */
  private String receiveStringOrException() throws IOException,
      SerializableException, ProtocolException
  {
    TypeTag tag = new TypeTag(socketInput);
    if (TypeTag.NOT_EXCEPTION.equals(tag))
    {
      String answer = socketInput.readLongUTF();
      return answer;
    }

    throw receiveException();
  }

  /**
   * Returns a boolean read from the stream or throws the SerializableException
   * that came instead.
   * 
   * @throws IOException stream or protocol error
   * @throws SerializableException coming from the controller
   * @throws ProtocolException protocol error
   */
  private boolean receiveBooleanOrException() throws IOException,
      SerializableException, ProtocolException
  {
    TypeTag tag = new TypeTag(socketInput);
    if (TypeTag.NOT_EXCEPTION.equals(tag))
    {
      boolean answer = socketInput.readBoolean();
      return answer;
    }

    throw receiveException();
  }

  /**
   * Returns a int read from the stream or throws the SerializableException that
   * came instead.
   * 
   * @throws IOException stream or protocol error
   * @throws SerializableException coming from the controller
   * @throws ProtocolException protocol error
   */
  private int receiveIntOrException() throws IOException,
      SerializableException, ProtocolException
  {
    TypeTag tag = new TypeTag(socketInput);
    if (TypeTag.NOT_EXCEPTION.equals(tag))
    {
      int answer = socketInput.readInt();
      return answer;
    }

    throw receiveException();
  }

  /**
   * Returns a long read from the stream or throws the SerializableException
   * that came instead.
   * 
   * @throws IOException stream or protocol error
   * @throws SerializableException coming from the controller
   * @throws ProtocolException protocol error
   */
  private long receiveLongOrException() throws IOException,
      SerializableException, ProtocolException
  {
    TypeTag tag = new TypeTag(socketInput);
    if (TypeTag.NOT_EXCEPTION.equals(tag))
    {
      long answer = socketInput.readLong();
      return answer;
    }

    throw receiveException();
  }

  /**
   * Serialize a savepoint on the output stream by sending only the needed
   * parameters to reconstruct it on the controller
   * 
   * @param savepoint the savepoint to send
   * @throws IOException if fails
   */
  private void savepointOnStream(Savepoint savepoint) throws IOException
  {
    writeExecutedInTransaction = true;

    try
    {
      socketOutput.writeLongUTF(savepoint.getSavepointName());
      return;
    }
    catch (SQLException ignore)
    {
      // Ignoring because we are dealing with an un-named savepoint
    }

    try
    {
      socketOutput.writeLongUTF(String.valueOf(savepoint.getSavepointId()));
      return;
    }
    catch (SQLException ignore)
    {
      // We should never get here
    }
  }

  /**
   * Check if the given release savepoint has been successfully performed.
   * 
   * @return true if the release savepoint has been successfully performed
   * @throws DriverSQLException if an error occured
   */
  private boolean retrieveReleaseSavepoint(Savepoint savepoint)
      throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveReleaseSavepoint);
      socketOutput.writeLongUTF(savepoint.getSavepointName());
      socketOutput.flush();
      return receiveBooleanOrException();
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given commit has been successfully performed.
   * 
   * @return the transaction id if the commit has been successfully performed
   * @throws DriverSQLException if an error occured on the commit
   */
  private long retrieveCommitResult() throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveCommitResult);
      socketOutput.flush();
      return receiveLongOrException();
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given rollback has been successfully performed.
   * 
   * @return the transaction id if the rollback has been successfully performed
   * @throws DriverSQLException if an error occured on the rollback
   */
  private long retrieveRollbackResult() throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveRollbackResult);
      socketOutput.flush();
      return receiveLongOrException();
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given query already executed or not on the controller we are
   * currently connected to.
   * 
   * @param request the stored procedure to check
   * @return null if not found or a List composed of a
   *         <code>java.sql.ResultSet</code> value, an <code>ArrayList</code>
   *         of OUT parameters, and a <code>HashMap</code> of named parameters
   *         result objects.
   * @throws DriverSQLException if an error occurs
   */
  private ResultAndWarnings retrieveExecuteQueryResultWithParameters(
      Request request) throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveExecuteQueryResultWithParameters);
      socketOutput.writeLong(request.getId());
      socketOutput.flush();
      SQLWarning sqlw = receiveSQLWarnings();
      DriverResultSet drs = receiveResultSet(getCurrentMethodName());
      if (drs == null)
        return null;
      List result = new ArrayList(3);
      result.add(drs);
      result.add(fetchOutParameters());
      result.add(fetchNamedParameters());
      return new ResultAndWarnings(result, sqlw);
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given query already executed or not on the controller we are
   * currently connected to.
   * 
   * @param request the request to check
   * @return -1 if not found or a List composed of an <code>Integer</code>
   *         (number of updated rows), an <code>ArrayList</code> of OUT
   *         parameters, and a <code>HashMap</code> of named parameters result
   *         objects.
   * @throws DriverSQLException if an error occurs
   */
  private ResultAndWarnings retrieveExecuteUpdateResultWithParameters(
      Request request) throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveExecuteUpdateResultWithParameters);
      socketOutput.writeLong(request.getId());
      socketOutput.flush();
      SQLWarning sqlw = receiveSQLWarnings();
      int updateCount = receiveIntOrException();
      if (updateCount == -1)
        return null; // No result found in failover
      List result = new ArrayList(3);
      result.add(new Integer(updateCount));
      result.add(fetchOutParameters());
      result.add(fetchNamedParameters());
      return new ResultAndWarnings(result, sqlw);
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given query already executed or not on the controller we are
   * currently connected to.
   * 
   * @param request the request to check
   * @return null if not found or a List composed of 1. a <code>List</code> of
   *         results <code>java.sql.ResultSet</code> value, 2. an
   *         <code>ArrayList</code> of OUT parameters, and 3. a
   *         <code>HashMap</code> of named parameters result objects.
   * @throws DriverSQLException if an error occurs
   */
  private ResultAndWarnings retrieveExecuteResultWithParameters(Request request)
      throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveExecuteResultWithParameters);
      socketOutput.writeLong(request.getId());
      socketOutput.flush();
      SQLWarning statementWarnings = receiveSQLWarnings();
      List results = fetchMultipleResultsFromStream(getCurrentMethodName());
      if (results == null)
        return null; // No result found in failover
      List result = new ArrayList(3);
      result.add(results);
      result.add(fetchOutParameters());
      result.add(fetchNamedParameters());
      return new ResultAndWarnings(result, statementWarnings);
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given query already executed or not on the controller we are
   * currently connected to.
   * 
   * @param request the request to check
   * @return int the number of updated rows or -1 if not found
   * @throws DriverSQLException if an error occurs
   */
  private ResultAndWarnings retrieveExecuteUpdateResult(Request request)
      throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveExecuteUpdateResult);
      socketOutput.writeLong(request.getId());
      socketOutput.flush();
      SQLWarning sqlw = receiveSQLWarnings();
      int uc = receiveIntOrException();
      return new ResultAndWarnings(uc, sqlw);
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given query already executed or not on the controller we are
   * currently connected to.
   * 
   * @param request the request to check
   * @return int the number of updated rows or -1 if not found
   * @throws DriverSQLException if an error occurs
   */
  private DriverGeneratedKeysResult retrieveExecuteUpdateWithKeysResult(
      Request request) throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveExecuteUpdateWithKeysResult);
      socketOutput.writeLong(request.getId());
      socketOutput.flush();

      SQLWarning sqlw = receiveSQLWarnings();
      int updateCount = receiveIntOrException();
      if (updateCount == -1)
        return null;

      // Fetch the ResultSet containing the autogenerated keys.
      TypeTag tag = new TypeTag(socketInput);
      if (TypeTag.RESULTSET.equals(tag))
      {
        DriverResultSet drs = new DriverResultSet(this);
        return new DriverGeneratedKeysResult(drs, updateCount, sqlw);
      }

      if (TypeTag.NULL_RESULTSET.equals(tag))
        return new DriverGeneratedKeysResult(null, updateCount, sqlw);

      // Error, unexpected answer
      throw new ProtocolException(getCurrentMethodName()
          + ": protocol corruption for request "
          + request.getSqlShortForm(ABBREV_REQUEST_LENGTH));
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  /**
   * Check if the given query already executed or not on the controller we are
   * currently connected to.
   * 
   * @param request the request to check
   * @return a <code>List</code> of results or null if not found
   * @throws DriverSQLException if an error occurs
   */
  private ResultAndWarnings retrieveExecuteResult(Request request)
      throws DriverSQLException
  {
    try
    {
      sendCommand(Commands.RetrieveExecuteResult);
      socketOutput.writeLong(request.getId());
      socketOutput.flush();
      SQLWarning statementWarnings = receiveSQLWarnings();
      List resList = fetchMultipleResultsFromStream(getCurrentMethodName());
      return new ResultAndWarnings(resList, statementWarnings);
    }
    catch (Throwable e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + " failed on new controller (" + e + ")");
    }
  }

  void reallyClose() throws IOException, DriverSQLException
  {
    sendCommand(Commands.Close);
  }

  /**
   * Try to reconnect to the next controller chosen according to the policy
   * specified in the JDBC URL of this connection.
   * 
   * @throws DriverSQLException if an error occured during reconnect
   */
  private synchronized void reconnect() throws DriverSQLException,
      VirtualDatabaseUnavailableException
  {
    // Get rid of current connection
    try
    {
      this.socket.close();
    }
    catch (IOException ignore)
    {
    }
    try
    {
      this.socketInput.close();
    }
    catch (IOException ignore)
    {
    }
    try
    {
      this.socketOutput.close();
    }
    catch (IOException ignore)
    {
    }
    // only one (Connection) accessing the pool at a time
    synchronized (driver.pendingConnectionClosing)
    {
      if (driver.pendingConnectionClosing.remove(this))
        System.out.println("Warning! Closed call before reconnect");
    }

    SequoiaUrl tempUrl = sequoiaUrl;
    if (persistentConnection)
    {
      /**
       * We do not want to create a persistent connection on the new connection,
       * because we will just close it during the restore operation. We need to
       * make a copy of sequoiaUrl, because it is shared with other connections.
       */
      try
      {
        tempUrl = sequoiaUrl.getTemporaryCloneForReconnection(vdbUser,
            vdbPassword);
      }
      catch (SQLException e)
      {
        // if we could not get a new sequoiaUrl we will just use the original
      }
    }

    Connection newconn = null;
    // At this point, the current controller is down and we have to try a
    // new one that will be allocated by the policy specified in the URL.
    try
    {
      newconn = driver.getConnectionToNewController(tempUrl);
      if (newconn != null)
        controllerInfo = newconn.getControllerInfo();
    }
    catch (AuthenticationException e)
    {
      // Should not happen, this probably mean an inconsistency in controller
      // configuration but safely ignore (see below)
      String msg = "Warning! Authentication exception received on connection retry, controller configuration might be inconsistent";
      if (sequoiaUrl.isInfoEnabled())
        System.out.println(msg);
      throw new DriverSQLException(msg, e);
    }
    catch (NoMoreControllerException nmc)
    {
      throw new DriverSQLException(nmc);
    }
    catch (GeneralSecurityException gse)
    {
      String msg = "Fatal General Security Exception received while trying to reconnect";
      if (sequoiaUrl.isInfoEnabled())
        System.out.println(msg);
      throw new DriverSQLException(msg, gse);
    }

    // newconn cannot be null here else an excepection would have been thrown
    // earlier.

    // Success: let's use the new connection for ourselves
    this.socket = newconn.socket;
    this.socketInput = newconn.socketInput;
    this.socketOutput = newconn.socketOutput;
    this.controllerInfo = newconn.controllerInfo;
    this.isClosed = false;
    try
    {
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Restoring connection state on controller "
            + controllerInfo);
      sendCommand(Commands.RestoreConnectionState);
      socketOutput.writeBoolean(writeExecutedInTransaction);
      if (mustBeginTransaction)
      { // Say that we are in autoCommit, begin will be done later
        // Fixes SEQUOIA-522
        socketOutput.writeBoolean(true);
      }
      else
      {
        socketOutput.writeBoolean(autoCommit);
        if (!autoCommit)
          socketOutput.writeLong(transactionId);
      }
      socketOutput.writeBoolean(persistentConnection);
      if (persistentConnection)
        socketOutput.writeLong(persistentConnectionId);
      socketOutput.writeBoolean(retrieveSQLWarnings);
      socketOutput.flush();
      // Read ack. We won't do anything with it but reading this it is the only
      // way to dectect that the controller is down (see SEQUOIA-632)
      socketInput.readBoolean();

      // Restore read-only state (not part of RestoreConnectionState command for
      // backward compatibility)
      setReadOnly(readOnly);
    }
    catch (IOException e)
    {
      reconnect();
    }
  }

  /**
   * Before sending a command code, checks that the controller is
   * synchronized/ready to accept it. Then sends it.
   * 
   * @param command to send
   * @throws IOException on socket error
   * @throws DriverSQLException on protocol corruption
   */
  private void sendCommand(int command) throws IOException, DriverSQLException
  {
    if (socketInput.readInt() != Commands.ControllerPrompt)
      throw new DriverSQLException(
          "Protocol corruption while trying to send command: " + command
              + ". Check the previous command");
    socketOutput.writeInt(Commands.CommandPrefix);
    socketOutput.writeInt(command);
  }

  /**
   * @see #close()
   */
  private void throwSQLExceptionIfClosed(String message)
      throws DriverSQLException
  {
    if (isClosed)
      throw new DriverSQLException(message);
  }

  /**
   * @see #close()
   */
  private void throwSQLExceptionIfClosed() throws DriverSQLException
  {
    // default message
    throwSQLExceptionIfClosed("Tried to operate on a closed Connection");
  }

  private DriverSQLException wrapIOExceptionInDriverSQLException(
      String callerName, IOException ioe)
  {
    return new DriverSQLException("I/O Error on method " + callerName + "():\n"
        + ioe.getLocalizedMessage(), ioe);
  }

  /**
   * Utility function to convert the given chain of backendException to a chain
   * of SQLWarnings
   * 
   * @param toConvert exception chain to convert
   */
  protected SQLWarning convertToSQLWarnings(SerializableException toConvert)
  {
    if (toConvert == null)
      return null;
    SQLWarning sqlw = new SQLWarning(toConvert.getMessage(), toConvert
        .getSQLState(), toConvert.getErrorCode());
    Throwable t = toConvert.getCause();
    if (t != null)
    {
      if (t instanceof SerializableException)
        sqlw.setNextWarning(convertToSQLWarnings((SerializableException) t));
      else if (sequoiaUrl.isDebugEnabled())
        System.out.println("Unexpected exception type " + t.getClass()
            + "while converting warning chain");
    }
    return sqlw;
  }

  /**
   * Performs a read request and return the reply.
   * 
   * @param request the read request to execute
   * @return a <code>java.sql.ResultSet</code> value
   * @exception DriverSQLException if an error occurs
   */
  protected synchronized DriverResultSet statementExecuteQuery(
      RequestWithResultSetParameters request) throws DriverSQLException,
      NotImplementedException
  {
    throwSQLExceptionIfClosed("Closed connection cannot process request '"
        + request.getSqlShortForm(ABBREV_REQUEST_LENGTH) + "'");
    beginTransactionIfNeeded();

    try
    {
      setConnectionParametersOnRequest(request);
      sendCommand(Commands.StatementExecuteQuery);
      request.sendToStream(socketOutput);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with request " + request);

      SQLWarning statementWarnings = receiveSQLWarnings();
      TypeTag tag = new TypeTag(socketInput);

      // First case, we received our ResultSet, let's fetch it.
      if (TypeTag.RESULTSET.equals(tag))
      {
        try
        {
          DriverResultSet drs = new DriverResultSet(this);
          drs.setStatementWarnings(statementWarnings);
          return drs;
        }
        catch (ProtocolException e)
        {
          throw new DriverSQLException("Protocol corruption in "
              + getCurrentMethodName() + " with request "
              + request.getSqlShortForm(ABBREV_REQUEST_LENGTH), e);
        }
        catch (IOException e)
        { // Error while reading, retry
          if (sequoiaUrl.isInfoEnabled())
            System.out.println("IOException occured trying to reconnect ("
                + e.getLocalizedMessage() + ")");
          reconnect();
          return statementExecuteQuery(request);
        }
      }

      if (TypeTag.NULL_RESULTSET.equals(tag))
        return null;

      // From this point on, we had an exception
      if (TypeTag.EXCEPTION.equals(tag))
      {
        Exception recvEx = null;
        recvEx = receiveException();
        // dirty hack until cleanup
        if (recvEx instanceof ControllerCoreException)
          recvEx = ((ControllerCoreException) recvEx)
              .compatibilityWrapperHack();

        if (recvEx instanceof NoMoreBackendException)
        {
          if (sequoiaUrl.isInfoEnabled())
            System.out.println("No more backend available on controller");
          throw new DriverSQLException(recvEx);
        }
        else if (recvEx instanceof IOException)
        {
          // We shouldn't have been able to receive this IOE because it means
          // that the socket died (at the controller side)
          throw new ProtocolException("Received exception of unexpected type ("
              + ((IOException) recvEx).getLocalizedMessage() + ")");
        }
        else if (recvEx instanceof BackendDriverException)
        {
          // TODO: temporary fix until DriverSQLException is fixed
          throw new DriverSQLException((SerializableException) recvEx);
        }
        else if (recvEx instanceof NotImplementedException)
        {
          // incredibly ugly.
          throw (NotImplementedException) recvEx;
        }
        else if (recvEx instanceof SQLException)
        {
          throw new DriverSQLException((SQLException) recvEx);
        }
      }

      // Error, unexpected answer
      throw new ProtocolException("Protocol corruption in "
          + getCurrentMethodName() + " for request "
          + request.getSqlShortForm(ABBREV_REQUEST_LENGTH));
    }
    catch (SerializableException se)
    {
      throw new DriverSQLException(se);
    }
    catch (RuntimeException e)
    {
      e.printStackTrace();
      throw new DriverSQLException(getCurrentMethodName()
          + ": error occured while request '"
          + request.getSqlShortForm(ABBREV_REQUEST_LENGTH)
          + "' was processed by Sequoia Controller", e);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the query
      try
      {
        if (sequoiaUrl.isInfoEnabled())
          System.out.println("IOException occured trying to reconnect ("
              + e.getMessage() + ")");
        reconnect();
        return statementExecuteQuery(request);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " with request '"
            + request.getSqlShortForm(ABBREV_REQUEST_LENGTH)
            + "' and automatic reconnect failed (" + e1 + ")", e1);
      }
    }
  }

  /**
   * Performs a write request and return the number of rows affected.
   * 
   * @param request the write request to execute
   * @return number of rows affected
   * @exception DriverSQLException if an error occurs
   */
  protected synchronized ResultAndWarnings statementExecuteUpdate(
      Request request) throws DriverSQLException
  {
    throwSQLExceptionIfClosed("Closed connection cannot process request '"
        + request.getSqlShortForm(ABBREV_REQUEST_LENGTH) + "'");
    beginTransactionIfNeeded();

    boolean requestIdIsSet = false;
    try
    {
      setConnectionParametersOnRequest(request);
      sendCommand(Commands.StatementExecuteUpdate);
      request.sendToStream(socketOutput);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with request " + request);

      request.setId(receiveLongOrException());
      requestIdIsSet = true;
      if (!autoCommit)
        writeExecutedInTransaction = true;

      SQLWarning statementWarnings = receiveSQLWarnings();
      int uc = receiveIntOrException();
      return new ResultAndWarnings(uc, statementWarnings);

    }
    catch (SerializableException se)
    {
      throw new DriverSQLException(se);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the query
      try
      {
        reconnect();
        if (requestIdIsSet)
        { // Controller handled the query, check if it was executed
          ResultAndWarnings result = retrieveExecuteUpdateResult(request);
          if (result != null && result.getUpdateCount() != -1)
          {
            return result;
          }
        }
        // At this point the query failed before any controller succeeded in
        // executing the query

        return statementExecuteUpdate(request);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " with request '"
            + request.getSqlShortForm(ABBREV_REQUEST_LENGTH)
            + "' and automatic reconnect failed (" + e1 + ")", e1);
      }
    }
  }

  /**
   * Call a request that returns a list of results.
   * 
   * @param request the request to execute
   * @return a <code>List</code> of results
   * @throws DriverSQLException if an error occurs
   */
  protected synchronized ResultAndWarnings statementExecute(
      RequestWithResultSetParameters request) throws DriverSQLException
  {
    throwSQLExceptionIfClosed("Closed Connection cannot process request '"
        + request.getSqlShortForm(ABBREV_REQUEST_LENGTH) + "'");
    beginTransactionIfNeeded();

    boolean requestIdIsSet = false;
    try
    {
      setConnectionParametersOnRequest(request);
      sendCommand(Commands.StatementExecute);
      request.sendToStream(socketOutput);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with request" + request);

      request.setId(receiveLongOrException());
      requestIdIsSet = true;
      if (!autoCommit)
        writeExecutedInTransaction = true;

      SQLWarning sqlw = receiveSQLWarnings();
      List res = fetchMultipleResultsFromStream(getCurrentMethodName());
      return new ResultAndWarnings(res, sqlw);
    }
    catch (RuntimeException e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + ": Error occured while request '"
          + request.getSqlShortForm(ABBREV_REQUEST_LENGTH)
          + "' was processed by Sequoia Controller", e);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the query
      try
      {
        reconnect();
        if (requestIdIsSet)
        { // Controller handled the query, check if it was executed
          ResultAndWarnings rww = retrieveExecuteResult(request);
          if (rww != null && rww.getResultList() != null)
            return rww;
        }
        // At this point the query failed before any controller succeeded in
        // executing the query

        return statementExecute(request);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " with request '"
            + request.getSqlShortForm(ABBREV_REQUEST_LENGTH)
            + "' and automatic reconnect failed (" + e1 + ")", e1);
      }
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
  }

  /**
   * Performs a write request and returns the auto-generated keys
   * 
   * @param request the write request to execute
   * @return auto generated keys
   * @exception DriverSQLException if an error occurs
   */
  protected synchronized DriverGeneratedKeysResult statementExecuteUpdateWithKeys(
      Request request) throws DriverSQLException
  {
    throwSQLExceptionIfClosed("Closed Connection cannot process request '"
        + request.getSqlShortForm(ABBREV_REQUEST_LENGTH) + "'");
    beginTransactionIfNeeded();

    boolean requestIdIsSet = false;
    try
    {
      setConnectionParametersOnRequest(request);
      sendCommand(Commands.StatementExecuteUpdateWithKeys);
      request.sendToStream(socketOutput);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with request: " + request);

      try
      {
        request.setId(receiveLongOrException());
        requestIdIsSet = true;
        if (!autoCommit)
          writeExecutedInTransaction = true;

        // Receive the warnings
        SQLWarning sqlw = receiveSQLWarnings();

        // Receive the update count or an exception
        int updateCount = receiveIntOrException();

        // Fetch the ResultSet containing the autogenerated keys.
        TypeTag tag = new TypeTag(socketInput);
        if (TypeTag.RESULTSET.equals(tag))
        {
          try
          {
            DriverResultSet drs = new DriverResultSet(this);
            return new DriverGeneratedKeysResult(drs, updateCount, sqlw);
          }
          catch (ProtocolException e)
          {
            throw new DriverSQLException("Protocol corruption in "
                + getCurrentMethodName() + " with request "
                + request.getSqlShortForm(ABBREV_REQUEST_LENGTH), e);
          }
          catch (IOException e)
          { // Error while reading, retry
            if (sequoiaUrl.isInfoEnabled())
              System.out.println("IOException occured trying to reconnect ("
                  + e.getLocalizedMessage() + ")");
            reconnect();
            return statementExecuteUpdateWithKeys(request);
          }
        }

        if (TypeTag.NULL_RESULTSET.equals(tag))
          return new DriverGeneratedKeysResult(null, updateCount, sqlw);

        // Error, unexpected answer
        throw new ProtocolException("Protocol corruption in "
            + getCurrentMethodName() + " for request "
            + request.getSqlShortForm(ABBREV_REQUEST_LENGTH));
      }
      catch (SerializableException e)
      {
        throw new DriverSQLException(e);
      }
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the query
      try
      {
        reconnect();
        if (requestIdIsSet)
        { // Controller handled the query, check if it was executed
          DriverGeneratedKeysResult result = retrieveExecuteUpdateWithKeysResult(request);
          if (result != null)
            return result;
        }
        // At this point the query failed before any controller succeeded in
        // executing the query
        return statementExecuteUpdateWithKeys(request);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " with request '"
            + request.getSqlShortForm(ABBREV_REQUEST_LENGTH)
            + "' and automatic reconnect failed (" + e1 + ")", e1);
      }
    }
  }

  /**
   * Call a stored procedure (with IN/OUT and/or named parameters) that returns
   * a ResultSet.
   * 
   * @param proc the stored procedure call
   * @return a List composed of a <code>java.sql.ResultSet</code> value, an
   *         <code>ArrayList</code> of OUT parameters, and a
   *         <code>HashMap</code> of named parameters result objects.
   * @exception DriverSQLException if an error occurs
   */
  protected synchronized ResultAndWarnings callableStatementExecuteQuery(
      RequestWithResultSetParameters proc) throws DriverSQLException
  {
    throwSQLExceptionIfClosed("Closed Connection cannot process request '"
        + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH) + "'");
    beginTransactionIfNeeded();

    boolean procIdIsSet = false;
    try
    {
      setConnectionParametersOnRequest(proc);
      sendCommand(Commands.CallableStatementExecuteQueryWithParameters);
      proc.sendToStream(socketOutput);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with procedure '" + proc + "'");

      proc.setId(receiveLongOrException());
      procIdIsSet = true;
      if (!autoCommit)
        writeExecutedInTransaction = true;

      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Received unique id " + proc.getId()
            + " for procedure '" + proc + "'");

      SQLWarning sqlw = receiveSQLWarnings();

      Exception recvEx = null;
      TypeTag tag = new TypeTag(socketInput);

      DriverResultSet drs = null;

      /*
       * TODO: the code below is a complete mess. One reason is it's still half
       * legacy design from old protocol, half from the new procotol. It could
       * easily be made much simpler. We should use #receiveResultSet() TODO:
       * test NoMoreBackendException
       */
      if (!TypeTag.NULL_RESULTSET.equals(tag))
      {
        if (TypeTag.EXCEPTION.equals(tag))
        {
          recvEx = receiveException();
          // dirty hack until cleanup
          if (recvEx instanceof ControllerCoreException)
            recvEx = ((ControllerCoreException) recvEx)
                .compatibilityWrapperHack();

          if (recvEx instanceof DriverSQLException)
            throw (DriverSQLException) recvEx;

          throw new DriverSQLException(recvEx);
        }
        else if (!TypeTag.RESULTSET.equals(tag))
          throw new DriverSQLException(getCurrentMethodName()
              + ": Unexpected response for request "
              + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH), recvEx);

        // Ok, we have a real non-null ResultSet, fetch it
        drs = new DriverResultSet(this);
        if (sequoiaUrl.isDebugEnabled())
          System.out.println("Retrieved ResultSet: " + drs);
      }

      // Now fetch the OUT parameters
      HashMap outParameters = fetchOutParameters();

      // Now fetch the named parameters
      HashMap namedParameters = fetchNamedParameters();

      List result = new ArrayList(3);
      result.add(drs);
      result.add(outParameters);
      result.add(namedParameters);
      return new ResultAndWarnings(result, sqlw);
    }
    catch (RuntimeException e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + ": Error occured while request '"
          + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH)
          + "' was processed by Sequoia Controller", e);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the query
      try
      {
        reconnect();
        if (procIdIsSet)
        { // Controller handled the query, check if it was executed
          ResultAndWarnings result = retrieveExecuteQueryResultWithParameters(proc);
          if (result != null && result.getResultList() != null)
            return result;
        }
        // At this point the query failed before any controller succeeded in
        // executing the query

        return callableStatementExecuteQuery(proc);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " on procedure request '"
            + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH)
            + "' and automatic reconnect failed (" + e1 + ")", e1);
      }
    }
    catch (SerializableException se)
    {
      throw new DriverSQLException(se);
    }
  }

  /**
   * Call a stored procedure that performs an update.
   * 
   * @param proc the stored procedure call
   * @return a List composed of an Integer (number of updated rows), an
   *         <code>ArrayList</code> of OUT parameters, and a
   *         <code>HashMap</code> of named parameters result objects.
   * @exception DriverSQLException if an error occurs
   */
  protected synchronized ResultAndWarnings callableStatementExecuteUpdate(
      Request proc) throws DriverSQLException
  {
    throwSQLExceptionIfClosed("Closed Connection cannot process request '"
        + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH) + "'");
    beginTransactionIfNeeded();

    boolean procIdIsSet = false;
    try
    {
      setConnectionParametersOnRequest(proc);
      sendCommand(Commands.CallableStatementExecuteUpdateWithParameters);
      proc.sendToStream(socketOutput);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with procedure '" + proc + "'");

      proc.setId(receiveLongOrException());
      procIdIsSet = true;
      if (!autoCommit)
        writeExecutedInTransaction = true;

      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Received unique id " + proc.getId()
            + " for procedure '" + proc + "'");

      // Receive SQLWarnings
      SQLWarning sqlw = receiveSQLWarnings();

      // Receive update count
      Integer updateCount = new Integer(receiveIntOrException());

      // Now fetch the OUT parameters
      HashMap outParameters = fetchOutParameters();

      // Now fetch the named parameters
      HashMap namedParameters = fetchNamedParameters();

      List result = new ArrayList(3);
      result.add(updateCount);
      result.add(outParameters);
      result.add(namedParameters);
      return new ResultAndWarnings(result, sqlw);

    }
    catch (SerializableException se)
    {
      throw new DriverSQLException(se);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the query
      try
      {
        reconnect();
        if (procIdIsSet)
        { // Controller handled the query, check if it was executed
          ResultAndWarnings result = retrieveExecuteUpdateResultWithParameters(proc);
          if (result != null && result.getResultList() != null)
            return result;
        }
        // At this point the query failed before any controller succeeded in
        // executing the query

        return callableStatementExecuteUpdate(proc);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " on procedure request '"
            + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH)
            + "' and automatic reconnect failed (" + e1 + ")", e1);
      }
    }
  }

  /**
   * Call a stored procedure that returns a list of results.
   * 
   * @param proc the stored procedure call
   * @return a List composed of 1. <code>List</code> of results, 2.
   *         <code>ArrayList</code> of OUT parameters, and 3.
   *         <code>HashMap</code> of named parameters result objects.
   * @exception DriverSQLException if an error occurs
   */
  protected synchronized ResultAndWarnings callableStatementExecute(
      RequestWithResultSetParameters proc) throws DriverSQLException
  {
    throwSQLExceptionIfClosed("Closed Connection cannot process request '"
        + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH) + "'");
    beginTransactionIfNeeded();

    boolean procIdIsSet = false;
    try
    {
      setConnectionParametersOnRequest(proc);
      sendCommand(Commands.CallableStatementExecuteWithParameters);
      proc.sendToStream(socketOutput);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName()
            + " with procedure '" + proc + "'");

      proc.setId(receiveLongOrException());
      procIdIsSet = true;
      if (!autoCommit)
        writeExecutedInTransaction = true;

      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Received unique id " + proc.getId()
            + " for procedure '" + proc + "'");

      // Fetch warnings
      SQLWarning sqlw = receiveSQLWarnings();

      // Fetch results
      List resultList = fetchMultipleResultsFromStream(getCurrentMethodName());

      // Now fetch the OUT parameters
      HashMap outParameters = fetchOutParameters();

      // Now fetch the named parameters
      HashMap namedParameters = fetchNamedParameters();

      List result = new ArrayList(3);
      result.add(resultList);
      result.add(outParameters);
      result.add(namedParameters);
      return new ResultAndWarnings(result, sqlw);
    }
    catch (RuntimeException e)
    {
      throw new DriverSQLException(getCurrentMethodName()
          + ": Error occured while request '"
          + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH)
          + "' was processed by Sequoia Controller", e);
    }
    catch (IOException e)
    { // Connection failed, try to reconnect and re-exec the query
      try
      {
        reconnect();
        if (procIdIsSet)
        { // Controller handled the query, check if it was executed
          ResultAndWarnings rww = retrieveExecuteResultWithParameters(proc);
          if (rww != null && rww.getResultList() != null)
            return rww;
        }
        // At this point the query failed before any controller succeeded in
        // executing the query

        return callableStatementExecute(proc);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " with procedure request'"
            + proc.getSqlShortForm(ABBREV_REQUEST_LENGTH)
            + "' and automatic reconnect failed (" + e1 + ")", e1);
      }
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
  }

  //
  // Database Metadata methods
  //

  /**
   * Closes the remote ResultSet given its cursor name.
   * 
   * @param cursorName cursor name of the ResultSet to close.
   * @throws SQLException if an error occurs
   */
  protected synchronized void closeRemoteResultSet(String cursorName)
      throws SQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.CloseRemoteResultSet);
      socketOutput.writeLongUTF(cursorName);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Closing remote ResultSet");

      receiveBooleanOrException();
    }
    catch (SerializableException se)
    {
      throw new DriverSQLException(se);
    }
    catch (IOException e)
    {
      throw wrapIOExceptionInDriverSQLException(getCurrentMethodName(), e);
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getAttributes(String catalog,
      String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetAttributes);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(typeNamePattern);
      socketOutput.writeLongUTF(attributeNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + typeNamePattern + ","
            + attributeNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getAttributes(catalog, schemaPattern, typeNamePattern,
            attributeNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String,
   *      java.lang.String, java.lang.String, int, boolean)
   */
  protected synchronized ResultSet getBestRowIdentifier(String catalog,
      String schema, String table, int scope, boolean nullable)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetBestRowIdentifier);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schema);
      socketOutput.writeLongUTF(table);
      socketOutput.writeInt(scope);
      socketOutput.writeBoolean(nullable);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schema + "," + table + "," + scope + "," + nullable + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getBestRowIdentifier(catalog, schema, table, scope, nullable);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getColumnPrivileges(String catalog,
      String schemaPattern, String tableName, String columnNamePattern)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetColumnPrivileges);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(tableName);
      socketOutput.writeLongUTF(columnNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + tableName + "," + columnNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getColumnPrivileges(catalog, schemaPattern, tableName,
            columnNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getColumns(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getColumns(String catalog,
      String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetColumns);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(tableNamePattern);
      socketOutput.writeLongUTF(columnNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + tableNamePattern + "," + columnNamePattern
            + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getColumns(catalog, schemaPattern, tableNamePattern,
            columnNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * Get the Sequoia controller version number.
   * 
   * @return a String containing the controller version
   * @exception DriverSQLException if an error occurs
   */
  protected synchronized String getControllerVersionNumber()
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.GetControllerVersionNumber);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Executing " + getCurrentMethodName());

      return receiveStringOrException();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getControllerVersionNumber();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getCrossReference(String primaryCatalog,
      String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetCrossReference);
      socketOutput.writeLongUTF(primaryCatalog);
      socketOutput.writeLongUTF(primarySchema);
      socketOutput.writeLongUTF(primaryTable);
      socketOutput.writeLongUTF(foreignCatalog);
      socketOutput.writeLongUTF(foreignSchema);
      socketOutput.writeLongUTF(foreignTable);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + primaryCatalog + ","
            + primarySchema + "," + primaryTable + "," + foreignCatalog + ","
            + foreignSchema + "," + foreignTable + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getCrossReference(primaryCatalog, primarySchema, primaryTable,
            foreignCatalog, foreignSchema, foreignTable);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see DatabaseMetaData#getDatabaseProductName()
   */
  protected synchronized String getDatabaseProductName()
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetDatabaseProductName);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName());

      return receiveStringOrException();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getDatabaseProductName();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getExportedKeys(String catalog,
      String schema, String table) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetExportedKeys);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schema);
      socketOutput.writeLongUTF(table);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schema + "," + table + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getExportedKeys(catalog, schema, table);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getImportedKeys(String catalog,
      String schema, String table) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetImportedKeys);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schema);
      socketOutput.writeLongUTF(table);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schema + "," + table + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getImportedKeys(catalog, schema, table);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String,
   *      java.lang.String, java.lang.String, boolean, boolean)
   */
  protected synchronized ResultSet getIndexInfo(String catalog, String schema,
      String table, boolean unique, boolean approximate)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetIndexInfo);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schema);
      socketOutput.writeLongUTF(table);
      socketOutput.writeBoolean(unique);
      socketOutput.writeBoolean(approximate);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schema + "," + table + "," + unique + "," + approximate + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getIndexInfo(catalog, schema, table, unique, approximate);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @param sqlTemplate sql template of the PreparedStatement
   * @see java.sql.PreparedStatement#getMetaData()
   */
  protected synchronized ResultSetMetaData preparedStatementGetMetaData(
      String sqlTemplate) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.PreparedStatementGetMetaData);
      socketOutput.writeLongUTF(sqlTemplate);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "()");

      return receiveResultSet(getCurrentMethodName()).getMetaData();
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (SQLException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return preparedStatementGetMetaData(sqlTemplate);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @param sqlTemplate sql template of the PreparedStatement
   * @see java.sql.PreparedStatement#getParameterMetaData()
   */
  protected synchronized ParameterMetaData preparedStatementGetParameterMetaData(
      String sqlTemplate) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.PreparedStatementGetParameterMetaData);
      socketOutput.writeLongUTF(sqlTemplate);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "()");

      TypeTag tag = new TypeTag(socketInput);

      if (TypeTag.PP_PARAM_METADATA.equals(tag))
      {
        if (socketInput.readBoolean() == false)
          return null;
        return new SequoiaParameterMetaData(socketInput);
      }

      if (TypeTag.EXCEPTION.equals(tag))
        throw receiveException();

      throw new ProtocolException(
          "Expected a prepared statement parameter metadata, received unexpected tag: "
              + tag);
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (SQLException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return preparedStatementGetParameterMetaData(sqlTemplate);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getPrimaryKeys(String catalog,
      String schemaPattern, String tableNamePattern) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetPrimaryKeys);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(tableNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + tableNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  protected synchronized java.sql.ResultSet getProcedures(String catalog,
      String schemaPattern, String procedureNamePattern)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetProcedures);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(procedureNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + procedureNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getProcedures(catalog, schemaPattern, procedureNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  protected synchronized java.sql.ResultSet getProcedureColumns(String catalog,
      String schemaPattern, String procedureNamePattern,
      String columnNamePattern) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetProcedureColumns);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(procedureNamePattern);
      socketOutput.writeLongUTF(columnNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + procedureNamePattern + ","
            + columnNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getProcedureColumns(catalog, schemaPattern,
            procedureNamePattern, columnNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSchemas()
   */
  protected synchronized ResultSet getSchemas() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();

    try
    {
      sendCommand(Commands.DatabaseMetaDataGetSchemas);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName());

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getSchemas();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getSuperTables(String catalog,
      String schemaPattern, String tableNamePattern) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetSuperTables);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(tableNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + tableNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getSuperTables(catalog, schemaPattern, tableNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getSuperTypes(String catalog,
      String schemaPattern, String typeNamePattern) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetSuperTypes);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(typeNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + typeNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getSuperTypes(catalog, schemaPattern, typeNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException(
            "Connection lost while executing getSuperTypes and automatic reconnect failed ",
            e1);
      }
    }
  }

  /**
   * Retrieve a static metadata from the controller.
   * 
   * @param key the "getXXX(Y,Z,...)" hash key of the metadata query
   * @return an Object that will be an <tt>Integer</tt> or <tt>Boolean</tt>
   *         or <tt>String</tt>
   * @throws DriverSQLException if fails
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#databaseStaticMetadata()
   * @see org.continuent.sequoia.controller.backend.DatabaseBackendMetaData#retrieveDatabaseMetadata()
   */
  protected synchronized Object getStaticMetadata(String key)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseStaticMetadata);
      socketOutput.writeLongUTF(key);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out.println("Getting " + key + " metadata");

      TypeTag tag = new TypeTag(socketInput);

      if (TypeTag.EXCEPTION.equals(tag))
        throw new DriverSQLException(receiveException());
      else
      {
        tag = new TypeTag(socketInput);
        Object result = SQLDataSerialization.getSerializer(tag)
            .receiveFromStream(socketInput);
        return result;
      }
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getStaticMetadata(key);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * Gets a description of the access rights for each table available in a
   * catalog. Note that a table privilege applies to one or more columns in the
   * table. It would be wrong to assume that this priviledge applies to all
   * columns (this may be true for some systems but is not true for all.) Only
   * privileges matching the schema and table name criteria are returned. They
   * are ordered by TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
   * 
   * @param catalog a catalog name; "" retrieves those without a catalog; null
   *          means drop catalog name from the selection criteria
   * @param schemaPattern a schema name pattern; "" retrieves those without a
   *          schema
   * @param tableNamePattern a table name pattern
   * @return <code>ResultSet</code> each row is a table privilege description
   * @throws DriverSQLException if a database access error occurs
   */
  protected synchronized ResultSet getTablePrivileges(String catalog,
      String schemaPattern, String tableNamePattern) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetTablePrivileges);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(tableNamePattern);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + tableNamePattern + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getTablePrivileges(catalog, schemaPattern, tableNamePattern);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.driver.DatabaseMetaData#getTables(String,
   *      String, String, String[])
   */
  protected synchronized ResultSet getTables(String catalog,
      String schemaPattern, String tableNamePattern, String[] types)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetTables);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(tableNamePattern);

      if (null == types)
        socketOutput.writeBoolean(false);
      else
      {
        socketOutput.writeBoolean(true);
        socketOutput.writeInt(types.length);
        for (int i = 0; i < types.length; i++)
          socketOutput.writeLongUTF(types[i]);
      }
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + tableNamePattern + "," + types + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getTables(catalog, schemaPattern, tableNamePattern, types);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * Gets the table types available in this database. The results are ordered by
   * table type.
   * 
   * @return <code>ResultSet</code> each row has a single String column that
   *         is a catalog name
   * @throws SQLException if a database error occurs
   */
  protected synchronized ResultSet getTableTypes() throws SQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetTableTypes);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName());

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getTableTypes();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getTypeInfo()
   */
  protected synchronized ResultSet getTypeInfo() throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetTypeInfo);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "()");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getTypeInfo();
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String,
   *      java.lang.String, int[])
   */
  protected synchronized ResultSet getUDTs(String catalog,
      String schemaPattern, String typeNamePattern, int[] types)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetUDTs);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schemaPattern);
      socketOutput.writeLongUTF(typeNamePattern);

      if (null == types)
        socketOutput.writeBoolean(false);
      else
      {
        socketOutput.writeBoolean(true);
        socketOutput.writeInt(types.length);
        for (int i = 0; i < types.length; i++)
          socketOutput.writeInt(types[i]);
      }
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schemaPattern + "," + typeNamePattern + "," + types + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getUDTs(catalog, schemaPattern, typeNamePattern, types);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  protected synchronized ResultSet getVersionColumns(String catalog,
      String schema, String table) throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.DatabaseMetaDataGetVersionColumns);
      socketOutput.writeLongUTF(catalog);
      socketOutput.writeLongUTF(schema);
      socketOutput.writeLongUTF(table);
      socketOutput.flush();

      if (sequoiaUrl.isDebugEnabled())
        System.out.println(getCurrentMethodName() + "(" + catalog + ","
            + schema + "," + table + ")");

      return receiveResultSet(getCurrentMethodName());
    }
    catch (SerializableException e)
    {
      throw new DriverSQLException(e);
    }
    catch (IOException e)
    {
      try
      {
        reconnect();
        return getVersionColumns(catalog, schema, table);
      }
      catch (DriverSQLException e1)
      {
        throw new DriverSQLException("Connection lost while executing "
            + getCurrentMethodName() + " and automatic reconnect failed ", e1);
      }
    }
  }

  /**
   * Send "FetchNextResultSetRows" command to controller. Throws an SQL
   * exception if controller returns an error; else returns void and lets the
   * caller receive its new rows by itself.
   * 
   * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread#fetchNextResultSetRows()
   * @param cursorName name of the ResultSet cursor
   * @param fetchSize number of rows to fetch
   * @throws DriverSQLException if an error occurs
   */
  protected synchronized void tryFetchNext(String cursorName, int fetchSize)
      throws DriverSQLException
  {
    throwSQLExceptionIfClosed();
    try
    {
      sendCommand(Commands.FetchNextResultSetRows);
      socketOutput.writeLongUTF(cursorName);
      socketOutput.writeInt(fetchSize);
      socketOutput.flush();
      if (sequoiaUrl.isDebugEnabled())
        System.out
            .println("Fetching next " + fetchSize + " from " + cursorName);

      TypeTag tag = new TypeTag(socketInput);

      if (TypeTag.EXCEPTION.equals(tag))
        throw new DriverSQLException(receiveException());

      if (!TypeTag.NOT_EXCEPTION.equals(tag))
        throw new ProtocolException();

      // success, now we can let the DriverResultSet caller receive its data.

    }
    catch (IOException e)
    {
      throw wrapIOExceptionInDriverSQLException(getCurrentMethodName(), e);
    }
  }

  /**
   * Gets the caller's method name.
   * 
   * @return method name of the calling function
   */
  static String getCurrentMethodName()
  {
    return new Throwable().getStackTrace()[1].getMethodName();
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    // we could use println() instead of LINE_SEPARATOR here
    return "url:"
        + getUrl()
        + LINE_SEPARATOR
        + socket
        + LINE_SEPARATOR
        + ((sequoiaUrl != null) && (sequoiaUrl.getParameters() != null)
            ? "properties:" + sequoiaUrl.getParameters() + LINE_SEPARATOR
            : "") + "user:" + getUserName() + LINE_SEPARATOR
        + "connection pooling:" + connectionPooling + LINE_SEPARATOR
        + "escape backslash:" + escapeBackslash + LINE_SEPARATOR
        + "escape char:" + escapeChar + LINE_SEPARATOR + "escape single quote:"
        + escapeSingleQuote + LINE_SEPARATOR;
  }

@Override
public <T> T unwrap(Class<T> iface) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public boolean isWrapperFor(Class<?> iface) throws SQLException {
	// TODO Auto-generated method stub
	return false;
}

@Override
public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public Clob createClob() throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Blob createBlob() throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public NClob createNClob() throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public SQLXML createSQLXML() throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public boolean isValid(int timeout) throws SQLException {
	// TODO Auto-generated method stub
	return false;
}

@Override
public void setClientInfo(String name, String value)
		throws SQLClientInfoException {
	// TODO Auto-generated method stub
	
}

@Override
public void setClientInfo(Properties properties) throws SQLClientInfoException {
	// TODO Auto-generated method stub
	
}

@Override
public String getClientInfo(String name) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Properties getClientInfo() throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Array createArrayOf(String typeName, Object[] elements)
		throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public Struct createStruct(String typeName, Object[] attributes)
		throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setSchema(String schema) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public String getSchema() throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void abort(Executor executor) throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public void setNetworkTimeout(Executor executor, int milliseconds)
		throws SQLException {
	// TODO Auto-generated method stub
	
}

@Override
public int getNetworkTimeout() throws SQLException {
	// TODO Auto-generated method stub
	return 0;
}
}
