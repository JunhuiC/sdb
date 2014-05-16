/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Julie Marguerite, Mathieu Peltier, Marc Herbert
 */

package org.continuent.sequoia.controller.requests;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.util.TransactionIsolationLevelConverter;

/**
 * An <code>AbstractRequest</code> defines the skeleton of an SQL request.
 * Requests have to be serializable (at least) for inter-controller
 * communications.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @version 1.0
 */
public abstract class AbstractRequest implements Serializable
{
  // Unneeded fields for controller-controller communication are all tagged
  // as transient

  /**
   * Rationale is to avoid the performance cost of .instanceof(), at
   * serialization time and elsewhere.
   */
  int                         requestType                 = RequestType.UNDEFINED;

  /** Request unique id (set by the controller). */
  protected long              id;

  /** Recovery log id (local to the controller, set by the controller) */
  protected transient long    logId;

  /**
   * SQL query (should be set in constructor) or query template if this is a
   * PreparedStatement.
   */
  protected String            sqlQueryOrTemplate;

  /**
   * Encoded PreparedStatement paremeters (null if this is a statement)
   */
  protected String            preparedStatementParameters = null;

  /**
   * Unique key identifying the query. If the request is executed in a
   * statement, the unique key is the same as sqlQuery else is it is a
   * combination of the sqlSkeleton and the parameters contained in sqlQuery.
   */
  protected transient String  uniqueKey                   = null;

  /**
   * Login used to issue this request (must be set by the
   * VirtualDatabaseWorkerThread).
   */
  protected String            login;

  /** Whether this request is cacheable or not. */
  protected transient int     cacheable                   = RequestType.UNCACHEABLE;

  /** Whether the SQL content has been parsed or not. */
  protected transient boolean isParsed                    = false;

  /** Where the macros in the SQL have been processed or not */
  private boolean             macrosAreProcessed          = false;

  /*
   * ResultSet Parameters
   */
  protected int               maxRows;
  protected int               fetchSize;
  protected String            cursorName;

  //
  // Connection related parameters
  //

  /** True if the connection has been set to read-only */
  protected boolean           isReadOnly                  = false;

  /**
   * Whether this request has been sent in <code>autocommit</code> mode or
   * not.
   */
  protected boolean           isAutoCommit;

  /**
   * Transaction identifier if this request belongs to a transaction. The value
   * is set by the VirtualDatabaseWorkerThread.
   */
  protected long              transactionId;

  /**
   * Transaction isolation level to use when executing the query inside a
   * transaction. The value is set by the VirtualDatabaseWorkerThread.
   */
  protected int               transactionIsolation;

  /**
   * Timeout for this request in seconds, value 0 means no timeout (should be
   * set in constructor). This timeout is in seconds, reflecting the jdbc-spec,
   * and is passed as-is to the backends jdbc-driver. Internally converted to ms
   * via getTimeoutMs().
   */
  protected int               timeoutInSeconds;

  /**
   * Should the backend driver do escape processing before sending to the
   * database? Simply forwarded to backend driver. No setter for this member,
   * should be set in constructor.
   * 
   * @see java.sql.Statement#setEscapeProcessing(boolean)
   */
  protected boolean           escapeProcessing            = true;

  /** @see #getLineSeparator() */
  private String              lineSeparator               = null;

  /**
   * True if this request executes on a persistent connection
   */
  private boolean             persistentConnection;

  /**
   * Persistent connection id if persistentConnection is true
   */
  private long                persistentConnectionId;

  /**
   * True if the controller must retrieve SQL warnings
   */
  private boolean             retrieveSQLWarnings         = false;

  //
  // Controller internal logic
  //

  private transient boolean   isLazyTransactionStart;

  /**
   * Ip addres of the client that sent the request
   */
  private String              clientIpAddress;

  /**
   * True if the request requires all connections to be closed to take effect.
   */
  protected boolean           requiresConnectionPoolFlush = false;

  /**
   * True if the request requires current connection to be closed instead of
   * beeing given back to the pool.
   */
  protected boolean           requiresConnectionFlush     = false;

  /**
   * True if the request will not check when running that the table exists in
   * the database schema. This is used to disable at the request level virtual
   * database behavior when enforceTableExistenceIntoSchema=true. By default,
   * disabledTableExistenceCheck = false implies that this request will be
   * executed according to virtual database setting.
   */
  protected boolean           disabledTableExistenceCheck = false;

  /**
   * Time in ms at which the request execution started
   */
  protected transient long    startTime;

  /**
   * Time in ms at which the request execution ended
   */
  protected transient long    endTime;

  /**
   * Sorted list of table names that must be locked in write. This list may be
   * null or empty if no table needs to be locked.
   */
  protected SortedSet         writeLockedTables           = null;

  /**
   * Default constructor Creates a new <code>AbstractRequest</code> object
   * 
   * @param sqlQuery the SQL query
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database ?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   * @param requestType the request type as defined in RequestType class
   * @see RequestType
   */
  public AbstractRequest(String sqlQuery, boolean escapeProcessing,
      int timeout, String lineSeparator, int requestType)
  {
    this.sqlQueryOrTemplate = sqlQuery;
    this.escapeProcessing = escapeProcessing;
    this.timeoutInSeconds = timeout;
    this.lineSeparator = lineSeparator;
    this.requestType = requestType;
  }

  //
  // Abstract methods
  //

  /**
   * Returns true if this request invalidates somehow the Aggregate List.
   * 
   * @return true if the aggregate list is altered.
   */
  public abstract boolean altersAggregateList();

  /**
   * Returns true if this request invalidates somehow the Database Catalog.
   * 
   * @return true if the database catalog is altered.
   */
  public abstract boolean altersDatabaseCatalog();

  /**
   * Returns true if this request invalidates somehow the Database Schema.
   * 
   * @return true if the database schema is altered.
   */
  public abstract boolean altersDatabaseSchema();

  /**
   * Returns true if this request invalidates somehow the Metadata Cache.
   * 
   * @return true if the metadata cache is altered.
   */
  public abstract boolean altersMetadataCache();

  /**
   * Returns true if this request invalidates somehow the Query Result Cache.
   * 
   * @return true if the query result cache is altered.
   */
  public abstract boolean altersQueryResultCache();

  /**
   * Returns true if this request invalidates somehow the Stored Procedure List.
   * 
   * @return true if the stored procedure list is altered.
   */
  public abstract boolean altersStoredProcedureList();

  /**
   * Returns true if this request invalidates somehow the User Defined Types.
   * 
   * @return true if the UDTs are altered.
   */
  public abstract boolean altersUserDefinedTypes();

  /**
   * Returns true if this request invalidates somehow the Users definition or
   * rights.
   * 
   * @return true if the users are altered.
   */
  public abstract boolean altersUsers();

  /**
   * Returns true if this request invalidates somehow any Sequoia internal
   * structure or caches (true if any of the other abstract altersXXX methods
   * returns true).
   * 
   * @return true if the request alters anything that triggers an invalidation
   *         in Sequoia.
   */
  public abstract boolean altersSomething();

  /**
   * Clones the parsing of a request.
   * 
   * @param request the parsed request to clone
   */
  public abstract void cloneParsing(AbstractRequest request);

  /**
   * Returns the list of database table names that must be write locked by the
   * execution of this request.
   * 
   * @return SortedSet of String containing table names to be write locked by
   *         this request. This list may be null or empty if no table needs to
   *         be locked.
   */
  public SortedSet getWriteLockedDatabaseTables()
  {
    return writeLockedTables;
  }

  /**
   * Returns <code>true</code> if this request requires macro (RAND(), NOW(),
   * ...) processing.
   * 
   * @return <code>true</code> if macro processing is required
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#handleMacros(AbstractRequest)
   */
  public abstract boolean needsMacroProcessing();

  /**
   * Parses the SQL request and extract the selected columns and tables given
   * the <code>DatabaseSchema</code> of the database targeted by this request.
   * <p>
   * An exception is thrown when the parsing fails. Warning, this method does
   * not check the validity of the request. In particular, invalid request could
   * be parsed without throwing an exception. However, valid SQL request should
   * never throw an exception.
   * 
   * @param schema a <code>DatabaseSchema</code> value
   * @param granularity parsing granularity as defined in
   *          <code>ParsingGranularities</code>
   * @param isCaseSensitive true if parsing must be case sensitive
   * @exception SQLException if the parsing fails
   */
  public abstract void parse(DatabaseSchema schema, int granularity,
      boolean isCaseSensitive) throws SQLException;

  //
  // Request type related functions
  //

  /**
   * Returns <code>true</code> if this request in a <code>ALTER</code>
   * statement.
   * 
   * @return a <code>boolean</code> value
   */
  public final boolean isAlter()
  {
    return RequestType.isAlter(this.requestType);
  }

  /**
   * Returns <code>true</code> if this request in a <code>CREATE</code>
   * statement.
   * 
   * @return a <code>boolean</code> value
   */
  public final boolean isCreate()
  {
    return RequestType.isCreate(this.requestType);
  }

  /**
   * Returns <code>true</code> if this request in a <code>DELETE</code>
   * statement.
   * 
   * @return a <code>boolean</code> value
   */
  public final boolean isDelete()
  {
    return RequestType.isDelete(this.requestType);
  }

  /**
   * Returns <code>true</code> if this request in a <code>DROP</code>
   * statement.
   * 
   * @return a <code>boolean</code> value
   */
  public final boolean isDrop()
  {
    return RequestType.isDrop(this.requestType);
  }

  /**
   * Returns <code>true</code> if this request in an <code>INSERT</code>
   * statement.
   * 
   * @return a <code>boolean</code> value
   */
  public final boolean isInsert()
  {
    return RequestType.isInsert(this.requestType);
  }

  /**
   * Returns <code>true</code> if this request in a <code>SELECT</code>
   * statement.
   * 
   * @return a <code>boolean</code> value
   */
  public final boolean isSelect()
  {
    return RequestType.isSelect(this.requestType);
  }

  /**
   * Returns <code>true</code> if this request in an <code>UPDATE</code>
   * statement.
   * 
   * @return a <code>boolean</code> value
   */
  public final boolean isUpdate()
  {
    return RequestType.isUpdate(this.requestType);
  }

  //
  // Getter/Setters
  //

  /**
   * Returns the cacheable status of this request. It can be:
   * {@link org.continuent.sequoia.controller.requests.RequestType#CACHEABLE},
   * {@link org.continuent.sequoia.controller.requests.RequestType#UNCACHEABLE}or
   * {@link org.continuent.sequoia.controller.requests.RequestType#UNIQUE_CACHEABLE}
   * 
   * @return a <code>int</code> value
   */
  public int getCacheAbility()
  {
    return cacheable;
  }

  /**
   * Set the cacheable status of this request. It can be:
   * {@link org.continuent.sequoia.controller.requests.RequestType#CACHEABLE},
   * {@link org.continuent.sequoia.controller.requests.RequestType#UNCACHEABLE}or
   * {@link org.continuent.sequoia.controller.requests.RequestType#UNIQUE_CACHEABLE}
   * 
   * @param cacheAbility a <code>int</code> value
   */
  public void setCacheAbility(int cacheAbility)
  {
    this.cacheable = cacheAbility;
  }

  /**
   * Returns the clientIpAddress value.
   * 
   * @return Returns the clientIpAddress.
   */
  public String getClientIpAddress()
  {
    return clientIpAddress;
  }

  /**
   * Sets the clientIpAddress value.
   * 
   * @param clientIpAddress The clientIpAddress to set.
   */
  public void setClientIpAddress(String clientIpAddress)
  {
    this.clientIpAddress = clientIpAddress;
  }

  /**
   * Returns the cursorName value.
   * 
   * @return Returns the cursorName.
   */
  public String getCursorName()
  {
    return cursorName;
  }

  /**
   * Sets the cursorName value.
   * 
   * @param cursorName The cursorName to set.
   */
  public void setCursorName(String cursorName)
  {
    this.cursorName = cursorName;
  }

  /**
   * Returns the endTime value.
   * 
   * @return Returns the endTime.
   */
  public long getEndTime()
  {
    return endTime;
  }

  /**
   * Sets the endTime value.
   * 
   * @param endTime The endTime to set.
   */
  public void setEndTime(long endTime)
  {
    this.endTime = endTime;
  }

  /**
   * Return the request execution time in milliseconds. If start time was not
   * set, 0 is returned. If endTime is not set, the diff between start time and
   * current time is returned. If start and end time are set, that elapsed time
   * between start and end is returned.
   * 
   * @return execution time in ms.
   */
  public long getExecTimeInMs()
  {
    long execTime = 0;
    if (startTime != 0)
    {
      if (endTime != 0)
        execTime = endTime - startTime;
      else
        execTime = System.currentTimeMillis() - startTime;
      if (execTime < 0)
        execTime = 0;
    }
    return execTime;
  }

  /**
   * Returns <code>true</code> if the driver should escape processing before
   * sending to the database?
   * 
   * @return a <code>boolean</code> value
   */
  public boolean getEscapeProcessing()
  {
    return escapeProcessing;
  }

  /**
   * Returns the fetchSize value.
   * 
   * @return Returns the fetchSize.
   */
  public int getFetchSize()
  {
    return fetchSize;
  }

  /**
   * Sets the fetchSize value.
   * 
   * @param fetchSize The fetchSize to set.
   * @see org.continuent.sequoia.driver.Statement
   */
  public void setFetchSize(int fetchSize)
  {
    this.fetchSize = fetchSize;
  }

  /**
   * Returns the unique id of this request.
   * 
   * @return the request id
   */
  public long getId()
  {
    return id;
  }

  /**
   * Sets the unique id of this request.
   * 
   * @param id the id to set
   */
  public void setId(long id)
  {
    this.id = id;
  }

  /**
   * Returns true if this request triggers a lazy transaction start that
   * requires a 'begin' to be logged into the reocvery log.
   * 
   * @return Returns the isLazyTransactionStart.
   */
  public final boolean isLazyTransactionStart()
  {
    return isLazyTransactionStart;
  }

  /**
   * Set a flag to tell whether this request triggers a lazy transaction start
   * that requires a 'begin' to be logged into the reocvery log.
   * 
   * @param isLazyStart true if a begin must be logged before this request
   */
  public void setIsLazyTransactionStart(boolean isLazyStart)
  {
    isLazyTransactionStart = isLazyStart;
  }

  /**
   * Returns the logId value.
   * 
   * @return Returns the logId.
   */
  public final long getLogId()
  {
    return logId;
  }

  /**
   * Sets the logId value.
   * 
   * @param logId The logId to set.
   */
  public final void setLogId(long logId)
  {
    this.logId = logId;
  }

  /**
   * Returns the login used to issue this request.
   * 
   * @return a <code>String</code> value
   */
  public String getLogin()
  {
    return login;
  }

  /**
   * Returns the lineSeparator used in the request string (required to parse
   * it). For practical reasons this is just
   * System.getProperty("line.separator") on the client. Used to be defined
   * per-request, now only once per-connection.
   * <http://www.unicode.org/reports/tr13/tr13-9.html>
   * 
   * @return the lineSeparator.
   */
  public String getLineSeparator()
  {
    return lineSeparator;
  }

  /**
   * Sets the lineSeparator value. Unfortunately needed at deserialization time.
   * Could be removed (and field made final) if lineSeparator was _always_
   * passed as an argument to the factory (currently only half of the time).
   * 
   * @param lineSeparator The lineSeparator to set.
   */
  public final void setLineSeparator(String lineSeparator)
  {
    this.lineSeparator = lineSeparator;
  }

  /**
   * Sets the login to use to issue this request.
   * 
   * @param login a <code>String</code> value
   */
  public void setLogin(String login)
  {
    this.login = login;
  }

  /**
   * Returns true if macros have already been processed in the request.
   * 
   * @return Returns the macrosAreProcessed value.
   */
  public boolean getMacrosAreProcessed()
  {
    return macrosAreProcessed;
  }

  /**
   * Sets the macrosAreProcessed value.
   * 
   * @param macrosAreProcessed The macrosAreProcessed to set.
   */
  public void setMacrosAreProcessed(boolean macrosAreProcessed)
  {
    this.macrosAreProcessed = macrosAreProcessed;
  }

  /**
   * Get the maximum number of rows the ResultSet can contain.
   * 
   * @return maximum number of rows
   * @see java.sql.Statement#getMaxRows()
   */
  public int getMaxRows()
  {
    return maxRows;
  }

  /**
   * Set the maximum number of rows in the ResultSet. Used only by Statement.
   * 
   * @param rows maximum number of rows
   * @see java.sql.Statement#setMaxRows(int)
   */
  public void setMaxRows(int rows)
  {
    maxRows = rows;
  }

  /**
   * Gets the SQL code of this request if this is a statement or sql template
   * (with ? as parameter placeholders) if this is a PreparedStatement.
   * 
   * @return the SQL query or the PreparedStatement template
   */
  public String getSqlOrTemplate()
  {
    return sqlQueryOrTemplate;
  }

  /**
   * Set the SQL statement or PreparedStatement template of this request.
   * Warning! The request parsing validity is not checked. The caller has to
   * recall {@link #parse(DatabaseSchema, int, boolean)} if needed.
   * 
   * @param sql SQL statement or statment template if this is a
   *          PreparedStatement
   * @see org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer#handleMacros(AbstractRequest)
   */
  public void setSqlOrTemplate(String sql)
  {
    this.sqlQueryOrTemplate = sql;
    // Reset the key (to make sure that getUniqueKey() is ok)
    uniqueKey = null;
  }

  /**
   * Get a short form of this request if the SQL statement exceeds
   * nbOfCharacters.
   * 
   * @param nbOfCharacters number of characters to include in the short form.
   * @return the nbOfCharacters first characters of the SQL statement
   */
  public String getSqlShortForm(int nbOfCharacters)
  {
    String key = getUniqueKey();
    if ((nbOfCharacters == 0) || (key.length() < nbOfCharacters))
      return key;
    else
      return key.substring(0, nbOfCharacters) + "...";
  }

  /**
   * Return the encoded parameters of the PreparedStatement (null if this is a
   * Statement)
   * 
   * @return encoded parameters
   */
  public String getPreparedStatementParameters()
  {
    return preparedStatementParameters;
  }

  /**
   * Set the parameters of the PreparedStatement
   * 
   * @param params parameters of the <code>PreparedStatement</code>.
   */
  public void setPreparedStatementParameters(String params)
  {
    preparedStatementParameters = params;
  }

  /**
   * Returns the startTime value.
   * 
   * @return Returns the startTime.
   */
  public long getStartTime()
  {
    return startTime;
  }

  /**
   * Sets the startTime value.
   * 
   * @param startTime The startTime to set.
   */
  public void setStartTime(long startTime)
  {
    this.startTime = startTime;
  }

  /**
   * Gets the timeout for this request in seconds.
   * 
   * @return timeout in seconds (0 means no timeout)
   */
  public int getTimeout()
  {
    return timeoutInSeconds;
  }

  /**
   * Sets the new timeout in seconds for this request.
   * 
   * @param timeout an <code>int</code> value
   * @see org.continuent.sequoia.controller.scheduler
   */
  public void setTimeout(int timeout)
  {
    this.timeoutInSeconds = timeout;
  }

  /**
   * Gets the identifier of the transaction if this request belongs to a
   * transaction, or -1 if this request does not belong to a transaction.
   * 
   * @return transaction identifier or -1
   */
  public long getTransactionId()
  {
    return transactionId;
  }

  /**
   * Sets the transaction identifier this request belongs to.
   * 
   * @param id transaction id
   */
  public void setTransactionId(long id)
  {
    transactionId = id;
  }

  /**
   * Returns the transaction isolation level.
   * 
   * @return Returns the transaction isolation.
   */
  public int getTransactionIsolation()
  {
    return transactionIsolation;
  }

  /**
   * Sets the transaction isolation level that must be used to execute this
   * request
   * 
   * @param isolationLevel the transaction isolation level
   */
  public void setTransactionIsolation(int isolationLevel)
  {
    this.transactionIsolation = isolationLevel;
  }

  /**
   * Get the type of the request as a String. <strong>This method is intended to
   * be used for debug and log only. Do not use in other cases.</strong>
   * 
   * @return the type of the request as a String.
   * @see RequestType#getTypeAsString(AbstractRequest)
   */
  public String getType()
  {
    return RequestType.getTypeAsString(this);
  }

  /**
   * Returns the unique key identifying this request.
   * 
   * @return Returns the uniqueKey.
   */
  public String getUniqueKey()
  {
    if (uniqueKey == null)
    {
      if (preparedStatementParameters == null)
        uniqueKey = sqlQueryOrTemplate;
      else
        uniqueKey = sqlQueryOrTemplate + "/" + preparedStatementParameters;
    }
    return uniqueKey;
  }

  /**
   * Returns <code>true</code> if the request should be executed in
   * <code>autocommit</code> mode.
   * 
   * @return a <code>boolean</code> value
   */
  public boolean isAutoCommit()
  {
    return isAutoCommit;
  }

  /**
   * Sets the autocommit mode for this request.
   * 
   * @param isAutoCommit <code>true</code> if <code>autocommit</code> should
   *          be used
   */
  public void setIsAutoCommit(boolean isAutoCommit)
  {
    this.isAutoCommit = isAutoCommit;
  }

  /**
   * Returns <code>true</code> if the request SQL content has been already
   * parsed.
   * 
   * @return a <code>boolean</code> value
   */
  public boolean isParsed()
  {
    return isParsed;
  }

  /**
   * Returns the persistentConnection value.
   * 
   * @return Returns the persistentConnection.
   */
  public final boolean isPersistentConnection()
  {
    return persistentConnection;
  }

  /**
   * Sets the persistentConnection value.
   * 
   * @param persistentConnection The persistentConnection to set.
   */
  public final void setPersistentConnection(boolean persistentConnection)
  {
    this.persistentConnection = persistentConnection;
  }

  /**
   * Returns the persistentConnectionId value.
   * 
   * @return Returns the persistentConnectionId.
   */
  public final long getPersistentConnectionId()
  {
    return persistentConnectionId;
  }

  /**
   * Sets the persistentConnectionId value.
   * 
   * @param persistentConnectionId The persistentConnectionId to set.
   */
  public final void setPersistentConnectionId(long persistentConnectionId)
  {
    this.persistentConnectionId = persistentConnectionId;
  }

  /**
   * Returns <code>true</code> if the connection is set to read-only
   * 
   * @return a <code>boolean</code> value
   */
  public boolean isReadOnly()
  {
    return isReadOnly;
  }

  /**
   * Sets the read-only mode for this request.
   * 
   * @param isReadOnly <code>true</code> if connection is read-only
   */
  public void setIsReadOnly(boolean isReadOnly)
  {
    this.isReadOnly = isReadOnly;
  }

  /**
   * Returns the requiresConnectionPoolFlush value.
   * 
   * @return Returns the requiresConnectionPoolFlush.
   */
  public boolean requiresConnectionPoolFlush()
  {
    return requiresConnectionPoolFlush;
  }

  /**
   * Returns the requiresConnectionFlush value.
   * 
   * @return Returns the requiresConnectionFlush.
   */
  public boolean requiresConnectionFlush()
  {
    return requiresConnectionFlush;
  }

  /**
   * Must the controller retrieve SQL warnings ?
   * 
   * @return true if SQLWarnings have to be fetched
   */
  public boolean getRetrieveSQLWarnings()
  {
    return retrieveSQLWarnings;
  }

  /**
   * Set the SQL Warnings retrieval property
   * 
   * @param isRetrieveSQLWarnings true if the controller should get SQL warnings
   */
  public void setRetrieveSQLWarnings(boolean isRetrieveSQLWarnings)
  {
    retrieveSQLWarnings = isRetrieveSQLWarnings;
  }

  //
  // Other methods and helpers
  //

  /**
   * Add depending tables to the list of tables already contained in the
   * provided set. No recursivity here since this is primarily design for
   * foreign keys which cannot have circular references.
   * 
   * @param schema the schema to use
   * @param lockedTables set of table names to add depending tables to
   */
  protected void addDependingTables(DatabaseSchema schema,
      SortedSet lockedTables)
  {
    if ((schema == null) || (lockedTables == null))
      return;

    List tablesToAdd = null;
    for (Iterator iter = lockedTables.iterator(); iter.hasNext();)
    {
      String tableName = (String) iter.next();

      // Find the depending tables for that table
      DatabaseTable table = schema.getTable(tableName, false);
      if (table == null)
        continue;
      ArrayList dependingTables = table.getDependingTables();
      if (dependingTables == null)
        continue;

      // Add these tables to a temp list so that we don't have a concurrent
      // modification exception on writeLockedTables on which we iterate
      if (tablesToAdd == null)
        tablesToAdd = new ArrayList();
      tablesToAdd.addAll(dependingTables);
    }
    // Finally add all depending tables
    if (tablesToAdd != null)
      lockedTables.addAll(tablesToAdd);
  }

  /**
   * Two requests are equal if they have the same unique id.
   * 
   * @param other an object
   * @return a <code>boolean</code> value
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof AbstractRequest))
      return false;

    AbstractRequest r = (AbstractRequest) other;
    return id == r.getId();
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return (int) id;
  }

  /**
   * If the query has a skeleton defined, return the skeleton with all carriage
   * returns and tabs replaced with spaces. If no SQL skeleton is defined, we
   * perform the same processing on the instanciated SQL statement.
   * <p>
   * Note that if no modification has to be done, the original string is
   * returned else a new string is constructed with the replaced elements.
   * 
   * @return statement with CR replaced by spaces
   */
  public String trimCarriageReturnAndTabs()
  {
    return replaceStringWithSpace(replaceStringWithSpace(sqlQueryOrTemplate,
        this.getLineSeparator()), "\t");
  }

  /**
   * Replaces any given <code>String</code> by a space in a given
   * <code>String</code>.
   * 
   * @param s the <code>String</code> to transform
   * @param toReplace the <code>String</code> to replace with spaces
   * @return the transformed <code>String</code>
   */
  private String replaceStringWithSpace(String s, String toReplace)
  {
    return s.replaceAll(toReplace, " ");
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return getUniqueKey();
  }

  /**
   * Same as toString but use a short form limited to the number of character
   * provided.
   * 
   * @param nbOfCharacters maximum number of characters to display (does not
   *          apply to the skeleton)
   * @return a String representation of the request
   * @see java.lang.Object#toString()
   */
  public String toStringShortForm(int nbOfCharacters)
  {
    return getSqlShortForm(nbOfCharacters);
  }

  /**
   * Displays useful information for parsing debugging
   * 
   * @return a string containing info about parsing results
   */
  public String getParsingResultsAsString()
  {
    StringBuffer sb = new StringBuffer(Translate.get("request.parsing.results"));
    sb.append(Translate.get("request.type", RequestType
        .getTypeAsString(requestType)));
    sb.append(Translate.get("request.unique.key", getUniqueKey()));
    if (writeLockedTables != null && writeLockedTables.size() > 0)
    {
      sb.append(Translate.get("request.locked.tables"));
      for (int i = 0; i < writeLockedTables.size(); i++)
      {
        sb.append(Translate.get("request.locked.table", writeLockedTables
            .toArray()[i]));
      }
    }
    return sb.toString();
  }

  /**
   * Returns a <code>String</code> containing debug information about this
   * request.
   * <p>
   * Information displayed (one line per info): id, query, parameters (if any),
   * login, autocommit, transaction id, cacheable status, isolation level, start
   * time, end time, timeout in seconds, write locked tables, persistent
   * connection id, client ip address.
   * 
   * @return a string containing:
   */
  public String toDebugString()
  {
    StringBuffer ret = new StringBuffer();
    ret.append("Request id: " + id);
    ret.append("\n   query: " + sqlQueryOrTemplate);
    if (preparedStatementParameters != null)
      ret.append("\n   parameters: " + preparedStatementParameters);
    ret.append("\n   login: " + login);
    ret.append("\n   autocommit: " + isAutoCommit);
    ret.append("\n   transaction id: " + transactionId);
    ret.append("\n   cacheable status: "
        + RequestType.getInformation(cacheable));
    ret.append("\n   isolation level: "
        + TransactionIsolationLevelConverter.toString(transactionIsolation));
    ret.append("\n   start time: " + startTime);
    ret.append("\n   end time: " + endTime);
    ret.append("\n   timeout in seconds: " + timeoutInSeconds);
    if (writeLockedTables != null && writeLockedTables.size() > 0)
    {
      ret.append("\n   locked tables:");
      for (int i = 0; i < writeLockedTables.size(); i++)
      {
        ret.append("\n      " + writeLockedTables.toArray()[i]);
      }
    }
    ret.append("\n   persistent connection id: " + persistentConnectionId);
    ret.append("\n   client ip address: " + clientIpAddress);

    return ret.toString();
  }

  /**
   * Displays some debugging information about this request.
   */
  public void debug()
  {
    System.out.println(toDebugString());
  }

  /**
   * Returns a one line <code>String</code> containing debug information about
   * this request.
   * <p>
   * Information displayed: id, query, parameters (if any), transaction id,
   * persistent connection id.
   * 
   * @return a string containing:
   */
  public String toShortDebugString()
  {
    StringBuffer ret = new StringBuffer();
    ret.append("Id=" + id);
    ret.append(", query=" + sqlQueryOrTemplate);
    if (preparedStatementParameters != null)
      ret.append(", params=" + preparedStatementParameters);
    ret.append(", transaction id=" + transactionId);
    ret.append(", persistent connection id=" + persistentConnectionId);
    return ret.toString();

  }

  /**
   * Returns the disabledTableExistenceCheck value.
   * 
   * @return Returns the disabledTableExistenceCheck.
   */
  public boolean tableExistenceCheckIsDisabled()
  {
    return disabledTableExistenceCheck;
  }

}
