/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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
 * Initial developer(s): Emmanuel Checchet.
 * Contributor(s): Olivier Fambon.
 */

package org.continuent.sequoia.controller.recoverylog.events;

import java.io.Serializable;

/**
 * This class defines a recovery log entry that needs to be stored in the
 * recovery database. This is also sent over the wire between controllers, when
 * copying recovery log entries to a remote controller (copyLogFromCheckpoint).
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class LogEntry implements Serializable
{
  private static final long  serialVersionUID      = 1363084035201225164L;

  private String             autoConnTrans;
  private String             executionStatus;
  private long               tid;
  private String             query;
  private String             queryParams;
  private String             login;
  private long               logId;
  private boolean            escapeProcessing      = false;
  private long               requestId             = 0;
  private long               executionTimeInMs     = 0;
  private int                updateCountResult     = -1;

  /**
   * auto_conn_trans value for a request in autoCommit mode (no persistent
   * connection)
   */
  public static final String AUTOCOMMIT            = "A";
  /** auto_conn_trans value for a persistent connection */
  public static final String PERSISTENT_CONNECTION = "C";
  /** auto_conn_trans value for a transaction */
  public static final String TRANSACTION           = "T";

  /** Execution status value to executing */
  public static final String EXECUTING             = "E";
  /** Execution status value to success */
  public static final String SUCCESS               = "S";
  /** Execution status value to failed */
  public static final String FAILED                = "F";
  /** Execution status value to unknown */
  public static final String UNKNOWN               = "U";
  /** Status that will be used if the statement is not found in the recovery log */
  public static final String MISSING               = "M";

  /**
   * Create a log entry with a default execution status set to EXECUTING.
   * 
   * @param logId the recovery log unique id
   * @param login login used for this request
   * @param query query to log
   * @param queryParams query parameters if this is a PreparedStatement
   * @param autoConnTrans AUTOCOMMIT, PERSISTENT_CONNECTION or TRANSACTION
   * @param tid transaction id of this request
   */
  public LogEntry(long logId, String login, String query, String queryParams,
      String autoConnTrans, long tid)
  {
    if (query == null)
      throw new NullPointerException("Invalid null query in log entry");
    if (login == null)
      throw new NullPointerException("Invalid null login in log entry");

    this.logId = logId;
    this.login = login;
    this.query = query;
    this.queryParams = queryParams;
    this.autoConnTrans = autoConnTrans;
    this.tid = tid;
    this.executionStatus = EXECUTING;
  }

  /**
   * Create a log object.
   * 
   * @param logId the recovery log unique id
   * @param login login used for this request
   * @param query query to log
   * @param queryParams query parameters if this is a PreparedStatement
   * @param autoConnTrans 'A' if autoCommit, 'C' if persistent connection, 'T'
   *          if transaction
   * @param tid transaction id of this request
   * @param escapeProcessing true if escape processing must be done
   * @param requestId the unique request id
   * @param executionTime the estimated execution time of the request in
   *          milliseconds
   * @param updateCountResult the result of this query if it returned an update
   *          count
   */
  public LogEntry(long logId, String login, String query, String queryParams,
      String autoConnTrans, long tid, boolean escapeProcessing, long requestId,
      long executionTime, int updateCountResult)
  {
    this(logId, login, query, queryParams, autoConnTrans, tid,
        escapeProcessing, requestId, executionTime);
    this.updateCountResult = updateCountResult;
  }

  /**
   * Create a log object.
   * 
   * @param logId the recovery log unique id
   * @param login login used for this request
   * @param query query to log
   * @param queryParams query parameters if this is a PreparedStatement
   * @param autoConnTrans 'A' if autoCommit, 'C' if persistent connection, 'T'
   *          if transaction
   * @param tid transaction id of this request
   * @param escapeProcessing true if escape processing must be done
   * @param requestId the unique request id
   * @param executionTime the estimated execution time of the request in
   *          milliseconds
   * @param updateCountResult the result of this query if it returned an update
   *          count
   * @param status execution status as defined in LogEntry constants
   */
  public LogEntry(long logId, String login, String query, String queryParams,
      String autoConnTrans, long tid, boolean escapeProcessing, long requestId,
      long executionTime, int updateCountResult, String status)
  {
    this(logId, login, query, queryParams, autoConnTrans, tid,
        escapeProcessing, requestId, executionTime, updateCountResult);
    this.executionStatus = status;
  }

  /**
   * Create a log object.
   * 
   * @param logId the recovery log unique id
   * @param login login used for this request
   * @param query query to log
   * @param queryParams query parameters if this is a PreparedStatement
   * @param autoConnTrans 'A' if autoCommit, 'C' if persistent connection, 'T'
   *          if transaction
   * @param tid transaction id of this request
   * @param escapeProcessing true if escape processing must be done
   * @param requestId the unique request id
   * @param executionTime the estimated execution time of the request in
   *          milliseconds
   */
  public LogEntry(long logId, String login, String query, String queryParams,
      String autoConnTrans, long tid, boolean escapeProcessing, long requestId,
      long executionTime)
  {
    this(logId, login, query, queryParams, autoConnTrans, tid);
    this.escapeProcessing = escapeProcessing;
    this.requestId = requestId;
    this.executionTimeInMs = executionTime;
  }

  /**
   * Returns the autoConnTrans value.
   * 
   * @return Returns the autoConnTrans.
   */
  public String getAutoConnTrans()
  {
    return autoConnTrans;
  }

  /**
   * @return true if escape processing is needed
   */
  public boolean getEscapeProcessing()
  {
    return escapeProcessing;
  }

  /**
   * Returns the executionStatus value.
   * 
   * @return Returns the executionStatus.
   */
  public final String getExecutionStatus()
  {
    return executionStatus;
  }

  /**
   * Sets the executionStatus value.
   * 
   * @param executionStatus The executionStatus to set.
   */
  public final void setExecutionStatus(String executionStatus)
  {
    this.executionStatus = executionStatus;
  }

  /**
   * Returns the executionTime value.
   * 
   * @return Returns the executionTime.
   */
  public long getExecutionTimeInMs()
  {
    return executionTimeInMs;
  }

  /**
   * @return the request id
   */
  public long getLogId()
  {
    return logId;
  }

  /**
   * @return the login used for this request
   */
  public String getLogin()
  {
    return login;
  }

  /**
   * @return the request itself
   */
  public String getQuery()
  {
    return query;
  }

  /**
   * Returns the queryParams value.
   * 
   * @return Returns the queryParams.
   */
  public final String getQueryParams()
  {
    return queryParams;
  }

  /**
   * Returns the requestId value.
   * 
   * @return Returns the requestId.
   */
  public long getRequestId()
  {
    return requestId;
  }

  /**
   * @return the transaction id
   */
  public long getTid()
  {
    return tid;
  }

  /**
   * Returns the result value.
   * 
   * @return Returns the result.
   */
  public int getUpdateCountResult()
  {
    return updateCountResult;
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
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Log entry: log id" + logId + " (" + autoConnTrans
        + ") transactionId:" + tid + " requestId:" + requestId + " vlogin:"
        + login + " status: " + executionStatus + " sql:" + query + " params:"
        + queryParams;
  }

}