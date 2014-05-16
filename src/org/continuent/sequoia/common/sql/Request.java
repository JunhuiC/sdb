/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Continuent, Inc.
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

package org.continuent.sequoia.common.sql;

import java.io.IOException;

import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;
import org.continuent.sequoia.driver.Connection;

/**
 * This class defines a Request object. This basically carries the SQL statement
 * and the SQL template if this is a PreparedStatement.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class Request
{
  /**
   * SQL query if this is a statement (should be set in constructor) or query
   * template if this is a PreparedStatement.
   */
  private String  sqlQueryOrTemplate;

  /**
   * PreparedStatement parameters
   */
  private String  preparedStatementParameters = null;

  //
  // Connection related parameters
  //

  /** True if the connection has been set to read-only */
  private boolean isReadOnly                  = false;

  /**
   * Whether this request has been sent in <code>autocommit</code> mode or
   * not.
   */
  private boolean isAutoCommit                = true;

  /**
   * Transaction isolation level to use when executing the query inside a
   * transaction. The value is set by the VirtualDatabaseWorkerThread.
   */
  private int     transactionIsolation;

  /**
   * Timeout for this request in seconds, value 0 means no timeout (should be
   * set in constructor). This timeout is in seconds, reflecting the jdbc-spec,
   * and is passed as-is to the backends jdbc-driver. Internally converted to ms
   * via getTimeoutMs().
   */
  private int     timeoutInSeconds;

  /**
   * Should the backend driver do escape processing before sending to the
   * database? Simply forwarded to backend driver. No setter for this member,
   * should be set in constructor.
   * 
   * @see java.sql.Statement#setEscapeProcessing(boolean)
   */
  private boolean escapeProcessing            = true;

  /**
   * Request identifier only used for failover purposes. This id is sent back by
   * the controller.
   */
  private long    id;

  //
  // Constructors
  //

  /**
   * Creates a new <code>Request</code> object
   * 
   * @param sqlTemplate the SQL template (using ? as parameter placeholders) or
   *          null
   * @param parameters the prepared statement parameters
   * @param escapeProcessing Should the backend driver do escape processing
   *          before sending to the database?
   * @param timeoutInSeconds Timeout for this request in seconds, value 0 means
   *          no timeout
   */
  public Request(String sqlTemplate, String parameters,
      boolean escapeProcessing, int timeoutInSeconds)
  {
    this.sqlQueryOrTemplate = sqlTemplate;
    this.preparedStatementParameters = parameters;
    this.escapeProcessing = escapeProcessing;
    this.timeoutInSeconds = timeoutInSeconds;
  }

  //
  // Serialization
  //

  /**
   * Creates a new <code>Request</code> object, deserializing it from an input
   * stream. Has to mirror the serialization method below.
   * 
   * @param in the input stream to read from
   * @throws IOException if a network error occurs
   */

  public Request(DriverBufferedInputStream in) throws IOException
  {
    this.sqlQueryOrTemplate = in.readLongUTF();
    this.escapeProcessing = in.readBoolean();
    this.timeoutInSeconds = in.readInt();

    this.isAutoCommit = in.readBoolean();

    // Does this request has a template with question marks "?"
    // true for PreparedStatements
    // (AND did we ask for them at connection time?)
    if (in.readBoolean())
      this.preparedStatementParameters = in.readLongUTF();

    // success, we received it all
  }

  /**
   * Serialize the request on the output stream by sending only the needed
   * parameters to reconstruct it on the controller. Has to mirror the
   * deserialization method above.
   * 
   * @param out destination DriverBufferedOutputStream
   * @throws IOException if fails
   */

  public void sendToStream(DriverBufferedOutputStream out) throws IOException
  {
    out.writeLongUTF(sqlQueryOrTemplate);
    out.writeBoolean(escapeProcessing);
    out.writeInt(timeoutInSeconds);

    out.writeBoolean(isAutoCommit);

    // Send the "un-"PreparedStatement if the controller wants it:
    // - either for better parsing
    // - or to process it
    // - && this is a PreparedStatement (of course)
    if (preparedStatementParameters != null)
    {
      out.writeBoolean(true);
      out.writeLongUTF(preparedStatementParameters);
    }
    else
      out.writeBoolean(false);
  }

  //
  // Getters/Setters
  //

  /**
   * Returns the escapeProcessing value.
   * 
   * @return Returns the escapeProcessing.
   */
  public final boolean isEscapeProcessing()
  {
    return escapeProcessing;
  }

  /**
   * Sets the escapeProcessing value.
   * 
   * @param escapeProcessing The escapeProcessing to set.
   */
  public final void setEscapeProcessing(boolean escapeProcessing)
  {
    this.escapeProcessing = escapeProcessing;
  }

  /**
   * Returns the isAutoCommit value.
   * 
   * @return Returns the isAutoCommit.
   */
  public final boolean isAutoCommit()
  {
    return isAutoCommit;
  }

  /**
   * Sets the isAutoCommit value.
   * 
   * @param isAutoCommit The isAutoCommit to set.
   */
  public final void setIsAutoCommit(boolean isAutoCommit)
  {
    this.isAutoCommit = isAutoCommit;
  }

  /**
   * Returns the isReadOnly value.
   * 
   * @return Returns the isReadOnly.
   */
  public final boolean isReadOnly()
  {
    return isReadOnly;
  }

  /**
   * Sets the isReadOnly value.
   * 
   * @param isReadOnly The isReadOnly to set.
   */
  public final void setIsReadOnly(boolean isReadOnly)
  {
    this.isReadOnly = isReadOnly;
  }

  /**
   * Returns the id value.
   * 
   * @return Returns the id.
   */
  public long getId()
  {
    return id;
  }

  /**
   * Sets the id value.
   * 
   * @param id The id to set.
   */
  public void setId(long id)
  {
    this.id = id;
  }

  /**
   * Returns the sqlQuery value.
   * 
   * @return Returns the sqlQuery.
   */
  public final String getSqlQueryOrTemplate()
  {
    return sqlQueryOrTemplate;
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
    if ((nbOfCharacters == 0) || (sqlQueryOrTemplate.length() < nbOfCharacters))
      return sqlQueryOrTemplate;
    else
      return sqlQueryOrTemplate.substring(0, nbOfCharacters) + "...";
  }

  /**
   * Returns the sqlTemplate value.
   * 
   * @return Returns the sqlTemplate.
   */
  public final String getPreparedStatementParameters()
  {
    return preparedStatementParameters;
  }

  /**
   * Returns the timeoutInSeconds value.
   * 
   * @return Returns the timeoutInSeconds.
   */
  public final int getTimeoutInSeconds()
  {
    return timeoutInSeconds;
  }

  /**
   * Sets the timeoutInSeconds value.
   * 
   * @param timeoutInSeconds The timeoutInSeconds to set.
   */
  public final void setTimeoutInSeconds(int timeoutInSeconds)
  {
    this.timeoutInSeconds = timeoutInSeconds;
  }

  /**
   * Returns the transactionIsolation value.
   * 
   * @return Returns the transactionIsolation.
   */
  public final int getTransactionIsolation()
  {
    return transactionIsolation;
  }

  /**
   * Sets the transactionIsolation value.
   * 
   * @param transactionIsolation The transactionIsolation to set.
   */
  public final void setTransactionIsolation(int transactionIsolation)
  {
    this.transactionIsolation = transactionIsolation;
  }
}
