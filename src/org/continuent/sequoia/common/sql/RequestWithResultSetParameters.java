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

/**
 * This class defines a Request with additional parameters indicating how the
 * ResulSet should be fetched. This is only useful for calls to
 * Statement.executeQuery() or Statement.execute() when ResultSets are returned.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class RequestWithResultSetParameters extends Request
{

  /*
   * ResultSet Parameters
   */
  private int    maxRows    = 0;
  private int    fetchSize  = 0;
  private String cursorName = null;

  //
  // Constructors
  //

  /**
   * Creates a new <code>RequestWithResultSetParameters</code> object
   * @param sqlTemplate the SQL template (using ? as parameter placeholders) or
   *          null
   * @param parameters the SQL statement
   * @param escapeProcessing Should the backend driver do escape processing
   *          before sending to the database?
   * @param timeoutInSeconds Timeout for this request in seconds, value 0 means
   *          no timeout
   */
  public RequestWithResultSetParameters(String sqlTemplate, String parameters,
      boolean escapeProcessing, int timeoutInSeconds)
  {
    super(sqlTemplate, parameters, escapeProcessing, timeoutInSeconds);
  }

  //
  // Serialization methods
  //

  /**
   * Creates a new <code>RequestWithResultSetParameters</code> object,
   * deserializing it from an input stream. Has to mirror the serialization
   * method below.
   * 
   * @param in input stream
   * @throws IOException stream error
   */
  public RequestWithResultSetParameters(DriverBufferedInputStream in)
      throws IOException
  {
    super(in);
    this.maxRows = in.readInt();
    this.fetchSize = in.readInt();

    if (in.readBoolean()) // do we have a cursor name ?
      this.cursorName = in.readLongUTF();
  }

  /**
   * Also serialize ResultSet parameters to the stream. Optionally used by
   * serializers of those derived requests that expect a ResultSet.
   * 
   * @see org.continuent.sequoia.common.sql.Request#sendToStream(org.continuent.sequoia.common.stream.DriverBufferedOutputStream)
   */
  public void sendToStream(DriverBufferedOutputStream out) throws IOException
  {
    super.sendToStream(out);

    out.writeInt(maxRows);
    out.writeInt(fetchSize);

    if (this.cursorName != null) // do we have a cursor name ?
    {
      out.writeBoolean(true);
      out.writeLongUTF(cursorName);
    }
    else
      out.writeBoolean(false);
  }

  //
  // Getter/Setter
  //

  /**
   * Returns the cursorName value.
   * 
   * @return Returns the cursorName.
   */
  public final String getCursorName()
  {
    return cursorName;
  }

  /**
   * Sets the cursorName value.
   * 
   * @param cursorName The cursorName to set.
   */
  public final void setCursorName(String cursorName)
  {
    this.cursorName = cursorName;
  }

  /**
   * Returns the fetchSize value.
   * 
   * @return Returns the fetchSize.
   */
  public final int getFetchSize()
  {
    return fetchSize;
  }

  /**
   * Sets the fetchSize value.
   * 
   * @param fetchSize The fetchSize to set.
   */
  public final void setFetchSize(int fetchSize)
  {
    this.fetchSize = fetchSize;
  }

  /**
   * Returns the maxRows value.
   * 
   * @return Returns the maxRows.
   */
  public final int getMaxRows()
  {
    return maxRows;
  }

  /**
   * Sets the maxRows value.
   * 
   * @param maxRows The maxRows to set.
   */
  public final void setMaxRows(int maxRows)
  {
    this.maxRows = maxRows;
  }

}
