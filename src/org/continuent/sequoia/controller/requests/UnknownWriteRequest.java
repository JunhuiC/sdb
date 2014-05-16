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
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.controller.requests;

import java.io.Serializable;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;

/**
 * An <code>UnknownWriteRequest</code> is an SQL request that does not match
 * any SQL query known by this software.<br>
 * Quite strangely, it extends AbstractWriteRequest but has a returnsResultSet()
 * method.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public class UnknownWriteRequest extends AbstractWriteRequest
    implements
      Serializable
{
  private static final long serialVersionUID          = 2620807724931159903L;

  private boolean           needsMacroProcessing;

  // Be conservative, if we cannot parse the query, assumes it invalidates
  // everything
  protected boolean         altersStoredProcedureList = true;
  protected boolean         altersUsers               = true;
  protected boolean         altersSomething           = true;
  protected boolean         altersDatabaseCatalog     = false;
  protected boolean         altersDatabaseSchema      = false;
  protected boolean         altersUserDefinedTypes    = false;
  protected boolean         altersAggregateList       = false;
  protected boolean         altersMetadataCache       = false;
  protected boolean         altersQueryResultCache    = false;

  /**
   * Creates a new <code>UnknownWriteRequest</code> instance.
   * 
   * @param sqlQuery the SQL query
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   */
  public UnknownWriteRequest(String sqlQuery, boolean escapeProcessing,
      int timeout, String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator,
        RequestType.UNKNOWN_WRITE);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersAggregateList()
   */
  public boolean altersAggregateList()
  {
    return this.altersAggregateList;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseCatalog()
   */
  public boolean altersDatabaseCatalog()
  {
    return this.altersDatabaseCatalog;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseSchema()
   */
  public boolean altersDatabaseSchema()
  {
    return this.altersDatabaseSchema;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersMetadataCache()
   */
  public boolean altersMetadataCache()
  {
    return this.altersMetadataCache;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersQueryResultCache()
   */
  public boolean altersQueryResultCache()
  {
    return this.altersQueryResultCache;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersSomething()
   */
  public boolean altersSomething()
  {
    return altersSomething;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersStoredProcedureList()
   */
  public boolean altersStoredProcedureList()
  {
    return this.altersStoredProcedureList;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUserDefinedTypes()
   */
  public boolean altersUserDefinedTypes()
  {
    return this.altersUserDefinedTypes;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUsers()
   */
  public boolean altersUsers()
  {
    return altersUsers;
  }

  /**
   * @return <code>false</code>
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#needsMacroProcessing()
   */
  public boolean needsMacroProcessing()
  {
    return needsMacroProcessing;
  }

  /**
   * Request is not parsed (isParsed is just set to true).
   * 
   * @see AbstractRequest#cloneParsing(AbstractRequest)
   */
  public void cloneParsing(AbstractRequest request)
  {
    isParsed = true;
  }

  /**
   * Request is not parsed (isParsed is just set to true).
   * 
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#parse(org.continuent.sequoia.common.sql.schema.DatabaseSchema,
   *      int, boolean)
   */
  public void parse(DatabaseSchema schema, int granularity,
      boolean isCaseSensitive) throws SQLException
  { // No parsing for unknown write request
    // Just let database try to execute the request
    isParsed = true;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getParsingResultsAsString()
   */
  public String getParsingResultsAsString()
  {
    StringBuffer sb = new StringBuffer(super.getParsingResultsAsString());
    sb.append(Translate.get("request.alters",
        new String[]{String.valueOf(altersAggregateList()),
            String.valueOf(altersDatabaseCatalog()),
            String.valueOf(altersDatabaseSchema()),
            String.valueOf(altersMetadataCache()),
            String.valueOf(altersQueryResultCache()),
            String.valueOf(altersSomething()),
            String.valueOf(altersStoredProcedureList()),
            String.valueOf(altersUserDefinedTypes()),
            String.valueOf(altersUsers())}));
    return sb.toString();
  }

  /**
   * {@inheritDoc} <br>
   * We do not need to override hashCode() in this class because it's already
   * done correctly upper in the hierarchy. For more details see
   * http://www.javaworld.com/javaworld/jw-01-1999/jw-01-object-p2.html
   * 
   * @see AbstractRequest#hashCode()
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#equals(java.lang.Object)
   */
  public boolean equals(Object other)
  {
    if (id != 0) // this is the regular, usual case
      return super.equals(other);

    // Special case: id is not set. This is a fake query (commit, rollback,...)
    // used for notification purposes. We have to perform a more "manual"
    // comparison below.
    if ((other == null) || !(other instanceof UnknownWriteRequest))
      return false;

    UnknownWriteRequest r = (UnknownWriteRequest) other;
    return transactionId == r.getTransactionId()
        && sqlQueryOrTemplate.equals(r.getSqlOrTemplate());
  }
}