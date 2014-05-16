/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.requests;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedure;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;

/**
 * <code>StoredProcedure</code> is encodes a stored procedure call.
 * <p>
 * It can have the following syntax:
 * 
 * <pre>
 *   {call &lt;procedure-name&gt;[&lt;arg1&gt;,&lt;arg2&gt;, ...]}
 *   {?=call &lt;procedure-name&gt;[&lt;arg1&gt;,&lt;arg2&gt;, ...]}
 * </pre>
 * 
 * For more information
 * 
 * @see org.continuent.sequoia.driver.CallableStatement
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class StoredProcedure extends AbstractRequest
{
  private static final long                     serialVersionUID                = 5360592917986126504L;

  /** <code>true</code> if this request might block. */
  private transient boolean                     blocking                        = true;

  /** Number of parameters of the stored procedure */
  protected int                                 nbOfParameters;

  /**
   * Map of named parameter values <String(name),String(type)>
   */
  private HashMap                               namedParameterValues            = null;

  /**
   * List of named parameters names
   */
  private List                                  namedParameterNames             = null;

  /**
   * Map of out parameter values (parameter index corresponds to index in list)
   */
  private SortedMap                             outParameterValues              = null;

  /**
   * List of out parameter types (parameter index corresponds to index in list)
   */
  private List                                  outParameterIndexes             = null;

  protected transient DatabaseProcedureSemantic semantic                        = null;

  protected String                              procedureKey                    = null;

  private String                                procedureName                   = "";

  // Be conservative, if we cannot parse the query, assumes it invalidates
  // everything
  protected boolean                             altersAggregateList             = true;
  protected boolean                             altersDatabaseCatalog           = true;
  protected boolean                             altersDatabaseSchema            = true;
  protected boolean                             altersMetadataCache             = true;
  protected boolean                             altersQueryResultCache          = true;
  protected boolean                             altersSomething                 = true;
  protected boolean                             altersStoredProcedureList       = true;
  protected boolean                             altersUserDefinedTypes          = true;
  protected boolean                             altersUsers                     = true;

  // Regular expression for parsing {call ...} statements
  private static final String                   STORED_PROCEDURE_PATTERN_STRING = "^((execute\\s+([^(\\s|\\()]+)(.*)?)|(\\{(\\s*\\?\\s*=)?\\s*call\\s+([^(\\s|\\()]+)(.*)?\\})|(\\s*call\\s+([^(\\s|\\()]+)(.*)?))";

  private static final Pattern                  STORED_PROCEDURE_PATTERN        = Pattern
                                                                                    .compile(
                                                                                        STORED_PROCEDURE_PATTERN_STRING,
                                                                                        Pattern.CASE_INSENSITIVE
                                                                                            | Pattern.DOTALL);

  private static int                            posSyntax1                      = 2;
  private static int                            posSyntax2                      = 5;
  private static int                            posSyntax3                      = 10;

  /**
   * Creates a new <code>StoredProcedure</code> instance.
   * 
   * @param sqlQuery the SQL request
   * @param escapeProcessing should the driver to escape processing before
   *          sending to the database ?
   * @param timeout an <code>int</code> value
   * @param lineSeparator the line separator used in the query
   */
  public StoredProcedure(String sqlQuery, boolean escapeProcessing,
      int timeout, String lineSeparator)
  {
    super(sqlQuery, escapeProcessing, timeout, lineSeparator,
        RequestType.STORED_PROCEDURE);
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersAggregateList()
   */
  public boolean altersAggregateList()
  {
    return altersAggregateList;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseCatalog()
   */
  public boolean altersDatabaseCatalog()
  {
    return altersDatabaseCatalog;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseSchema()
   */
  public boolean altersDatabaseSchema()
  {
    return altersDatabaseSchema;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersMetadataCache()
   */
  public boolean altersMetadataCache()
  {
    return altersMetadataCache;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersQueryResultCache()
   */
  public boolean altersQueryResultCache()
  {
    return altersQueryResultCache;
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
    return altersStoredProcedureList;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUserDefinedTypes()
   */
  public boolean altersUserDefinedTypes()
  {
    return altersUserDefinedTypes;
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
    return true;
  }

  /**
   * Returns the the value of a named parameter.
   * 
   * @param name parameter name
   * @return Returns the named parameter value.
   */
  public Object getNamedParameterValue(String name)
  {
    return namedParameterValues.get(name);
  }

  /**
   * Returns the named parameter names or none if this stored procedure call has
   * no named parameter.
   * 
   * @return Returns the namedParameterNames.
   */
  public List getNamedParameterNames()
  {
    return namedParameterNames;
  }

  /**
   * Returns the nbOfParameters value.
   * 
   * @return Returns the nbOfParameters.
   */
  public final int getNbOfParameters()
  {
    return nbOfParameters;
  }

  /**
   * Returns the the value of an out parameter.
   * 
   * @param idx parameter index
   * @return Returns the out parameter value.
   */
  public Object getOutParameterValue(Object idx)
  {
    return outParameterValues.get(idx);
  }

  /**
   * Returns the list of out parameter indexes (null if none).
   * 
   * @return Returns the outParameterIndexes.
   */
  public List getOutParameterIndexes()
  {
    return outParameterIndexes;
  }

  /**
   * Get the stored procedure key
   * 
   * @return the stored procedure key
   * @see DatabaseProcedure#buildKey(String, int)
   */
  public String getProcedureKey()
  {
    if (procedureKey == null)
      try
      {
        parse(null, 0, true);
      }
      catch (SQLException e)
      {
        return null;
      }
    return procedureKey;
  }

  /**
   * Returns the stored procedure name.
   * 
   * @return the stored procedure name
   */
  public String getProcedureName()
  {
    return procedureName;
  }

  /**
   * Returns the semantic value.
   * 
   * @return Returns the semantic.
   */
  public DatabaseProcedureSemantic getSemantic()
  {
    return semantic;
  }

  /**
   * Tests if this request might block.
   * 
   * @return <code>true</code> if this request might block
   */
  public boolean mightBlock()
  {
    return blocking;
  }

  /**
   * Sets if this request might block.
   * 
   * @param blocking a <code>boolean</code> value
   */
  public void setBlocking(boolean blocking)
  {
    this.blocking = blocking;
  }

  /**
   * Set a named parameter on this stored procedure call.
   * 
   * @param paramName name of the parameter
   * @param val named parameter value
   */
  public synchronized void setNamedParameterValue(String paramName, Object val)
  {
    if (namedParameterValues == null)
      namedParameterValues = new HashMap();
    namedParameterValues.put(paramName, val);
  }

  /**
   * Set a named parameter name on this stored procedure call.
   * 
   * @param paramName name of the parameter
   */
  public synchronized void setNamedParameterName(String paramName)
  {
    if (namedParameterNames == null)
      namedParameterNames = new ArrayList();
    namedParameterNames.add(paramName);
  }

  /**
   * Set an out parameter value on this stored procedure call.
   * 
   * @param paramIdx index of the parameter
   * @param val value of the parameter
   */
  public synchronized void setOutParameterValue(Object paramIdx, Object val)
  {
    if (outParameterValues == null)
      outParameterValues = new TreeMap();
    outParameterValues.put(paramIdx, val);
  }

  /**
   * Set an out parameter index on this stored procedure call.
   * 
   * @param paramIdx index of the parameter
   */
  public synchronized void setOutParameterIndex(int paramIdx)
  {
    if (outParameterIndexes == null)
      outParameterIndexes = new ArrayList();
    outParameterIndexes.add(new Integer(paramIdx));
  }

  /**
   * Just get the stored procedure name.
   * 
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#parse(org.continuent.sequoia.common.sql.schema.DatabaseSchema,
   *      int, boolean)
   */
  public void parse(DatabaseSchema schema, int granularity,
      boolean isCaseSensitive) throws SQLException
  {
    Matcher matcher;
    sqlQueryOrTemplate = sqlQueryOrTemplate.trim();

    matcher = STORED_PROCEDURE_PATTERN.matcher(sqlQueryOrTemplate);
    if (!matcher.matches())
      return;

    String parameterList = null;

    if (matcher.group(posSyntax1) != null)
    {
      procedureName = matcher.group(posSyntax1 + 1);
      parameterList = matcher.group(posSyntax1 + 2);
    }
    else if (matcher.group(posSyntax2) != null)
    {
      procedureName = matcher.group(posSyntax2 + 2);
      parameterList = matcher.group(posSyntax2 + 3);
    }
    else
    {
      procedureName = matcher.group(posSyntax3);
      parameterList = matcher.group(posSyntax3 + 1);
    }

    int parenthesis = parameterList.indexOf('(');

    if (parenthesis == -1)
    {
      // sometimes, parameters are not inside ()
      // we will check if we find ',' after the name of the stored procedure
      // to check for parameters
      int commaIdx = parameterList.indexOf(',');
      if (commaIdx == -1)
      {
        if (parameterList.trim().length() != 0)
          nbOfParameters = 1;
        else
          nbOfParameters = 0;
      }
      else
      {
        nbOfParameters = 1;
        while (commaIdx != -1)
        {
          nbOfParameters++;
          commaIdx = parameterList.indexOf(',', commaIdx + 1);
        }
      }
    }
    else
    {
      // Note that indexOf is tolerant and accept values greater than the String
      // size so there is no need to check for the limit.
      int commaIdx = parameterList.indexOf(',', parenthesis + 1);

      // Here we just count the number of commas to find the number of
      // parameters (which is equal to nb of commas plus one if there is at
      // least one comma).

      if (commaIdx == -1)
      { // Check if we have 0 or 1 parameter
        int closingParenthesis = parameterList.indexOf(')', parenthesis + 1);
        try
        {
          if (parameterList.substring(parenthesis + 1, closingParenthesis)
              .trim().length() == 0)
            nbOfParameters = 0;
          else
            nbOfParameters = 1;
        }
        catch (RuntimeException e)
        { // Malformed query, just use -1 as the number of parameters
          nbOfParameters = -1;
        }
      }
      else
      {
        nbOfParameters = 1;
        while (commaIdx != -1)
        {
          nbOfParameters++;
          commaIdx = parameterList.indexOf(',', commaIdx + 1);
        }
      }
    }

    // Build the key and retrieve the associated semantic
    procedureKey = DatabaseProcedure.buildKey(procedureName, nbOfParameters);

    if (schema != null)
    {
      DatabaseProcedure dbProc = schema.getProcedure(procedureKey);
      if (dbProc == null)
        semantic = null;
      else
      {
        semantic = dbProc.getSemantic();
        if (semantic != null)
        {
          writeLockedTables = semantic.getWriteTables();
          addDependingTables(schema, writeLockedTables);
          altersDatabaseSchema = semantic.hasDDLWrite();
          altersSomething = !semantic.isReadOnly();
        }
      }
    }
    isParsed = true;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#cloneParsing(org.continuent.sequoia.controller.requests.AbstractRequest)
   */
  public void cloneParsing(AbstractRequest request)
  {
    if (!request.isParsed)
      return;
    StoredProcedure other = (StoredProcedure) request;
    procedureKey = other.getProcedureKey();
    nbOfParameters = other.getNbOfParameters();
    semantic = other.getSemantic();
    writeLockedTables = other.writeLockedTables;
  }

  /**
   * Copy (simple assignment, no object cloning so be sure to not modify the
   * original object else you expose yourself to nasty side effects) named
   * parameter values and names ; and out parameter values and indexes of this
   * stored procedure call.
   * 
   * @param otherProc the other stored procedure that has the result to copy
   *          into this object
   */
  public void copyNamedAndOutParameters(StoredProcedure otherProc)
  {
    this.outParameterValues = otherProc.outParameterValues;
    this.outParameterIndexes = otherProc.outParameterIndexes;
    this.namedParameterValues = otherProc.namedParameterValues;
    this.namedParameterNames = otherProc.namedParameterNames;
  }

  /**
   * @see org.continuent.sequoia.controller.requests.AbstractRequest#getParsingResultsAsString()
   */
  public String getParsingResultsAsString()
  {
    StringBuffer sb = new StringBuffer(super.getParsingResultsAsString());
    sb.append(Translate.get("request.storedproc.name", procedureName));
    sb.append(Translate.get("request.blocking", blocking));
    sb.append(Translate.get("request.storedproc.parameters.number",
        nbOfParameters));
    if (namedParameterNames != null && namedParameterNames.size() > 0)
    {
      sb.append(Translate.get("request.storedproc.named.parameters.names"));
      for (int i = 0; i < namedParameterNames.size(); i++)
      {
        sb.append(Translate.get("request.storedproc.named.parameters.names",
            namedParameterNames.get(i)));
      }
    }
    if (namedParameterValues != null && namedParameterValues.size() > 0)
    {
      sb.append(Translate.get("request.storedproc.named.parameters.values"));
      for (int i = 0; i < namedParameterValues.size(); i++)
      {
        sb.append(Translate.get("request.storedproc.named.parameters.value",
            new String[]{
                namedParameterValues.keySet().toArray()[i].toString(),
                namedParameterValues.get(
                    namedParameterValues.keySet().toArray()[i]).toString()}));
      }
    }
    if (outParameterIndexes != null && outParameterIndexes.size() > 0)
    {
      sb.append(Translate.get("request.storedproc.out.parameters.indexes"));
      for (int i = 0; i < outParameterIndexes.size(); i++)
      {
        sb.append(Translate.get("request.storedproc.out.parameters.index",
            outParameterIndexes.get(i)));
      }
    }
    if (outParameterValues != null && outParameterValues.size() > 0)
    {
      sb.append(Translate.get("request.storedproc.out.parameters.values"));
      for (int i = 0; i < outParameterValues.size(); i++)
      {
        sb
            .append(Translate.get("request.storedproc.out.parameters.value",
                new String[]{
                    outParameterValues.keySet().toArray()[i].toString(),
                    outParameterValues.get(
                        outParameterValues.keySet().toArray()[i]).toString()}));
      }
    }
    sb.append(Translate.get("request.storedproc.semantic", semantic));
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

}