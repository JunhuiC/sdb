/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): Emmanuel Cecchet, Edward Archibald
 */

package org.continuent.sequoia.common.sql.schema;

import java.util.ArrayList;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * Represents a database stored procedure and its metadata.
 * 
 * @author <a href="mailto:ed.archibald@continuent.com">Edward Archibald</a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public class DatabaseProcedure
{
  /** May return a result */
  public static final int           ProcedureResultUnknown = 0;
  /** Does not return a result */
  public static final int           ProcedureNoResult      = 1;
  /** Returns a result */
  public static final int           ProcedureReturnsResult = 2;

  ArrayList                         parameters;
  private String                    name;
  private String                    remarks;
  private int                       procedureType;
  private DatabaseProcedureSemantic semantic;

  /**
   * @param name of the procedure
   * @param remarks of the procedure
   * @param procedureType see above types
   */
  public DatabaseProcedure(String name, String remarks, int procedureType)
  {
    this.name = name;
    this.remarks = remarks;
    this.procedureType = procedureType;
    this.parameters = new ArrayList();
  }

  /**
   * Add a parameter to this procedure
   * 
   * @param param to add
   */
  public void addParameter(DatabaseProcedureParameter param)
  {
    parameters.add(param);
  }

  /**
   * Build a unique key based on a stored procedure name and its number of
   * parameters. Please note that this is enough for some DBMS: Postgresql 8 for
   * instance allows full function overloading.
   * 
   * @param storedProcedureName stored procedure name
   * @param nbOfParameters number of parameters
   * @return a String representation of the key (used by HashMap in schema)
   */
  public static String buildKey(String storedProcedureName, int nbOfParameters)
  {
    return storedProcedureName + "(" + nbOfParameters + ")";
  }

  /**
   * Returns a unique key identifying this stored procedure.
   * 
   * @return the unique key
   */
  public String getKey()
  {
    if (procedureType == ProcedureReturnsResult)
      // Strip return value from the parameter list
      return buildKey(name, parameters.size() - 1);
    else
      return buildKey(name, parameters.size());
  }

  /**
   * @return Returns the name.
   */
  public String getName()
  {
    return name;
  }

  /**
   * @param name The name to set.
   */
  public void setName(String name)
  {
    this.name = name;
  }

  /**
   * @return Returns the parameters.
   */
  public ArrayList getParameters()
  {
    return parameters;
  }

  /**
   * @param parameters The parameters to set.
   */
  public void setParameters(ArrayList parameters)
  {
    this.parameters = parameters;
  }

  /**
   * @return Returns the procedureType.
   */
  public int getProcedureType()
  {
    return procedureType;
  }

  /**
   * @param procedureType The procedureType to set.
   */
  public void setProcedureType(int procedureType)
  {
    this.procedureType = procedureType;
  }

  /**
   * @return Returns the remarks.
   */
  public String getRemarks()
  {
    return remarks;
  }

  /**
   * @param remarks The remarks to set.
   */
  public void setRemarks(String remarks)
  {
    this.remarks = remarks;
  }

  /**
   * Returns the procedureNoResult value.
   * 
   * @return Returns the procedureNoResult.
   */
  public static int getProcedureNoResult()
  {
    return ProcedureNoResult;
  }

  /**
   * Returns the stored procedure semantic.
   * 
   * @return Returns the semantic.
   */
  public DatabaseProcedureSemantic getSemantic()
  {
    return semantic;
  }

  /**
   * Sets the stored procedure semantic.
   * 
   * @param semantic The semantic to set.
   */
  public void setSemantic(DatabaseProcedureSemantic semantic)
  {
    this.semantic = semantic;
  }

  /**
   * Convert type from string to integer
   * 
   * @param type as a string
   * @return ProcedureNoResult or ProcedureReturnsResult or
   *         ProcedureResultUnknown if not found
   */
  public static int getTypeFromString(String type)
  {
    if (type.equals(DatabasesXmlTags.VAL_noResult))
      return ProcedureNoResult;
    if (type.equals(DatabasesXmlTags.VAL_returnsResult))
      return ProcedureReturnsResult;
    else
      return ProcedureResultUnknown;
  }

  /**
   * Convert type from integer to string
   * 
   * @param type as an int
   * @return string value conforms to xml tags.
   */
  public static String getTypeFromInt(int type)
  {
    switch (type)
    {
      case ProcedureNoResult :
        return DatabasesXmlTags.VAL_noResult;
      case ProcedureReturnsResult :
        return DatabasesXmlTags.VAL_returnsResult;
      default :
        return DatabasesXmlTags.VAL_resultUnknown;
    }
  }

  /**
   * Get xml information about this procedure.
   * 
   * @return xml formatted information on this database procedure.
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_DatabaseProcedure + " "
        + DatabasesXmlTags.ATT_name + "=\"" + name + "\" "
        + DatabasesXmlTags.ATT_returnType + "=\""
        + getTypeFromInt(procedureType) + "\">");
    for (int i = 0; i < parameters.size(); i++)
      info.append(((DatabaseProcedureParameter) parameters.get(i)).getXml());
    info.append("</" + DatabasesXmlTags.ELT_DatabaseProcedure + ">");
    return info.toString();
  }

  /**
   * Two <code>DatabaseProcedure</code> are considered equal if they have the
   * same name and the same parameters.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the DatabaseProcedures are equal
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof DatabaseProcedure))
      return false;

    DatabaseProcedure p = (DatabaseProcedure) other;
    return getKey().equals(p.getKey());
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return getKey() + " " + getSemantic();
  }

}
