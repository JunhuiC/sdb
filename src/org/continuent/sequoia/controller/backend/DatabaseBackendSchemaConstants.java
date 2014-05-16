/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): 
 */

package org.continuent.sequoia.controller.backend;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * Mapping for dynamic schema gathering and validation
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public abstract class DatabaseBackendSchemaConstants
{
  /** Static level no dynamic schema */
  public static final int DynamicPrecisionStatic     = 0;
  /** Table level for dynamic schema */
  public static final int DynamicPrecisionTable      = 1;
  /** Column level for dynamic schema */
  public static final int DynamicPrecisionColumn     = 2;
  /** procedures names level for dynamic schema */
  public static final int DynamicPrecisionProcedures = 3;
  /** All level for dynamic schema, procedures parameters are retrieved */
  public static final int DynamicPrecisionAll        = 4;

  /**
   * Get the dynamic schema level from string to int
   * 
   * @param stringLevel as a string from <code>DatabaseXmlTags</code>
   * @return an int
   */
  public static int getDynamicSchemaLevel(String stringLevel)
  {
    if (stringLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_static))
      return DynamicPrecisionStatic;
    else if (stringLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_table))
      return DynamicPrecisionTable;
    else if (stringLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_column))
      return DynamicPrecisionColumn;
    else if (stringLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_procedures))
      return DynamicPrecisionProcedures;
    else if (stringLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_all))
      return DynamicPrecisionAll;
    else
      throw new IllegalArgumentException("Invalid dynamic precision "
          + stringLevel);
  }

  /**
   * Get the dynamic schema level from int to string
   * 
   * @param intLevel as an int
   * @return string taken from <code>DatabaseXmlTags</code>
   */
  public static String getDynamicSchemaLevel(int intLevel)
  {
    switch (intLevel)
    {
      case DynamicPrecisionStatic :
        return DatabasesXmlTags.VAL_static;
      case DynamicPrecisionTable :
        return DatabasesXmlTags.VAL_table;
      case DynamicPrecisionColumn :
        return DatabasesXmlTags.VAL_column;
      case DynamicPrecisionProcedures :
        return DatabasesXmlTags.VAL_procedures;
      case DynamicPrecisionAll :
        return DatabasesXmlTags.VAL_all;
      default :
        return DatabasesXmlTags.VAL_all;
    }
  }
}