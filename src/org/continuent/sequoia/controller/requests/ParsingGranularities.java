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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Sara Bouchenak.
 */

package org.continuent.sequoia.controller.requests;

/**
 * Defines SQL queries parsing granularities.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @author <a href="mailto:Sara.Bouchenak@epfl.ch">Sara Bouchenak</a>
 * @version 1.0
 */
public class ParsingGranularities
{
  /** The request is not parsed. */
  public static final int NO_PARSING    = 0;

  /**
   * Table granularity. Only table dependencies are computed.
   */
  public static final int TABLE         = 1;

  /**
   * Column granularity. Column dependencies are computed (both select and where
   * clauses).
   */
  public static final int COLUMN        = 2;

  /**
   * Column granularity with <code>UNIQUE</code> queries.
   * <p>
   * Same as <code>COLUMN</code> except that <code>UNIQUE</code> queries
   * that select a single row based on a key are flagged <code>UNIQUE</code>
   * (and should not be invalidated on <code>INSERTs</code>).
   */
  public static final int COLUMN_UNIQUE = 3;

  /**
   * Returns the granularity value in a <code>String</code> form.
   * 
   * @param granularity a granularity value
   * @return the <code>String</code> form of the granularity
   */
  public static String getInformation(int granularity)
  {
    switch (granularity)
    {
      case NO_PARSING :
        return "NO_PARSING";
      case TABLE :
        return "TABLE";
      case COLUMN :
        return "COLUMN";
      case COLUMN_UNIQUE :
        return "COLUMN_UNIQUE";
      default :
        return "Illegal parsing granularity";
    }
  }
}
