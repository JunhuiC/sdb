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

package org.continuent.sequoia.controller.cache.result;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This class defines request cache granularities.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 * @version 1.0
 */
public class CachingGranularities
{
  /**
   * Database granularity: entries in the cache are invalidated every time a
   * write (INSERT/UPDATE/DELETE/DROP/...) is sent to the database.
   */
  public static final int DATABASE = 0;

  /**
   * Table granularity: entries in the cache are invalidated based on table
   * dependencies.
   */
  public static final int TABLE = 1;

  /**
   * Column granularity: entries in the cache are invalidated based on column
   * dependencies.
   */
  public static final int COLUMN = 2;

  /**
   * Column granularity with <code>UNIQUE</code> queries: same as <code>COLUMN</code>
   * except that <code>UNIQUE</code> queries that selects a single row based
   * on a key are invalidated only when needed.
   */
  public static final int COLUMN_UNIQUE = 3;

  /**
   * Gets the name corresponding to a cache granularity level.
   * 
   * @param cacheGrain cache granularity level
   * @return the name of the granularity level
   */
  public static final String getGranularityName(int cacheGrain)
  {
    switch (cacheGrain)
    {
      case DATABASE :
        return "DATABASE";
      case TABLE :
        return "TABLE";
      case COLUMN :
        return "COLUMN";
      case COLUMN_UNIQUE :
        return "COLUMN_UNIQUE";
      default :
        return "UNSUPPORTED";
    }
  }

  /**
   * This method is needed to convert the value into the corresponding xml
   * attribute value. If fails, returns noInvalidation granularity value so the
   * xml retrieved can be used.
   * 
   * @param cacheGrain cache granularity level
   * @return the xml attribute value of the granularity level
   */
  public static final String getGranularityXml(int cacheGrain)
  {
    switch (cacheGrain)
    {
      case DATABASE :
        return DatabasesXmlTags.VAL_database;
      case TABLE :
        return DatabasesXmlTags.VAL_table;
      case COLUMN :
        return DatabasesXmlTags.VAL_column;
      case COLUMN_UNIQUE :
        return DatabasesXmlTags.VAL_columnUnique;
      default :
        return DatabasesXmlTags.VAL_noInvalidation;
    }
  }
}
