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
 * Contributor(s): Nicolas Modrzyk.
 */

package org.continuent.sequoia.controller.cache.result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.continuent.sequoia.common.sql.schema.TableColumn;
import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.cache.result.schema.CacheDatabaseColumn;
import org.continuent.sequoia.controller.cache.result.schema.CacheDatabaseTable;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.UpdateRequest;

/**
 * This is a query cache implementation with a column granularity:
 * <ul>
 * <li><code>COLUMN</code>: column granularity, entries in the cache are
 * invalidated based on column dependencies</li>
 * </ul>
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */

public class ResultCacheColumn extends ResultCache
{
  /**
   * Builds a new ResultCache with a Column granularity.
   * 
   * @param maxEntries maximum number of entries
   * @param pendingTimeout pending timeout for concurrent queries
   */
  public ResultCacheColumn(int maxEntries, int pendingTimeout)
  {
    super(maxEntries, pendingTimeout);
    parsingGranularity = ParsingGranularities.COLUMN;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#processAddToCache(AbstractResultCacheEntry)
   */
  public void processAddToCache(AbstractResultCacheEntry qe)
  {
    SelectRequest request = qe.getRequest();
    ArrayList selectedColumns = request.getSelect();
    // Update the tables columns dependencies
    if (selectedColumns == null || selectedColumns.isEmpty())
    {
      logger
          .warn("No parsing of select clause found - Fallback to table granularity");
      Collection from = request.getFrom();
      if (from == null)
        return;
      for (Iterator i = from.iterator(); i.hasNext();)
      {
        CacheDatabaseTable table = cdbs.getTable((String) i.next());
        table.addCacheEntry(qe);
        // Add all columns, entries will be added below.
        ArrayList columns = table.getColumns();
        for (int j = 0; j < columns.size(); j++)
        {
          ((CacheDatabaseColumn) columns.get(j)).addCacheEntry(qe);
        }
        return;
      }
    }
    for (Iterator i = selectedColumns.iterator(); i.hasNext();)
    {
      TableColumn tc = (TableColumn) i.next();
      cdbs.getTable(tc.getTableName()).getColumn(tc.getColumnName())
          .addCacheEntry(qe);
    }
    if (request.getWhere() != null)
      for (Iterator i = request.getWhere().iterator(); i.hasNext();)
      {
        TableColumn tc = (TableColumn) i.next();
        cdbs.getTable(tc.getTableName()).getColumn(tc.getColumnName())
            .addCacheEntry(qe);
      }
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.AbstractResultCache#isUpdateNecessary(org.continuent.sequoia.controller.requests.UpdateRequest)
   */
  public boolean isUpdateNecessary(UpdateRequest request)
  {
    return true;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#processWriteNotify(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  protected void processWriteNotify(AbstractWriteRequest request)
  {
    // Sanity check
    if (request.getColumns() == null)
    {
      logger.warn("No column parsing found - Fallback to table granularity ("
          + request.getUniqueKey() + ")");
      cdbs.getTable(request.getTableName()).invalidateAll();
      return;
    }
    if (request.isAlter())
    {
      cdbs.getTable(request.getTableName()).invalidateAll();
      return;
    }
    for (Iterator i = request.getColumns().iterator(); i.hasNext();)
    {
      TableColumn tc = (TableColumn) i.next();
      cdbs.getTable(tc.getTableName()).getColumn(tc.getColumnName())
          .invalidateAll();
    }
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#getName()
   */
  public String getName()
  {
    return "column";
  }

}