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
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.cache.result.schema.CacheDatabaseColumn;
import org.continuent.sequoia.controller.cache.result.schema.CacheDatabaseTable;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.DeleteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.RequestType;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.UpdateRequest;

/**
 * This is a query cache implementation with a column unique granularity:
 * <ul>
 * <li><code>COLUMN_UNIQUE</code>: same as <code>COLUMN</code> except that
 * <code>UNIQUE</code> queries that selects a single row based on a key are
 * invalidated only when needed.
 * </ul>
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ResultCacheColumnUnique extends ResultCache
{

  /**
   * Builds a new ResultCache with a column unique granularity.
   * 
   * @param maxEntries maximum number of entries
   * @param pendingTimeout pending timeout for concurrent queries
   */
  public ResultCacheColumnUnique(int maxEntries, int pendingTimeout)
  {
    super(maxEntries, pendingTimeout);
    parsingGranularity = ParsingGranularities.COLUMN_UNIQUE;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#processAddToCache(AbstractResultCacheEntry)
   */
  public void processAddToCache(AbstractResultCacheEntry qe)
  {
    SelectRequest request = qe.getRequest();
    ArrayList<?> selectedColumns = request.getSelect();
    // Update the tables columns dependencies
    Collection<?> from = request.getFrom();
    if (from == null)
      return;
    if (selectedColumns == null || selectedColumns.isEmpty())
    {
      logger
          .warn("No parsing of select clause found - Fallback to table granularity");
      for (Iterator<?> i = from.iterator(); i.hasNext();)
      {
        CacheDatabaseTable table = cdbs.getTable((String) i.next());
        table.addCacheEntry(qe);
        // Add all columns, entries will be added below.
        ArrayList<?> columns = table.getColumns();
        for (int j = 0; j < columns.size(); j++)
        {
          ((CacheDatabaseColumn) columns.get(j)).addCacheEntry(qe);
        }
        return;
      }
    }
    for (Iterator<?> i = request.getSelect().iterator(); i.hasNext();)
    {
      TableColumn tc = (TableColumn) i.next();
      cdbs.getTable(tc.getTableName()).getColumn(tc.getColumnName())
          .addCacheEntry(qe);
    }
    if (request.getWhere() != null)
    { // Add all columns dependencies
      for (Iterator<?> i = request.getWhere().iterator(); i.hasNext();)
      {
        TableColumn tc = (TableColumn) i.next();
        cdbs.getTable(tc.getTableName()).getColumn(tc.getColumnName())
            .addCacheEntry(qe);
      }
      if (request.getCacheAbility() == RequestType.UNIQUE_CACHEABLE)
      { // Add a specific entry for this pk
        String tableName = (String) from.iterator().next();
        AbstractResultCacheEntry entry = cdbs.getTable(tableName)
            .getPkResultCacheEntry(request.getPkValue());
        if (entry != null)
        {
          if (entry.isValid())
          { // Do not add an entry which has a lower selection than the current
            // one
            if (entry.getRequest().getSelect().size() >= request.getSelect()
                .size())
              return;
          }
        }
        cdbs.getTable(tableName).addPkCacheEntry(request.getPkValue(), qe);
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.AbstractResultCache#isUpdateNecessary(org.continuent.sequoia.controller.requests.UpdateRequest)
   */
  public boolean isUpdateNecessary(UpdateRequest request)
  {
    if (request.getCacheAbility() != RequestType.UNIQUE_CACHEABLE)
      return true;
    CacheDatabaseTable cacheTable = cdbs.getTable(request.getTableName());
    if (request.getColumns() == null)
      return true;
    String pk = request.getPk();
    AbstractResultCacheEntry qce = cacheTable.getPkResultCacheEntry(pk);
    if (qce != null)
    {
      if (!qce.isValid())
        return true;
      ControllerResultSet rs = qce.getResult();
      if (rs == null)
        return true;
      else
        return needInvalidate(rs, request)[1];
    }
    else
      return true;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#processWriteNotify(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  protected void processWriteNotify(AbstractWriteRequest request)
  {
    // Sanity check
    CacheDatabaseTable cacheTable = cdbs.getTable(request.getTableName());
    if (request.getColumns() == null)
    {
      logger.warn("No column parsing found - Fallback to table granularity ("
          + request.getUniqueKey() + ")");
      cacheTable.invalidateAll();
      return;
    }
    if (request.isInsert())
    {
      for (Iterator<?> i = request.getColumns().iterator(); i.hasNext();)
      {
        TableColumn tc = (TableColumn) i.next();
        cdbs.getTable(tc.getTableName()).getColumn(tc.getColumnName())
            .invalidateAllNonUnique();
      }
    }
    else
    {
      if (request.getCacheAbility() == RequestType.UNIQUE_CACHEABLE)
      {
        if (request.isUpdate())
        {
          String pk = ((UpdateRequest) request).getPk();
          AbstractResultCacheEntry qce = cacheTable.getPkResultCacheEntry(pk);
          if (qce != null)
          {
            boolean[] invalidate = needInvalidate(qce.getResult(),
                (UpdateRequest) request);
            if (invalidate[0])
            { // We must invalidate this entry
              cacheTable.removePkResultCacheEntry(pk);
              return;
            }
            else
            {
              if (logger.isDebugEnabled())
                logger.debug("No invalidate needed for request:"
                    + request.getSqlShortForm(20));
              return; // We don't need to invalidate
            }
          }
        }
        else if (request.isDelete())
        { // Invalidate the corresponding cache entry
          cacheTable
              .removePkResultCacheEntry(((DeleteRequest) request).getPk());
          return;
        }
      }
      // At this point this is a non unique write query or a request
      // we didn't handle properly (unknown request for example)
      for (Iterator<?> i = request.getColumns().iterator(); i.hasNext();)
      {
        TableColumn tc = (TableColumn) i.next();
        CacheDatabaseTable table = cdbs.getTable(tc.getTableName());
        table.invalidateAll(); // Pk are associated to tables
        if (!request.isAlter())
          table.getColumn(tc.getColumnName()).invalidateAll();
      }
    }
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#getName()
   */
  public String getName()
  {
    return "columnUnique";
  }

}