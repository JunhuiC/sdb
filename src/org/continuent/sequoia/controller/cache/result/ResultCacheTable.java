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
 * Contributor(s): Nicolas Modzyk.
 */

package org.continuent.sequoia.controller.cache.result;

import java.util.Collection;
import java.util.Iterator;

import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.cache.result.schema.CacheDatabaseTable;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.UpdateRequest;

/**
 * This is a query cache implementation with a table granularity:
 * <ul>
 * <li><code>TABLE</code>: table granularity, entries in the cache are
 * invalidated based on table dependencies.</li>
 * </ul>
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ResultCacheTable extends ResultCache
{
  /**
   * Builds a new ResultCache with a table granularity.
   * 
   * @param maxEntries maximum number of entries
   * @param pendingTimeout pending timeout for concurrent queries
   */
  public ResultCacheTable(int maxEntries, int pendingTimeout)
  {
    super(maxEntries, pendingTimeout);
    parsingGranularity = ParsingGranularities.TABLE;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#processAddToCache
   */
  protected void processAddToCache(AbstractResultCacheEntry qe)
  {
    SelectRequest request = qe.getRequest();
    Collection from = request.getFrom();
    if (from == null)
      return;
    for (Iterator i = from.iterator(); i.hasNext();)
      cdbs.getTable((String) i.next()).addCacheEntry(qe);
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
    CacheDatabaseTable cdt = cdbs.getTable(request.getTableName());

    if (cdt != null)
      cdt.invalidateAll();
    else
    {
      logger.warn("Table " + request.getTableName()
          + " not found in cache schema. Flushing whole cache.");
      flushCache();
    }
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#getName()
   */
  public String getName()
  {
    return "table";
  }
}