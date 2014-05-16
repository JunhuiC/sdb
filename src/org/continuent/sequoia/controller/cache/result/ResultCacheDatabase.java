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

import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.UpdateRequest;

/**
 * This is a query cache implementation with a database granularity:
 * <ul>
 * <li><code>DATABASE</code>: the cache is flushed each time the database is
 * updated (every INSERT, UPDATE, DELETE, ... statement).</li>
 * </ul>
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */

public class ResultCacheDatabase extends ResultCache
{

  /**
   * Builds a new ResultCache with a database granularity.
   * 
   * @param maxEntries maximum number of entries
   * @param pendingTimeout pending timeout for concurrent queries
   */
  public ResultCacheDatabase(int maxEntries, int pendingTimeout)
  {
    super(maxEntries, pendingTimeout);
    parsingGranularity = ParsingGranularities.NO_PARSING;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#processAddToCache
   */
  protected void processAddToCache(AbstractResultCacheEntry qe)
  {
    return;
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
    flushCache();
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.ResultCache#getName()
   */
  public String getName()
  {
    return "database";
  }

}