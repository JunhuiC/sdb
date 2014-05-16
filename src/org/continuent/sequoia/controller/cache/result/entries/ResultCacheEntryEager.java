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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.cache.result.entries;

import java.util.Date;

import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * A <code>CacheEntry</code> that is to be recognized as Eager entry.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ResultCacheEntryEager extends AbstractResultCacheEntry
{
  private AbstractResultCache cache;
  private long                timeout;
  private long                deadline;

  /**
   * Create a new Eager Query Cache entry
   * 
   * @param cache The query cache we belong to
   * @param request Select request to cache
   * @param result ResultSet to cache
   * @param timeout The timeout to use for the deadline (0 for no timeout)
   */
  public ResultCacheEntryEager(AbstractResultCache cache,
      SelectRequest request, ControllerResultSet result, long timeout)
  {
    super(request, result);
    this.cache = cache;
    if (timeout > 0)
      this.deadline = System.currentTimeMillis() + timeout;
    else
      this.deadline = NO_DEADLINE;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#invalidate()
   */
  public void invalidate()
  {
    state = CACHE_INVALID;
    if (cache != null)
      cache.removeFromCache(request);
    if (result != null)
      result = null;
    cache = null;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#getType()
   */
  public String getType()
  {
    return "Eager";
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#toStringTable()
   */
  public String[] toStringTable()
  {
    return new String[]{request.getUniqueKey(), getType(), getState(),
        new Date(getDeadline()).toString(), String.valueOf(getSizeOfResult())};
  }

  /**
   * Returns the deadline value.
   * 
   * @return Returns the deadline.
   */
  public long getDeadline()
  {
    return deadline;
  }

  /**
   * Returns the timeout value.
   * 
   * @return Returns the timeout.
   */
  public long getTimeout()
  {
    return timeout;
  }
}