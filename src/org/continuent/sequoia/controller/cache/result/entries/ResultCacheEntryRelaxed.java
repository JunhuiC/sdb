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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.cache.result.entries;

import java.util.Date;

import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * A <code>CacheEntry</code> that is to be recognized as Relaxed entry.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ResultCacheEntryRelaxed extends AbstractResultCacheEntry
{
  private long    timeout;
  private long    deadline;
  private boolean keepIfNotDirty;

  /**
   * Create a new Relaxed Query Cache entry
   * 
   * @param request Select request to cache
   * @param result ResultSet to cache
   * @param timeout timeout in ms for this entry
   * @param keepIfNotDirty true if entry must be kept in cache if not dirty once
   *          timeout has expired
   */
  public ResultCacheEntryRelaxed(SelectRequest request,
      ControllerResultSet result, long timeout, boolean keepIfNotDirty)
  {
    super(request, result);
    this.timeout = timeout;
    this.deadline = System.currentTimeMillis() + timeout;
    this.keepIfNotDirty = keepIfNotDirty;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#invalidate()
   */
  public void invalidate()
  {
    state = CACHE_DIRTY;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#getType()
   */
  public String getType()
  {
    return "Relaxed";
  }

  /**
   * Get the expiration deadline
   * 
   * @return the expiration deadline
   */
  public long getDeadline()
  {
    return deadline;
  }

  /**
   * Set the expiration deadline
   * 
   * @param deadline time in ms relative to current time
   */
  public void setDeadline(long deadline)
  {
    this.deadline = deadline;
  }

  /**
   * Get the timeout for this entry.
   * 
   * @return timeout in ms
   */
  public long getTimeout()
  {
    return timeout;
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
   * Should the entry must be kept in the cache if the entry is not dirty once
   * the timeout has expired.
   * 
   * @return true if yes
   */
  public boolean getKeepIfNotDirty()
  {
    return keepIfNotDirty;
  }

}