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

/**
 * A <code>CacheEntry</code> that simulates a NoCacheEntry.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ResultCacheEntryNoCache extends AbstractResultCacheEntry
{
  static final String[] STRING_DATA =
    new String[] { "", "NoCache", "Invalid", "", "0" };
  /**
   * Create a new No Caching Query Cache entry
   */
  public ResultCacheEntryNoCache()
  {
    super(null, null);
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#invalidate()
   */
  public void invalidate()
  {
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#getType()
   */
  public String getType()
  {
    return "NoCache";
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry#toStringTable()
   */
  public String[] toStringTable()
  {
    return STRING_DATA;
  }
}
