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

package org.continuent.sequoia.controller.cache.result;

import java.util.Hashtable;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.cache.result.rules.EagerCaching;
import org.continuent.sequoia.controller.cache.result.rules.NoCaching;
import org.continuent.sequoia.controller.cache.result.rules.RelaxedCaching;

/**
 * Create a cache that conforms to AbstractResultCache, that is implementation
 * independant
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public class ResultCacheFactory
{
  /**
   * Get an instance of the current cache implementation
   * 
   * @param granularityValue of the parsing
   * @param maxEntries to the cache
   * @param pendingTimeout before pending query timeout
   * @return <code>ResultCache</code> implementation of the
   *         <code>AbstractResultCache</code>
   * @throws InstantiationException if parsing granularity is not valid
   */
  public static AbstractResultCache getCacheInstance(int granularityValue,
      int maxEntries, int pendingTimeout) throws InstantiationException
  {
    AbstractResultCache currentRequestCache = null;
    switch (granularityValue)
    {
      case CachingGranularities.TABLE :
        currentRequestCache = new ResultCacheTable(maxEntries, pendingTimeout);
        break;
      case CachingGranularities.DATABASE :
        currentRequestCache = new ResultCacheDatabase(maxEntries,
            pendingTimeout);
        break;
      case CachingGranularities.COLUMN :
        currentRequestCache = new ResultCacheColumn(maxEntries, pendingTimeout);
        break;
      case CachingGranularities.COLUMN_UNIQUE :
        currentRequestCache = new ResultCacheColumnUnique(maxEntries,
            pendingTimeout);
        break;
      default :
        throw new InstantiationException("Invalid Granularity Value");
    }
    return currentRequestCache;
  }

  /**
   * Get an instance of a cache behavior for this cache
   * 
   * @param behaviorString representation of this cache behavior, xml tag
   * @param options for different cache rules
   * @return an instance of a cache behavior
   */
  public static CacheBehavior getCacheBehaviorInstance(String behaviorString,
      Hashtable options)
  {
    if (behaviorString.equalsIgnoreCase(DatabasesXmlTags.ELT_NoCaching))
      return new NoCaching();
    if (behaviorString.equals(DatabasesXmlTags.ELT_EagerCaching))
    {
      // Timeout is in seconds: *1000      
      // 0, is no timeout, and 0x1000=0 !
      long timeout = 1000 * Long.parseLong((String) options
          .get(DatabasesXmlTags.ATT_timeout));
      return new EagerCaching(timeout);
    }
    if (behaviorString.equals(DatabasesXmlTags.ELT_RelaxedCaching))
    {
      // Timeout is in seconds: *1000
      long timeout = 1000 * Long.parseLong((String) options
          .get(DatabasesXmlTags.ATT_timeout));
      boolean keepIfNotDirty = new Boolean((String) options
          .get(DatabasesXmlTags.ATT_keepIfNotDirty)).booleanValue();
      return new RelaxedCaching(keepIfNotDirty, timeout);
    }
    else
      return null;
  }
}