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
 * Contributor(s): 
 */

package org.continuent.sequoia.common.jmx.monitoring.cache;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;

/**
 * Count total of entries in the cache
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 */
public class CacheEntriesCollector extends AbstractCacheStatsDataCollector
{
  private static final long serialVersionUID = 8773640943678418819L;

  /**
   * create collector
   * 
   * @param virtualDatabaseName to get cache info from
   */
  public CacheEntriesCollector(String virtualDatabaseName)
  {
    super(virtualDatabaseName);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public long getValue(Object cache)
  {
    return ((AbstractResultCache) cache).getCacheSize();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getDescription()
   */
  public String getDescription()
  {
    return Translate.get("monitoring.cache.number.entries"); //$NON-NLS-1$
  }
}
