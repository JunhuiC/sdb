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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): 
 */

package org.continuent.sequoia.controller.cache.result.rules;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.cache.result.CacheBehavior;
import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.cache.result.entries.ResultCacheEntryEager;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * EagerCaching means that all entries in the cache are always coherent and any
 * update query (insert,delete,update,...) will automatically invalidate the
 * corresponding entry in the cache. This was the previous cache behavior for
 * all queries
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class EagerCaching extends CacheBehavior
{
  private long timeout;

  /**
   * Define this CacheBehavior as EagerCaching
   * 
   * @param timeout Timeout for this cache entry
   */
  public EagerCaching(long timeout)
  {
    this.timeout = timeout;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.CacheBehavior#getCacheEntry(SelectRequest,
   *           ControllerResultSet, AbstractResultCache)
   */
  public AbstractResultCacheEntry getCacheEntry(SelectRequest sqlQuery,
      ControllerResultSet result, AbstractResultCache cache)
  {
    return new ResultCacheEntryEager(cache, sqlQuery, result, timeout);
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    return "<" + DatabasesXmlTags.ELT_EagerCaching + "/>";
  }

}