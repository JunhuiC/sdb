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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.cache.result.rules;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.cache.result.CacheBehavior;
import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.cache.result.entries.ResultCacheEntryRelaxed;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * RelaxedCaching means we set a timeout value for this entry, and when expired
 * we keep in the cache if no write has modified the corresponding result, we
 * wait for the same amount of time again. RelaxedCaching may provide stale
 * data. The timeout defines the maximum staleness of a cache entry. It means
 * that the cache may return an entry that is out of date. timeout: is a value
 * in seconds and 0 means no timeout (always in the cache) keepIfNotDirty: if
 * true the entry is kept in the cache and the timeout is reset, if false, the
 * entry is removed from the cache after the timeout has expired even if the
 * entry was not affected by a write.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class RelaxedCaching extends CacheBehavior
{
  private long    timeout;
  private boolean keepIfNotDirty;

  /**
   * Create new RelaxedCaching action
   * 
   * @param timeout before we check the validity of an entry
   * @param keepIfNotDirty true if non-dirty entries must be kept in the cache
   */
  public RelaxedCaching(boolean keepIfNotDirty, long timeout)
  {
    this.keepIfNotDirty = keepIfNotDirty;
    this.timeout = timeout;
  }

  /**
   * @see org.continuent.sequoia.controller.cache.result.CacheBehavior#getCacheEntry(SelectRequest,
   *      ControllerResultSet, AbstractResultCache)
   */
  public AbstractResultCacheEntry getCacheEntry(SelectRequest sqlQuery,
      ControllerResultSet result, AbstractResultCache cache)
  {
    return new ResultCacheEntryRelaxed(sqlQuery, result, timeout,
        keepIfNotDirty);
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    return "<" + DatabasesXmlTags.ELT_RelaxedCaching + " "
        + DatabasesXmlTags.ATT_timeout + "=\"" + timeout / 1000 + "\" "
        + DatabasesXmlTags.ATT_keepIfNotDirty + "=\"" + keepIfNotDirty + "\"/>";
  }

}