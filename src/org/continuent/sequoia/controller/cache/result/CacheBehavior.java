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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.cache.result;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * Abstract class for the different cache actions. We need this class for adding
 * versatility in the parameters of each Caching action.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public abstract class CacheBehavior
{
  Trace logger = Trace.getLogger(CacheBehavior.class.getName());

  protected CacheBehavior()
  {
    logger.debug(Translate.get("cachebehavior.new.action", getType()));
  }

  /**
   * The name of the class instance
   * 
   * @return class name of the current type
   */
  public String getType()
  {
    return this.getClass().getName();
  }

  /**
   * Builds a cache entry from a <code>SelectRequest</code> and a
   * <code>ControllerResultSet</code>. This cache entry can then be inserted
   * in the cache.
   * 
   * @param sqlQuery entry to add in the cache
   * @param result value to add in the cache
   * @param cache reference for EagerCaching in case the entry needs to remove
   *          itself from the cache.
   * @return the query cache entry to add to the cache
   */
  public abstract AbstractResultCacheEntry getCacheEntry(
      SelectRequest sqlQuery, ControllerResultSet result,
      AbstractResultCache cache);

  /**
   * Implementation specific xml dump of the cache behavior.
   * 
   * @return xml dump of the cache behavior
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public abstract String getXml();
}