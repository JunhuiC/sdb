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

package org.continuent.sequoia.common.jmx.monitoring.cache;

import org.continuent.sequoia.common.exceptions.DataCollectorException;
import org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.monitoring.datacollector.DataCollector;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;

/**
 * Abstract template to factor code for cache collectors
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public abstract class AbstractCacheStatsDataCollector
    extends AbstractDataCollector
{
  /**
	 * 
	 */
	private static final long serialVersionUID = 1449138589332066669L;
private String virtualDatabaseName;

  /**
   * new collector
   * 
   * @param virtualDatabaseName database accessed to get data
   */
  public AbstractCacheStatsDataCollector(String virtualDatabaseName)
  {
    super();
    this.virtualDatabaseName = virtualDatabaseName;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public long collectValue() throws DataCollectorException
  {
    VirtualDatabase vdb = ((Controller) controller)
        .getVirtualDatabase(virtualDatabaseName);
    AbstractResultCache cache = vdb.getRequestManager().getResultCache();
    if (cache == null)
      throw new DataCollectorException(DataCollector.NO_CACHE_ENABLED);
    return this.getValue(cache);
  }

  /**
   * We have the cache object so let's get the value we want from ot
   * 
   * @param cache as an object to allow it through RMI, but IS a
   *          <code>AbstractResultCache</code>
   * @return the collected value
   */
  public abstract long getValue(Object cache);

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getTargetName()
   */
  public String getTargetName()
  {
    return virtualDatabaseName;
  }
}
