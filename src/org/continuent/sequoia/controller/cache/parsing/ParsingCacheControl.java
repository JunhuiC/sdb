/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent.
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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.cache.parsing;

import javax.management.NotCompliantMBeanException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.mbeans.ParsingCacheMBean;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;

/**
 * RequestFactoryControlMBean implemementation. Used to manage RequestFactory
 * 
 * @see org.continuent.sequoia.controller.requests.RequestFactory
 * @see org.continuent.sequoia.common.jmx.mbeans.ParsingCacheMBean
 */
public class ParsingCacheControl extends AbstractStandardMBean
    implements
      ParsingCacheMBean
{
  private ParsingCache     managedCache;
  private static final int ENTRIES_PER_DUMP     = 100;
  private int              numberOfCacheEntries = 0;
  private int              currentDumpIndex     = 0;

  /**
   * Creates a new <code>ParsingCacheControl</code> object
   * 
   * @param cache the managed cache
   * @throws NotCompliantMBeanException if this mbean is not compliant
   */
  public ParsingCacheControl(ParsingCache cache)
      throws NotCompliantMBeanException
  {
    super(ParsingCacheMBean.class);
    this.managedCache = cache;
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "requestfactory"; //$NON-NLS-1$
  }

  /**
   * @see ParsingCacheMBean#dumpCacheConfig()
   */
  public String dumpCacheConfig()
  {
    return managedCache.dumpCacheConfig();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ParsingCacheMBean#dumpNextCacheEntries()
   */
  public String dumpNextCacheEntries()
  {
    StringBuffer ret = new StringBuffer();
    if (currentDumpIndex > numberOfCacheEntries)
    {
      // last dump reached the end of the list, return null and reset counters
      currentDumpIndex = 0;
      return null;
    }
    if (currentDumpIndex == 0)
    {
      // This is a new dump => get the cache size and print a description
      numberOfCacheEntries = managedCache.getNumberOfCacheEntries();
      ret.append(Translate.get("cache.entries")); //$NON-NLS-1$
    }
    if (numberOfCacheEntries == 0) // no entry, stop dump
    {
      currentDumpIndex = 0;
      return null;
    }
    try
    {
      ret.append(managedCache.dumpCacheEntries(currentDumpIndex,
          ENTRIES_PER_DUMP));
      currentDumpIndex += ENTRIES_PER_DUMP;
    }
    catch (OutOfMemoryError e)
    {
      // stop dump
      currentDumpIndex = 0;
      return null;
    }
    return ret.toString();
  }

  /**
   * @see ParsingCacheMBean#resetDump()
   */
  public void resetDump()
  {
    currentDumpIndex = 0;
    numberOfCacheEntries = 0;
  }

  /**
   * @see ParsingCacheMBean#dumpCurrentlyParsedEntries()
   */
  public String dumpCurrentlyParsedEntries()
  {
    // Assume that the number of currently parsed entries is small enough to fit
    // into a string (this is actually realistic...)
    return managedCache.dumpCurrentlyParsedEntries();
  }
}
