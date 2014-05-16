/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.jmx.mbeans;

/**
 * MBean Interface to manage the parsing cache
 * 
 * @see org.continuent.sequoia.common.jmx.JmxConstants#getParsingCacheObjectName(String)
 */
public interface ParsingCacheMBean
{
  /**
   * Dumps the parsing cache configuration into a String.
   * 
   * @return parsing cache configuration
   */
  String dumpCacheConfig();

  /**
   * Dumps cache entries from the parsing cache.<br>
   * The dump will be done by chuncks of predefined size. First call will start
   * at entry index 0. Next calls will dump the next bunch of entries, until the
   * end of the entry list. When all entries have been fetched, the function
   * will return null and reset all counters to zero, making next call re-start
   * a full dump
   * 
   * @see org.continuent.sequoia.controller.cache.parsing.ParsingCacheControl#ENTRIES_PER_DUMP
   * @return a dump of the next parsing cache entries, or null if all entries
   *         have been fetched
   */
  String dumpNextCacheEntries();

  /**
   * Resets the dump of the cache entries (next dump will restart from first
   * entry)
   * 
   * @see #dumpNextCacheEntries()
   */
  void resetDump();

  /**
   * Retrieves the requests that are currently beeing parsed
   * 
   * @return currently parsed entries as a string
   */
  String dumpCurrentlyParsedEntries();
}
