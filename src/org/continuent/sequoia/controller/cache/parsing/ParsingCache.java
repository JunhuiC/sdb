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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.controller.cache.parsing;

import java.sql.SQLException;
import java.util.Hashtable;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requestmanager.ParserThread;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.RequestType;

/**
 * This class implements a request parsing cache.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ParsingCache
{
  private static Trace   logger = Trace.getLogger(ParsingCache.class.getName());
  // SQL -> parsed request
  private Hashtable<String, AbstractRequest>      cache;
  // SQL -> CurrentlyParsingEntry
  private Hashtable<String, CurrentlyParsingEntry>      currentlyParsing;
  private RequestManager requestManager;
  private int            granularity;
  private int            maxNbOfEntries;
  // Default is parse when needed
  private boolean        backgroundParsing;
  // Default is case insensitive
  private boolean        caseSensitiveParsing;

  /**
   * CurrentlyParsingEntry contains a (Request,ParserThread) which is an element
   * of the currentlyParsing Hashtable.
   * 
   * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
   * @version 1.0
   */
  private class CurrentlyParsingEntry
  {
    private ParserThread    parserThread;
    private AbstractRequest request;

    /**
     * Constructor for CurrentlyParsingEntry.
     * 
     * @param parserThread creating parser thread
     * @param request request to parse
     */
    public CurrentlyParsingEntry(ParserThread parserThread,
        AbstractRequest request)
    {
      this.parserThread = parserThread;
      this.request = request;
    }

    /**
     * Returns the parserThread.
     * 
     * @return ParserThread
     */
    public ParserThread getParserThread()
    {
      return parserThread;
    }

    /**
     * Returns the request.
     * 
     * @return AbstractRequest
     */
    public AbstractRequest getRequest()
    {
      return request;
    }

  }

  /**
   * Constructor for ParsingCache.
   * 
   * @param size maximum cache size in nb of entries
   * @param backgroundParsing true if the parsing should be done in background
   *          by a ParserThread
   */
  public ParsingCache(int size, boolean backgroundParsing)
  {
    cache = new Hashtable<String, AbstractRequest>(size == 0 ? 10000 : size);
    currentlyParsing = new Hashtable<String, CurrentlyParsingEntry>();
    if (size < 0)
      throw new RuntimeException(Translate.get("cache.parsing.invalid.size",
          size));
    if (size == 0)
      this.maxNbOfEntries = Integer.MAX_VALUE;
    else
      this.maxNbOfEntries = size;
    this.backgroundParsing = backgroundParsing;
    caseSensitiveParsing = false;
  }

  /**
   * Returns the granularity value.
   * 
   * @return Returns the granularity.
   */
  public int getGranularity()
  {
    return granularity;
  }

  /**
   * Sets the granularity value.
   * 
   * @param granularity The granularity to set.
   */
  public void setGranularity(int granularity)
  {
    this.granularity = granularity;
  }

  /**
   * Returns the requestManager value.
   * 
   * @return Returns the requestManager.
   */
  public RequestManager getRequestManager()
  {
    return requestManager;
  }

  /**
   * Sets the requestManager value.
   * 
   * @param requestManager The requestManager to set.
   */
  public void setRequestManager(RequestManager requestManager)
  {
    this.requestManager = requestManager;
  }

  /**
   * If the same SQL query is found in the cache, the parsing is cloned into the
   * given request. If backgroundParsing is set to true, then a ParserThread
   * starts parsing the request in background else nothing is done on a cache
   * miss.
   * 
   * @param request the request you look for
   */
  public void getParsingFromCache(AbstractRequest request)
  {
    if (request.isParsed())
      return;

    String sql = request.getUniqueKey();
    AbstractRequest parsedRequest = (AbstractRequest) cache.get(sql);

    if (parsedRequest != null)
    { // Cache hit, clone the parsing
      request.cloneParsing(parsedRequest);
      return;
    }
    else if (backgroundParsing)
    { // Cache miss, start parsing the request in background
      synchronized (currentlyParsing)
      {
        if (!currentlyParsing.contains(sql))
        { // Nobody else is trying to parse the same SQL query
          ParserThread pt = new ParserThread(request, requestManager
              .getDatabaseSchema(), granularity, caseSensitiveParsing);
          currentlyParsing.put(sql, new CurrentlyParsingEntry(pt, request));
        }
      }
    }
  }

  /**
   * Method getParsingFromCacheAndParseIfMissing.
   * 
   * @param request the request we look for
   * @exception SQLException if an error occurs
   */
  public void getParsingFromCacheAndParseIfMissing(AbstractRequest request)
      throws SQLException
  {
    if (request.isParsed())
      return;

    // Check cache
    String instanciatedSQL = request.getUniqueKey();
    AbstractRequest parsedRequest = (AbstractRequest) cache
        .get(instanciatedSQL);

    try
    {
      if (parsedRequest == null)
      { // Cache miss
        String sqlSkeleton = request.getSqlOrTemplate();
        String sql;
        if (sqlSkeleton != null)
        { // Missed with instanciated query, try with skeleton
          sql = sqlSkeleton;
          parsedRequest = (AbstractRequest) cache.get(sql);
          if (parsedRequest != null)
          { // Cache hit with skeleton
            request.cloneParsing(parsedRequest);
            return;
          }
        }
        else
          sql = instanciatedSQL;

        // Full cache miss. Note that the underlying cache Hashtable is
        // synchronized and we usually do not need to synchronize on it.
        // As we will have to add a cache entry, check if the cache size is ok
        // else remove the first entry of the hashtable.
        while (cache.size() > maxNbOfEntries)
        { // Remove first entry from Hashtable. We need to synchronize here to
          // be
          // sure that we are not trying to concurrently remove the first cache
          // entry.
          synchronized (cache)
          {
            try
            {
              cache.remove(cache.keys().nextElement());
            }
            catch (Exception ignore)
            {
              break;
            }
          }
        }

        // Both skeleton and instanciated missed
        if (backgroundParsing)
        {
          // Find the parsing thread and request (note that Hasthtable is
          // synchronized)
          CurrentlyParsingEntry cpe = (CurrentlyParsingEntry) currentlyParsing
              .get(sql);
          if (cpe != null)
          {
            ParserThread pt = cpe.getParserThread();
            try
            {
              if (pt != null)
              {
                // Wait for completion
                pt.join();
                synchronized (currentlyParsing)
                {
                  currentlyParsing.remove(sql);
                }

                // Update cache
                if ((granularity != ParsingGranularities.COLUMN_UNIQUE)
                    || (sqlSkeleton == null))
                  // No skeleton or no uniqueness criteria, add the query
                  cache.put(instanciatedSQL, cpe.getRequest());
                else
                { // We have a skeleton and COLUMN_UNIQUE parsing
                  if (request.getCacheAbility() != RequestType.UNIQUE_CACHEABLE)
                    // It is NOT UNIQUE, add the skeleton
                    cache.put(sqlSkeleton, cpe.getRequest());
                  else
                    // It is UNIQUE, add the instanciated query
                    cache.put(instanciatedSQL, cpe.getRequest());
                }
              }
            }
            catch (InterruptedException failed)
            {
              throw new SQLException(Translate.get(
                  "cache.parsing.failed.join.parser.thread", new String[]{
                      "" + request.getId(), failed.getMessage()}));
            }
          }
        }
        // Parse it now because we didn't parse in background or
        // backgroundParsing has failed for any obscure reason.
        request.parse(requestManager.getDatabaseSchema(), granularity,
            caseSensitiveParsing);

        // Update cache
        if ((sqlSkeleton != null)
            && (granularity == ParsingGranularities.COLUMN_UNIQUE)
            && (request.getCacheAbility() == RequestType.UNIQUE_CACHEABLE))
          // If this is a unique request, we must put the instanciated query in
          // the cache to retrieve the exact pk value.
          cache.put(instanciatedSQL, request);
        else
          cache.put(sql, request);
      }
      else
        // Cache hit
        request.cloneParsing(parsedRequest);
    }
    catch (OutOfMemoryError oome)
    {
      synchronized (cache)
      {
        cache.clear();
      }
      System.gc();
      logger.warn(Translate.get("cache.memory.error.cache.flushed", this
          .getClass()));
    }
  }

  /**
   * Returns the backgroundParsing.
   * 
   * @return boolean
   */
  public boolean isBackgroundParsing()
  {
    return backgroundParsing;
  }

  /**
   * Sets the background parsing. If true the request are parsed in background
   * by a separate thread that is created for this purpose.
   * 
   * @param backgroundParsing The backgroundParsing to set
   */
  public void setBackgroundParsing(boolean backgroundParsing)
  {
    this.backgroundParsing = backgroundParsing;
  }

  /**
   * Sets the parsing case sensitivity
   * 
   * @param isCaseSensitiveParsing true if parsing is case sensitive
   */
  public void setCaseSensitiveParsing(boolean isCaseSensitiveParsing)
  {
    this.caseSensitiveParsing = isCaseSensitiveParsing;
  }

  /**
   * Returns the caseSensitiveParsin.
   * 
   * @return boolean
   */
  public boolean isCaseSensitiveParsing()
  {
    return caseSensitiveParsing;
  }

  /**
   * Get xml information about this ParsingCache
   * 
   * @return <code>String</code> in xml formatted text
   */
  public String getXml()
  {
    return "<" + DatabasesXmlTags.ELT_ParsingCache + " "
        + DatabasesXmlTags.ATT_backgroundParsing + "=\"" + backgroundParsing
        + "\" " + DatabasesXmlTags.ATT_maxNbOfEntries + "=\"" + maxNbOfEntries
        + "\"/>";
  }

  /**
   * Retrieves the current request parsing cache configuration. The returned
   * string contains:
   * <ul>
   * <li>granularity
   * <li>maximum number of entries
   * <li>background parsing flag
   * <li>case sensitivity
   * </ul>
   * 
   * @return a String containing the configuration of the cache
   */
  public String dumpCacheConfig()
  {
    StringBuffer sb = new StringBuffer();
    sb.append(Translate.get("cache.dump")); //$NON-NLS-1$
    sb.append(Translate.get(
        "cache.granularity", ParsingGranularities.getInformation(granularity))); //$NON-NLS-1$
    sb.append(Translate.get("cache.max.entries", maxNbOfEntries)); //$NON-NLS-1$
    sb.append(Translate.get("cache.background.parsing", backgroundParsing)); //$NON-NLS-1$
    sb.append(Translate.get("cache.case.sensitive", caseSensitiveParsing)); //$NON-NLS-1$
    return sb.toString();
  }

  /**
   * Retrieves the number of entries currently contained in the cache
   * 
   * @return number of cache entries
   */
  public int getNumberOfCacheEntries()
  {
    return cache.size();
  }

  /**
   * Prints entries of the cache, from beginIndex to (beginIndex + max) or to
   * the last entry if beginIndex+max is out of bounds
   * 
   * @param beginIndex entry from which to start dump
   * @param max maximum number of entries to dump
   * @return a string containing the cache entries (request unique key + short
   *         description string) from beginIndex to the last entry or to
   *         beginIndex+max.
   */
  public String dumpCacheEntries(int beginIndex, int max)
  {
    StringBuffer sb = new StringBuffer();
    int i = beginIndex;
    Object[] keys = cache.keySet().toArray();
    while (i < keys.length && i < (beginIndex + max))
    {
      sb.append(Translate.get("cache.entry", new String[]{ //$NON-NLS-1$
          keys[i].toString(),
              ((AbstractRequest) cache.get(keys[i])).toShortDebugString()}));
      i++;
    }
    return sb.toString();
  }

  /**
   * Dumps requests that are currently beeing parsed
   * 
   * @return a String containing the entries (request unique key + short
   *         description string) currently beeing parsed
   */
  public String dumpCurrentlyParsedEntries()
  {
    StringBuffer sb = new StringBuffer();
    if (currentlyParsing != null && currentlyParsing.size() > 0)
    {
      sb.append(Translate.get("cache.currently.parsing.entries")); //$NON-NLS-1$
      for (int i = 0; i < currentlyParsing.size(); i++)
      {
        sb.append(Translate.get("cache.currently.parsing.entry", new String[]{ //$NON-NLS-1$
                currentlyParsing.keySet().toArray()[i].toString(),
                ((CurrentlyParsingEntry) currentlyParsing.get(currentlyParsing
                    .keySet().toArray()[i])).request.toShortDebugString()}));
      }
    }
    return sb.toString();
  }
}
