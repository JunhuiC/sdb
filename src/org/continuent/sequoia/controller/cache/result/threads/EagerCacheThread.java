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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.cache.result.threads;

import java.util.ArrayList;
import java.util.Iterator;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.cache.result.ResultCache;
import org.continuent.sequoia.controller.cache.result.entries.ResultCacheEntryEager;

/**
 * This thread manages eager cache entries and remove them from the cache if
 * they have expired.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public final class EagerCacheThread extends Thread
{
  private final ResultCache cache;
  private long              threadWakeUpTime = 0;
  int                       refreshCacheRate = 60;
  int                       refreshCacheTime = 60 / refreshCacheRate;
  private Trace             logger           = Trace
                                                 .getLogger(EagerCacheThread.class
                                                     .getName());
  private boolean           isKilled         = false;

  /**
   * Creates a new <code>EagerCacheThread</code> object
   * 
   * @param cache ResultCache creating this thread
   */
  public EagerCacheThread(ResultCache cache)
  {
    super("EagerCacheThread");
    this.cache = cache;
  }

  /**
   * Returns the threadWakeUpTime value.
   * 
   * @return Returns the threadWakeUpTime.
   */
  public long getThreadWakeUpTime()
  {
    return threadWakeUpTime;
  }

  /**
   * @see java.lang.Runnable#run()
   */
  public void run()
  {
    ResultCacheEntryEager entry;
    long now;
    long sleep;
    // Keep trace of relaxed cache entries to delete
    ArrayList<ResultCacheEntryEager> toRemoveFromEagerCache = new ArrayList<ResultCacheEntryEager>();
    while (!isKilled)
    {
      synchronized (this)
      {
        try
        {
          threadWakeUpTime = 0;
          if (cache.getEagerCache().isEmpty())
          { // Nothing in the cache, just sleep!
            if (logger.isDebugEnabled())
              logger.debug(Translate.get("cachethread.cache.empty.sleeping"));
            wait();
          }
          else
          { // Look for first deadline
            now = System.currentTimeMillis();
            for (Iterator<?> iter = cache.getEagerCache().iterator(); iter
                .hasNext();)
            {
              entry = (ResultCacheEntryEager) iter.next();
              if (entry.getDeadline() < now)
              { // Deadline has expired, remove entry
                toRemoveFromEagerCache.add(entry);
                continue;
              }

              // Recompute next wakeup time
              if ((threadWakeUpTime == 0)
                  || (entry.getDeadline() < threadWakeUpTime))
                threadWakeUpTime = entry.getDeadline();
            }

            // Clean up all the entries from the eager cache
            int size = toRemoveFromEagerCache.size();
            for (int i = 0; i < size; i++)
            {
              entry = (ResultCacheEntryEager) toRemoveFromEagerCache.get(i);
              if (logger.isDebugEnabled())
                logger.debug(Translate.get(
                    "cachethread.remove.entry.from.cache", entry.getRequest()
                        .getUniqueKey()));
              try
              {
                cache.removeFromCache(entry.getRequest());
              }
              catch (Exception e)
              {
                logger.warn("cachethread.remove.entry.error", e);
              }
              try
              {
                cache.getEagerCache().remove(entry);
              }
              catch (Exception e)
              {
                logger.warn("cachethread.remove.entry.error", e);
              }
            }
            toRemoveFromEagerCache.clear();
            if (threadWakeUpTime == 0)
            { // All entries were not kept in the cache, therefore
              // there is no next deadline. (and no cache entry to wait for)
              continue;
            }
            else
            { // Sleep until the next deadline
              sleep = (threadWakeUpTime - now) / 1000 + refreshCacheTime;
              if (logger.isDebugEnabled())
              {
                logger.debug(Translate.get("cachethread.sleeping", sleep));
              }
              sleep = (sleep) * 1000;
              wait(sleep);
            }
          }
        }
        catch (Exception e)
        {
          logger.warn(e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Shutdown the current thread.
   */
  public synchronized void shutdown()
  {
    isKilled = true;
    notify();
  }

}