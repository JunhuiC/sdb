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
import org.continuent.sequoia.controller.cache.result.entries.ResultCacheEntryRelaxed;

/**
 * This thread manages relaxed cache entries and remove them from the cache if
 * their deadline has expired or they are dirty.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public final class RelaxedCacheThread extends Thread
{
  private long              threadWakeUpTime = 0;
  private final ResultCache cache;
  int                       refreshCacheRate = 60;
  int                       refreshCacheTime = 60 / refreshCacheRate;
  private Trace             logger           = Trace
                                                 .getLogger(RelaxedCacheThread.class
                                                     .getName());

  private boolean           isKilled         = false;

  /**
   * Creates a new <code>RelaxedCacheThread</code> object
   * 
   * @param cache ResultCache creating this thread
   */
  public RelaxedCacheThread(ResultCache cache)
  {
    super("RelaxedCacheThread");
    this.cache = cache;
  }

  /**
   * Creates a new <code>RelaxedCacheThread</code> object
   * 
   * @param cache ResultCache creating this thread
   * @param refreshCacheRate cache refresh rate in seconds
   */
  public RelaxedCacheThread(ResultCache cache, int refreshCacheRate)
  {
    this(cache);
    this.refreshCacheRate = refreshCacheRate;
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
    ResultCacheEntryRelaxed entry;
    long now;
    long sleep;
    // Keep trace of relaxed cache entries to delete
    ArrayList<ResultCacheEntryRelaxed> toRemoveFromRelaxedCache = new ArrayList<ResultCacheEntryRelaxed>();
    while (!isKilled)
    {
      synchronized (this)
      {
        try
        {
          threadWakeUpTime = 0;
          if (cache.getRelaxedCache().isEmpty())
          { // Nothing in the cache, just sleep!
            if (logger.isDebugEnabled())
              logger.debug(Translate.get("cachethread.cache.empty.sleeping"));
            wait();
          }
          else
          { // Look for first deadline
            now = System.currentTimeMillis();
            for (Iterator<?> iter = cache.getRelaxedCache().iterator(); iter
                .hasNext();)
            {
              entry = (ResultCacheEntryRelaxed) iter.next();
              if (entry.getDeadline() < now)
              { // Deadline has expired
                if (entry.isDirty() || !entry.getKeepIfNotDirty())
                { // Remove this entry
                  toRemoveFromRelaxedCache.add(entry);
                  continue;
                }
                else
                  // Entry is still valid, reset deadline
                  entry.setDeadline(now + entry.getTimeout());
              }

              // Recompute next wakeup time if needed
              if (threadWakeUpTime == 0
                  || (entry.getDeadline() < threadWakeUpTime))
                threadWakeUpTime = entry.getDeadline();
            }

            // Clean up all dirty entries from the relaxed cache
            int size = toRemoveFromRelaxedCache.size();
            for (int i = 0; i < size; i++)
            {
              entry = (ResultCacheEntryRelaxed) toRemoveFromRelaxedCache.get(i);
              if (logger.isDebugEnabled())
                logger.debug(Translate.get(
                    "cachethread.remove.entry.from.cache", entry.getRequest()
                        .getUniqueKey()));
              this.cache.removeFromCache(entry.getRequest());
              cache.getRelaxedCache().remove(entry);
            }
            toRemoveFromRelaxedCache.clear();
            if (threadWakeUpTime == 0)
            { // All entries were dirty and not kept in the cache, therefore
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