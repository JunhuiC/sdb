/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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
 * Initial developer(s): Damian Arregui.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.requests.AbstractRequest;


/**
 * This class defines a RequestResultFailoverCache.<br>
 * <br>
 * It is used to implement the transparent failover feature. It temporarily
 * stores requests results so that they can be retrieved by the driver. Results
 * are stored/retrieved with a request ID.<br>
 * <br>
 * An associated clean-up thread is started at instantiation time. It takes care
 * of removing cache entries which are too old.
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public class RequestResultFailoverCache implements Runnable
{

  /**
   * Time elapsed between two clean-up runs (in ms).
   */
  private static final long CACHE_CLEANUP_TIMEOUT  = 10000;

  /**
   * Period of time after which a cache entry is considered to be too old (in
   * ms).
   */
  private long              entryTimeout;

  /** Distribued virtual database logger */
  private Trace             logger;

  // Request result cache
  // request ID -> result
  private Map<Long, CachedResult>               requestIdResult        = new HashMap<Long, CachedResult>();

  // Transaction ID to request ID mapping
  // transaction ID -> request ID
  private Map<Long, Long>               transactionIdRequestId = new HashMap<Long, Long>();

  // Connection ID to request ID mapping
  // connection ID -> request ID
  private Map<Long, Long>               connectionIdRequestId  = new HashMap<Long, Long>();

  private boolean           isKilled               = false;

  // This class is used to store in cache a expirationDate together with each
  // result
  private class CachedResult
  {
    private Serializable result;
    private long         expirationDate;

    /**
     * Creates a new <code>CachedResult</code> object containing a result and
     * automatically associating a expirationDate to it
     * 
     * @param result result to store in this entry
     */
    public CachedResult(Serializable result, long entryTimeout)
    {
      this.result = result;
      expirationDate = System.currentTimeMillis() + entryTimeout;
    }

    /**
     * Return the result
     * 
     * @return result stored in the entry
     */
    public Serializable getResult()
    {
      return result;
    }

    /**
     * Timestamp (in ms) when the result expires
     * 
     * @return creation expirationDate
     */
    public long getExpirationDate()
    {
      return expirationDate;
    }
  }

  /**
   * Creates a new <code>RequestResultFailoverCache</code> object and starts
   * its associated clean-up thread.
   * 
   * @param logger logger to use to display messages
   * @param entryTimeout time in ms after which an entry is removed from the
   *          cache
   */
  public RequestResultFailoverCache(Trace logger, long entryTimeout)
  {
    this.logger = logger;
    this.entryTimeout = entryTimeout;
    (new Thread(this, "RequestResultFailoverCacheCleanupThread")).start();
  }

  /**
   * Stores in cache a result associated with a given request.
   * 
   * @param request request executed
   * @param result result of the execution of the request
   */
  public synchronized void store(AbstractRequest request, Serializable result)
  {
    Long requestId = new Long(request.getId());

    // Add to the cache first (else other lists would temporarily point to a
    // non-existing entry).
    synchronized (requestIdResult)
    {
      if (requestIdResult.isEmpty())
      {
        // Wake up thread to purge results as needed
        requestIdResult.notify();
      }
      requestIdResult.put(requestId, new CachedResult(result, entryTimeout));
    }

    if (!request.isAutoCommit())
    { // Replace last result for the transaction
      Long transactionId = new Long(request.getTransactionId());
      if (transactionIdRequestId.containsKey(transactionId))
      {
        requestIdResult.remove(transactionIdRequestId.get(transactionId));
      }
      transactionIdRequestId.put(transactionId, requestId);
    }

    if (request.isPersistentConnection())
    { // Replace the last result for the persistent connection
      Long connectionId = new Long(request.getPersistentConnectionId());
      if (connectionIdRequestId.containsKey(connectionId))
      {
        requestIdResult.remove(connectionIdRequestId.get(connectionId));
      }
      connectionIdRequestId.put(connectionId, requestId);
    }

    if (logger.isDebugEnabled())
      logger.debug("Stored result for request ID: " + request.getId() + " -> "
          + result);
  }

  /**
   * Retrieves from cache the result associated with a request ID.
   * 
   * @param requestId id of the request to retrieve
   * @return result or null if result not found
   */
  public synchronized Serializable retrieve(long requestId)
  {
    Serializable res = null;
    Long requestIdLong = new Long(requestId);
    if (requestIdResult.containsKey(requestIdLong))
    {
      res = ((CachedResult) requestIdResult.get(requestIdLong)).getResult();
      if (logger.isDebugEnabled())
        logger.debug("Retrieved result for request ID: " + requestId + " -> "
            + res);
    }
    else
    { // Not found
      if (logger.isDebugEnabled())
        logger.debug("No result found in failover cache for request "
            + requestId);
    }
    return res;
  }

  /**
   * Takes care of removing cache entries which are too old.
   * 
   * @see java.lang.Runnable#run()
   */
  public void run()
  {
    // Thread runs forever
    while (!isKilled)
    {
      try
      {
        synchronized (requestIdResult)
        {
          // Wait if there is no result else just sleep for the configured time
          // interval
          if (requestIdResult.isEmpty())
            requestIdResult.wait();
          else
            requestIdResult.wait(CACHE_CLEANUP_TIMEOUT);
        }
      }
      catch (InterruptedException e)
      {
        // Ignore
      }
      removeOldEntries();
    }
  }

  /**
   * Shutdown this thread so that it terminates asap. Note that if the thread
   * was waiting it will still proceed to the cleanup operations before
   * terminating.
   */
  public void shutdown()
  {
    isKilled = true;
    synchronized (requestIdResult)
    {
      requestIdResult.notifyAll();
    }
  }

  private void removeOldEntries()
  {
    if (logger.isDebugEnabled())
      logger.debug("Cleaning-up request result failover cache...");
    synchronized (this)
    {
      long currentTimeMillis = System.currentTimeMillis();

      // Remove expired entries from the cache (iterate over request IDs)
      for (Iterator<?> iter = requestIdResult.entrySet().iterator(); iter
          .hasNext();)
      {
        CachedResult cachedResult = (CachedResult) ((Entry<?, ?>) iter.next())
            .getValue();
        if ((currentTimeMillis > cachedResult.getExpirationDate()))
        {
          iter.remove();
          if (logger.isDebugEnabled())
            logger.debug("Removed result from failover cache: "
                + cachedResult.getResult());
        }
      }

      // Iterate over transaction IDs
      for (Iterator<?> iter = transactionIdRequestId.entrySet().iterator(); iter
          .hasNext();)
      {
        Entry<?, ?> entry = (Entry<?, ?>) iter.next();
        if (requestIdResult.get(entry.getValue()) == null)
        { // No more in the cache, the transaction has completed
          iter.remove();
          if (logger.isDebugEnabled())
            logger.debug("Removed transaction " + entry.getKey()
                + " from failover cache");
        }
      }

      // Iterate over connection IDs
      for (Iterator<?> iter = connectionIdRequestId.entrySet().iterator(); iter
          .hasNext();)
      {
        Entry<?, ?> entry = (Entry<?, ?>) iter.next();
        if (requestIdResult.get(entry.getValue()) == null)
        { // No more in the cache, the connection is closed
          iter.remove();
          if (logger.isDebugEnabled())
            logger.debug("Removed persistent connection " + entry.getKey()
                + " from failover cache");
        }
      }

    }
  }
}
