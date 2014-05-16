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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): _________________________.
 */

package org.continuent.sequoia.controller.scheduler.schema;

import java.util.Iterator;
import java.util.LinkedList;

import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * A <code>TransactionExclusiveLock</code> is an exclusive lock that let the
 * owner of the lock acquire several times the lock (but it needs to be released
 * only once). Acquire supports timeout and graceful withdrawal of timed out
 * requests.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class TransactionExclusiveLock
{
  private boolean    isLocked    = false;

  /** Transaction id of the lock holder. */
  private long       locker;

  /** <code>ArrayList</code> of <code>WaitingListElement</code>. */
  private LinkedList waitingList = new LinkedList();

  /**
   * The element stored in the waiting list is the waiting thread and the
   * transaction id of the request waiting.
   */
  private class WaitingListElement
  {
    /** Waiting thread */
    Thread thread;

    /** Transaction id of the request waiting. */
    long   transactionId;

    /**
     * Creates a new <code>WaitingListElement</code> instance.
     * 
     * @param thread the waiting thread.
     * @param transactionId the transaction id of the request waiting.
     */
    WaitingListElement(Thread thread, long transactionId)
    {
      this.thread = thread;
      this.transactionId = transactionId;
    }

    /**
     * Returns the transaction id of the request waiting.
     * 
     * @return an <code>int</code> value
     */
    public long getTransactionId()
    {
      return transactionId;
    }

    /**
     * Returns the waiting thread.
     * 
     * @return a <code>Thread</code> value
     */
    public Thread getThread()
    {
      return thread;
    }
  }

  /**
   * Acquires an exclusive lock on this table. If the lock is already held by
   * the same transaction as the given request, this method is non-blocking else
   * the caller is blocked until the transaction holding the lock releases it at
   * commit/rollback time.
   * 
   * @param request request asking for the lock (timeout field is used and
   *          updated upon waiting)
   * @return boolean true is the lock has been successfully acquired, false on
   *         timeout or error
   * @see #release()
   */
  public boolean acquire(AbstractRequest request)
  {
    long tid = request.getTransactionId();

    synchronized (Thread.currentThread())
    {
      WaitingListElement wle = null;
      synchronized (this)
      {
        if (!isLocked)
        { // Lock is free, take it
          locker = tid;
          isLocked = true;
          return true;
        }
        else
        {
          if (locker == tid)
            return true; // We already have the lock
          else
          { // Wait for the lock
            wle = new WaitingListElement(Thread.currentThread(), tid);
            waitingList.addLast(wle);
          }
        }
      }
      // At this point, we have to wait for the lock.
      try
      {
        int timeout = request.getTimeout();
        if (timeout == 0)
        {
          Thread.currentThread().wait(); // No timeout
          // Note: isLocked and locker are already set.
          return true;
        }
        else
        { // Wait with timeout
          long start = System.currentTimeMillis();
          // Convert seconds to milliseconds for wait call
          long lTimeout = timeout * 1000L;
          Thread.currentThread().wait(lTimeout);
          long end = System.currentTimeMillis();
          int remaining = (int) (lTimeout - (end - start));
          if (remaining > 0)
          { // Ok
            request.setTimeout(remaining);
            // Note: isLocked and locker are already set.
            return true;
          }
          else
          { // Too late, remove ourselves from the waiting list
            synchronized (this)
            {
              for (Iterator iter = waitingList.iterator(); iter.hasNext();)
              {
                WaitingListElement e = (WaitingListElement) iter.next();
                if (e.equals(wle))
                {
                  iter.remove();
                  return false;
                }
              }
              // Not found, we got the lock before being able to acquire the
              // lock on "this". Give the lock to the next one.
              release();
            }
            return false;
          }
        }
      }
      catch (InterruptedException ie)
      {
        synchronized (this)
        { // Something wrong happened, remove ourselves from the waiting list
          waitingList.remove(Thread.currentThread());
        }
        return false;
      }
    }
  }

  /**
   * Releases the lock on this table.
   * 
   * @see #acquire(AbstractRequest)
   */
  public synchronized void release()
  {
    if (!waitingList.isEmpty())
    {
      // Wake up the first waiting thread and update locker transaction id
      WaitingListElement e = (WaitingListElement) waitingList.removeFirst();
      Thread thread = e.getThread();
      locker = e.getTransactionId();
      synchronized (thread)
      {
        thread.notify();
        // isLocked remains true
      }
    }
    else
      isLocked = false;
  }

  /**
   * Returns <code>true</code> if the lock is owned by someone.
   * 
   * @return <code>boolean</code> value
   */
  public boolean isLocked()
  {
    return isLocked;
  }

  /**
   * Returns the transaction id of the lock owner. The return value is undefined
   * if the lock is not owned (usually it is the last owner).
   * 
   * @return int the transaction id.
   */
  public long getLocker()
  {
    return locker;
  }

  /**
   * Returns the waitingList.
   * 
   * @return an <code>LinkedList</code> of <code>WaitingListElement</code>
   */
  public LinkedList getWaitingList()
  {
    return waitingList;
  }

  /**
   * Returns <code>true</code> if the given transaction id is contained in
   * this lock waiting queue.
   * 
   * @param transactionId a transaction id
   * @return a <code>boolean</code> value
   */
  public synchronized boolean isWaiting(long transactionId)
  {
    for (Iterator iter = waitingList.iterator(); iter.hasNext();)
    {
      WaitingListElement e = (WaitingListElement) iter.next();
      if (e.getTransactionId() == transactionId)
        return true;
    }
    return false;
  }
}