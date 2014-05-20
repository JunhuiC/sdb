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
 * Contributor(s): _________________________.
 */

package org.continuent.sequoia.common.locks;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

//import org.continuent.sequoia.common.locks.TransactionLogicalLock.WaitingListElement;
//import org.continuent.sequoia.common.locks.TransactionLogicalLock.WaitingListElement;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This class defines a <code>TransactionLogicalLock</code> which is an
 * exclusive lock that let the owner of the lock acquire several times the lock
 * (but it needs to be released only once). Acquire is not blocking but just a
 * hint whether the lock has been granted or the request has been queued.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class TransactionLogicalLock
{
  private boolean    isLocked    = false;

  /** Transaction id of the lock holder. */
  private long       locker;

  /** <code>ArrayList</code> of <code>WaitingListElement</code>. */
  private LinkedList<WaitingListElement> waitingList = new LinkedList<WaitingListElement>();

  /**
   * The element stored in the waiting list is the waiting request and the
   * transaction id of the request.
   */
  protected class WaitingListElement
  {
    /** Waiting request */
    AbstractRequest request;

    /** Transaction id of the request waiting. */
    long            transactionId;

    /**
     * Creates a new <code>WaitingListElement</code> instance.
     * 
     * @param request the waiting request.
     * @param transactionId the transaction id of the request waiting.
     */
    WaitingListElement(AbstractRequest request, long transactionId)
    {
      this.request = request;
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
     * Returns the waiting request.
     * 
     * @return a <code>Thread</code> value
     */
    public AbstractRequest getRequest()
    {
      return request;
    }

    public String toString()
    {
      return "txId : " + transactionId + " / reqId : " + request.getId();
    }

  }

  /**
   * Acquires an exclusive lock on this table. If the lock is already held by
   * the same transaction as the given request, this method is non-blocking else
   * the caller is blocked until the transaction holding the lock releases it at
   * commit/rollback time.
   * 
   * @param request request asking for the lock (timeout field is used and
   *            updated upon waiting)
   * @return boolean true is the lock has been successfully acquired, false on
   *         timeout or error
   * @see #release(long)
   */
  public boolean acquire(AbstractRequest request)
  {
    long tid = request.getTransactionId();

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
          if (!isWaiting(tid))
          { // We are not yet in the waiting list, append to the queue
            WaitingListElement wle = new WaitingListElement(request, tid);
            waitingList.addLast(wle);
          }
          return false;
        }
      }
    }
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
   * Returns <code>true</code> if the lock is owned by someone.
   * 
   * @return <code>boolean</code> value
   */
  public boolean isLocked()
  {
    return isLocked;
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
    for (Iterator<WaitingListElement> iter = waitingList.iterator(); iter.hasNext();)
    {
      WaitingListElement e = (WaitingListElement) iter.next();
      if (e.getTransactionId() == transactionId)
        return true;
    }
    return false;
  }

  /**
   * Releases the lock on this table or remove the transaction from the waiting
   * list if it was not holding the lock.
   * 
   * @param transactionId the transaction releasing the lock
   * @return true if the transaction had the lock or was in the waiting list
   * @see #acquire(AbstractRequest)
   */
  public synchronized boolean release(long transactionId)
  {
    if (!waitingList.isEmpty())
    {
      if (locker == transactionId)
      { // We were holding the lock, give it to the first in the waiting list
        locker = ((WaitingListElement) waitingList.removeFirst())
            .getTransactionId();
        return true;
      }

      // We were in the waiting list, remove ourselves from the list
      for (Iterator<WaitingListElement> iter = waitingList.iterator(); iter.hasNext();)
      {
        WaitingListElement e = (WaitingListElement) iter.next();
        if (e.getTransactionId() == transactionId)
        {
          iter.remove();
          return true;
        }
      }
      return false;
    }
    else
    {
      if (locker == transactionId)
      {
        isLocked = false;
        return true;
      }
      else
        return false;
    }
  }

  /**
   * Returns the list of requests waiting for this lock.
   * 
   * @return list of <code>WaitingListElement</code> type objects.
   */
  public List<WaitingListElement> getWaitingList()
  {
    return waitingList;
  }
}