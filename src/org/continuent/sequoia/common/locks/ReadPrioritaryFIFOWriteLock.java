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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.locks;

import java.util.ArrayList;

/**
 * Reader/Writer lock with write priority.
 * <p>
 * If a reader holds the lock, all incoming readers are allowed to acquire the
 * lock until a write arrives. A writer must wait for all current readers to
 * release the lock before acquiring it. <br>
 * When a writer has the lock, everybody else is blocked. <br>
 * When a writer release the lock, there is a writer priority so if another
 * writer is waiting it will have the lock even if it arrived later than
 * readers. Writers are prioritary against readers but all writers get the lock
 * in a FIFO order.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ReadPrioritaryFIFOWriteLock
{
  /** Threads executing read. */
  private int       activeReaders;

  /** Is there an active writer? */
  private boolean   activeWriter;

  /** Threads not yet in read. */
  private int       waitingReaders;

  /** Threads not yet in write. */
  private int       waitingWriters;

  private Object    readSync;
  private ArrayList writeWaitingQueue;

  /**
   * Creates a new <code>ReadPrioritaryFIFOWriteLock</code> instance.
   */
  public ReadPrioritaryFIFOWriteLock()
  {
    activeReaders = 0;
    activeWriter = false;
    waitingReaders = 0;
    waitingWriters = 0;
    readSync = new Object();
    writeWaitingQueue = new ArrayList();
  }

  /**
   * Acquires the lock for a read.
   * 
   * @throws InterruptedException if wait failed (the lock should remain in a
   *           correct state)
   */
  public void acquireRead() throws InterruptedException
  {
    synchronized (this)
    {
      if ((waitingWriters == 0) && !activeWriter)
      { // No active or pending writer
        activeReaders++;
        return;
      }
    }

    // Beyond this point, we have to wait
    synchronized (readSync)
    {
      // It becomes a little bit tricky here but a writer could have
      // released the lock between the first test at the beginning of
      // this function and this point. Therefore we have to recheck if
      // the lock is not completely idle else we'll never wake up !
      synchronized (this)
      {
        if (!activeWriter)
        { // No active or pending writer
          activeReaders++;
          return;
        }
      }

      waitingReaders++;
      try
      {
        readSync.wait();
      }
      catch (InterruptedException ie)
      {
        waitingReaders--; // roll back state
        throw ie;
      }
      waitingReaders--;
    }
    synchronized (this)
    {
      activeReaders++;
    }
  }

  /**
   * Releases a lock previously acquired for reading.
   */
  public synchronized void releaseRead()
  {
    activeReaders--;
    if ((activeReaders == 0) && (waitingWriters > 0))
    { // Wake up first waiting write if any
      Object thread = writeWaitingQueue.remove(0);
      synchronized (thread)
      {
        thread.notify();
        activeWriter = true;
        waitingWriters--;
      }
    }
  }

  /**
   * Acquires the lock for a write.
   * 
   * @throws InterruptedException if wait failed (the lock should remain in a
   *           correct state)
   */
  public void acquireWrite() throws InterruptedException
  {
    synchronized (Thread.currentThread())
    {
      synchronized (this)
      {
        if ((activeReaders == 0) && !activeWriter)
        {
          activeWriter = true;
          return;
        }
        else
        {
          waitingWriters++;
          writeWaitingQueue.add(Thread.currentThread());
        }
      }
      try
      {
        Thread.currentThread().wait();
      }
      catch (InterruptedException ie)
      {
        releaseWrite();
        throw ie;
      }
    }
  }

  /**
   * Releases a lock previously acquired for writing.
   */
  public void releaseWrite()
  {
    // Writer priority
    synchronized (this)
    {
      activeWriter = false;
      if (waitingWriters > 0)
      { // Wake up first waiting write if any
        Object thread = writeWaitingQueue.remove(0);
        synchronized (thread)
        {
          thread.notify();
          activeWriter = true;
          waitingWriters--;
        }
        return;
      }
    }

    // Wake up readers
    //
    // Note that we cannot do the following inside synchronized(this) because
    // acquireRead() takes synchronized(readSync) first and then
    // synchronized(this).
    synchronized (readSync)
    {
      if (waitingReaders > 0)
        readSync.notifyAll();
    }
  }

  /**
   * Tests if the lock is currently held by at least one reader.
   * 
   * @return <code>true</code> if the lock is held by a reader
   */
  public final synchronized boolean isReadLocked()
  {
    return activeReaders > 0;
  }

  /**
   * Tests if the lock is currently held by a writer.
   * 
   * @return <code>true</code> if the lock is held by a writer
   */
  public final synchronized boolean isWriteLocked()
  {
    return activeWriter;
  }
}