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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.management;

import org.continuent.sequoia.common.exceptions.NotImplementedException;

/**
 * This class defines an AbstractAdminOperation that is used to account for
 * currently executing administrative operations on a virtual database. A
 * virtual database shutdown will not be allowed as long as one of such
 * operation is pending.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class AbstractAdminOperation
{
  private long            startTime;
  private long            endTime;
  protected int           operationStatus;

  /**
   * Status of an operation that has not started yet
   */
  public static final int STATUS_NOT_STARTED = 0;
  /**
   * Status of an operation that is currently executing
   */
  public static final int STATUS_EXECUTING   = 1;
  /**
   * Status of an operation that has successfully completed
   */
  public static final int STATUS_SUCCESS     = 2;
  /**
   * Status of an operation that has failed
   */
  public static final int STATUS_FAILED      = 3;

  // Object for status completion synchronization
  private final Object    completionStatus   = new Object();

  /**
   * Creates a new <code>AbstractAdminOperation</code> object by initializing
   * its start time to the current time and its status to STATUS_NOT_STARTED
   */
  public AbstractAdminOperation()
  {
    startTime = System.currentTimeMillis();
    operationStatus = STATUS_NOT_STARTED;
  }

  /**
   * Cancel the current operation.
   * 
   * @throws NotImplementedException if the operation is not supported
   */
  public abstract void cancel() throws NotImplementedException;

  /**
   * Returns a String containing a human readable description of the executing
   * operation.
   * 
   * @return operation description
   */
  public abstract String getDescription();

  /**
   * Returns the operation completion time.
   * 
   * @return Returns the endTime.
   */
  public final long getEndTime()
  {
    return endTime;
  }

  /**
   * Sets the operation end time value.
   * 
   * @param endTime end time in ms.
   */
  public final void setEndTime(long endTime)
  {
    this.endTime = endTime;
  }

  /**
   * Returns the operation starting time.
   * 
   * @return Returns the start time.
   */
  public final long getStartTime()
  {
    return startTime;
  }

  /**
   * Sets the operation starting time value.
   * 
   * @param startTime start time in ms.
   */
  public final void setStartTime(long startTime)
  {
    this.startTime = startTime;
  }

  /**
   * Return the current status of the operation.
   * 
   * @return operation status
   * @see #STATUS_NOT_STARTED
   * @see #STATUS_EXECUTING
   * @see #STATUS_SUCCESS
   * @see #STATUS_FAILED
   */
  public int getStatus()
  {
    return operationStatus;
  }

  /**
   * Notify the completion of the operation with success or not. This updates
   * the operation status and end time.
   * 
   * @param isSuccess true if the operation was successful
   */
  public void notifyCompletion(boolean isSuccess)
  {
    synchronized (completionStatus)
    {
      endTime = System.currentTimeMillis();
      if (isSuccess)
        operationStatus = STATUS_SUCCESS;
      else
        operationStatus = STATUS_FAILED;
      completionStatus.notifyAll();
    }
  }

  /**
   * Wait for the operation completion. This method blocks until the command has
   * completed (successfully or not)
   */
  public void waitForCompletion()
  {
    synchronized (completionStatus)
    {
      while ((operationStatus == STATUS_EXECUTING)
          || (operationStatus == STATUS_NOT_STARTED))
      {
        try
        {
          completionStatus.wait();
        }
        catch (InterruptedException ignore)
        {
        }
      }
    }
  }

}
