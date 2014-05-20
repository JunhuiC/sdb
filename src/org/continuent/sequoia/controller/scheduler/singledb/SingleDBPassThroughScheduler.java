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
 * Contributor(s): Jean-Bernard van Zuylen.
 */

package org.continuent.sequoia.controller.scheduler.singledb;

import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.RollbackException;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.ParsingGranularities;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;

/**
 * This scheduler provides scheduling for a single backend instance. It is
 * really trivial and does not yet support distribution.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public class SingleDBPassThroughScheduler extends AbstractScheduler
{

  //
  // Constructor
  //

  /**
   * Creates a new Single Database Scheduler
   */
  @SuppressWarnings("deprecation")
public SingleDBPassThroughScheduler()
  {
    super(RAIDbLevels.SingleDB, ParsingGranularities.NO_PARSING);
  }

  //
  // Request Handling
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedReadRequest(SelectRequest)
   */
  public final void scheduleNonSuspendedReadRequest(SelectRequest request)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#readCompletedNotify(SelectRequest)
   */
  public final void readCompletedNotify(SelectRequest request)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleWriteRequest(AbstractWriteRequest)
   */
  public final void scheduleNonSuspendedWriteRequest(
      AbstractWriteRequest request)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyWriteCompleted(AbstractWriteRequest)
   */
  public final void notifyWriteCompleted(AbstractWriteRequest request)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#scheduleNonSuspendedStoredProcedure(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final void scheduleNonSuspendedStoredProcedure(StoredProcedure proc)
      throws SQLException, RollbackException
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#notifyStoredProcedureCompleted(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public final void notifyStoredProcedureCompleted(StoredProcedure proc)
  {
  }

  //
  // Transaction Management
  //

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#commitTransaction(long)
   */
  protected final void commitTransaction(long transactionId)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#rollbackTransaction(long)
   */
  protected final void rollbackTransaction(long transactionId)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#rollbackTransaction(long,
   *      String)
   */
  protected final void rollbackTransaction(long transactionId,
      String savepointName)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#setSavepointTransaction(long,
   *      String)
   */
  protected final void setSavepointTransaction(long transactionId, String name)
  {
  }

  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#releaseSavepointTransaction(long,
   *      String)
   */
  protected final void releaseSavepointTransaction(long transactionId,
      String name)
  {
  }

  //
  // Debug/Monitoring
  //
  /**
   * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler#getXmlImpl()
   */
  public String getXmlImpl()
  {
    return "<" + DatabasesXmlTags.ELT_SingleDBScheduler + " "
        + DatabasesXmlTags.ATT_level + "=\"" + DatabasesXmlTags.VAL_passThrough
        + "\"/>";
  }
}
