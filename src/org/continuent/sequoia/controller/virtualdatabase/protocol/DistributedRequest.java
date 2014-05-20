/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): Damian Arregui.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseStartingException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * <code>DistributedRequest</code> is an abstract class that defines the
 * interface for distributed execution of a request (horizontal scalability).
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui </a>
 * @version 1.0
 */
public abstract class DistributedRequest
    extends DistributedVirtualDatabaseMessage
{
  /**
	 * 
	 */
	private static final long serialVersionUID = 5341860189747169818L;
protected AbstractRequest request;

  /**
   * Builds a new <code>DistributedRequest</code> object.
   * 
   * @param request request to execute
   */
  public DistributedRequest(AbstractRequest request)
  {
    this.request = request;
  }

  /**
   * Returns the request value.
   * 
   * @return Returns the request.
   */
  public final AbstractRequest getRequest()
  {
    return request;
  }

  /**
   * Schedule the request. This method blocks until the request is scheduled.
   * 
   * @param drm a distributed request manager
   * @return the object inserted in the total order queue
   * @throws SQLException if an error occurs.
   */
  public abstract Object scheduleRequest(DistributedRequestManager drm)
      throws SQLException;

  /**
   * Code to be executed by the distributed request manager receiving the
   * request.
   * 
   * @param drm a distributed request manager
   * @return an Object to be sent back to the caller
   * @throws SQLException if an error occurs.
   */
  public abstract Serializable executeScheduledRequest(
      DistributedRequestManager drm) throws SQLException;

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    if (!dvdb.isVirtualDatabaseStarted())
    {
      request = null; // prevent further logging
      return new VirtualDatabaseStartingException();
    }

    Trace logger = dvdb.getLogger();

    if (logger.isDebugEnabled() && (getRequest() != null))
      logger.debug(dvdb.getControllerName()
          + ": Scheduling distributedRequest " + getRequest().getId()
          + " from " + sender);

    // Take current time to profile query execution time
    if (request != null)
      request.setStartTime(System.currentTimeMillis());

    // Distributed request logger
    Trace distributedRequestLogger = dvdb.getDistributedRequestLogger();
    if (distributedRequestLogger.isInfoEnabled())
      distributedRequestLogger.info(toString());

    try
    {
      return scheduleRequest((DistributedRequestManager) dvdb
          .getRequestManager());
    }
    catch (SQLException e)
    {
      return e;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    Trace logger = dvdb.getLogger();

    if (handleMessageSingleThreadedResult instanceof Exception)
    {
      // The scheduling part more likely failed, report the exception back
      if (logger.isInfoEnabled() && (getRequest() != null))
        logger.info("Distributed query "
            + getRequest().getSqlShortForm(dvdb.getSqlShortFormLength())
            + " failed on this controller",
            (Throwable) handleMessageSingleThreadedResult);
      return (Serializable) handleMessageSingleThreadedResult;
    }

    if ((logger.isDebugEnabled()) && (getRequest() != null))
      logger.debug(dvdb.getControllerName() + ": Executing distributedRequest "
          + getRequest().getId() + " from " + sender);
    try
    {
      return executeScheduledRequest((DistributedRequestManager) dvdb
          .getRequestManager());
    }
    catch (SQLException e)
    {
      return e;
    }
  }
}