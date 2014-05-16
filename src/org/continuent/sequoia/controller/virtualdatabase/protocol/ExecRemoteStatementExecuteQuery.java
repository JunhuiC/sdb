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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * Execute a remote read request. This should only happen in case of a request
 * arriving at a controller without backend enabled (or having no backend
 * capable of executing the request). In that case, the request is forwarded to
 * a remote controller using this message for a remote execution.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ExecRemoteStatementExecuteQuery extends DistributedRequest
{
  private static final long serialVersionUID = -7183844510032678987L;

  /**
   * Creates a new <code>ExecRemoteStatementExecuteQuery</code> object.
   * 
   * @param request select request to execute
   */
  public ExecRemoteStatementExecuteQuery(SelectRequest request)
  {
    super(request);
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#scheduleRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Object scheduleRequest(DistributedRequestManager drm)
      throws SQLException
  {
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest#executeScheduledRequest(org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager)
   */
  public Serializable executeScheduledRequest(DistributedRequestManager drm)
      throws SQLException
  {
    // Check if the transaction has been started
    if (!request.isAutoCommit())
    {
      long tid = request.getTransactionId();
      try
      {
        drm.getTransactionMetaData(new Long(tid));
      }
      catch (SQLException e)
      { // Transaction not started. If we start a new transaction now, it will
        // never be commited if this is a read-only transaction since the commit
        // will never get distributed and reach us (read-only transactions are
        // only commited locally). Therefore, we decide to execute this read in
        // autoCommit mode which should not be a real big issue (TODO: check
        // impact on transaction isolation).
        // Note that further write queries on that transaction will really start
        // a transaction and subsequent reads would then execute in the proper
        // transaction.
        request.setIsAutoCommit(true);
      }
    }

    try
    {
      return drm.execLocalStatementExecuteQuery((SelectRequest) request);
    }
    catch (SQLException e)
    {
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.read.sqlexception", e
              .getMessage()), e);
      return e;
    }
    catch (RuntimeException re)
    {
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.read.exception", re
              .getMessage()), re);
      return new SQLException(re.getMessage());
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "S " + request.getId() + " " + request.getTransactionId() + " "
        + request.getUniqueKey();
  }

}