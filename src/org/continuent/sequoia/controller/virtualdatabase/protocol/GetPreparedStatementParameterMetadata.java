/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2008 Continuent, Inc.
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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a GetPreparedStatementParameterMetadata used to execute
 * remotely the getPreparedStatementGetParameterMetaData function
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class GetPreparedStatementParameterMetadata
    extends DistributedVirtualDatabaseMessage
{

  private static final long serialVersionUID = 3063217926401800667L;
  private AbstractRequest request;

  /**
   * Creates a new <code>GetPreparedStatementParameterMetadata</code> object
   * 
   * @param request the request containing the statement to get parameter
   *          metadata for.
   */
  public GetPreparedStatementParameterMetadata(AbstractRequest request)
  {
    this.request = request;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    try
    {
      return dvdb.getRequestManager().getPreparedStatementGetParameterMetaData(
          request);
    }
    catch (SQLException e)
    {
      return new ControllerException(e);
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
    return (Serializable) handleMessageSingleThreadedResult;
  }

}
