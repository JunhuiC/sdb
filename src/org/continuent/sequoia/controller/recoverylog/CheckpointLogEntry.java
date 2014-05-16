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

package org.continuent.sequoia.controller.recoverylog;

import java.io.Serializable;

/**
 * This class defines a CheckpointLogEntry used to carry information about the
 * log entry pointed by a checkpoint
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class CheckpointLogEntry implements Serializable
{
  private static final long serialVersionUID = 755365554583076662L;

  private String            sql;
  private String            vLogin;
  private String            autoConnTran;
  private long              transactionId;
  private long              requestId;

  /**
   * Creates a new <code>CheckpointLogEntry</code> object
   * 
   * @param sql sql statement
   * @param login virtual login
   * @param autoConnTran 'A', 'T' or 'C'
   * @param transactionId transaction id
   * @param requestId request id
   */
  public CheckpointLogEntry(String sql, String login, String autoConnTran,
      long transactionId, long requestId)
  {
    this.autoConnTran = autoConnTran;
    this.requestId = requestId;
    this.sql = sql;
    this.transactionId = transactionId;
    this.vLogin = login;
  }

  /**
   * Returns the autoConnTran value.
   * 
   * @return Returns the autoConnTran.
   */
  public final String getAutoConnTran()
  {
    return autoConnTran;
  }

  /**
   * Returns the requestId value.
   * 
   * @return Returns the requestId.
   */
  public final long getRequestId()
  {
    return requestId;
  }

  /**
   * Returns the sql value.
   * 
   * @return Returns the sql.
   */
  public final String getSql()
  {
    return sql;
  }

  /**
   * Returns the transactionId value.
   * 
   * @return Returns the transactionId.
   */
  public final long getTransactionId()
  {
    return transactionId;
  }

  /**
   * Returns the vLogin value.
   * 
   * @return Returns the vLogin.
   */
  public final String getVLogin()
  {
    return vLogin;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Checkpoint log entry (" + autoConnTran + ") transactionId:"
        + transactionId + " requestId:" + requestId + " vlogin:" + vLogin
        + " sql:" + sql;
  }

}
