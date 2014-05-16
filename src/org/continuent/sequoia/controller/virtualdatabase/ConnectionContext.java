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

package org.continuent.sequoia.controller.virtualdatabase;

import java.io.Serializable;

/**
 * This class defines a ConnectionContext to carry information about the
 * connection to gather metadata.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ConnectionContext implements Serializable
{
  private static final long serialVersionUID = 4211807376311527783L;

  private String            login;
  private boolean           isPersistentConnection;
  private long              persistentConnectionId;
  private long              currentTid;
  private boolean           isStartedTransaction;

  /**
   * Creates a new <code>ConnectionContext</code> object
   * 
   * @param login login of the connection
   * @param isTransaction true if the connection is not in autocommit
   * @param transactionId transaction id (if isTransaction is true)
   * @param isPersistent true if the connection is persistent
   * @param persistentId persistent connection id if the connection is
   *          persistent
   */
  public ConnectionContext(String login, boolean isTransaction,
      long transactionId, boolean isPersistent, long persistentId)
  {
    this.login = login;
    isStartedTransaction = isTransaction;
    currentTid = transactionId;
    isPersistentConnection = isPersistent;
    persistentConnectionId = persistentId;
  }

  /**
   * Returns the transaction id (only makes sense if isStartedTransaction is
   * true).
   * 
   * @return Returns the transaction id.
   */
  public final long getTransactionId()
  {
    return currentTid;
  }

  /**
   * Returns the login value.
   * 
   * @return Returns the login.
   */
  public final String getLogin()
  {
    return login;
  }

  /**
   * Returns the persistentConnectionId value.
   * 
   * @return Returns the persistentConnectionId.
   */
  public final long getPersistentConnectionId()
  {
    return persistentConnectionId;
  }

  /**
   * Returns the isPersistentConnection value.
   * 
   * @return Returns the isPersistentConnection.
   */
  public final boolean isPersistentConnection()
  {
    return isPersistentConnection;
  }

  /**
   * Returns the isStartedTransaction value.
   * 
   * @return Returns the isStartedTransaction.
   */
  public final boolean isStartedTransaction()
  {
    return isStartedTransaction;
  }

  /**
   * Sets the isStartedTransaction value.
   * 
   * @param isStartedTransaction The isStartedTransaction to set.
   */
  public final void setStartedTransaction(boolean isStartedTransaction)
  {
    this.isStartedTransaction = isStartedTransaction;
  }

}
