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
 * Contributor(s): Peter Royal.
 */

package org.continuent.sequoia.controller.requestmanager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.continuent.sequoia.common.locks.TransactionLogicalLock;
import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * This class carry transaction metadata including the transaction state.
 * <p>
 * Metadata include a transaction id, a login and a timeout. The state is used
 * to track if a transaction is read-only or read-write and what kind of write
 * accesses have been made in the transaction in case invalidations are needed
 * at commit or rollback time.
 * <p>
 * The type is made serializable only for unlog messages that have to be sent
 * between controllers.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 2.2
 */
public class TransactionMetaData implements Serializable
{
  private static final long serialVersionUID          = -5251053474824677394L;

  private final long        transactionId;
  private long              timeout;
  private long              timestamp;
  private String            login;
  private final boolean     isPersistentConnection;
  private final long        persistentConnectionId;
  /** Recovery log id of the last commit/rollback operation on this transaction */
  private transient long    logId;
  /** Map of Lists of locksMap taken by this task, indexed by backend */
  private transient Map     locksMap                  = new HashMap();
  private transient boolean altersAggregateList       = false;
  private transient boolean altersDatabaseCatalog     = false;
  private transient boolean altersDatabaseSchema      = false;
  private transient boolean altersMetadataCache       = false;
  private transient boolean altersQueryResultCache    = false;
  private transient boolean altersSomething           = false;
  private transient boolean altersStoredProcedureList = false;
  private transient boolean altersUserDefinedTypes    = false;
  private transient boolean altersUsers               = false;
  private transient boolean isReadOnly                = true;


  /**
   * Creates a new <code>TransactionMetaData</code>.
   * 
   * @param transactionId the transaction identifier.
   * @param timeout the transaction timeout in seconds.
   * @param login the user login.
   * @param isPersistentConnection true if the transaction is started on a
   *          persistent connection
   * @param persistentConnectionId persistent connection id if the transaction
   *          must be started on a persistent connection
   */
  public TransactionMetaData(long transactionId, long timeout, String login,
      boolean isPersistentConnection, long persistentConnectionId)
  {
    this.transactionId = transactionId;
    this.timeout = timeout;
    this.login = login;
    this.isPersistentConnection = isPersistentConnection;
    this.persistentConnectionId = persistentConnectionId;
    this.timestamp = System.currentTimeMillis();
  }

  /**
   * Add a lock to the list of locks acquired by the given backend on this
   * transaction
   * 
   * @param backend backend acquiring the lock
   * @param lock the lock that has been acquired
   */
  public synchronized void addAcquiredLock(DatabaseBackend backend,
      TransactionLogicalLock lock)
  {
    LinkedList acquiredLocks = (LinkedList) locksMap.get(backend);
    if (acquiredLocks == null)
    {
      acquiredLocks = new LinkedList();
      locksMap.put(backend, acquiredLocks);
    }
    acquiredLocks.add(lock);
  }

  /**
   * Add locks to the list of locks acquired by the given backend on this
   * transaction
   * 
   * @param backend backend acquiring the locks
   * @param locks the locks that have been acquired
   */
  public synchronized void addAcquiredLocks(DatabaseBackend backend, List locks)
  {
    LinkedList acquiredLocks = (LinkedList) locksMap.get(backend);
    if (acquiredLocks == null)
    {
      acquiredLocks = new LinkedList();
      locksMap.put(backend, acquiredLocks);
    }
    acquiredLocks.addAll(locks);
  }

  /**
   * Returns the altersAggregateList value.
   * 
   * @return Returns the altersAggregateList.
   */
  public final boolean altersAggregateList()
  {
    return altersAggregateList;
  }

  /**
   * Returns the altersDatabaseCatalog value.
   * 
   * @return Returns the altersDatabaseCatalog.
   */
  public final boolean altersDatabaseCatalog()
  {
    return altersDatabaseCatalog;
  }

  /**
   * Returns the altersDatabaseSchema value.
   * 
   * @return Returns the altersDatabaseSchema.
   */
  public final boolean altersDatabaseSchema()
  {
    return altersDatabaseSchema;
  }

  /**
   * Returns the altersMetadataCache value.
   * 
   * @return Returns the altersMetadataCache.
   */
  public final boolean altersMetadataCache()
  {
    return altersMetadataCache;
  }

  /**
   * Returns the altersQueryResultCache value.
   * 
   * @return Returns the altersQueryResultCache.
   */
  public final boolean altersQueryResultCache()
  {
    return altersQueryResultCache;
  }

  /**
   * Returns the altersSomething value.
   * 
   * @return Returns the altersSomething.
   */
  public final boolean altersSomething()
  {
    return altersSomething;
  }

  /**
   * Returns the altersStoredProcedureList value.
   * 
   * @return Returns the altersStoredProcedureList.
   */
  public final boolean altersStoredProcedureList()
  {
    return altersStoredProcedureList;
  }

  /**
   * Returns the altersUserDefinedTypes value.
   * 
   * @return Returns the altersUserDefinedTypes.
   */
  public final boolean altersUserDefinedTypes()
  {
    return altersUserDefinedTypes;
  }

  /**
   * Returns the altersUsers value.
   * 
   * @return Returns the altersUsers.
   */
  public final boolean altersUsers()
  {
    return altersUsers;
  }

  /**
   * Return the list of locks acquired by the given backend for this transaction
   * or null if no lock has been acquired so far.
   * 
   * @param backend backend for which we want to retrieve to set of locks
   * @return the list of acquired locks for the given backend
   */
  public List getAcquiredLocks(DatabaseBackend backend)
  {
    return (List) locksMap.get(backend);
  }

  /**
   * Returns the logId value.
   * 
   * @return Returns the logId.
   */
  public final long getLogId()
  {
    return logId;
  }

  /**
   * Sets the logId value.
   * 
   * @param logId The logId to set.
   */
  public final void setLogId(long logId)
  {
    this.logId = logId;
  }

  /**
   * Gets creation's timestamp of this TransactionMetaData.
   * 
   * @return the timestamp corresponding to the creation of this TransactionMetaData.
   */
  public long getTimestamp()
  {
    return timestamp;
  }

  /**
   * Returns the login.
   * 
   * @return String
   */
  public String getLogin()
  {
    return login;
  }

  /**
   * Returns the persistent connection id (only meaningful is
   * isPersistentConnection is true).
   * 
   * @return Returns the persistent connection id.
   * @see #isPersistentConnection()
   */
  public final long getPersistentConnectionId()
  {
    return persistentConnectionId;
  }

  /**
   * Returns the timeout.
   * 
   * @return long
   */
  public long getTimeout()
  {
    return timeout;
  }

  /**
   * Returns the transactionId.
   * 
   * @return int
   */
  public long getTransactionId()
  {
    return transactionId;
  }

  /**
   * Returns true if at least one backend is holding locks for this
   * transactions.
   * 
   * @return true if a backend has locks for the transactions
   */
  public boolean isLockedByBackends()
  {
    return !locksMap.isEmpty();
  }

  /**
   * Returns true if the transaction executes on a persistent connection
   * (retrieve the value using getPersistentConnectionId).
   * 
   * @return Returns true if transaction uses a persistent connection.
   * @see #getPersistentConnectionId()
   */
  public final boolean isPersistentConnection()
  {
    return isPersistentConnection;
  }

  /**
   * Returns true if the transaction is read-only (true by default).
   * 
   * @return Returns the isReadOnly.
   */
  public final boolean isReadOnly()
  {
    return isReadOnly;
  }

  /**
   * Remove and return the lock list belonging to a backend.
   * 
   * @param backend backend which locks should be removed
   * @return the lock list or null if no lock were found for this backend
   */
  public List removeBackendLocks(DatabaseBackend backend)
  {
    return (List) locksMap.remove(backend);
  }

  /**
   * Sets the altersAggregateList value.
   * 
   * @param altersAggregateList The altersAggregateList to set.
   */
  public final void setAltersAggregateList(boolean altersAggregateList)
  {
    this.altersAggregateList = altersAggregateList;
  }

  /**
   * Sets the altersDatabaseCatalog value.
   * 
   * @param altersDatabaseCatalog The altersDatabaseCatalog to set.
   */
  public final void setAltersDatabaseCatalog(boolean altersDatabaseCatalog)
  {
    this.altersDatabaseCatalog = altersDatabaseCatalog;
  }

  /**
   * Sets the altersDatabaseSchema value.
   * 
   * @param altersDatabaseSchema The altersDatabaseSchema to set.
   */
  public final void setAltersDatabaseSchema(boolean altersDatabaseSchema)
  {
    this.altersDatabaseSchema = altersDatabaseSchema;
  }

  /**
   * Sets the altersMetadataCache value.
   * 
   * @param altersMetadataCache The altersMetadataCache to set.
   */
  public final void setAltersMetadataCache(boolean altersMetadataCache)
  {
    this.altersMetadataCache = altersMetadataCache;
  }

  /**
   * Sets the altersQueryResultCache value.
   * 
   * @param altersQueryResultCache The altersQueryResultCache to set.
   */
  public final void setAltersQueryResultCache(boolean altersQueryResultCache)
  {
    this.altersQueryResultCache = altersQueryResultCache;
  }

  /**
   * Sets the altersSomething value.
   * 
   * @param altersSomething The altersSomething to set.
   */
  public final void setAltersSomething(boolean altersSomething)
  {
    this.altersSomething = altersSomething;
  }

  /**
   * Sets the altersStoredProcedureList value.
   * 
   * @param altersStoredProcedureList The altersStoredProcedureList to set.
   */
  public final void setAltersStoredProcedureList(
      boolean altersStoredProcedureList)
  {
    this.altersStoredProcedureList = altersStoredProcedureList;
  }

  /**
   * Sets the altersUserDefinedTypes value.
   * 
   * @param altersUserDefinedTypes The altersUserDefinedTypes to set.
   */
  public final void setAltersUserDefinedTypes(boolean altersUserDefinedTypes)
  {
    this.altersUserDefinedTypes = altersUserDefinedTypes;
  }

  /**
   * Sets the altersUsers value.
   * 
   * @param altersUsers The altersUsers to set.
   */
  public final void setAltersUsers(boolean altersUsers)
  {
    this.altersUsers = altersUsers;
  }

  /**
   * Sets the isReadOnly value.
   * 
   * @param isReadOnly The isReadOnly to set.
   */
  public final void setReadOnly(boolean isReadOnly)
  {
    this.isReadOnly = isReadOnly;
  }

  /**
   * Sets the login.
   * 
   * @param login the login to set.
   */
  public void setLogin(String login)
  {
    this.login = login;
  }

  /**
   * Sets the timeout.
   * 
   * @param timeout the timeout to set.
   */
  public void setTimeout(long timeout)
  {
    this.timeout = timeout;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj)
  {
    if (obj instanceof TransactionMetaData)
    {
      TransactionMetaData tm = (TransactionMetaData) obj;
      return tm.getTransactionId() == transactionId;
    }
    return false;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return (int) transactionId;
  }
}
