/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Initial developer(s): Jean-Bernard van Zuylen.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;

/**
 * Execute a distributed set savepoint
 * 
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class DistributedSetSavepoint extends DistributedTransactionMarker
{
  private static final long serialVersionUID = 1429582815473734482L;

  // Login that sets the savepoint. This is used in case the remote
  // controller has to lazily start the transaction.
  private String            login;

  private String            savepointName;

  /**
   * Creates a new <code>SetSavepoint</code> message.
   * 
   * @param login login that sets the savepoint
   * @param transactionId id of the transaction
   * @param savepointName the savepoint name
   */
  public DistributedSetSavepoint(String login, long transactionId,
      String savepointName)
  {
    super(transactionId);
    this.login = login;
    this.savepointName = savepointName;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedTransactionMarker#scheduleCommand(DistributedRequestManager)
   */
  public Object scheduleCommand(DistributedRequestManager drm)
      throws SQLException
  {
    LinkedList<Object> totalOrderQueue = drm.getVirtualDatabase().getTotalOrderQueue();
    if (totalOrderQueue != null)
    {
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.addLast(this);
      }
    }
    return this;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedTransactionMarker#executeCommand(DistributedRequestManager)
   */
  public Serializable executeCommand(DistributedRequestManager drm)
      throws SQLException
  {
    boolean hasBeenScheduled = false;

    // Let's find the transaction marker since it will be used even for
    // logging purposes
    Long tid = new Long(transactionId);
    TransactionMetaData tm;
    try
    {
      tm = drm.getTransactionMetaData(tid);
    }
    catch (SQLException e)
    {
      // Lazily start the transaction using a fake request
      FakeRequest fr = new FakeRequest();
      fr.setIsAutoCommit(false);
      fr.setTransactionId(transactionId);
      fr.setLogin(login);
      drm.lazyTransactionStart(fr);
      tm = drm.getTransactionMetaData(tid);
    }

    try
    {
      // Wait for the scheduler to give us the authorization to execute
      drm.getScheduler().setSavepoint(tm, savepointName, this);
      hasBeenScheduled = true;

      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get("transaction.setsavepoint", new String[]{
                savepointName, String.valueOf(transactionId)}));

      // Send to load balancer
      drm.getLoadBalancer().setSavepoint(tm, savepointName);

      // Update recovery log
      drm.getRecoveryLog().logRequestCompletion(tm.getLogId(), true, 0);

      // Notify scheduler for completion
      drm.getScheduler().savepointCompleted(transactionId);
    }
    catch (NoMoreBackendException e)
    {
      if (drm.getLogger().isDebugEnabled())
        drm.getLogger().debug(
            Translate.get(
                "virtualdatabase.distributed.setsavepoint.logging.only",
                new String[]{savepointName, String.valueOf(transactionId)}));

      addSavepointFailureOnAllBackends(drm, hasBeenScheduled, tm);
      throw e;
    }
    catch (SQLException e)
    {
      addSavepointFailureOnAllBackends(drm, hasBeenScheduled, tm);
      drm.getLogger().warn(
          Translate
              .get("virtualdatabase.distributed.setsavepoint.sqlexception"), e);
      return e;
    }
    catch (RuntimeException re)
    {
      addSavepointFailureOnAllBackends(drm, hasBeenScheduled, tm);
      drm.getLogger().warn(
          Translate.get("virtualdatabase.distributed.setsavepoint.exception"),
          re);
      throw new SQLException(re.getMessage());
    }
    catch (AllBackendsFailedException e)
    {
      addSavepointFailureOnAllBackends(drm, hasBeenScheduled, tm);
      if (drm.getLogger().isDebugEnabled())
        drm
            .getLogger()
            .debug(
                Translate
                    .get(
                        "virtualdatabase.distributed.setsavepoint.all.backends.locally.failed",
                        new String[]{savepointName,
                            String.valueOf(transactionId)}));
      return e;
    }

    // Add savepoint name to list of savepoints for this transaction
    drm.addSavepoint(tid, savepointName);
    return Boolean.TRUE;
  }

  private void addSavepointFailureOnAllBackends(DistributedRequestManager drm,
      boolean hasBeenScheduled, TransactionMetaData tm)
  {
    AbstractRequest request = new UnknownWriteRequest("savepoint "
        + savepointName, false, 0, "\n");
    request.setTransactionId(transactionId);
    request.setLogId(tm.getLogId());
    drm.addFailedOnAllBackends(request, hasBeenScheduled);
  }

  /**
   * Returns the savepointName value.
   * 
   * @return Returns the savepointName.
   */
  public String getSavepointName()
  {
    return savepointName;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object obj)
  {
    if (super.equals(obj))
      return savepointName.equals(((DistributedSetSavepoint) obj)
          .getSavepointName());
    else
      return false;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "Set savepoint " + savepointName + " to transaction "
        + transactionId;
  }

  // This is used in case the remote
  // controller has to lazily start the transaction.
  private class FakeRequest extends AbstractRequest
  {
    private static final long serialVersionUID = 5694250075466219713L;

    /**
     * Creates a new <code>FakeRequest</code> object with a null SQL
     * statement.
     */
    public FakeRequest()
    {
      super(null, false, 0, null, 0);
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersAggregateList()
     */
    public boolean altersAggregateList()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseCatalog()
     */
    public boolean altersDatabaseCatalog()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersDatabaseSchema()
     */
    public boolean altersDatabaseSchema()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersMetadataCache()
     */
    public boolean altersMetadataCache()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersQueryResultCache()
     */
    public boolean altersQueryResultCache()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersStoredProcedureList()
     */
    public boolean altersStoredProcedureList()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUserDefinedTypes()
     */
    public boolean altersUserDefinedTypes()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersUsers()
     */
    public boolean altersUsers()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#altersSomething()
     */
    public boolean altersSomething()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#cloneParsing(org.continuent.sequoia.controller.requests.AbstractRequest)
     */
    public void cloneParsing(AbstractRequest request)
    {
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#needsMacroProcessing()
     */
    public boolean needsMacroProcessing()
    {
      return false;
    }

    /**
     * @see org.continuent.sequoia.controller.requests.AbstractRequest#parse(org.continuent.sequoia.common.sql.schema.DatabaseSchema,
     *      int, boolean)
     */
    public void parse(DatabaseSchema schema, int granularity,
        boolean isCaseSensitive) throws SQLException
    {
    }

  }

}
