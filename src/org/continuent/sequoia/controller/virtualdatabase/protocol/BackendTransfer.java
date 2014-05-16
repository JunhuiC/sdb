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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.util.LinkedList;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a BackendTransfer message used to transfer the backend
 * information and re-enable the backend on the remote controller (checkpoint
 * transfer have already been taken care of).
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class BackendTransfer extends DistributedVirtualDatabaseMessage
{
  private static final long    serialVersionUID = 520265407391630486L;

  private BackendInfo          info;
  private String               controllerDest;
  private String               checkpointName;

  private transient LinkedList totalOrderQueue;

  /**
   * Creates a new <code>BackendTransfer</code> object
   * 
   * @param info the info on the backend to transfer
   * @param controllerDest the JMX name of the target controller
   * @param checkpointName the name of the ckeckpoint from which to restore
   */
  public BackendTransfer(String controllerDest, String checkpointName,
      BackendInfo info)
  {
    this.info = info;
    this.controllerDest = controllerDest;
    this.checkpointName = checkpointName;
  }

  /**
   * Returns the controllerDest value.
   * 
   * @return Returns the controllerDest.
   */
  public String getControllerDest()
  {
    return controllerDest;
  }

  /**
   * Returns the info value.
   * 
   * @return Returns the info.
   */
  public BackendInfo getInfo()
  {
    return info;
  }

  /**
   * Returns the checkpointName value.
   * 
   * @return Returns the checkpointName.
   */
  public String getCheckpointName()
  {
    return checkpointName;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    totalOrderQueue = dvdb.getTotalOrderQueue();
    if (totalOrderQueue == null)
      return new VirtualDatabaseException(Translate
          .get("virtualdatabase.no.total.order.queue", dvdb.getVirtualDatabaseName()));

    synchronized (totalOrderQueue)
    {
      totalOrderQueue.addLast(this);
      return this;
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

    // Wait for our turn to execute
    if (!dvdb.waitForTotalOrder(handleMessageSingleThreadedResult, false))
      logger
          .error("BackendTransfer was not found in total order queue, posting out of order ("
              + checkpointName + ")");
    else
      synchronized (totalOrderQueue)
      {
        totalOrderQueue.removeFirst();
        totalOrderQueue.notifyAll();
      }

    if (logger.isInfoEnabled())
      logger.info(dvdb.getControllerName()
          + ": Received transfer command. Checkpoint: " + getCheckpointName());
    DatabaseBackend backend = new DatabaseBackend(info);

    try
    {
      dvdb.addBackend(backend);
    }
    catch (VirtualDatabaseException e)
    {
      // No cleanup expected there (if addBackend() behaves). The backend does
      // not exist here, and still exists (though disabled) on the originating
      // vdb peer (controller). Just return the exception.
      return e;
    }

    try
    {
      if (logger.isInfoEnabled())
        logger.info(dvdb.getControllerName() + ": Enable backend from "
            + checkpointName);
      dvdb.enableBackendFromCheckpoint(backend.getName(), getCheckpointName(), true);
    }
    catch (VirtualDatabaseException e)
    {
      // The backend was added, but could not be enabled. Remove it from this
      // vdb peer, since it remains (in a disabled state) on the originating
      // peer.
      cleanup(dvdb, backend);
      return e;
    }
    return Boolean.TRUE;
  }

  void cleanup(DistributedVirtualDatabase dvdb, DatabaseBackend backend)
  {
    try
    {
      dvdb.removeBackend(backend);
    }
    catch (VirtualDatabaseException e)
    {
      dvdb.getLogger().error(
          "Could not cleanup vdb after transfer backend failure", e);
    }
  }
}