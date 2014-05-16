/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
 * Contact: sequoia@continuent.org
 * 
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
 * Initial developer(s): Olivier Fambon.
 * Contributor(s): Damian Arregui.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.IOException;
import java.io.Serializable;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.controller.backup.Backuper;
import org.continuent.sequoia.controller.backup.DumpTransferInfo;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This message is used to prepare the sending of a dump to a remote
 * controller's vdb backup manager. This is used as an integrated remote-copy
 * facility in the occurence of restore, e.g. after a rebuild of the remote
 * recovery log from a live one. Upon reception of this message, the remote
 * backup manager initiates a transfer onto the sending controller's backuper.
 * 
 * @author <a href="mailto:Olivier.Fambon@emicnetworks.com>Olivier Fambon </a>
 * @author <a href="mailto:Damian.Arregui@continuent.com>Damian Arregui </a> *
 * @version 1.0
 */
public class InitiateDumpCopy extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = 4674422809133556752L;

  private DumpInfo          dumpInfo;
  private DumpTransferInfo  dumpTransferInfo;

  // private int timeout;

  /**
   * Creates a new <code>ReplicateLogEntries</code> message
   * 
   * @param dumpInfo The DumpInfo object returned by the Backuper.
   * @param dumpTransferInfo The dump transfer information
   */
  public InitiateDumpCopy(DumpInfo dumpInfo, DumpTransferInfo dumpTransferInfo)
  {
    this.dumpInfo = dumpInfo;
    this.dumpTransferInfo = dumpTransferInfo;
  }

  /**
   * Returns the dump info (name, checkpoint, etc).
   * 
   * @return Returns the dump info (on the sending side).
   */
  public DumpInfo getDumpInfo()
  {
    return dumpInfo;
  }

  /**
   * Returns the dump checkpoint name (global).
   * 
   * @return Returns the dump CheckpointName.
   */
  public String getDumpCheckpointName()
  {
    return dumpInfo.getCheckpointName();
  }

  /**
   * Return the dump name (sending side).
   * 
   * @return the dump name (sending side).
   */
  public String getDumpName()
  {
    return dumpInfo.getDumpName();
  }

  /**
   * Returns the session key to be used to authenticate the destination on the
   * sender.
   * 
   * @return the session key
   */
  public DumpTransferInfo getDumpTransferInfo()
  {
    return dumpTransferInfo;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    try
    {
      // check that no dump already exists locally
      if (!dvdb.isDumpNameAvailable(dumpInfo.getDumpName()))
        return new ControllerException("Remote dump '" + dumpInfo.getDumpName()
            + "' already exists.");

      // hand-off copy to backuper, if a copy is required
      if (dumpTransferInfo != null)
      {
        Backuper backuper = dvdb.getRequestManager().getBackupManager()
            .getBackuperByFormat(dumpInfo.getDumpFormat());

        if (backuper == null)
          return new ControllerException("No backuper can handle format '"
              + dumpInfo.getDumpFormat() + "' on this controller");

        backuper.fetchDump(dumpTransferInfo, dumpInfo.getDumpPath(), dumpInfo
            .getDumpName());
      }

      // update local recovery log dump tables
      dvdb.getRecoveryLog().setDumpInfo(dumpInfo);
    }
    catch (IOException e)
    {
      return new ControllerException(e);
    }
    catch (BackupException e)
    {
      return new ControllerException(e);
    }
    catch (VirtualDatabaseException e)
    {
      return new ControllerException(e);
    }
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    return null;
  }

}