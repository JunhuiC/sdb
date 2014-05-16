/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Olivier Fambon.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.controller.recoverylog.events.LogEntry;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This class defines a CopyLogEntry message. It is used to send recovery log
 * entries over to a remote peer. Entries are sent one by one instead of as big
 * bunch because each log entry can potentially be a huge object, e.g. if it
 * contains a blob, and it should fit in memory.
 * 
 * @author <a href="mailto:olivier.fambon@emicnetworks.com">Olivier Fambon </a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class CopyLogEntry extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = 1L;

  private LogEntry          entry;

  /**
   * Creates a new <code>CopyLogEntry</code> object
   * 
   * @param entry the entry to be sent over to the remote peer.
   */
  public CopyLogEntry(LogEntry entry)
  {
    this.entry = entry;
  }

  /**
   * Returns the recovery LogEntry to be copied.
   * 
   * @return the entry
   */
  public LogEntry getEntry()
  {
    return entry;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    if (!dvdb.hasRecoveryLog())
    {
      dvdb.getLogger().warn(
          "Tentative CopyLogEntry on vdb with no recovery log");
      return null;
    }

    dvdb.getRequestManager().getRecoveryLog().logLogEntry(entry);
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    return null;
  }
}
