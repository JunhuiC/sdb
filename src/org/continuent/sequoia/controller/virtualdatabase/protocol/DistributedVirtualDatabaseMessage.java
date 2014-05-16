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

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * This interface defines a DistributedVirtualDatabaseMessage
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class DistributedVirtualDatabaseMessage implements Serializable
{

  /**
   * Single threaded part of the handler that will execute in mutual execution
   * in the total order delivery thread of the group communication.
   * <p>
   * NEVER BLOCK IN THIS HANDLER !!!
   * 
   * @param dvdb the local instance of the distributed virtual database
   * @param sender the message sender
   * @return an optional value to be passed to the multithreaded handler
   */
  public abstract Object handleMessageSingleThreaded(
      DistributedVirtualDatabase dvdb, Member sender);

  /**
   * Multi-threaded part of the message handler.
   * 
   * @param dvdb the local instance of the distributed virtual database
   * @param sender the message sender
   * @param handleMessageSingleThreadedResult value returned by the single
   *          threaded handler
   * @return a Serializable object to be returned to the sender
   */
  public abstract Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult);

  /**
   * Cancel the message in a "best effort"-basis. Should try to undo message
   * side-effects and clean-up reources.
   * 
   * @param dvdb the local instance of the distributed virtual database
   */
  public void cancel(DistributedVirtualDatabase dvdb)
  {
    // Does nothing by default.
  }

}
