/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent.
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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.jmx.mbeans;

import java.util.Hashtable;

import javax.management.openmbean.TabularData;

import org.continuent.sequoia.common.jmx.management.TransactionDataSupport;

/**
 * MBean Interface to manage the request schedulers
 * 
 * @see org.continuent.sequoia.common.jmx.JmxConstants#getAbstractSchedulerObjectName(String)
 */
public interface AbstractSchedulerControlMBean
{
  /**
   * Returns the list of active transactions. Active transactions those that had
   * a "begin" but no "commit" yet. Note that for persistent connections, a
   * "begin" is sent after each "commit" ; thus, these ids can reference
   * transactions that havn't any statement
   * 
   * @return an array of the transaction ids
   */
  long[] listActiveTransactionIds();

  /** 
   * Returns a TabularData representations of transactions.
   * 
   * @see TransactionDataSupport
   */
  TabularData getActiveTransactions() throws Exception;
  
  /**
   * Returns the list of write requests that have been scheduled for execution.
   * The list also include stored procedures (even read-only ones)
   * 
   * @return an array of scheduled write request ids, or an empty array if there
   *         are no pending write requests
   */
  long[] listPendingWriteRequestIds();

  /**
   * Returns the list of read requests that have been scheduled for execution.
   * The list does not include stored procedures (not even read-only ones)
   * 
   * @return an array of scheduled read request ids, or an empty array if there
   *         are no pending read requests
   */
  long[] listPendingReadRequestIds();

  /**
   * Returns a hashtable of all the open persistent connections (and their
   * associated login).
   * 
   * @return persistent connection hashtable
   */
  Hashtable<?, ?> listOpenPersistentConnections();

  /**
   * Returns a string containing information about the given request (if found
   * in the current scheduler)
   * 
   * @param requestId identifier of the request to dump
   * @return a String representation of the request information
   */
  String dumpRequest(long requestId);
}
