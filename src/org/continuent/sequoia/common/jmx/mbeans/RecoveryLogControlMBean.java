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

import java.util.Map;

import javax.management.openmbean.TabularData;

/**
 * MBean Interface to manage the recovery log of a virtual database.
 * 
 * @see org.continuent.sequoia.common.jmx.JmxConstants#getRecoveryLogObjectName(String)
 */
public interface RecoveryLogControlMBean
{

  /**
   * Dumps the content of the recovery log table as a TabularData from a given
   * starting index.<br />
   * The number of entries retrieved by a call to this method is given by
   * <code>getEntriesPerPage()</code>. It can not be more than that (but it
   * can be less).<br />
   * To retrieve the whole content of the recovery log from its start, the
   * client has to call this method with <code>from = 0</code> and
   * subsenquentely with the index value of the last feteched entry until the
   * tabular data returned by the method is empty. <br />
   * The returned type is TabularData for 2 reasons: 1/ it makes it compatible
   * with generic JMX clients 2/ it shields the JMX clients (including ours)
   * from the internal representation of the recovery log table
   * 
   * @param from starting index of the entries to dump (<code>0</code> to
   *          dump from the beginning of the recovery log)
   * @return a TabularData representing the content of the recovery log table
   *         starting from the <code>from</code> index
   * @see #getEntriesPerDump()
   */
  TabularData dump(long from);
  
  TabularData getRequestsInTransaction(long tid);

  /**
   * Returns the max number of entries which can be returned by the
   * <code>dump(int)</code> method.
   * 
   * @return the max number of entries which can be returned by the
   *         <code>dump(int)</code> method
   * @see #dump(int)
   */
  int getEntriesPerDump();

  /**
   * Returns an array of String representing the column names of the recovery
   * log table.<br />
   * <em>This array can be used as an hint to order the content of the recovery log retrieved
   * as  a TabularData.</em>
   * 
   * @return an array of String representing the column names of the recovery
   *         lgo table
   */
  String[] getHeaders();

  /**
   * Returns an array of <strong>2</strong> <code>long</code> representing
   * the min and max indexes currently contained in the recovery log.
   * <ul>
   * <li>the first <code>long</code> corresponds to the <em>min index</em>
   * in the recovery log</li>
   * <li>the second <code>long</code> corresponds to the <em>max index</em>
   * in the recovery log</li>
   * </ul>
   * 
   * @return an array of <strong>2</strong> <code>long</code> representing
   *         the min and max indexes currently contained in the recovery log or
   *         <code>null</code> if the indexes have not been computed
   */
  long[] getIndexes();

  /**
   * Returns the number of entries currently contained in the recovery log.
   * 
   * @return an long representing the number of entries currently contained in
   *         the recovery log or <code>-1</code> if the number of entries is
   *         unknown
   */
  long getEntries();

  /**
   * Returns a <code>Map&lt;String, String&gt;</code> where the keys are the
   * checkpoint names and the values are the corresponding IDs in the recovery
   * log. The returned Map is ordered by log ID (newest first).
   * 
   * @return an ordered <code>Map&lt;String, String&gt;</code> where the keys
   *         are the checkpoint names and the values are the log IDs
   */
  Map/* <String, String> */getCheckpoints();
}
