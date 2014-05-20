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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.List;
import java.util.StringTokenizer;

import javax.management.openmbean.TabularData;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.management.TransactionDataSupport;
import org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to dump a given scheduler queues
 */
public class DumpSchedulerQueues extends AbstractAdminCommand
{

  /**
   * Creates a new <code>DumpSchedulerQueues</code> object
   * 
   * @param module the commands is attached to
   */
  public DumpSchedulerQueues(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    StringTokenizer st = new StringTokenizer(commandText.trim());
    if (st.countTokens() != 0)
    {
      console.printError(getUsage());
      return;
    }

    AbstractSchedulerControlMBean ascMbean = jmxClient.getAbstractScheduler(
        dbName, user, password);

    // active transactions
    TabularData transactions = ascMbean.getActiveTransactions();
    List<?> entries = JMXUtils.sortEntriesByKey(transactions, "tid");

    console.println(ConsoleTranslate.get(
        "DumpSchedulerQueues.activeTransactions", entries.size()));
    if (entries.size() > 0)
    {
      String[][] entriesStr = JMXUtils
          .from(entries, TransactionDataSupport.NAMES);
      console.println(TableFormatter.format(new String[]{"tid", "time (in s)"},
          entriesStr, true));
    }
    console.println();
    
    // pending read requests
    long[] prids = ascMbean.listPendingReadRequestIds();
    console.println(ConsoleTranslate.get("DumpSchedulerQueues.pendingReads",
        prids.length));
    for (int i = 0; i < prids.length; i++)
    {
      console.print(" " + prids[i]);
    }
    console.println();

    // pending write requests
    long[] pwids = ascMbean.listPendingWriteRequestIds();
    console.println(ConsoleTranslate.get("DumpSchedulerQueues.pendingWrites",
        pwids.length));
    for (int i = 0; i < pwids.length; i++)
    {
      console.print(" " + pwids[i]);
    }
    console.println();
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "dump scheduler queues"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ""; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("DumpSchedulerQueues.description"); //$NON-NLS-1$
  }
}
