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
import org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

public class DumpTransaction extends AbstractAdminCommand
{

  public DumpTransaction(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    StringTokenizer st = new StringTokenizer(commandText.trim());
    if (st.countTokens() != 1)
    {
      console.printError(getUsage());
      return;
    }
    
    long tid = Long.parseLong(st.nextToken());

    RecoveryLogControlMBean recoveryLog = jmxClient.getRecoveryLog(
        dbName, user, password);
    
    String[] headers = recoveryLog.getHeaders();
    // first headers is used to sort the entries
    final String idKey = headers[0];
    TabularData logEntries = recoveryLog.getRequestsInTransaction(tid);

    if (logEntries.isEmpty())
    {
      console.printInfo(ConsoleTranslate.get("DumpTransaction.empty", tid)); //$NON-NLS-1$
      return;
    }

    List<?> entries = JMXUtils.sortEntriesByKey(logEntries, idKey);
    String[][] entriesStr = JMXUtils.from(entries, headers);
    console.println(TableFormatter.format(headers, entriesStr, true));

  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "dump transaction"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return "<transaction ID>"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("DumpTransaction.description"); //$NON-NLS-1$
  }
}
