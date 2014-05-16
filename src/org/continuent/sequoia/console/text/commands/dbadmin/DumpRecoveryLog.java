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

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to dump the recovery log.
 */
public class DumpRecoveryLog extends AbstractAdminCommand
{
  /**
   * Creates a new <code>DumpRecoveryLog</code> object
   * 
   * @param module the commands is attached to
   */
  public DumpRecoveryLog(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    if ("indexes".equals(commandText.trim())) //$NON-NLS-1$
    {
      printIndexes();
      return;
    }
    StringTokenizer tokenizer = new StringTokenizer(commandText.trim());

    long min = 0;
    if (tokenizer.hasMoreTokens())
    {
      String minStr = tokenizer.nextToken();
      try
      {
        min = Long.parseLong(minStr);
      }
      catch (NumberFormatException e)
      {
        console.printError(getUsage());
        return;
      }
    }

    RecoveryLogControlMBean recoveryLog = jmxClient.getRecoveryLog(dbName,
        user, password);

    long max;
    
    // if a negative min is supplied, dump from tail
    if (min < 0) {
        max = recoveryLog.getIndexes()[1];
        min = max + min +1;
    }
    else {   
        String maxStr;
        if (tokenizer.hasMoreTokens())
        {
          maxStr = tokenizer.nextToken();
          try
          {
            max = Long.parseLong(maxStr);
          }
          catch (NumberFormatException e)
          {
            console.printError(getUsage());
            return;
          }
        }
        else
        {
          max = recoveryLog.getIndexes()[1];
        }
    }
    
    if (min < 0 || max < 0)
    {
      console.printError("Negative indexes are not allowed");
      return;
    }

    boolean verbose = false;
    if (tokenizer.hasMoreTokens()) {
        String verboseStr = tokenizer.nextToken();
        if (verboseStr.equals("-v")) {
            verbose = true;
        } else {
            console.printError(getUsage());
            return;
        }
    }
    String[] headers = recoveryLog.getHeaders();

    // zap sql params column - which can be huge - if not printing
    // verbose (default)
    if (!verbose) {
        List l = new ArrayList(Arrays.asList(headers));
        l.remove(3); // this is the sql params column
        headers = (String[]) l.toArray(new String[l.size()]);
    }
    
    // first headers is used to sort the entries
    final String idKey = headers[0];
    TabularData logEntries = recoveryLog.dump(min);

    if (logEntries.isEmpty())
    {
      console.printInfo(ConsoleTranslate.get("DumpRecoveryLog.empty")); //$NON-NLS-1$
      return;
    }
    long lastIndex = 0;
    while (!logEntries.isEmpty())
    {
      List entries = JMXUtils.sortEntriesByKey(logEntries, idKey);
      lastIndex = getIndexOfLastEntry(entries, idKey);
      if (lastIndex > max)
      {
        removeAfterIndex(entries, max, idKey);
      }
      String[][] entriesStr = JMXUtils.from(entries, headers);
      console.println(TableFormatter.format(headers, entriesStr, true));

      if (lastIndex > max)
      {
        break;
      }
      logEntries = recoveryLog.dump(lastIndex + 1);
    }
  }
  
  /*
   * Remove all entries whose indexes are greater than the index parameter
   */
  private void removeAfterIndex(List entries, long index, String idKey)
  {
    Iterator iter = entries.iterator();
    while (iter.hasNext())
    {
      CompositeData data = (CompositeData) iter.next();
      String idStr = (String) data.get(idKey);
      long id = Long.parseLong(idStr);
      if (id > index)
      {
        iter.remove();
      }
    }
  }

  private void printIndexes()
  {
    try
    {
      RecoveryLogControlMBean recoveryLog = jmxClient.getRecoveryLog(dbName,
          user, password);
      long[] indexes = recoveryLog.getIndexes();
      String minIndex = (indexes != null)
          ? Long.toString(indexes[0])
          : ConsoleTranslate.get("DumpRecoveryLog.notAvailable"); //$NON-NLS-1$
      String maxIndex = (indexes != null)
          ? Long.toString(indexes[1])
          : ConsoleTranslate.get("DumpRecoveryLog.notAvailable"); //$NON-NLS-1$
      console.println(ConsoleTranslate
          .get("DumpRecoveryLog.minIndex", minIndex)); //$NON-NLS-1$
      console.println(ConsoleTranslate
          .get("DumpRecoveryLog.maxIndex", maxIndex)); //$NON-NLS-1$
      console.println(ConsoleTranslate.get(
          "DumpRecoveryLog.numberOfEntries", recoveryLog.getEntries())); //$NON-NLS-1$
    }
    catch (IOException e)
    {
      console.printError(e.getMessage());
    }
  }

  /**
   * Returns the index of the last entry in the <code>entries</code> List
   * 
   * @param entries a List&lt;CompositeData&gt;
   * @param idKey the key corresponding to the id of the index
   * @return the index of the last entry in the <code>entries</code> List
   */
  private static int getIndexOfLastEntry(List entries, String idKey)
  {
    CompositeData data = (CompositeData) entries.get(entries.size() - 1);
    String idStr = (String) data.get(idKey);
    return Integer.parseInt(idStr);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {

    return "dump recoverylog"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return "[indexes | {<min> <max>} | -<nb entries> [-v]]"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("DumpRecoveryLog.description"); //$NON-NLS-1$
  }
}
