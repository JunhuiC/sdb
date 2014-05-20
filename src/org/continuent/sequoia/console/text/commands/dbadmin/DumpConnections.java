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
 * Initial developer(s): Damian Arregui.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to dump a given scheduler queues
 */
public class DumpConnections extends AbstractAdminCommand
{

  /**
   * Creates a new <code>DumpConnections</code> object
   * 
   * @param module the commands is attached to
   */
  public DumpConnections(VirtualDatabaseAdmin module)
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

    Map<String, StringBuffer> loginToConnId = new HashMap<String, StringBuffer>();
    Hashtable<?, ?> connIdToLogin = ascMbean.listOpenPersistentConnections();
    Long connId;
    String login;
    for (Iterator<?> iter = connIdToLogin.entrySet().iterator(); iter.hasNext();)
    {
      Map.Entry<?,?> entry = (Map.Entry<?,?>) iter.next();
      connId = (Long) entry.getKey();
      login = (String) entry.getValue();
      if (!loginToConnId.containsKey(login))
      {
        loginToConnId.put(login, new StringBuffer("\n\t" + login + ": "));
      }
      loginToConnId.get(login).append(connId + " ");
    }

    StringBuffer disp = new StringBuffer();
    disp.append(ConsoleTranslate.get("DumpConnections.connections"));
    for (Iterator<StringBuffer> iter = loginToConnId.values().iterator(); iter.hasNext();)
    {
      StringBuffer sb = iter.next();
      disp.append(sb);
    }

    console.println(disp.toString());
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "dump connections"; //$NON-NLS-1$
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
    return ConsoleTranslate.get("DumpConnections.description"); //$NON-NLS-1$
  }
}
