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

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to dump a given scheduler queues
 */
public class DumpRequest extends AbstractAdminCommand
{

  /**
   * Creates a new <code>DumpRequest</code> object
   * 
   * @param module the commands is attached to
   */
  public DumpRequest(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
public void parse(String commandText) throws Exception
  {
    // args: <vdb name> <request id>

    StringTokenizer st = new StringTokenizer(commandText.trim());
    String rIdStr;
    long requestId = 0;
    if (st.countTokens() != 1)
    {
      console.printError(getUsage());
      return;
    }

    rIdStr = st.nextToken();
    try
    {
      requestId = Long.valueOf(rIdStr).longValue();
    }
    catch (NumberFormatException e)
    {
      console.printError(ConsoleTranslate.get("DumpRequest.badId", rIdStr));
    }
    AbstractSchedulerControlMBean ascMbean = jmxClient.getAbstractScheduler(
        dbName, user, password);
    
    console.println(ascMbean.dumpRequest(requestId));
 }
  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "dump request"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("DumpRequest.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("DumpRequest.description"); //$NON-NLS-1$
  }
}
