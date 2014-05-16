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

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.RequestManagerMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines the command used to get the parsing result of a given sql
 * query.
 */
public class ParseRequest extends AbstractAdminCommand
{
  protected static final String LINE_SEPARATOR = System
                                                   .getProperty("line.separator");

  /**
   * Creates a new <code>DumpBackendSchema</code> object
   * 
   * @param module the commands is attached to
   */
  public ParseRequest(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    // args: request
    // remove possible '"' chars
    if (commandText.indexOf('"', 0) != -1)
      commandText = commandText.replace('"', ' ');
    String request = commandText.trim();
    if (request.length() == 0)
    {
      console.printError(getUsage());
      return;
    }

    RequestManagerMBean requestManager = jmxClient.getRequestManager(dbName, user,
        password);
    console.println(requestManager.parseSqlRequest(request, LINE_SEPARATOR));
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {

    return "parse request"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("ParseRequest.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("ParseRequest.description"); //$NON-NLS-1$
  }
}
