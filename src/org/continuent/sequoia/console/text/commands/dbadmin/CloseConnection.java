/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Damian Arregui
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines an CloseConnection
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui </a>
 * @version 1.0
 */
public class CloseConnection extends AbstractAdminCommand
{

  /**
   * Creates a new <code>CloseConnection</code> object
   * 
   * @param module the commands is attached to
   */
  public CloseConnection(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(String)
   */
  public void parse(String commandText) throws Exception
  {
    VirtualDatabaseMBean db = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);

    String login = null;
    String ocid = null;
    StringTokenizer st = new StringTokenizer(commandText.trim());

    try
    {
      login = st.nextToken();
      ocid = st.nextToken();
    }
    catch (Exception e)
    {
      console.printError(getUsage());
      return;
    }

    if (login == null || ocid == null)
    {
      console.printError(getUsage());
      return;
    }

    db.closePersistentConnection(login, Long.parseLong(ocid));
    console.println(ConsoleTranslate.get(
        "admin.command.close.connection.ok", new Object[]{ocid, login})); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "close connection"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.close.connection.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.close.connection.params"); //$NON-NLS-1$
  }
}