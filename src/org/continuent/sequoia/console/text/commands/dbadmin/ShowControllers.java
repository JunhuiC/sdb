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
 * Initial developer(s): Nick Burch
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a ShowControllers
 * 
 * @author <a href="mailto:nick@torchbox.com">Nick Burch </a>
 * @version 1.0
 */
public class ShowControllers extends AbstractAdminCommand
{

  /**
   * Creates a new <code>ShowControllers.java</code> object
   * 
   * @param module the commands is attached to
   */
  public ShowControllers(VirtualDatabaseAdmin module)
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
    String[] controllers = db.getControllers();
    console.println(ConsoleTranslate.get(
        "admin.command.show.controllers.number", new Object[]{dbName, //$NON-NLS-1$
            new Integer(controllers.length)}));
    for (int i = 0; i < controllers.length; i++)
    {
      console.println("\t" + controllers[i]); //$NON-NLS-1$
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "show controllers"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.show.controllers.description"); //$NON-NLS-1$
  }
}