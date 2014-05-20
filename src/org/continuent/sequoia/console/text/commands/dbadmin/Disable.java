/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a Disable
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 * @param <E>
 */
public class Disable extends AbstractAdminCommand
{
  /**
   * Creates a new <code>Disable.java</code> object
   * 
   * @param module the command is attached to
   */
  public Disable(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    String backendName = null;

    StringTokenizer st = new StringTokenizer(commandText);
    if (st.countTokens() != 1)
    {
      console.printError(getUsage());
      return;
    }
    try
    {
      backendName = st.nextToken();
    }
    catch (Exception e)
    {
      console.printError(getUsage());
      return;
    }

    VirtualDatabaseMBean vjdc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    if ("*".equals(backendName)) //$NON-NLS-1$
    {
      console.printInfo(ConsoleTranslate
          .get("admin.command.disable.backend.all.with.checkpoint")); //$NON-NLS-1$
      List<?> backendNames = vjdc.getAllBackendNames();
      for (Iterator<?> iter = backendNames.iterator(); iter.hasNext();)
      {
        String backend = (String) iter.next();
        vjdc.disableBackendWithCheckpoint(backend);
      }
    }
    else
    {
      vjdc.disableBackendWithCheckpoint(backendName);
      console.printInfo(ConsoleTranslate.get(
          "admin.command.disable.backend.with.checkpoint", backendName)); //$NON-NLS-1$
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.disable.backend.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "disable"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.disable.backend.description"); //$NON-NLS-1$
  }

}
