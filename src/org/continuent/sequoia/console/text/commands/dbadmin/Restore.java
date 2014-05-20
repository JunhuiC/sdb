/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a Restore
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class Restore extends AbstractAdminCommand
{

  /**
   * Creates a new <code>Restore.java</code> object
   * 
   * @param module the command is attached to
   */
  public Restore(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    String dumpName = null;
    String backendName = null;
    ArrayList<String> tables = null;
    StringTokenizer st = new StringTokenizer(commandText.trim());

    try
    {
      backendName = st.nextToken();
      dumpName = st.nextToken();
      if (st.hasMoreTokens())
      {
        tables = new ArrayList<String>();
        while (st.hasMoreTokens())
        {
          tables.add(st.nextToken());
        }
      }
    }
    catch (Exception e)
    {
      console.printError(getUsage());
      return;
    }

    if (backendName == null || dumpName == null)
    {
      console.printError(getUsage());
      return;
    }
    String login = console.readLine(ConsoleTranslate.get("admin.restore.user")); //$NON-NLS-1$
    if (login == null)
      return;

    String pwd = console.readPassword(ConsoleTranslate
        .get("admin.restore.password")); //$NON-NLS-1$
    if (pwd == null)
      return;

    console.printInfo(ConsoleTranslate.get("admin.command.restore.echo", //$NON-NLS-1$
        new String[]{backendName, dumpName}));
    VirtualDatabaseMBean vjdc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    vjdc.restoreDumpOnBackend(backendName, login, pwd, dumpName, tables);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {

    return "restore backend"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.restore.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.restore.params"); //$NON-NLS-1$
  }
}