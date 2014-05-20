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
 * This class defines the Backup command for the text console. It backups a
 * backend to a dump file.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class Backup extends AbstractAdminCommand
{

  /**
   * Creates a new <code>Backup.java</code> object
   * 
   * @param module the command is attached to
   */
  public Backup(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {

    StringTokenizer st = new StringTokenizer(commandText.trim());

    if (st == null || st.countTokens() < 4)
    {
      console.printError(getUsage());
      return;
    }
    String backendName = st.nextToken();
    String dumpName = st.nextToken();
    String backuperName = st.nextToken();
    String path = st.nextToken();
    ArrayList<String> tables = null;
    boolean force = false;
    if (st.hasMoreTokens())
    {
      String tok = st.nextToken();
      if ("-force".equalsIgnoreCase(tok))
      {
        force = true;
      }
      else
      {
        tables = new ArrayList<String>();
        tables.add(tok);
      }
    }
    
    if (st.hasMoreTokens())
    {
      if (tables == null)
        tables = new ArrayList<String>();
      while (st.hasMoreTokens())
      {
        tables.add(st.nextToken());
      }
    }

    String login = console.readLine(ConsoleTranslate.get("admin.backup.user")); //$NON-NLS-1$
    if (login == null)
      return;

    String pwd = console.readPassword(ConsoleTranslate
        .get("admin.backup.password")); //$NON-NLS-1$
    if (pwd == null)
      return;

    console.printInfo(ConsoleTranslate.get("admin.command.backup.echo", //$NON-NLS-1$
        new String[]{backendName, dumpName}));
    if (tables != null)
      console.printInfo(ConsoleTranslate.get("admin.command.backup.tables", //$NON-NLS-1$
          tables));
    VirtualDatabaseMBean vdjc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    vdjc.backupBackend(backendName, login, pwd, dumpName, backuperName, path,
        force, tables);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "backup"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.backup.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.backup.params"); //$NON-NLS-1$
  }

}