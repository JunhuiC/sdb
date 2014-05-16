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
 * Initial developer(s): Jeff Mesnil
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a InitializeCluster command.
 */
public class InitializeCluster extends AbstractAdminCommand
{

  /**
   * Creates a new <code>InitializeCluster.java</code> object
   * 
   * @param module the commands is attached to
   */
  public InitializeCluster(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {	  
    StringTokenizer tokenizer = new StringTokenizer(commandText.trim());

    if (tokenizer.countTokens() < 1)
    {
      console.printError(getUsage());
      return;
    }

    String backendName = tokenizer.nextToken();
    boolean force = false;

    if (tokenizer.hasMoreTokens())
    {
      if (! "force".equals(tokenizer.nextToken())) //$NON-NLS-1$
      {
    	  console.printError(getUsage());
    	  return; 
      }
      force = true;
    }

    if ("".equals(backendName)) //$NON-NLS-1$
    {
      console.printError(getUsage());
      return;
    }
    
    VirtualDatabaseMBean db = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    db.initializeFromBackend(backendName, force);
    console.printInfo(ConsoleTranslate.get("admin.command.initialize.success", new String[] {db.getVirtualDatabaseName(), backendName}));  //$NON-NLS-1$   
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "initialize"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.initialize.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.initialize.description"); //$NON-NLS-1$
  }

}
