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

import java.util.HashMap;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a Replicate
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class Replicate extends AbstractAdminCommand
{

  /**
   * Creates a new <code>Replicate.java</code> object
   * 
   * @param module admin module
   */
  public Replicate(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    StringTokenizer st = new StringTokenizer(commandText, "; "); //$NON-NLS-1$
    if (st.countTokens() < 3)
    {
      console.printError(getUsage());
      return;
    }
    
    String backend1 = st.nextToken();
    String backend2 = st.nextToken();
    String url = st.nextToken();

    HashMap<String, String> parameters = new HashMap<String, String>();
    parameters.put("url",url); //$NON-NLS-1$
    StringTokenizer st2;
    while (st.hasMoreTokens())
    {
      st2 = new StringTokenizer(st.nextToken(), "="); //$NON-NLS-1$
      if (st2.countTokens() == 2)
      {
        String param = st2.nextToken();
        String value = st2.nextToken();
        parameters.put(param, value);
        console.printInfo(ConsoleTranslate.get("admin.command.replicate.param", //$NON-NLS-1$
            new String[]{param, value}));
      }
    }

    console.printInfo(ConsoleTranslate.get("admin.command.replicate.echo", //$NON-NLS-1$
        new String[]{backend1, backend2, url}));
    jmxClient.getVirtualDatabaseProxy(dbName, user, password).replicateBackend(
        backend1, backend2, parameters);

  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.replicate.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "clone backend config"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.replicate.description"); //$NON-NLS-1$
  }

}