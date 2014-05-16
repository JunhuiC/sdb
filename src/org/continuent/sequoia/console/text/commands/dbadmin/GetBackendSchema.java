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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * Command to get the schema of a database backend
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class GetBackendSchema extends AbstractAdminCommand
{

  /**
   * Creates a new <code>GetBackendSchema</code> object
   * 
   * @param module owning module
   */
  public GetBackendSchema(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {

    StringTokenizer st = new StringTokenizer(commandText);
    int tokens = st.countTokens();
    if (tokens < 1)
    {
      console.printError(getUsage());
      return;
    }

    String backendName = st.nextToken();
    String fileName = null;
    if (tokens >= 2)
    {
      fileName = st.nextToken().trim();
    }

    VirtualDatabaseMBean vdjc = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);

    if (fileName == null)
    {
      // Write to standard output
      console.println(vdjc.getBackendSchema(backendName));
    }
    else
    {
      console.printInfo(ConsoleTranslate.get(
          "admin.command.get.backend.schema.echo", new String[]{backendName, //$NON-NLS-1$
              fileName}));
      // Write to file
      File f = new File(fileName);
      BufferedWriter writer = new BufferedWriter(new FileWriter(f));
      writer.write(vdjc.getBackendSchema(backendName));
      writer.flush();
      writer.close();
    }

  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "get backend schema"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.get.backend.schema.description"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.get.backend.schema.params"); //$NON-NLS-1$
  }

}