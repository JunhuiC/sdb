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

package org.continuent.sequoia.console.text.commands.controller;

import java.util.StringTokenizer;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.jmx.RmiJmxClient;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.ConsoleLauncher;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * This class defines a Bind
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 * @param <E>
 */
public class Bind extends ConsoleCommand
{

  /**
   * Creates a new <code>Bind.java</code> object
   * 
   * @param module the command is attached to
   */
  public Bind(AbstractConsoleModule module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    StringTokenizer st = new StringTokenizer(commandText.trim());

    String host = null;
    String port = null;

    if (st == null || st.countTokens() != 2)
    {
      console.printError(getUsage());
      return;
    }

    try
    {
      host = st.nextToken();
      port = st.nextToken();

      if (jmxClient == null)
      {
        jmxClient = new RmiJmxClient("" + port, host, null);
        console.setJmxClient(jmxClient);
      }

      jmxClient.connect(port, host, jmxClient.getCredentials());
      console.printInfo(ConsoleTranslate.get(
          "controller.command.bind.success", //$NON-NLS-1$
          new String[]{ConsoleLauncher.PRODUCT_NAME, host, port}));
    }
    catch (Exception e)
    {
      throw new ConsoleException(ConsoleTranslate.get(
          "controller.command.bind.failed", //$NON-NLS-1$
          new String[]{ConsoleLauncher.PRODUCT_NAME, host, port}), e);
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "connect controller"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("controller.command.bind.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("controller.command.bind.description", ConsoleLauncher.PRODUCT_NAME); //$NON-NLS-1$
  }

}
