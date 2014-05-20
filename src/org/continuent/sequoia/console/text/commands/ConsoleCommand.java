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
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.console.text.commands;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.jmx.RmiJmxClient;
import org.continuent.sequoia.console.text.Console;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * This class defines a ConsoleCommand
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 * @param <E>
 */
public abstract class ConsoleCommand implements Comparable<Object>
{
  protected Console               console;
  protected RmiJmxClient          jmxClient;
  protected AbstractConsoleModule module;

  /**
   * Creates a new <code>ConsoleCommand.java</code> object
   * 
   * @param module module that owns this commands
   */
  public ConsoleCommand(AbstractConsoleModule module)
  {
    this.console = module.getConsole();
    this.module = module;
    jmxClient = console.getJmxClient();
  }

  /**
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Object o)
  {
    if (o instanceof ConsoleCommand)
    {
      ConsoleCommand c = (ConsoleCommand) o;
      return getCommandName().compareTo(c.getCommandName());
    }
    else
    {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Check that a JMX connection is established with a controller. If no
   * connection is available, a ConsoleException is thrown indicating that the
   * console must be connected to a controller in order to execute the command.
   * 
   * @throws ConsoleException if no JMX connection is available
   */
  protected void checkJmxConnectionToController() throws ConsoleException
  {
    // Force a refresh of jmxClient in case we have reconnected to another
    // controller
    jmxClient = console.getJmxClient();

    if (jmxClient == null)
      throw new ConsoleException(ConsoleTranslate
          .get("console.command.requires.connection")); //$NON-NLS-1$
  }

  /**
   * Parse the text of the command
   * 
   * @param commandText the command text
   * @throws Exception if connection with the mbean server is lost or command
   *           does not have the proper format
   */
  public abstract void parse(String commandText) throws Exception;

  /**
   * Check if the JMX connection is still valid. Otherwise reconnect.
   * 
   * @param commandText the parameters to execute the command with
   * @throws Exception if fails
   */
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  /**
   * Get the name of the command
   * 
   * @return <code>String</code> of the command name
   */
  public abstract String getCommandName();

  /**
   * Return a <code>String</code> description of the parameters of this
   * command.
   * 
   * @return <code>String</code> like &lt;driverPathName&gt;
   */
  public String getCommandParameters()
  {
    return ""; //$NON-NLS-1$
  }

  /**
   * Get the description of the command
   * 
   * @return <code>String</code> of the command description
   */
  public abstract String getCommandDescription();

  /**
   * Get the usage of the command.
   * 
   * @return <code>String</code> of the command usage ()
   */
  public String getUsage()
  {
    String usage = ConsoleTranslate
        .get(
            "command.usage", new String[]{getCommandName(), getCommandParameters()}); //$NON-NLS-1$
    usage += "\n   " + getCommandDescription(); //$NON-NLS-1$
    return usage;
  }
}
