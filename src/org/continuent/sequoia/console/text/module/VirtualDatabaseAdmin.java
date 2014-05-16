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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Mathieu Peltier, Nicolas Modrzyk
 */

package org.continuent.sequoia.console.text.module;

import java.util.HashSet;
import java.util.Set;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.ControllerMBean;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.console.text.Console;
import org.continuent.sequoia.console.text.ConsoleException;

/**
 * This is the Sequoia controller console virtual database administration
 * module.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class VirtualDatabaseAdmin extends AbstractConsoleModule
{
  private String virtualDbName, login, password;

  /**
   * Returns the login value.
   * 
   * @return Returns the login.
   */
  public String getLogin()
  {
    return login;
  }

  /**
   * Returns the password value.
   * 
   * @return Returns the password.
   */
  public String getPassword()
  {
    return password;
  }

  /**
   * Returns the virtualDbName value.
   * 
   * @return Returns the virtualDbName.
   */
  public String getVirtualDbName()
  {
    return virtualDbName;
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#login(java.lang.String[])
   */
  public void login(String[] params) throws ConsoleException
  {
    // In case a login has failed before
    quit = false;
    String vdbName = params[0];
      console.getConsoleReader().addCompletor(this.getCompletor());
      console.getConsoleReader().removeCompletor(
          console.getControllerModule().getCompletor());

      if (vdbName == null || vdbName.trim().equals(""))
      {
        vdbName = console.readLine(ConsoleTranslate.get("admin.login.dbname"));
        if (vdbName == null)
          return;
      }

      login = console.readLine(ConsoleTranslate.get("admin.login.user"));
      if (login == null)
        return;

      password = console.readPassword(ConsoleTranslate
          .get("admin.login.password"));
      if (password == null)
        return;

      try
      {
        ControllerMBean mbean = console.getJmxClient().getControllerProxy();
        if (!mbean.getVirtualDatabaseNames().contains(vdbName))
        {
          throw new ConsoleException(ConsoleTranslate.get("module.database.invalid",
              vdbName));
        }
        VirtualDatabaseMBean vdb = console.getJmxClient()
            .getVirtualDatabaseProxy(vdbName, login, password);
        if (!vdb.checkAdminAuthentication(login, password))
        {
          throw new ConsoleException(ConsoleTranslate.get("module.database.login.fail", login));
        }
      } catch (ConsoleException e)
      {
        quit();
        throw e;
      }
      catch (Exception e)
      {
        quit();
        throw new ConsoleException(e);
      }

      virtualDbName = vdbName;
      // Reload commands because target has changed
      loadCommands();
      console.printInfo(ConsoleTranslate.get("admin.login.ready", virtualDbName));
    }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getDescriptionString()
   */
  public String getDescriptionString()
  {
    return "VirtualDatabase Administration";
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getPromptString()
   */
  public String getPromptString()
  {
    return virtualDbName + "(" + login + ")";
  }

  /**
   * Create a <code>Set</code> of expert commands. This <code>Set</code> of
   * commands can be added/removed dynamically to the list of admin commands
   * with the methods {@link #addExpertCommands()} and
   * {@link #removeExpertCommands()}
   * 
   * @return the set of expert commands
   */
  private Set expertCommandsSet()
  {
    Set expertCmds = new HashSet();
    String expertCommandsList = loadCommandsFromProperties("admin.expert");
    String[] expertCommands = parseCommands(expertCommandsList);
    addCommands(expertCommands, expertCmds);
    return expertCmds;
  }
  
  /**
   * Create a <code>Set</code> of debug commands. This <code>Set</code> of
   * commands can be added/removed dynamically to the list of admin commands
   * with the methods {@link #addDebugCommands()} and
   * {@link #removeDebugCommands()}
   * 
   * @return the set of expert commands
   */
  private Set debugCommandsSet()
  {
    Set debugCmds = new HashSet();
    String debugCommandsList = loadCommandsFromProperties("admin.debug");
    String[] debugCommands = parseCommands(debugCommandsList);
    addCommands(debugCommands, debugCmds);
    return debugCmds;
  }

  /**
   * Add the expert commands to the list of admin commands.
   */
  public void addExpertCommands()
  {
    commands.addAll(expertCommandsSet());
    // reload the completor or the newly added
    // commands won't be taken into account
    reloadCompletor();
  }

  /**
   * Revmoe the expert commands from the list of admin commands.
   */
  public void removeExpertCommands()
  {
    commands.removeAll(expertCommandsSet());
    // reload the completor or the removed
    // commands will still be taken into account
    reloadCompletor();
  }

  /**
   * Add the debug commands to the list of admin commands.
   */
  public void addDebugCommands()
  {
    commands.addAll(debugCommandsSet());
    // reload the completor or the newly added
    // commands won't be taken into account
    reloadCompletor();
  }

  /**
   * Revmoe the debug commands from the list of admin commands.
   */
  public void removeDebugCommands()
  {
    commands.removeAll(debugCommandsSet());
    // reload the completor or the removed
    // commands will still be taken into account
    reloadCompletor();
  }

  /**
   * Creates a new <code>VirtualDatabaseAdmin</code> instance.
   * 
   * @param console console console
   */
  public VirtualDatabaseAdmin(Console console)
  {
    super(console);
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getModuleID()
   */
  protected String getModuleID()
  {
    return "admin"; //$NON-NLS-1$
  }
}