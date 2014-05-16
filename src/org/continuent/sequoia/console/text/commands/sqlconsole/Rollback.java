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

package org.continuent.sequoia.console.text.commands.sqlconsole;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Savepoint;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.VirtualDatabaseConsole;

/**
 * This class defines a "rollback" sql command
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class Rollback extends ConsoleCommand
{

  /**
   * Creates a new <code>Rollback</code> object
   * 
   * @param module the command is attached to
   */
  public Rollback(VirtualDatabaseConsole module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws IOException, ConsoleException
  {
    VirtualDatabaseConsole consoleModule = (VirtualDatabaseConsole) module;
    Connection connection = consoleModule.getConnection();

    String savePointName = commandText.trim();

    try
    {
      if (consoleModule.getRequestDelimiter().equals(savePointName)
          || "".equals(savePointName)) //$NON-NLS-1$
      {
        connection.rollback();
        console.printInfo(ConsoleTranslate.get("sql.command.rollback.done")); //$NON-NLS-1$
      }
      else
      {
        Savepoint savepoint = ((VirtualDatabaseConsole) module)
            .getSavePoint(savePointName);
        if (savepoint == null)
        {
          console.printError(ConsoleTranslate.get(
              "sql.command.rollback.no.savepoint", savePointName)); //$NON-NLS-1$
          return;
        }
        connection.rollback(savepoint);
        console.printInfo(ConsoleTranslate.get(
            "sql.command.rollback.to.savepoint", savePointName)); //$NON-NLS-1$
      }
      connection.setAutoCommit(true);
    }
    catch (Exception e)
    {
      console.printError(ConsoleTranslate.get("sql.display.exception", e), e); //$NON-NLS-1$
    }

  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "rollback"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("sql.command.rollback.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("sql.command.rollback.description"); //$NON-NLS-1$
  }
}