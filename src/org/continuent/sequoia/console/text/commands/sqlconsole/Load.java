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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.VirtualDatabaseConsole;

/**
 * This class defines a "load" sql command
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class Load extends ConsoleCommand
{

  /**
   * Creates a new <code>Load</code> object
   * 
   * @param module the command is attached to
   */
  public Load(VirtualDatabaseConsole module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws IOException, ConsoleException
  {
    String fileName = commandText.trim();
    if ("".equals(fileName)) //$NON-NLS-1$
    {
      console.printError(getUsage());
      return;
    }
    load(commandText.trim());
  }

  /**
   * Executes all the SQL requests contained in the specified file.
   *    sqlRequestCommand.parse(cmd);
      
   * @param fileName the file name to open
   */
  public void load(String fileName)
  {
    Connection connection = ((VirtualDatabaseConsole)module).getConnection();
    
    BufferedReader file = null;
    try
    {
      file = new BufferedReader(new FileReader(fileName));
    }
    catch (Exception e)
    {
      console.printError(
          ConsoleTranslate.get("sql.command.load.file.error", e), e); //$NON-NLS-1$
      return;
    }

    console.println(ConsoleTranslate.get("sql.command.loading.file", fileName)); //$NON-NLS-1$
    try
    {
      String request;

      while ((request = file.readLine()) != null)
      {
        request = request.trim();
        console.println(request);

        if (request.equalsIgnoreCase("begin")) //$NON-NLS-1$
          connection.setAutoCommit(false);
        else if (request.equalsIgnoreCase("commit")) //$NON-NLS-1$
          connection.commit();
        else if (request.equalsIgnoreCase("rollback")) //$NON-NLS-1$
          connection.rollback();
        else
        { // Regular SQL request
          ((VirtualDatabaseConsole)module).execSQL(request, false);
        }
      }
    }
    catch (Exception e)
    {
      console.printError(ConsoleTranslate.get("sql.command.load.execute.error", //$NON-NLS-1$
          e), e);
    }
    finally
    {
      try
      {
        file.close();
      }
      catch (IOException ignore)
      {
      }
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "load"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("sql.command.load.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("sql.command.load.description"); //$NON-NLS-1$
  }
}