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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */
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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.formatter.ResultSetFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseConsole;

/**
 * This class defines a "timeout" sql command
 * 
 * @author <a href="mailto:jeff.mesnil@emicnetworks.com">Jeff Mesnil</a>
 */
public class CallStoredProcedure extends ConsoleCommand
{

  /**
   * Creates a new <code>SetTimeout</code> object
   * 
   * @param module the command is attached to
   */
  public CallStoredProcedure(VirtualDatabaseConsole module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws IOException, ConsoleException
  {
    callStoredProcedure(getCommandName() + " " + commandText); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "{call"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("sql.command.call.stored.procedure.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate
        .get("sql.command.call.stored.procedure.description"); //$NON-NLS-1$
  }

  /**
   * Call a store procedure.
   * 
   * @param proc the stored procedure to call
   */
  private synchronized void callStoredProcedure(String proc)
  {
    Connection connection = ((VirtualDatabaseConsole) module).getConnection();
    int fetchsize = ((VirtualDatabaseConsole) module).getFetchsize();
    CallableStatement cs = null;
    try
    {
      cs = connection.prepareCall(proc);
      cs.setQueryTimeout(((VirtualDatabaseConsole) module).getTimeout());

      long start = System.currentTimeMillis();
      long end;
      ResultSet rs = cs.executeQuery();
      end = System.currentTimeMillis();
      ResultSetFormatter.formatAndDisplay(rs, fetchsize, console);
      console.printInfo(ConsoleTranslate.get("sql.display.query.time", //$NON-NLS-1$
          new Long[]{new Long((end - start) / 1000),
              new Long((end - start) % 1000)}));
    }
    catch (Exception e)
    {
      console.printError(ConsoleTranslate.get(
          "sql.command.call.stored.procedure.failed", e), e); //$NON-NLS-1$
    }
    finally
    {
      try
      {
        cs.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }
}