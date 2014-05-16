/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.sqlconsole;

import java.sql.Connection;
import java.sql.SQLException;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.module.VirtualDatabaseConsole;

/**
 * This class defines a "setreadonly" sql command
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class SetReadOnly extends ConsoleCommand
{

  /**
   * Creates a new <code>SetReadOnly</code> object
   * 
   * @param module the command is attached to
   */
  public SetReadOnly(VirtualDatabaseConsole module)
  {
    super(module);
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("sql.command.readonly.description"); //$NON-NLS-1$
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "setreadonly"; //$NON-NLS-1$
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("sql.command.readonly.params"); //$NON-NLS-1$
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    try
    {
      boolean readOnly = Boolean.valueOf(commandText.trim()).booleanValue();
      Connection connection = ((VirtualDatabaseConsole) module).getConnection();
      connection.setReadOnly(readOnly);
      console.printInfo(ConsoleTranslate.get("sql.command.readonly.value", //$NON-NLS-1$
          readOnly));
    }
    catch (SQLException e)
    {
      throw new ConsoleException(ConsoleTranslate
          .get("sql.command.readonly.failed"), e); //$NON-NLS-1$
    }
  }

}
