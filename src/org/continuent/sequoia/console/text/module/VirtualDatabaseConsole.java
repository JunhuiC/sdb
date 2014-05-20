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
 * Contributor(s): Mathieu Peltier, Nicolas Modrzyk.
 */

package org.continuent.sequoia.console.text.module;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Hashtable;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.console.text.Console;
import org.continuent.sequoia.console.text.ConsoleException;
import org.continuent.sequoia.console.text.ConsoleLauncher;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.formatter.ResultSetFormatter;

/**
 * Sequoia Controller Virtual Database Console module.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class VirtualDatabaseConsole extends AbstractConsoleModule
{
  private Connection          connection                 = null;
  /**
   * contains a hash of <String, SavePoint> to handle savepoints (used by
   * SetSavePoint and Rollback commands)
   */
  private Hashtable<String, Savepoint>           savePoints                 = new Hashtable<String, Savepoint>();

  /** Default query timeout. */
  private int                 timeout                    = 0;

  private int                 fetchsize                  = 0;

  private int                 maxrows                    = 0;

  private String              login;
  private String              url;

  // for backwards compatiblity, multiline statement is disabled by default
  private boolean             multilineStatementsEnabled = false;
  // request delimiter to use when multiline statement is enabled
  private String              delimiter                  = DEFAULT_REQUEST_DELIMITER;
  private static final String DEFAULT_REQUEST_DELIMITER  = ";";
  // a buffer used when multiline statement is enabled to keep in
  // memory the statement across each handlePrompt()
  private StringBuffer        currentRequest             = new StringBuffer("");

  /**
   * Creates a new <code>VirtualDatabaseAdmin</code> instance. Loads the
   * driver
   * 
   * @param console console console
   */
  public VirtualDatabaseConsole(Console console)
  {
    super(console);
    try
    {
      Class.forName(System.getProperty("console.driver",
          "org.continuent.sequoia.driver.Driver"));
    }
    catch (Exception e)
    {
      console.printError(ConsoleTranslate.get(
          "sql.login.cannot.load.driver", ConsoleLauncher.PRODUCT_NAME), e);
      Runtime.getRuntime().exit(1);
    }
    if (console.isInteractive())
      console.println(ConsoleTranslate.get("sql.login.loaded.driver",
          new String[] {ConsoleLauncher.PRODUCT_NAME, Constants.VERSION}));
  }

  /**
   * Get the JDBC connection used by the sql console.
   * 
   * @return a JDBC connection
   */
  public Connection getConnection()
  {
    return connection;
  }

  /**
   * Create a new connection from the driver.
   * 
   * @param url the Sequoia url
   * @param login the login to use to open the connection
   * @param password the password to use to open the connection
   * @return a new connection
   * @throws ConsoleException if login failed
   */
  public Connection createConnection(String url, String login, String password)
      throws ConsoleException
  {
    try
    {
      return DriverManager.getConnection(url, login, password);
    }
    catch (Exception e)
    {
      throw new ConsoleException(ConsoleTranslate.get(
          "sql.login.connection.failed", new String[]{url, e.getMessage()}));
    }
  }

  /**
   * Executes a SQL statement.
   * 
   * @param request the SQL request to execute
   * @param displayResult <code>true</code> if the result must be printed on
   *          the standard output
   */
  public synchronized void execSQL(String request, boolean displayResult)
  {
    PreparedStatement stmt = null;
    try
    {
      stmt = connection.prepareStatement(request);
      stmt.setQueryTimeout(timeout);
      if (fetchsize != 0)
        stmt.setFetchSize(fetchsize);
      if (maxrows != 0)
        stmt.setMaxRows(maxrows);

      long start = System.currentTimeMillis();
      long end;
      if (request.toLowerCase().matches("^(\\(|\\s+)*select.*"))
      {
        ResultSet rs = stmt.executeQuery();
        end = System.currentTimeMillis();
        if (displayResult)
          ResultSetFormatter.display(rs, fetchsize, console);
        rs.close();
      }
      else
      {
        boolean hasResult = stmt.execute();
        end = System.currentTimeMillis();

        int updatedRows;
        do
        {
          updatedRows = -1;
          if (hasResult)
          {
            ResultSet rs = stmt.getResultSet();
            if (displayResult)
              ResultSetFormatter.display(rs, fetchsize, console);
            rs.close();
          }
          else
          {
            updatedRows = stmt.getUpdateCount();
            if (displayResult)
              console.printInfo(ConsoleTranslate.get("sql.display.affected.rows",
                  updatedRows));
          }
          hasResult = stmt.getMoreResults();
          updatedRows = stmt.getUpdateCount();
        }
        while (hasResult || (updatedRows != -1));
      }
      console.printInfo(ConsoleTranslate.get("sql.display.query.time",
          new Long[]{new Long((end - start) / 1000),
              new Long((end - start) % 1000)}));
    }
    catch (Exception e)
    {
      console.printError(ConsoleTranslate.get("sql.command.sqlquery.error", e),
          e);
    }
    finally
    {
      try
      {
        stmt.close();
      }
      catch (Exception ignore)
      {
      }
    }
  }

  /**
   * Connects to a virtual database.
   */
  public void handlePrompt()
  {
    while (!quit)
    {
      try
      {
        String cmd = console.readLine(this.getPromptString());
        if (cmd == null)
        {
          quit();
          break;
        }

        if (cmd.length() == 0)
          continue;

        ConsoleCommand currentCommand = findConsoleCommand(cmd,
            getHashCommands());

        if (currentCommand == null)
        {
          if (multilineStatementsEnabled)
          {
            // append a whitespace beacuse console.readLine() trim the cmd
            currentRequest.append(" ").append(cmd);
            if (currentRequest.toString().endsWith(delimiter))
            {
              String request = removeDelimiterAndTrim(currentRequest.toString());
              execSQL(request, true);
              currentRequest = new StringBuffer("");
            }
          }
          else
          {
            execSQL(cmd, true);
          }
        }
        else
        {
          currentCommand.execute(cmd.substring(
              currentCommand.getCommandName().length()).trim());
        }
      }
      catch (Exception e)
      {
        console.printError(ConsoleTranslate.get("sql.display.exception", e), e);
        if ((e instanceof RuntimeException)
            || (!console.isInteractive() && console.isExitOnError()))
        {
          System.exit(1);
        }
      }
    }
  }

  /**
   * Removes the delimiter at the end of the request and trims the resulting
   * substring.
   * 
   * @param request the SQL request
   * @return a String representing the SQL request without its delimiter
   * @see #delimiter
   */
  private String removeDelimiterAndTrim(String request)
  {
    if (request == null)
    {
      return "";
    }
    return request.substring(0, request.length() - delimiter.length()).trim();
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getDescriptionString()
   */
  public String getDescriptionString()
  {
    return "SQL Console";
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getModuleID()
   */
  protected String getModuleID()
  {
    return "sql"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getPromptString()
   */
  public String getPromptString()
  {
    int ind1 = url.lastIndexOf('?');
    int ind2 = url.lastIndexOf(';');
    if (ind1 != -1 || ind2 != -1)
    {
      String prompt;
      prompt = (ind1 != -1) ? url.substring(0, ind1) : url;
      prompt = (ind2 != -1) ? url.substring(0, ind2) : url;
      return prompt + " (" + login + ")";
    }
    else
      return url + " (" + login + ")";
  }

  /**
   * Get a <code>SavePoint</code> identified by its <code>name</code>
   * 
   * @param name name fo the <code>SavePoint</code>
   * @return a <code>SavePoint</code> or <code>null</code> if no
   *         <code>SavePoint</code> with such a name has been previously added
   */
  public Savepoint getSavePoint(String name)
  {
    return (Savepoint) savePoints.get(name);
  }

  /**
   * add a <code>SavePoint</code>
   * 
   * @param savePoint the <code>SavePoint</code> to add
   * @throws SQLException if the <code>savePoint</code> is unnamed
   */
  public void addSavePoint(Savepoint savePoint) throws SQLException
  {
    savePoints.put(savePoint.getSavepointName(), savePoint);
  }

  /**
   * Get the timeout value (in seconds)
   * 
   * @return the timeout value (in seconds)
   */
  public int getTimeout()
  {
    return timeout;
  }

  /**
   * Set the timeout value (in seconds)
   * 
   * @param timeout new timeout value (in seconds)
   */
  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }

  /**
   * Get the fetchsize value
   * 
   * @return the fetchsize value
   */
  public int getFetchsize()
  {
    return fetchsize;
  }

  /**
   * Set the fetchsize
   * 
   * @param fetchsize new fetchsize value
   */
  public void setFetchsize(int fetchsize)
  {
    this.fetchsize = fetchsize;
  }

  /**
   * Set the maxrows
   * 
   * @param maxrows new maxrows value
   */
  public void setMaxrows(int maxrows)
  {
    this.maxrows = maxrows;
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#login(java.lang.String[])
   */
  public void login(String[] params) throws Exception
  {
    quit = false;

    login = null;
    url = (params.length > 0 && params[0] != null) ? params[0].trim() : null;
    try
    {
      if ((url == null) || url.trim().equals(""))
      {
        url = console.readLine(ConsoleTranslate
            .get("sql.login.prompt.url", ConsoleLauncher.PRODUCT_NAME));
        if (url == null)
          return;
      }
      login = console.readLine(ConsoleTranslate.get("sql.login.prompt.user"));
      if (login == null)
        return;
      String password = console.readPassword(ConsoleTranslate
          .get("sql.login.prompt.password"));
      if (password == null)
        return;

      connection = createConnection(url, login, password);

      // Change console reader completor
      console.getConsoleReader().removeCompletor(
          console.getControllerModule().getCompletor());
      console.getConsoleReader().addCompletor(this.getCompletor());
    }
    catch (ConsoleException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      throw new ConsoleException(ConsoleTranslate.get("sql.login.exception", e));
    }
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#quit()
   */
  public void quit()
  {
    if (connection != null)
    {
      try
      {
        connection.close();
      }
      catch (Exception e)
      {
        // ignore
      }
    }
    super.quit();
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#help()
   */
  public void help()
  {
    super.help();
    console.println();
    console.println(ConsoleTranslate.get("sql.command.description"));
  }

  /**
   * Sets the request delimiter to use when multiline statement is enabled.
   * 
   * @param delimiter the String to use as request delimiter
   * @see #DEFAULT_REQUEST_DELIMITER
   */
  public void setRequestDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

  /**
   * Returns the request delimiter
   * 
   * @return a String representing the request delimiter
   */
  public String getRequestDelimiter()
  {
    return delimiter;
  }

  /**
   * Enables multiline statements to be able to write one statement on several
   * lines. A multiline statement is executed when the user inputs the request
   * delimiter String at the end of a line. By default multiline statement is
   * disabled (for backwards compatibility)
   * 
   * @param multilineStatementsEnabled <code>true</code> if multiline
   *          statement must be enabled, <code>false</code> else
   */
  public void enableMultilineStatement(boolean multilineStatementsEnabled)
  {
    this.multilineStatementsEnabled = multilineStatementsEnabled;
  }
}
