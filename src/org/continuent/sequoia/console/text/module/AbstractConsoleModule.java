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

package org.continuent.sequoia.console.text.module;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.FileNameCompletor;
import jline.SimpleCompletor;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.text.Console;
import org.continuent.sequoia.console.text.commands.ConsoleCommand;
import org.continuent.sequoia.console.text.commands.Help;
import org.continuent.sequoia.console.text.commands.History;
import org.continuent.sequoia.console.text.commands.Quit;

/**
 * This class defines a AbstractConsoleModule
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public abstract class AbstractConsoleModule
{
  Console                     console;

  TreeSet<ConsoleCommand>                     commands;

  boolean                     quit                            = false;

  protected Completor         consoleCompletor;

  /**
   * location of the default command lists for the console modules
   */
  public static final String DEFAULT_COMMAND_PROPERTIES_FILE = "org/continuent/sequoia/console/text/console.ini";

  /**
   * Creates a new <code>AbstractConsoleModule.java</code> object
   * 
   * @param console to refer from
   */
  public AbstractConsoleModule(Console console)
  {
    this.console = console;
    this.commands = new TreeSet<ConsoleCommand>();
    commands.add(new Help(this));
    commands.add(new History(this));
    commands.add(new Quit(this));
    if (console.isInteractive())
      console.printInfo(ConsoleTranslate.get("module.loading",
          getDescriptionString()));
    this.loadCommands();
    this.loadCompletor();
  }

  /**
   * Loads the commands for this module
   */
  protected final void loadCommands()
  {
    commands.clear();
    String commandClassesAsString = loadCommandsFromProperties(getModuleID());
    String[] commandClasses = parseCommands(commandClassesAsString);
    addCommands(commandClasses, commands);
  }

  /**
   * Parses a String representing a list of command classes (separated by
   * commas) and returns an String[] representing the command classes
   * 
   * @param commandClassesAsString a String representing a list of command
   *          classes (separated by commas)
   * @return a (eventually empty) String[] where each String represents a
   *         command class
   */
  protected String[] parseCommands(String commandClassesAsString)
  {
    if (commandClassesAsString == null)
    {
      return new String[0];
    }
    String[] cmds = commandClassesAsString.split("\\s*,\\s*"); //$NON-NLS-1$
    return cmds;
  }

  /**
   * Add commands to this module. Commands instances are created by reflection
   * based on the command class names passed in parameter
   * 
   * @param commandClasses a String[] containing the class names of the command
   *          to instantiate
   * @param commands Set where the commands are added
   */
  protected void addCommands(String[] commandClasses, Set<ConsoleCommand> commands)
  {
    for (int i = 0; i < commandClasses.length; i++)
    {
      String commandClass = commandClasses[i].trim();
      Class<?> clazz;
      try
      {
        clazz = Class.forName(commandClass);
        Constructor<?> constructor;
        try
        {
          constructor = clazz.getConstructor(new Class[]{this.getClass()});
        }
        catch (NoSuchMethodException e)
        {
          constructor = clazz
              .getConstructor(new Class[]{AbstractConsoleModule.class});
        }
        ConsoleCommand command = (ConsoleCommand) constructor
            .newInstance(new Object[]{this});
        commands.add(command);
      }
      catch (Exception e)
      {
        // fail silently: the command won't be added to the commands list
      }
    }
  }

  /**
   * Extracts the commands from the command properties file as a single
   * <code>String</code> containing a list of comma-separated command classes.
   * 
   * @param moduleID module ID used as the key in the properties file
   * @return a single <code>String</code> containing a list of comma-separated
   *         command classes corresponding to the module identified by
   */
  protected String loadCommandsFromProperties(String moduleID)
  {
    Properties props = new Properties();
    try
    {
      String propertiesFile = System.getProperty("console.commands",
          DEFAULT_COMMAND_PROPERTIES_FILE);
      props.load(ClassLoader.getSystemResourceAsStream(propertiesFile));
    }
    catch (IOException e)
    {
      // fail silently: no commands will be loaded
    }
    String commandClassesAsString = props.getProperty(moduleID, "");
    return commandClassesAsString;
  }

  /**
   * Returns an unique ID which identifies the module. This value is used to
   * identify the commands to load for this given module.
   * 
   * @return an unique <code>String</code> which identifies the module
   */
  protected abstract String getModuleID();

  /**
   * Loads the commands for this module
   */
  protected void loadCompletor()
  {
    List<Completor> completors = new LinkedList<Completor>();
    int size = commands.size();
    if (size > 0)
    {
      TreeSet<String> set = new TreeSet<String>();
      Iterator<ConsoleCommand> it = commands.iterator();
      while (it.hasNext())
      {
        set.add(it.next().getCommandName());
      }
      completors.add(new SimpleCompletor(set
          .toArray(new String[size])));
    }
    completors.add(new FileNameCompletor());

    Completor[] completorsArray = completors
        .toArray(new Completor[completors.size()]);
    consoleCompletor = new ArgumentCompletor(completorsArray,
        new CommandDelimiter());
  }

  /**
   * Reload the completor associated with this module. This method must be
   * called if the list of commands has been dynamically modified.
   */
  protected synchronized void reloadCompletor()
  {
    console.getConsoleReader().removeCompletor(consoleCompletor);
    loadCompletor();
    console.getConsoleReader().addCompletor(consoleCompletor);
  }

  /**
   * Text description of this module
   * 
   * @return <code>String</code> description to display
   */
  public abstract String getDescriptionString();

  /**
   * Display help for this module
   */
  public void help()
  {
    console.println(ConsoleTranslate.get("module.commands.available",
        getDescriptionString()));
    ConsoleCommand command;
    Iterator<ConsoleCommand> it = commands.iterator();
    while (it.hasNext())
    {
      command = it.next();
      console.println(command.getCommandName() + " "
          + command.getCommandParameters());
      console.println("   " + command.getCommandDescription());
    }
  }

  /**
   * Quit this module
   */
  public void quit()
  {
    quit = true;
    console.getConsoleReader().removeCompletor(getCompletor());
    console.getConsoleReader().addCompletor(
        console.getControllerModule().getCompletor());
  }

  /**
   * Get all the commands for this module
   * 
   * @return <code>TreeSet</code> of commands (commandName|commandObject)
   */
  public TreeSet<ConsoleCommand> getCommands()
  {
    return commands;
  }

  /**
   * Get the prompt string for this module
   * 
   * @return <code>String</code> to place before prompt
   */
  public abstract String getPromptString();

  /**
   * Handle a serie of commands
   */
  public void handlePrompt()
  {

    if (quit)
    {
      if (console.isInteractive())
        console.printError(ConsoleTranslate.get("module.quitting",
            getDescriptionString()));
      return;
    }

    // login();
    quit = false;
    while (!quit)
    {

      Hashtable<String, ConsoleCommand> hashCommands = getHashCommands();
      try
      {
        String commandLine = console.readLine(getPromptString());
        if (commandLine == null)
        {
          quit();
          break;
        }
        if (commandLine.equals(""))
          continue;

        handleCommandLine(commandLine, hashCommands);

      }
      catch (Exception e)
      {
        // try to get the cause exception instead of the useless jmx
        // encapsulating one
        if (e instanceof UndeclaredThrowableException)
        {
          try
          {
            e = (Exception) ((UndeclaredThrowableException) e)
                .getUndeclaredThrowable();
          }
          catch (Throwable ignored)
          {
          }
        }
        console.printError(ConsoleTranslate.get("module.command.got.error", e
            .getMessage()), e);
        if (!console.isInteractive() && console.isExitOnError())
        {
          System.exit(1);
        }
      }
    }
  }

  /**
   * Get the list of commands as strings for this module
   * 
   * @return <code>Hashtable</code> list of <code>String</code> objects
   */
  public final Hashtable<String, ConsoleCommand> getHashCommands()
  {
    Hashtable<String, ConsoleCommand> hashCommands = new Hashtable<String, ConsoleCommand>();
    ConsoleCommand consoleCommand;
    Iterator<ConsoleCommand> it = commands.iterator();
    while (it.hasNext())
    {
      consoleCommand = it.next();
      hashCommands.put(consoleCommand.getCommandName(), consoleCommand);
    }
    return hashCommands;
  }

  /**
   * Handle module command
   * 
   * @param commandLine the command line to handle
   * @param hashCommands the list of commands available for this module
   * @throws Exception if fails *
   */
  public final void handleCommandLine(String commandLine, Hashtable<String, ConsoleCommand> hashCommands)
      throws Exception
  {
    StringTokenizer st = new StringTokenizer(commandLine);
    if (st.hasMoreTokens())
    {
      ConsoleCommand command = findConsoleCommand(commandLine, hashCommands);
      if (command != null)
      {
        command.execute(commandLine
            .substring(command.getCommandName().length()));
        return;
      }
    }
    throw new Exception(ConsoleTranslate.get("module.command.not.supported",
        commandLine));
  }

  /**
   * Find the <code>ConsoleCommand</code> based on the name of the command
   * from the <code>commandLine</code> in the <code>hashCommands</code>. If
   * more than one <code>ConsoleCommand</code>'s command name start the same
   * way, return the <code>ConsoleCommand</code> with the longest one.
   * 
   * @param commandLine the command line to handle
   * @param hashCommands the list of commands available for this module
   * @return the <code>ConsoleCommand</code> corresponding to the name of the
   *         command from the <code>commandLine</code> or <code>null</code>
   *         if there is no matching
   */
  public ConsoleCommand findConsoleCommand(String commandLine,
      Hashtable<String, ConsoleCommand> hashCommands)
  {
    ConsoleCommand foundCommand = null;
    for (Iterator<?> iter = hashCommands.entrySet().iterator(); iter.hasNext();)
    {
      Map.Entry<?,?> commandEntry = (Map.Entry<?,?>) iter.next();
      String commandName = (String) commandEntry.getKey();
      if (commandLine.startsWith(commandName))
      {
        ConsoleCommand command = (ConsoleCommand) commandEntry.getValue();
        if (foundCommand == null)
        {
          foundCommand = command;
        }
        if (command.getCommandName().length() > foundCommand.getCommandName()
            .length())
        {
          foundCommand = command;
        }
      }
    }
    return foundCommand;
  }

  /**
   * Handles login in this module
   * 
   * @param params parameters to use to login in this module
   * @throws Exception if fails
   */
  public abstract void login(String[] params) throws Exception;

  /**
   * Get access to the console
   * 
   * @return <code>Console</code> instance
   */
  public Console getConsole()
  {
    return console;
  }

  /**
   * Returns the console completor to use for this module.
   * 
   * @return <code>Completor</code> object.
   */
  public Completor getCompletor()
  {
    return consoleCompletor;
  }

  /**
   * This class defines a CommandDelimiter used to delimit a command from user
   * input
   */
  class CommandDelimiter extends ArgumentCompletor.AbstractArgumentDelimiter
  {
    /**
     * @see jline.ArgumentCompletor.AbstractArgumentDelimiter#isDelimiterChar(java.lang.String,
     *      int)
     */
    public boolean isDelimiterChar(String buffer, int pos)
    {
      String tentativeCmd = buffer.substring(0, pos);
      return isACompleteCommand(tentativeCmd);
    }

    /**
     * Test if the String input by the user insofar is a complete command or
     * not.
     * 
     * @param input Text input by the user
     * @return <code>true</code> if the text input by the user is a complete
     *         command name, <code>false</code> else
     */
    private boolean isACompleteCommand(String input)
    {
      boolean foundCompleteCommand = false;
      for (Iterator<ConsoleCommand> iter = commands.iterator(); iter.hasNext();)
      {
        ConsoleCommand command = iter.next();
        if (input.equals(command.getCommandName()))
        {
          foundCompleteCommand = !otherCommandsStartWith(command
              .getCommandName());
        }
      }
      return foundCompleteCommand;
    }

    private boolean otherCommandsStartWith(String commandName)
    {
      for (Iterator<ConsoleCommand> iter = commands.iterator(); iter.hasNext();)
      {
        ConsoleCommand command = iter.next();
        if (command.getCommandName().startsWith(commandName)
            && !command.getCommandName().equals(commandName))
        {
          return true;
        }
      }
      return false;
    }
  }
}