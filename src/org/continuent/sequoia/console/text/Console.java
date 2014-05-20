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
 * Initial developer(s): Emmanuel Cecchet. 
 * Contributor(s): Mathieu Peltier,Nicolas Modrzyk, Marc Herbert
 */

package org.continuent.sequoia.console.text;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.prefs.Preferences;

import jline.ConsoleReader;
import jline.History;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.jmx.RmiJmxClient;
import org.continuent.sequoia.console.text.module.ControllerConsole;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;
import org.continuent.sequoia.console.text.module.VirtualDatabaseConsole;

/**
 * This is the Sequoia controller console that allows remote administration and
 * monitoring of any Sequoia controller.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class Console
{
  private static final Character PASSWORD_CHAR = new Character('\u0000');

  /** <code>ConsoleReader</code> allowing to reading input. */
  private final ConsoleReader    consoleReader;

  private final PrintWriter      consoleOutputWriter;

  /** <code>true</code> if the console is used in interactive mode. */
  private boolean                interactive;

  private RmiJmxClient           jmxClient;

  /** Virtual database administration console. */
  private VirtualDatabaseAdmin   adminModule;

  /** Virtual database console. */
  private VirtualDatabaseConsole consoleModule;

  /** Controller Console */
  private ControllerConsole      controllerModule;

  /** Debug Mode */
  private boolean                debug;
  private boolean                silent        = false;

  private boolean                exitOnError;

  /**
   * The number of history items to store in java Preferences file
   */
  private static final int       STORED_HISTORY_SIZE  = 100;

  /**
   * <code>true</code> if colors should be displayed in interactive mode (work
   * only on non Windows system).
   */
  private boolean                printColor;

  private boolean sqlClientOnly;

  /**
   * Creates a new <code>Console</code> instance.
   * 
   * @param jmxClient to connect to the jmxServer
   * @param in the input stream to get the command from
   * @param interactive if set to <code>true</code> will display prompt
   * @param debug <code>true</code> if debug mode should be activated.
   * @param silent <code>true</code> if silent mode is activated
   * @param exitOnError <code>true</code> if the console should exit on error
   *          in non interactive mode.
   * @param sqlClientOnly  set to <code>true</code> if the console should
   *        behave as a sql client only
   * @throws IOException 
   */
  public Console(RmiJmxClient jmxClient, InputStream in, boolean interactive,
      boolean debug, boolean silent, boolean exitOnError, boolean sqlClientOnly)
      throws IOException
  {
    this.consoleOutputWriter = new PrintWriter(System.out, true);
    this.consoleReader = new ConsoleReader(in, consoleOutputWriter);
    this.interactive = interactive;
    this.jmxClient = jmxClient;
    this.debug = debug;
    this.silent = silent;
    this.exitOnError = exitOnError;
    this.sqlClientOnly = sqlClientOnly;

    controllerModule = new ControllerConsole(this);
    adminModule = new VirtualDatabaseAdmin(this);
    consoleModule = new VirtualDatabaseConsole(this);
    setPrintColor(true);
    consoleReader.addCompletor(controllerModule.getCompletor());
    consoleReader.setHistory(loadHistory());

    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      public void run()
      {
        storeHistory();
      }
    });
  }

  private History loadHistory()
  {
    jline.History jHistory = new jline.History();
    try
    {
      Preferences prefs = Preferences.userRoot()
          .node(this.getClass().getName());
      String[] historyKeys = prefs.keys();
      Arrays.sort(historyKeys, 0, historyKeys.length);
      for (int i = 0; i < historyKeys.length; i++)
      {
        String key = historyKeys[i];
        String value = prefs.get(key, ""); //$NON-NLS-1$
        jHistory.addToHistory(value);
      }
    }
    catch (Exception e)
    {
      // unable to load prefs: do nothing
    }
    return jHistory;
  }

  /**
   * Retrieve the command history
   * 
   * @return a List including the command history
   */
  public List<?> getHistory()
  {
    return consoleReader.getHistory().getHistoryList();
  }

  /**
   * Store the current command history
   */
  public void storeHistory()
  {
    List<?> history = consoleReader.getHistory().getHistoryList();
    try
    {
      Preferences prefs = Preferences.userRoot()
          .node(this.getClass().getName());
      prefs.clear();
      int historySize = history.size();
      int start = Math.max(0, historySize - STORED_HISTORY_SIZE);
      // save up to the last 100th history items only
      // witht the stored index starting at 0 
      for (int i = start; i < historySize; i++)
      {
        prefs.put(String.valueOf(i -start), (String) history.get(i + start));
      }
      prefs.flush();
    }
    catch (Exception e)
    {
      // unable to store prefs: do nothing
    }
  }

  /**
   * Should this console display color in interactive mode? Warning, colors only
   * work on non Windows system.
   * 
   * @param b <code>true</code> if the console should display color (ignored
   *          on Windows system).
   */
  public void setPrintColor(boolean b)
  {
    String os = System.getProperty("os.name").toLowerCase();
    boolean windows = os.indexOf("nt") > -1 || os.indexOf("windows") > -1;
    if (windows)
      printColor = false;
    else
      printColor = b;

    if (System.getProperty("sequoia.console.nocolor") != null)
      printColor = false;
  }

  /**
   * Returns the interactive value.
   * 
   * @return Returns the interactive.
   */
  public boolean isInteractive()
  {
    return interactive;
  }

  /**
   * Test if the console should exit on error in non interactive mode.
   * 
   * @return <code>true</code> if the console should exit on error in non
   *         interactive mode.
   */
  public boolean isExitOnError()
  {
    return exitOnError;
  }

  /**
   * Main menu prompt handling.
   */
  public void handlePrompt() throws Exception
  {
        if (sqlClientOnly)
            sqlClientConsole();
        else
            controllerModule.handlePrompt();
  }

  private void sqlClientConsole()
  {
      try
      {
          consoleModule.login(new String[] { "" });
      }
      catch (Exception e)
      {
          printError(ConsoleTranslate.get("module.command.got.error", e
                  .getMessage()), e);
          System.exit(1);
      }
      consoleModule.handlePrompt();
  }

  /**
     * Reads a line from the console.
     * 
     * @param prompt
     *            the prompt to display
     * @return the trimmed line read from the console
     * @throws ConsoleException
     *             if an error occured
     */
  public String readLine(String prompt) throws ConsoleException
  {
    String line = "";
    try
    {
      if (interactive)
        line = consoleReader.readLine(beautifiedPrompt(prompt));
      else
        // if the console is not interactive (i.e. read from a file),
        // we also bypass jline console and we read the input stream ourselves
        // to circumvent jline limitation wrt to utf-8 encoding on
        // 3-bytes characters (like CKJ charsets)
        // Ideally disabling JLine would be a distinct option (think
        // interactive + CKJ characters).
        // An interesting question is why do we echo but don't prompt.
        line = readLineBypassJLine(false);

    }
    catch (IOException e)
    {
      throw new ConsoleException(ConsoleTranslate.get(
          "console.read.command.failed", e));
    }
    if (line != null)
      line = line.trim();
    return line;
  }

  private String beautifiedPrompt(String barePrompt)
  {
    String prompt = barePrompt + " > ";
    if (printColor)
    {
      prompt = ColorPrinter.getColoredMessage(prompt, ColorPrinter.PROMPT);
    }
    return prompt;
  }

  boolean lastWasCR   = false;
  List<Byte>    currentLine = new ArrayList<Byte>();

  /**
   * Implements SEQUOIA-887. We would like to create a BufferedReader to use its
   * readLine() method but can't because its eager cache would steal bytes from
   * JLine and drop them when we return, so painfully implement here our own
   * readLine() method, tyring to be bug for bug compatible with JLine.
   * <p>
   * This is a quite ugly hack. Among others we cannot use any read buffering
   * since consoleReader is exported and might be used elsewhere. At the very
   * least we would like to encapsulate the consoleReader so we can avoid
   * creating one in non-JLine mode. Re-open SEQUOIA-887 for such a proper fix.
   */
  private String readLineBypassJLine(boolean maskInput) throws IOException
  {
    // If JLine implements any kind of internal read buffering, we
    // are screwed.
    InputStream jlineInternal = consoleReader.getInput();

    /*
     * Unfortunately we can't do this because InputStreamReader returns -1/EOF
     * after every line!? So we have to decode bytes->characters by ourselves,
     * see below. Because of this we will FAIL with exotic locales, see
     * SEQUOIA-911
     */
    // Reader jlineInternal = new InputStreamReader(consoleReader.getInput());
    currentLine.clear();

    int ch = jlineInternal.read();

    if (ch == -1 /* EOF */|| ch == 4 /* ASCII EOT */)
      return null;

    /**
     * @see java.io.BufferedReader#readLine(boolean)
     * @see java.io.DataInputStream#readLine() and also the less elaborate JLine
     *      keybinding.properties
     */
    // discard any LF following a CR
    if (lastWasCR && ch == '\n')
      ch = jlineInternal.read();

    // EOF also counts as an end of line. Not sure this is what JLine does but
    // it looks good.
    while (ch != -1 && ch != '\n' && ch != '\r')
    {
      currentLine.add(new Byte((byte) ch));
      ch = jlineInternal.read();
    }

    // SEQUOIA-911 FIXME: we may have found a '\n' or '\r' INSIDE a multibyte
    // character. Definitely not a real newline.

    lastWasCR = (ch == '\r');

    // "cast" byte List into a primitive byte array
    byte[] encoded = new byte[currentLine.size()];
    Iterator<Byte> it = currentLine.iterator();
    for (int i = 0; it.hasNext(); i++)
      encoded[i] = ((Byte) it.next()).byteValue();

    /**
     * This String ctor is using the "default" java.nio.Charset encoding which
     * is locale-dependent; a Good Thing.
     */
    String line = new String(encoded);

    if (maskInput)
      consoleOutputWriter.println();
    else
      consoleOutputWriter.println(line);

    return line;
  }

  /**
   * Read password from the console.
   * 
   * @param prompt the promp to display
   * @return the password read from the console
   * @throws ConsoleException if an error occured
   */
  public String readPassword(String prompt) throws ConsoleException
  {
    try
    {
      if (interactive)
        return consoleReader.readLine(beautifiedPrompt(prompt), PASSWORD_CHAR);

      return readLineBypassJLine(true);
    }
    catch (IOException e)
    {
      throw new ConsoleException(ConsoleTranslate.get(
          "console.read.password.failed", e));
    }
  }

  /**
   * Prints a String on the console. Use this method to print <em>things</em>
   * returned by Sequoia controller.
   * 
   * @param s the String to print
   */
  public void print(String s)
  {
    System.out.print(s);
  }

  /**
   * Prints a String on the console.<br />
   * Use this method to print <em>things</em> returned by Sequoia controller.
   * 
   * @param s the String to print
   */
  public void println(String s)
  {
    System.out.println(s);
  }

  /**
   * Print in color
   * 
   * @param s the message to display
   * @param color the color to use
   */
  private void println(String s, int color)
  {
    if (printColor)
      ColorPrinter.printMessage(s, System.out, color);
    else
      System.out.println(s);
  }

  /**
   * Prints a new line.
   */
  public void println()
  {
    System.out.println();
  }

  /**
   * Prints an error message.<br />
   * Use this method to print <em>error</em> messages coming either from
   * Sequoia controller or from the console.
   * 
   * @param message error message to print
   */
  public void printError(String message)
  {
    if (printColor)
      ColorPrinter.printMessage(message, System.err, ColorPrinter.ERROR);
    else
      System.err.println(message);
  }

  /**
   * Prints an info message.<br />
   * Use this method to print <em>info</em> messages coming from the console.
   * An info message should not contain essential information that the user
   * can't deduce from the commands he/she typed.
   * 
   * @param message informational message to print
   */
  public void printInfo(String message)
  {
    if (!silent)
    {
      println(message, ColorPrinter.INFO);
    }
  }

  /**
   * Prints an error message (and displays the stack trace of an Exception if
   * the debug option is active).<br />
   * Use this method to print <em>error</em> messages coming either from
   * Sequoia controller or from the console.
   * 
   * @param message error message to print
   * @param e an exception
   */
  public void printError(String message, Exception e)
  {
    if (debug)
      e.printStackTrace();
    printError(message);
  }

  /**
   * Returns the jmxClient value.
   * 
   * @return Returns the jmxClient.
   */
  public RmiJmxClient getJmxClient()
  {
    return jmxClient;
  }

  /**
   * Sets a new JmxClient (used when console started without being connected).
   * 
   * @param jmxClient the new JMX client to use
   */
  public void setJmxClient(RmiJmxClient jmxClient)
  {
    this.jmxClient = jmxClient;
  }

  /**
   * Returns the adminModule value.
   * 
   * @return Returns the adminModule.
   */
  public VirtualDatabaseAdmin getAdminModule()
  {
    return adminModule;
  }

  /**
   * Returns the consoleModule value.
   * 
   * @return Returns the consoleModule.
   */
  public VirtualDatabaseConsole getConsoleModule()
  {
    return consoleModule;
  }

  /**
   * Returns the controllerModule value.
   * 
   * @return Returns the controllerModule.
   */
  public ControllerConsole getControllerModule()
  {
    return controllerModule;
  }

  /**
   * Returns the consoleReader value.
   * 
   * @return Returns the consoleReader.
   */
  public final ConsoleReader getConsoleReader()
  {
    return consoleReader;
  }

  /**
   * (ugly!) pass-through setter to set the request delimiter on the
   * VirtualDatabaseConsole from the command line options of the console
   * 
   * @param delimiter the String representing the request delimiter
   * @see VirtualDatabaseConsole#setRequestDelimiter(String)
   */
  public void setRequestDelimiter(String delimiter)
  {
    consoleModule.setRequestDelimiter(delimiter);
  }

  /**
   * (ugly!) pass-through setter to enable/disabled multiline statement on the
   * VirtualDatabaseConsole from the command line options of the console
   * 
   * @param multilineStatementEnabled <code>true</code> if multiline stamement
   *          is enabled, <code>false</code> else
   * @see VirtualDatabaseConsole#setRequestDelimiter(String)
   */
  public void enableMultilineStatements(boolean multilineStatementEnabled)
  {
    consoleModule.enableMultilineStatement(multilineStatementEnabled);
  }
}
