/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks
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
 * Contributor(s): Dylan Hansen, Mathieu Peltier.
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backup.Backuper;

/**
 * This abstract class provides base methods for PostgreSQL backupers.
 * <p>
 * Currently the Backupers takes 5 parameters (all are optional):
 * <ul>
 * <li>bindir: path to PostgreSQL binaries (if not set, the commands are
 * searched in the path)</li>
 * <li>encoding: the encoding of the database that is created upon restore</li>
 * <li>authentication: flag to use PostgreSQL authentication (default false)</li>
 * <li>dumpServer: address of interface to offer dumps</li>
 * <li>preRestoreScript: location of script to run before restoring</li>
 * <li>postRestoreScript: location of script to run after restoring</li>
 * <li>dumpTimeout: Timeout period (seconds) while performing DB dump.</li>
 * <li>restoreTimeout: Timeout period (seconds) while performin DB restore.</li>
 * <li>pgDumpFlags: Extra pg_dump command-line options to use while performing
 * DB dump.</li>
 * </ul>
 * More options can be easily added. This class makes calls to the pg_dump,
 * createdb, dropdb, psql and pg_restore commands.
 * 
 * @author <a href="mailto:dhansen@h2st.com">Dylan Hansen</a>
 * @author <a href="mailto:mathieu.peltier@emicnetworks.com">Mathieu Peltier</a>
 * @version 1.1
 */
public abstract class AbstractPostgreSQLBackuper extends AbstractBackuper
{
  private static final String DEFAULT_POSTGRESQL_HOST = "localhost";

  // Logger
  private static Trace        logger                  = Trace
                                                          .getLogger(AbstractPostgreSQLBackuper.class
                                                              .getName());

  /** end user logger */
  static Trace                endUserLogger           = Trace
                                                          .getLogger("org.continuent.sequoia.enduser");

  // CommandExec instance for running native commands.
  protected NativeCommandExec nativeCmdExec           = new NativeCommandExec();

  // Static variables for option values
  protected static String     binDir                  = null;
  protected static String     encoding                = null;
  protected static boolean    useAuthentication       = false;
  protected static String     dumpServer              = null;
  protected static String     preRestoreScript        = null;
  protected static String     postRestoreScript       = null;
  protected static String     pgDumpFlags             = null;
  protected static String     dumpTimeout             = null;
  protected static String     restoreTimeout          = null;
  protected static String     splitSize               = "1000m";

  protected String getPsqlCommand()
  {
    return "psql";
  }

  protected String getJdbcUrlPrefix()
  {
    return "jdbc:postgresql:";
  }

  protected int getDefaultPort()
  {
    return 5432;
  }

  /**
   * Creates a new <code>AbstractPostgreSQLBackuper</code> object
   */
  public AbstractPostgreSQLBackuper()
  {
  }

  /**
   * @see Backuper#getOptions()
   */
  public String getOptions()
  {
    return optionsString;
  }

  /**
   * @see Backuper#setOptions(java.lang.String)
   */
  public void setOptions(String options)
  {
    super.setOptions(options);

    // Check the HashMap for options. Only set once, easier than
    // checking HashMap every time a value is needed.
    if (optionsMap.containsKey("bindir"))
      binDir = (String) optionsMap.get("bindir");
    if (optionsMap.containsKey("encoding"))
      encoding = (String) optionsMap.get("encoding");
    if (optionsMap.containsKey("authentication")
        && ((String) optionsMap.get("authentication")).equalsIgnoreCase("true"))
      useAuthentication = true;
    if (optionsMap.containsKey("dumpServer"))
      dumpServer = (String) optionsMap.get("dumpServer");
    if (optionsMap.containsKey("preRestoreScript"))
      preRestoreScript = (String) optionsMap.get("preRestoreScript");
    if (optionsMap.containsKey("postRestoreScript"))
      postRestoreScript = (String) optionsMap.get("postRestoreScript");
    if (optionsMap.containsKey("dumpTimeout"))
      dumpTimeout = (String) optionsMap.get("dumpTimeout");
    if (optionsMap.containsKey("restoreTimeout"))
      restoreTimeout = (String) optionsMap.get("restoreTimeout");
    if (optionsMap.containsKey("pgDumpFlags"))
      pgDumpFlags = (String) optionsMap.get("pgDumpFlags");
    if (optionsMap.containsKey("splitSize"))
      splitSize = (String) optionsMap.get("splitSize");

  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#deleteDump(java.lang.String,
   *      java.lang.String)
   */
  public void deleteDump(String path, String dumpName) throws BackupException
  {
    File toRemove = new File(getDumpPhysicalPath(path, dumpName));
    if (logger.isDebugEnabled())
      logger.debug("Deleting dump " + toRemove);
    toRemove.delete();
  }

  /**
   * Get the dump physical path from its logical name
   * 
   * @param path the path where the dump is stored
   * @param dumpName dump logical name
   * @return path to dump file
   */
  protected String getDumpPhysicalPath(String path, String dumpName)
  {
    String fullPath = null;

    if (path.endsWith(File.separator))
      fullPath = path + dumpName;
    else
      fullPath = path + File.separator + dumpName;

    return fullPath;
  }

  /**
   * Creates a command string based on given info. Will append binDir if
   * supplied.
   * 
   * @param command Command to execute
   * @param info URL info to parse and create parameters
   * @param options Additional parameters
   * @param login User login
   * @return String containing full command to run
   */
  protected String makeCommand(String command, PostgreSQLUrlInfo info,
      String options, String login)
  {
    String prefix = binDir != null ? binDir + File.separator : "";
    return prefix + command + " " + info.getHostParametersString() + " -U "
        + login + " " + options + " " + info.dbName;
  }

  /**
   * Creates a command String array used my exec(). This method uses the
   * "expect" command to pass the password parameter to the command being run,
   * thus allowing PostgreSQL to allow authentication. Also uses binDir if
   * supplied. Note the "psql" command has different output needed by "expect"
   * than other commands.
   * 
   * @param command Command to execute
   * @param info URL info to parse adn add parameters
   * @param options Additional parameters
   * @param login User login
   * @param password User password
   * @param isPsql Is the command being run a psql command?
   * @return String array containing full command to run
   */
  protected String[] makeCommandWithAuthentication(String command,
      PostgreSQLUrlInfo info, String options, String login, String password,
      boolean isPsql)
  {
    // Build params for "spawn" command used by expect
    StringBuffer cmdBuff = new StringBuffer("spawn ");

    if (binDir != null)
      cmdBuff.append(binDir + File.separator);

    cmdBuff.append(command);
    cmdBuff.append(" ");
    cmdBuff.append(info.getHostParametersString());
    cmdBuff.append(" -U ");
    cmdBuff.append(login);
    cmdBuff.append(" ");
    cmdBuff.append(options);
    cmdBuff.append(" ");
    cmdBuff.append(info.dbName);
    cmdBuff.append("; ");
    // "psql" command has different message for password input
    if (isPsql)
      cmdBuff.append("expect \"Password:\"; ");
    else
      cmdBuff.append("expect \"Password for user " + login + ":\"; ");
    cmdBuff.append("send \"");
    cmdBuff.append(password);
    cmdBuff.append("\"; ");
    cmdBuff.append("send \"\\r\"; ");
    cmdBuff.append("expect eof;");

    String[] commands = {"expect", "-c", cmdBuff.toString()};
    return commands;
  }

  /**
   * Creates a Splitted command string based on given info. Will append BINDIR
   * if supplied.
   * 
   * @param command Command to execute
   * @param info URL info to parse and create parameters
   * @param options Additional parameters
   * @param login User login
   * @return String containing full command to run
   */
  protected String[] makeSplitCommand(String command, PostgreSQLUrlInfo info,
      String options, String login)
  {
    String prefix = binDir != null ? binDir + File.separator : "";
    String cmd = prefix + command + " " + info.dbName + " "
        + info.getHostParametersString() + " -U " + login + " " + options;
    String[] cmdArray = {"bash", "-c", cmd};
    return cmdArray;
    // return prefix + command + " " + info.dbName + " " +
    // info.getHostParametersString() + " -U " + login + " " + options;
  }

  /**
   * Creates an expect dialogue String array to be used as stadard input to a
   * "expect" command, used to pass the password parameter to the command being
   * run, thus allowing PostgreSQL to allow authentication. Also uses binDir if
   * supplied. Note that the "psql" command has different password promting than
   * other Postgres utilities. This expect-dialogue do a much better job at
   * detecting errors than the detection you get when using
   * makeCommandWithAuthentication() and executeNativeCommand(). Above all, it
   * will report failure if the PG- command never promts for a password.
   * Secondly, it will report failure if the invoked command exits with a
   * non-zero exit-status.
   * 
   * @param command Command to execute
   * @param info URL info to parse adn add parameters
   * @param options Additional parameters
   * @param login User login
   * @param password User password
   * @param timeout Is the command being run a psql command?
   * @return String array containing full Expect dialogue
   */
  protected String[] makeExpectDialogueWithAuthentication(String command,
      PostgreSQLUrlInfo info, String options, String login, String password,
      int timeout)
  {
    String[] cmdBuff = new String[27];

    if (binDir != null)
      cmdBuff[0] = new String("spawn " + binDir + File.separator + command
          + " " + info.getHostParametersString() + " -U " + login + " -W "
          + options + " " + info.getDbName());
    else
      cmdBuff[0] = new String("spawn " + command + " "
          + info.getHostParametersString() + " -U " + login + " -W " + options
          + " " + info.getDbName());
    cmdBuff[1] = new String("proc abort {} {");
    cmdBuff[2] = new String("  system kill [exp_pid]");
    cmdBuff[3] = new String("  exit 1");
    cmdBuff[4] = new String("}");
    cmdBuff[5] = new String("set exitcode 1");
    cmdBuff[6] = new String("expect {");
    cmdBuff[7] = new String("  \"Password for user " + login
        + ":\" { set exitcode 0 }");
    cmdBuff[8] = new String("  \"assword:\" { set exitcode 0 }");
    cmdBuff[9] = new String("  timeout { abort }");
    cmdBuff[10] = new String("}");
    cmdBuff[11] = new String("send \"" + password + "\"");
    cmdBuff[12] = new String("send \"\r\"");
    cmdBuff[13] = new String("set timeout " + timeout);
    cmdBuff[14] = new String("expect {");
    cmdBuff[15] = new String(
        "  \"ERROR:\" { set exitcode 1; exp_continue -continue_timer }");
    cmdBuff[16] = new String(
        "  \"FATAL:\" { set exitcode 1; exp_continue -continue_timer }");
    cmdBuff[17] = new String("  timeout { abort }");
    cmdBuff[18] = new String("  eof { if {$exitcode != 0} { abort }}");
    cmdBuff[19] = new String("}");
    cmdBuff[20] = new String("set rc [wait]");
    cmdBuff[21] = new String("set os_error [lindex $rc 2]");
    cmdBuff[22] = new String("set status [lindex $rc 3]");
    cmdBuff[23] = new String("if {$os_error != 0 || $status != 0} {");
    cmdBuff[24] = new String("  exit 1");
    cmdBuff[25] = new String("}");
    cmdBuff[26] = new String("exit 0");
    return cmdBuff;
  }

  /**
   * Creates a Splitted command String array used my exec(). This method uses
   * the "expect" command to pass the password parameter to the command being
   * run, thus allowing PostgreSQL to allow authentication. Also uses BINDIR if
   * supplied. Note the "psql" command has different output needed by "expect"
   * than other commands.
   * 
   * @param command Command to execute
   * @param info URL info to parse adn add parameters
   * @param options Additional parameters
   * @param login User login
   * @param password User password
   * @param isPsql Is the command being run a psql command?
   * @return String array containing full command to run
   */
  protected String[] makeSplitCommandWithAuthentication(String command,
      PostgreSQLUrlInfo info, String options, String login, String password,
      boolean isPsql)
  {
    // Build params for "spawn" command used by expect
    StringBuffer cmdBuff = new StringBuffer("spawn ");

    if (binDir != null)
      cmdBuff.append(binDir + File.separator);

    cmdBuff.append(command);
    cmdBuff.append(" ");
    cmdBuff.append(info.dbName); // gurkan
    cmdBuff.append(" "); // gurkan
    cmdBuff.append(info.getHostParametersString());
    cmdBuff.append(" -U ");
    cmdBuff.append(login);
    cmdBuff.append(" ");
    cmdBuff.append(options);
    // cmdBuff.append(" "); //gurkan
    // cmdBuff.append(info.dbName); //gurkan
    cmdBuff.append("; ");
    // "psql" command has different message for password input
    if (isPsql)
      cmdBuff.append("expect \"Password:\"; ");
    else
      cmdBuff.append("expect \"Password for user " + login + ":\"; ");
    cmdBuff.append("send \"");
    cmdBuff.append(password);
    cmdBuff.append("\"; ");
    cmdBuff.append("send \"\\r\"; ");
    cmdBuff.append("expect eof;");

    // may need to be bash not expect //not sure ; gurkan
    String[] commands = {"expect", "-c", "bash", "-c", cmdBuff.toString()}; // gurkan
    // String[] commands = {"expect", "-c", cmdBuff.toString()};
    return commands;
  }

  /**
   * Executes a native operating system command with a standard-input feed. The
   * commands are supposed to be one of the Postgres-utilities psql - dropdb -
   * createdb - pg_dump - pg_restore. Output of the command is captured and
   * logged.
   * 
   * @param stdinFeed Array of input strings
   * @param commands Array of strings (command + args) to execute
   * @return 0 if successful, any number otherwise
   * @throws IOException
   * @throws InterruptedException
   */
  protected int executeNativeCommand(String[] stdinFeed, String[] commands)
      throws IOException, InterruptedException
  {
    NativeCommandInputSource input = NativeCommandInputSource
        .createArrayInputSource(stdinFeed);
    return nativeCmdExec.executeNativeCommand(commands, input, null, 0,
        getIgnoreStdErrOutput());
  }

  /**
   * Creates a expect command reading the expect-script from standard-input.
   * Hours of experiments has shown that the only reliable way of
   * programatically invoking expect with an expect-script, is via stdin or
   * explicit script-file. The expect "-c command" option simply does not work
   * the same way, and do cause a lot of problems.
   * 
   * @return String array containing full command to run
   */
  protected String[] makeExpectCommandReadingStdin()
  {
    String[] commands = {"expect", "-"};
    // String[] commands = {"expect", "-d", "-"}; // Diagnostic output
    // String[] commands = {"cat"}; // Echoes the expect-script instead of
    // running it
    return commands;
  }

  /**
   * Executes a native operating system command, which currently are one of: -
   * psql - dropdb - createdb - pg_dump - pg_restore Output of these commands is
   * captured and logged.
   * 
   * @param command String of command to execute
   * @return 0 if successful, any number otherwise
   * @throws IOException
   * @throws InterruptedException
   */
  protected int executeNativeCommand(String command) throws IOException,
      InterruptedException
  {
    return nativeCmdExec.executeNativeCommand(command, null, null, 0,
        getIgnoreStdErrOutput());
  }

  /**
   * Executes a native operating system command, which currently are one of:
   * 
   * <pre>
   *    - psql
   *    - dropdb
   *    - createdb
   *    - pg_dump
   *    - pg_restore
   * </pre>
   * 
   * Output of these commands is captured and logged.
   * 
   * @param commands String array of command to execute
   * @return 0 if successful, any number otherwise
   * @throws IOException
   * @throws InterruptedException
   */
  protected int executeNativeCommand(String[] commands) throws IOException,
      InterruptedException
  {
    return nativeCmdExec.executeNativeCommand(commands, null, null, 0,
        getIgnoreStdErrOutput());
  }

  /**
   * Execute a native command with careful logging of output and errors.
   * 
   * @param cmd Command to execute
   * @param inputArray Array of input lines
   * @param timeout Timeout or 0 for no timeout
   * @return True if command is successful
   */
  protected boolean safelyExecNativeCommand(String cmd, String[] inputArray,
      int timeout)
  {
    NativeCommandInputSource input = NativeCommandInputSource
        .createArrayInputSource(inputArray);
    return nativeCmdExec.safelyExecNativeCommand(cmd, input, null, timeout,
        getIgnoreStdErrOutput());
  }

  /**
   * Execute a native command with careful logging of output and errors.
   * 
   * @param cmds Command array to execute
   * @param inputArray Array of input lines
   * @param timeout Timeout or 0 for no timeout
   * @return True if command is successful
   */
  protected boolean safelyExecNativeCommand(String[] cmds, String[] inputArray,
      int timeout)
  {
    NativeCommandInputSource input = NativeCommandInputSource
        .createArrayInputSource(inputArray);
    return nativeCmdExec.safelyExecNativeCommand(cmds, input, null, timeout,
        getIgnoreStdErrOutput());
  }

  /**
   * Prints contents of error output (stderr).
   */
  protected void printErrors()
  {
    ArrayList<?> errors = nativeCmdExec.getStderr();
    Iterator<?> it = errors.iterator();
    while (it.hasNext())
    {
      String msg = (String) it.next();
      logger.info(msg);
      endUserLogger.error(msg);
    }
  }

  /**
   * Prints contents of regular output (stdout).
   */
  protected void printOutput()
  {
    ArrayList<?> errors = nativeCmdExec.getStderr();
    Iterator<?> it = errors.iterator();
    while (it.hasNext())
    {
      String msg = (String) it.next();
      logger.info(msg);
      endUserLogger.error(msg);
    }
  }

  /**
   * Allow to parse PostgreSQL URL.
   */
  protected class PostgreSQLUrlInfo
  {
    private boolean isLocal;
    private String  host;
    private String  port;
    private String  dbName;

    // Used to parse url
    // private Pattern pattern =
    // Pattern.compile("jdbc:postgresql:((//([a-zA-Z0-9_\\-.]+|\\[[a-fA-F0-9:]+])((:(\\d+))|))/|)([a-zA-Z][a-zA-Z0-9_]*)");
    private Pattern pattern = Pattern
                           .compile(getJdbcUrlPrefix() + "((//([a-zA-Z0-9_\\-.]+|\\[[a-fA-F0-9:]+])((:(\\d+))|))/|)([^\\s?]*).*$");
    Matcher         matcher;

    /**
     * Creates a new <code>PostgreSQLUrlInfo</code> object, used to parse the
     * postgresql jdbc options. If host and/or port aren't specified, will
     * default to localhost:5432. Note that database name must be specified.
     * 
     * @param url the Postgresql jdbc url to parse
     */
    public PostgreSQLUrlInfo(String url)
    {
      matcher = pattern.matcher(url);

      if (matcher.matches())
      {
        if (matcher.group(3) != null)
          host = matcher.group(3);
        else
          host = DEFAULT_POSTGRESQL_HOST;

        if (matcher.group(6) != null)
          port = matcher.group(6);
        else
          port = String.valueOf(getDefaultPort());

        dbName = matcher.group(7);
      }
    }

    /**
     * Gets the HostParameters of this postgresql jdbc url as a String that can
     * be used to pass into cmd line/shell calls.
     * 
     * @return a string that can be used to pass into a cmd line/shell call.
     */
    public String getHostParametersString()
    {
      if (isLocal)
      {
        return "";
      }
      else
      {
        return "-h " + host + " -p " + port;
      }
    }

    /**
     * Gets the database name part of this postgresql jdbc url.
     * 
     * @return the database name part of this postgresql jdbc url.
     */
    public String getDbName()
    {
      return dbName;
    }

    /**
     * Gets the host part of this postgresql jdbc url.
     * 
     * @return the host part of this postgresql jdbc url.
     */
    public String getHost()
    {
      return host;
    }

    /**
     * Gets the port part of this postgresql jdbc url.
     * 
     * @return the port part of this postgresql jdbc url.
     */
    public String getPort()
    {
      return port;
    }

    /**
     * Checks whether this postgresql jdbc url refers to a local db or not, i.e.
     * has no host specified, e.g. jdbc:postgresql:myDb.
     * 
     * @return true if this postgresql jdbc url has no host specified, i.e.
     *         refers to a local db.
     */
    public boolean isLocal()
    {
      return isLocal;
    }

  }
}
