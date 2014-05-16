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

package org.continuent.sequoia.console.text;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.console.jmx.RmiJmxClient;
import org.continuent.sequoia.console.text.module.AbstractConsoleModule;

/**
 * This class defines a ConsoleLauncher
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:mathieu.peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public class ConsoleLauncher
{
  /**
   * Product name to be displayed at console startup
   */
  public static final String PRODUCT_NAME;

  static
  {
    Properties props = new Properties();
    try
    {
      String propertiesFile = System.getProperty("console.commands", //$NON-NLS-1$
          AbstractConsoleModule.DEFAULT_COMMAND_PROPERTIES_FILE);
      props.load(ClassLoader.getSystemResourceAsStream(propertiesFile));
      PRODUCT_NAME = props.getProperty("product.name"); //$NON-NLS-1$
    }
    catch (IOException e)
    {
      throw new ExceptionInInitializerError(e.getMessage());
    }
  }

  /**
   * Launchs the Sequoia console. The available options are: <il>
   * <li><code>-d</code> or <code>--debug</code>: show stack trace when
   * error occurs.</li>
   * <li><code>-f</code> or <code>--file</code>: use a given file as the
   * source of commands instead of reading commands interactively.</li>
   * <li><code>-h</code> or <code>--help</code>: displays usage
   * information.</li>
   * <li><code>-i</code> or <code>--ip</code>: IP address of the host name
   * where the JMX Server hosting the controller is running (the default is
   * '0.0.0.0').</li>
   * <li><code>-n</code> or <code>--nocolor</code>: do not print colors in
   * interactive mode for supported systems.</li>
   * <li><code>-e</code> or <code>--exitonerror</code>: exit on error in
   * non interactive mode.</li>
   * <li><code>-p</code> or <code>--port</code>: JMX/RMI Port number of
   * (the default is
   * {@link org.continuent.sequoia.common.jmx.JmxConstants#DEFAULT_JMX_RMI_PORT}).
   * </li>
   * <li><code>-s</code> or <code>--secret</code>: password for JMX
   * connection.</li>
   * <li><code>-u</code> or <code>--username</code>: username for JMX
   * connection.</li>
   * <li><code>-m</code> or <code>--multiline</code>: enable multiline
   * statements in the SQL console (disabled by default for backwards
   * compatibility)</li>
   * <li><code>-r</code> or <code>--requestdelimiter</code>: Request
   * delimiter to use when multiline statement is enabled (<code>;</code> by
   * default) connection.</li>
   * <li><code>-v</code> or <code>--version</code>: displays version
   * information.</li>
   * </ul>
   * 
   * @param args command line arguments (see above)
   * @throws Exception if fails
   */
  public static void main(String[] args) throws Exception
  {
    // Create options object
    Options options = createOptions();

    // Parse command line
    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = null;
    try
    {
      commandLine = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.err.println("Syntax error (" + e + ")");
      printUsage(options);
      System.exit(1);
    }

    // Non-recognized options
    int n = commandLine.getArgs().length;
    for (int i = 0; i < n; i++)
    {
      System.err.println("Syntax error (unrecognized option: "
          + commandLine.getArgs()[i] + ")");
      printUsage(options);
      System.exit(1);
    }

    // Handle --help option
    if (commandLine.hasOption('h'))
    {
      if (commandLine.getOptions().length > 1)
        System.err.println("Syntax error");

      printUsage(options);
      System.exit(1);
    }

    // Handle --version option
    if (commandLine.hasOption('v'))
    {
      if (commandLine.getOptions().length > 1)
      {
        System.err.println("Syntax error");
        printUsage(options);
      }
      else
        System.out.println(ConsoleTranslate.get("console.version",
            new String[]{PRODUCT_NAME, Constants.VERSION}));

      System.exit(1);
    }

    startTextConsole(commandLine);
  }

  /**
   * Starts the text console with the given commandline
   * 
   * @param commandLine parameters for the text console
   * @throws Exception if fails
   */
  public static void startTextConsole(CommandLine commandLine) throws Exception
  {
    // check if we are in interactive mode, and if so, output no traces
    boolean isInteractive = !commandLine.hasOption('f');

    // Handle --ip option
    String ip;
    try
    {
      // localhost
      ip = InetAddress.getByName(null).getHostName();
    }
    catch (UnknownHostException e1)
    {
      // not IPv6 compliant, but "null" is never unknown anyway
      ip = "127.0.0.1";
    }
    if (commandLine.hasOption('i'))
    {
      String tmp = commandLine.getOptionValue('i');
      if (tmp != null)
      {
        ip = tmp;
      }
    }

    boolean exitOnError = commandLine.hasOption('e');

    // Handle --debug option
    boolean debug = commandLine.hasOption('d');
    // Handle --silent option
    boolean silent = commandLine.hasOption('l');

    // Launch the console (handle --file option)
    Console console;
    InputStream in = null;
    if (commandLine.hasOption('f'))
    {
      String filename = commandLine.getOptionValue('f');
      if ("-".equals(filename))
      {
        in = System.in;
      }
      else
      {
        try
        {
          in = new FileInputStream(filename);
        }
        catch (FileNotFoundException e)
        {
          System.err.println("Failed to open file '" + filename + "' (" + e
              + ")");
          System.exit(1);
        }
      }
    }
    else
    {
      System.out.println(ConsoleTranslate.get(
          "console.interactive.mode", PRODUCT_NAME)); //$NON-NLS-1$
      in = System.in;
    }
    
    RmiJmxClient jmxClient = null;
    boolean sqlClientOnly = commandLine.hasOption('q');
    if (! sqlClientOnly)
    {
    	jmxClient = getJmxClient(commandLine, ip, exitOnError);
    }
    console = new Console(jmxClient, in, isInteractive, debug, silent,
        exitOnError, sqlClientOnly);
    console.setPrintColor(!commandLine.hasOption('n'));
    console.enableMultilineStatements(commandLine.hasOption('m'));
    if (commandLine.hasOption('r'))
    {
      console.setRequestDelimiter(commandLine.getOptionValue('r'));
    }
    console.handlePrompt();
    System.exit(0);
  }

  static RmiJmxClient getJmxClient(CommandLine commandLine, String ip,
			boolean exitOnError) throws IOException
  {
		// Handle --port option
		int port;
		if (commandLine.hasOption('p')) {
			String s = commandLine.getOptionValue('p');
			if (s == null) {
				port = JmxConstants.DEFAULT_JMX_RMI_PORT;
			} else
				try {
					port = Integer.parseInt(s);
				} catch (NumberFormatException e) {
					System.err.println("Bad port number (" + e
							+ "), using default "
							+ JmxConstants.DEFAULT_JMX_RMI_PORT
							+ " port number");
					port = JmxConstants.DEFAULT_JMX_RMI_PORT;
				}
		} else {
			port = JmxConstants.DEFAULT_JMX_RMI_PORT;
		}
		// Handle --secret and --username options
		RmiJmxClient jmxClient = null;
		if (commandLine.hasOption('u') && commandLine.hasOption('s')) {
			String username = commandLine.getOptionValue('u');
			String password = commandLine.getOptionValue('s');
			jmxClient = new RmiJmxClient("" + port, ip, username, password);
		} else {
			try {
				jmxClient = new RmiJmxClient("" + port, ip, null);
			} catch (Exception e) {
				System.err
						.println("Cannot connect to the administration port of the controller. Is a controller running at "
								+ ip + ":" + port + " ?");
				// SEQUOIA-714: exit the console if the flag ''--exitonerror' is
				// set
				if (exitOnError) {
					System.exit(1);
				}
				// SEQUOIA-708: let the console start if the user wants to use
				// the SQL
				// console.
			}
		}

		return jmxClient;
	}
  
  /**
   * Creates <code>Options</code> object that contains all available options
   * that can be used launching Sequoia console.
   * 
   * @return an <code>Options</code> instance
   */
  private static Options createOptions()
  {
    Options options = new Options();
    OptionGroup group = new OptionGroup();

    // help, verbose, text only console and file options (mutually exclusive
    // options)
    group.addOption(new Option("h", "help", false,
        "Displays usage information."));
    group.addOption(new Option("t", "text", false,
        "Ignored - only for previous version compatibility."));
    group.addOption(new Option("v", "version", false,
        "Displays version information."));
    group
        .addOption(new Option(
            "f",
            "file",
            true,
            "Use a given file as the source of commands instead of reading commands interactively."));
    options.addOptionGroup(group);

    /**
     * Controller JMX ip option, defined in
     * {@link org.continuent.sequoia.controller.core.ControllerConstants.DEFAULT_IP}
     */
    String defaultIp = "0.0.0.0";
    // should probably better be: InetAddress.anyLocalAddress().getHostAddress()
    options.addOption(new Option("i", "ip", true,
        "The JMX server of the controller binds to this address (the default is '"
            + defaultIp + "')."));

    // controller port option
    options.addOption(new Option("p", "port", true,
        "JMX/RMI port number of (the default is "
            + JmxConstants.DEFAULT_JMX_RMI_PORT + ")."));

    // JMX options
    options.addOption(new Option("u", "username", true,
        "Username for JMX connection."));
    options.addOption(new Option("s", "secret", true,
        "Password for JMX connection."));

    options.addOption(new Option("d", "debug", false,
        "Show stack trace when error occurs."));
    options.addOption(new Option("l", "silent", false,
        "Show only most meaningful messages."));

    options.addOption(new Option("n", "nocolor", false,
        "Do not print colors in interactive mode for supported systems."));

    options.addOption(new Option("e", "exitonerror", false,
        "Stop on error in non interactive mode."));

    options.addOption(new Option("r", "requestdelimiter", true,
        "Request delimiter for multiline statements in the SQL console."));

    options.addOption(new Option("m", "multiline", false,
        "Enable multiline statements in the SQL console."));

    options.addOption(new Option("q", "sqlclient", false,
    "Launch SQL client console."));

    return options;
  }

  /**
   * Displays usage message.
   * 
   * @param options available command line options
   */
  private static void printUsage(Options options)
  {
    String header = ConsoleTranslate.get("console.launches", PRODUCT_NAME)
        + System.getProperty("line.separator") + "Options:";

    (new HelpFormatter()).printHelp(80, "console(.sh|.bat) [options]", header,
        options, "");
  }

}