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
 * Contributor(s): Mathieu Peltier, Nicolas Modrzyk, Duncan Smith.
 */

package org.continuent.sequoia.controller.core;

import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.Hashtable;

import javax.management.ObjectName;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.continuent.sequoia.common.authentication.PasswordAuthenticator;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.JmxException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.net.SSLConfiguration;
import org.continuent.sequoia.controller.core.security.ControllerSecurityManager;
import org.continuent.sequoia.controller.jmx.HttpAdaptor;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.jmx.RmiConnector;
import org.continuent.sequoia.controller.monitoring.datacollector.DataCollector;
import org.continuent.sequoia.controller.xml.ControllerParser;

/**
 * The <code>ControllerConfiguration</code> class prepares a
 * <code>Controller</code> object by configurating ports, security, loaded
 * databases.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:duncan@mightybot.com">Duncan Smith </a>
 * @version 1.0
 */
public class ControllerConfiguration extends Hashtable
{
  private static final long  serialVersionUID   = -3766086549425915891L;

  /**
   * The different fields that can be set on the command line.
   */
  /** The Rmi port value */
  public static final String RMI_PORT           = "rmiPort";

  /** The jmx port value */
  public static final String JMX_PORT           = "jmxPort";

  /** The jmx enable value */
  public static final String JMX_ENABLE         = "jmxEnable";

  /** The xml file possibly used to configure controller */
  public static final String XML_FILE           = "xmlFile";

  /** The NIC IP address to bind the controller to */
  public static final String CONTROLLER_IP      = "controllerIP";

  /** The controller port number */
  public static final String CONTROLLER_PORT    = "controllerPort";

  /** The controller backlog size */
  public static final String CONTROLLER_BACKLOG = "controllerBackLogSize";

  /** Add driver enable */
  public static final String ADD_DRIVER_ENABLE  = "addDriverEnable";

  /** Logger instance. */
  static Trace               logger             = Trace
                                                    .getLogger(Controller.class
                                                        .getName());
  static Trace               endUserLogger      = Trace
                                                    .getLogger("org.continuent.sequoia.enduser");

  private Controller         controller         = null;

  /**
   * Configure the controller with parameters
   * 
   * @param args parameters from the command line
   */
  public ControllerConfiguration(String[] args)
  {
    System.setProperty("org.xml.sax.driver",
        "org.apache.crimson.parser.XMLReaderImpl");

    this.put(CONTROLLER_IP, ControllerConstants.DEFAULT_IP);
    this.put(CONTROLLER_PORT, "" + ControllerConstants.DEFAULT_PORT);
    this.put(CONTROLLER_BACKLOG, "" + ControllerConstants.DEFAULT_BACKLOG_SIZE);

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
      String msg = Translate.get("controller.configure.commandline.error");
      logger.fatal(msg, e);
      endUserLogger.fatal(msg, e);
      printUsage(options);
      Runtime.getRuntime().exit(1);
    }

    // Non-recognized options
    int n = commandLine.getArgs().length;
    for (int i = 0; i < n; i++)
    {
      String msg = Translate.get("controller.configure.unknown.option",
          commandLine.getArgs()[i]);
      logger.fatal(msg);
      endUserLogger.fatal(msg);
      printUsage(options);
      Runtime.getRuntime().exit(1);
    }
    // Handle --help option
    if (commandLine.hasOption('h'))
    {
      if (commandLine.getOptions().length > 1)
      {
        String msg = Translate.get("controller.configure.commandline.error");
        logger.fatal(msg);
        endUserLogger.fatal(msg);
      }
      printUsage(options);
      Runtime.getRuntime().exit(1);
    }

    // Handle --version option
    if (commandLine.hasOption('v'))
    {
      if (commandLine.getOptions().length > 1)
      {
        String msg = Translate.get("controller.configure.commandline.error");
        logger.fatal(msg);
        endUserLogger.fatal(msg);
        printUsage(options);
      }
      else
      {
        ControllerInfo.printOn(System.out);
      }
      Runtime.getRuntime().exit(1);
    }

    // Handle -f option
    if (commandLine.hasOption('f'))
    {
      String filePath = commandLine.getOptionValue('f');
      File f = new File(filePath);
      logger.debug(f.getAbsolutePath());
      if (f.exists() == false || f.isFile() == false)
      {
        String msg = Translate.get(
            "controller.configure.optional.file.invalid", filePath);
        logger.fatal(msg);
        endUserLogger.fatal(msg);
        System.exit(1);
      }
      else
        this.put(XML_FILE, filePath);
    }
    else
    // default controller configuration file
    {
      URL defaultControllerXmlFile = ControllerConfiguration.class
          .getResource("/" + ControllerConstants.DEFAULT_CONFIG_FILE);
      if (defaultControllerXmlFile == null)
      {
        String msg = Translate
            .get("controller.configure.file.not.in.classpath");
        logger.fatal(msg);
        endUserLogger.fatal(msg);
        System.exit(1);
      }
      else
      {
        String file = URLDecoder.decode(defaultControllerXmlFile.getFile());
        this.put(XML_FILE, file);
      }
    }

    // Handle -rmi option
    if (commandLine.hasOption('r'))
    {
      String s = commandLine.getOptionValue('r');
      if (s != null)
      {
        this.put(JMX_ENABLE, "true");
        this.put(RMI_PORT, s);
        this.put(JmxConstants.ADAPTOR_TYPE_RMI, s);
      }
    }

    // Handle -jmx option
    if (commandLine.hasOption('j'))
    {
      String s = commandLine.getOptionValue('j');
      if (s != null)
      {
        this.put(JMX_ENABLE, "true");
        this.put(JMX_PORT, s);
        this.put(JmxConstants.ADAPTOR_TYPE_HTTP, s);
      }
    }

    // Handle --ip option
    if (commandLine.hasOption('i'))
    {
      String ipAddress = commandLine.getOptionValue('i');
      if (ipAddress != null)
        this.put(CONTROLLER_IP, ipAddress);
    }

    // Handle --port option
    if (commandLine.hasOption('p'))
    {
      String port = commandLine.getOptionValue('p');
      if (port != null)
        this.put(CONTROLLER_PORT, port);
    }
  }

  /**
   * This method is going to call a <code>ControllerParser</code> object to
   * configure controller while parsing file. This method will call <method>
   * setUpRmi() </method> and <method>setUpJmx() </method> as well as <method>
   * setUpVirtualDatabases </method> while parsing.
   * 
   * @param filename path to the xml file to parse from
   * @throws Exception if configuration fails
   */
  public void setUpByXml(String filename) throws Exception
  {
    logger.info(Translate.get("controller.configure.loading.file", filename));
    FileReader fileReader = null;
    try
    {
      fileReader = new FileReader(filename);
      ControllerParser cparser = new ControllerParser(this);
      cparser.readXML(fileReader, true);
      fileReader.close();
    }
    catch (Exception e)
    {

      logger.warn(Translate.get("controller.configure.xml.file.error", e), e);
      throw e;
    }
    finally
    {
      if (fileReader != null)
        fileReader.close();
    }
  }

  /**
   * Test if there is a file to take configuration from, if so call <method>
   * setUpByXml() </method>
   * 
   * @return an instanciated and configured object of class
   *         <code>Controller</code>
   * @throws Exception if configuration fails
   */
  private Controller setup() throws Exception
  {
    String xml = (String) this.get(XML_FILE);

    int portNumber = Integer.parseInt((String) this.get(CONTROLLER_PORT));
    int backlog = Integer.parseInt((String) this.get(CONTROLLER_BACKLOG));
    /**
     * @see org.continuent.sequoia.controller.xml.ControllerParser#configureController(Attributes)
     *      for how CONTROLLER_IP is set (unfortunately not the bare user's
     *      entry).
     */
    String ipAddress = (String) this.get(CONTROLLER_IP);

    controller = new Controller(ipAddress, portNumber, backlog);
    controller.setConfiguration(this);
    org.continuent.sequoia.controller.management.Controller managedController = new org.continuent.sequoia.controller.management.Controller(
        controller);
    ObjectName name = JmxConstants.getControllerObjectName();
    MBeanServerManager.registerMBean(managedController, name);
    controller.setNotificationBroadcasterSupport(managedController
        .getBroadcaster());

    if (xml != null)
    {
      try
      {
        setUpByXml(xml);
      }
      catch (Exception e)
      {
        logger.error(Translate.get(
            "controller.configure.load.file.failed.minimum.configuration",
            new String[]{xml, e.getMessage()}), e);
      }
    }
    else
      setUpJmx();

    return this.controller;
  }

  /**
   * Retrieve the controller associated with this
   * <code>ControllerConfiguration</code> instance.
   * 
   * @return <code>Controller</code> object. Can be null if this method is
   *         called before setup
   * @throws Exception if an error occurs
   */
  public Controller getController() throws Exception
  {
    if (controller == null)
      setup();
    return this.controller;
  }

  /**
   * Start up the jmx services if enabled.
   * 
   * @throws JmxException an exception
   */
  public void setUpJmx() throws JmxException
  {
    boolean jmxEnable = new Boolean((String) get(JMX_ENABLE)).booleanValue();
    if (jmxEnable == false)
    {
      MBeanServerManager.setJmxEnabled(false);
      logger.info(Translate.get("jmx.configure.disabled"));
    }
    else
    {
      MBeanServerManager.setJmxEnabled(true);
      logger.info(Translate.get("jmx.configure.enabled"));
      // Create and start the JMX agent
      try
      {
        new DataCollector(controller);
        String hostIP = controller.getIPAddress();

        logger.info(Translate.get("controller.configure.start.jmx", hostIP));

        if (this.containsKey(JmxConstants.ADAPTOR_TYPE_HTTP))
        {
          int port = Integer.parseInt((String) this
              .get(JmxConstants.ADAPTOR_TYPE_HTTP));
          HttpAdaptor http = new HttpAdaptor(hostIP, port, null);
          http.start();
        }
        if (this.containsKey(JmxConstants.ADAPTOR_TYPE_RMI))
        {
          SSLConfiguration ssl = null;
          PasswordAuthenticator authenticator = null;
          int registryPort = Integer.parseInt((String) this
              .get(JmxConstants.ADAPTOR_TYPE_RMI));
          int serverPort = 0;
          if (this.containsKey(JmxConstants.ADAPTOR_TYPE_RMI_SERVER))
          {
            String rmiServerPort = (String) this
                .get(JmxConstants.ADAPTOR_TYPE_RMI_SERVER);
            try
            {
              serverPort = Integer.parseInt(rmiServerPort);
            }
            catch (NumberFormatException nfe)
            {
              logger.warn("Could not parse RMI Server Port '" + rmiServerPort);
            }
          }
          if (this.containsKey(JmxConstants.CONNECTOR_AUTH_USERNAME))
          {
            String username = (String) this
                .get(JmxConstants.CONNECTOR_AUTH_USERNAME);
            String password = (String) this
                .get(JmxConstants.CONNECTOR_AUTH_PASSWORD);
            authenticator = new PasswordAuthenticator(username, password);
          }
          if (this.containsKey(JmxConstants.CONNECTOR_RMI_SSL))
          {
            ssl = (SSLConfiguration) this.get(JmxConstants.CONNECTOR_RMI_SSL);
          }
          RmiConnector rmi = new RmiConnector(controller.getControllerName(),
              hostIP, registryPort, serverPort, authenticator, ssl);
          rmi.start();
        }
        logger.debug(Translate.get("controller.configure.jmx.started"));
      }
      catch (Exception e)
      {
        logger
            .error(Translate.get("controller.configure.jmx.fail.start", e), e);
      }
    }
    controller.setJmxEnable(jmxEnable);
  }

  /**
   * Set up security settings if needed here.
   * 
   * @param security to enforce
   */
  public void setUpSecurity(ControllerSecurityManager security)
  {
    controller.setSecurity(security);
  }

  /**
   * Will load the <code>VirtualDatabase</code> configuration into the
   * controller.
   * 
   * @param filePath the path to xml definition of the virtual database
   * @param virtualName the name of the virtualDatabase to load
   * @param autoLoad specified if backend should be enabled.
   * @param checkPoint the check point to load the database from.
   */
  public void setUpVirtualDatabase(String filePath, String virtualName,
      int autoLoad, String checkPoint)
  {
    try
    {
      controller.loadXmlConfiguration(filePath, virtualName, autoLoad,
          checkPoint);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("controller.configure.file.autoload",
            new String[]{filePath, "" + autoLoad}));

    }
    catch (Exception e)
    {
      logger.error(Translate.get("controller.configure.load.file.failed",
          new String[]{filePath, e.getMessage()}), e);
    }
  }

  /**
   * Displays usage message.
   * 
   * @param options available command line options
   */
  private static void printUsage(Options options)
  {
    String header = Translate.get("controller.commandline.header",
        ControllerConstants.PRODUCT_NAME);
    header += System.getProperty("line.separator");
    header += Translate.get("controller.commandline.options");
    String footer = Translate.get("controller.commandline.footer");

    (new HelpFormatter()).printHelp(80, "controller(.sh|.bat) [options]",
        header, options, footer);
  }

  /**
   * Creates <code>Options</code> object that contains all available options
   * that can be used launching Sequoia controller.
   * 
   * @return an <code>Options</code> instance
   */
  private static Options createOptions()
  {
    Options options = new Options();
    OptionGroup group = new OptionGroup();

    // help and verbose options
    group.addOption(new Option("h", "help", false, Translate
        .get("controller.commandline.option.help")));
    group.addOption(new Option("v", "version", false, Translate
        .get("controller.commandline.option.version")));
    options.addOptionGroup(group);

    // RMI port option
    options.addOption(new Option("r", "rmi", true, Translate.get(
        "controller.commandline.option.rmi", ""
            + JmxConstants.DEFAULT_JMX_RMI_PORT)));
    // JMX port option
    options.addOption(new Option("j", "jmx", true, Translate.get(
        "controller.commandline.option.jmx", ""
            + JmxConstants.DEFAULT_JMX_HTTP_PORT)));

    // IP option
    String defaultIp = "127.0.0.1";
    try
    {
      defaultIp = InetAddress.getLocalHost().getHostAddress();
    }
    catch (UnknownHostException e)
    {

    }
    options.addOption(new Option("i", "ip", true, Translate.get(
        "controller.commandline.option.ip", "" + defaultIp)));

    // Port options
    options.addOption(new Option("p", "port", true, Translate.get(
        "controller.commandline.option.port", ""
            + ControllerConstants.DEFAULT_PORT)));

    // configuration file option
    options.addOption(new Option("f", "file", true, Translate
        .get("controller.commandline.option.file")));

    return options;
  }
}