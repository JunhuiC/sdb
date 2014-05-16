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
 * Free Software Foundation; either version 2.1 of the License, or any later
 * version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General public final License
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General public final License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *
 * Initial developer(s): Emmanuel Cecchet. 
 * Contributor(s): Mathieu Peltier, Nicolas Modrzyk.
 */

package org.continuent.sequoia.controller.core;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.notifications.SequoiaNotificationList;
import org.continuent.sequoia.common.log.LogManager;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.common.xml.ControllerXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.common.xml.XmlTools;
import org.continuent.sequoia.controller.core.security.ControllerSecurityManager;
import org.continuent.sequoia.controller.core.shutdown.ControllerShutdownHook;
import org.continuent.sequoia.controller.core.shutdown.ControllerShutdownThread;
import org.continuent.sequoia.controller.interceptors.InterceptorConfigurator;
import org.continuent.sequoia.controller.interceptors.InterceptorException;
import org.continuent.sequoia.controller.interceptors.impl.ControllerPropertyInterceptorConfigurator;
import org.continuent.sequoia.controller.interceptors.impl.InterceptorManagerAdapter;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.jmx.RmiConnector;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.xml.DatabasesParser;

/**
 * The Sequoia controller main class. It loads its configuration file and wait
 * for virtual database to be loaded.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:duncan@mightybot.com">Duncan Smith </a>
 * @version 1.0
 */
public final class Controller implements XmlComponent
{

  /** Sequoia controller port number listening for driver connections */
  private int                              portNumber;
  private int                              backlogSize;

  /**
   * The IP address to bind the controller to. Useful for machines that contain
   * multiple network interface cards and wish to bind to a specific card.
   * Default evaluates to localhost IP address (127.0.0.1).
   */
  private String                           ipAddress;

  /** Thread that listens for driver connections */
  private ControllerServerThread           connectionThread;

  /** Logger instances. */
  static Trace                             logger               = Trace
                                                                    .getLogger("org.continuent.sequoia.controller.core.Controller");
  static Trace                             endUserLogger        = Trace
                                                                    .getLogger("org.continuent.sequoia.enduser");

  /** Hashtable of <code>VirtualDatabase</code> objects. */
  private Hashtable                        virtualDatabases;

  /** Hashtable of options */
  private Hashtable                        configuration;

  /** Security Manager */
  private ControllerSecurityManager        security;

  /** Report Manager */
  private ReportManager                    report;

  private boolean                          isShuttingDown;

  protected NotificationBroadcasterSupport notificationBroadcasterSupport;

  protected int                            notificationSequence = 0;

  /* Constructor(s) */

  /**
   * Creates a new <code>Controller</code> instance.
   * 
   * @param ipAddress bind the controller to this ipAddress
   * @param port bind the controller to listen to this port
   * @param backlog backlog connection size
   */
  public Controller(String ipAddress, int port, int backlog)
  {
    virtualDatabases = new Hashtable();
    this.ipAddress = ipAddress;
    this.portNumber = port;
    this.backlogSize = backlog;
  }

  /**
   * Sets the NotificationBroadcasterSupport associated with the MBean managing
   * this controller.
   * 
   * @param notificationBroadcasterSupport the notificationBroadcasterSupport
   *          associated with the mbean managing this controller
   */
  public void setNotificationBroadcasterSupport(
      NotificationBroadcasterSupport notificationBroadcasterSupport)
  {
    this.notificationBroadcasterSupport = notificationBroadcasterSupport;
  }

  /**
   * Sends a JMX Notification on behalf of the MBean associated with this
   * controller
   * 
   * @param type type of the JMX notification
   * @param message message associated with the notification
   * @see SequoiaNotificationList
   */
  protected void sendJmxNotification(String type, String message)
  {
    if (!MBeanServerManager.isJmxEnabled())
    {
      // do not send jmx notification if jmx is not enabled
      return;
    }
    try
    {
      notificationBroadcasterSupport.sendNotification(new Notification(type,
          JmxConstants.getControllerObjectName(), notificationSequence++,
          message));
    }
    catch (MalformedObjectNameException e)
    {
      // unable to get a correct controller object name: do nothing
      logger.warn("Unable to send JMX notification", e);
    }
  }

  //
  // Virtual databases management
  //

  /**
   * Adds virtual databases contained in the XML document given as a String. If
   * a virtual database name is provided, only this database is loaded with the
   * provided autoLoad and checkpoint information.
   * 
   * @param xml XML configuration file content
   * @param vdbName optional virtual database name to autoload
   * @param autoEnable autoenable backend mode for virtual database (or init)
   * @param checkpoint checkpoint name if autoEnable is set to force
   * @throws ControllerException if an error occurs
   */
  public void addVirtualDatabases(String xml, String vdbName, int autoEnable,
      String checkpoint) throws ControllerException
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("controller.add.virtualdatabase", vdbName));
    if (vdbName != null && this.hasVirtualDatabase(vdbName))
    {
      throw new ControllerException(Translate.get(
          "controller.add.virtualdatabase.already.used", vdbName));
    }
    try
    {
      DatabasesParser parser = new DatabasesParser(this, vdbName, autoEnable,
          checkpoint);
      parser.readXML(xml, true);
    }
    catch (Exception e)
    {
      String msg = Translate.get("controller.add.virtualdatabases.failed",
          e.getMessage());
      logger.warn(msg, e);
      throw new ControllerException(msg);
    }
  }

  /**
   * Register a VirtualDatabase with default options
   * 
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#addVirtualDatabases(String)
   */
  public void addVirtualDatabases(String xml) throws ControllerException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate.get("controller.loading.virtualdatabase"));
    }
    this.addVirtualDatabases(xml, null, ControllerConstants.AUTO_ENABLE_FALSE,
        null);
  }

  /**
   * Registers a new <code>VirtualDatabase</code> in this controller.
   * 
   * @param vdb the <code>VirtualDatabase</code> to register
   * @throws ControllerException if an error occurs
   */
  public void addVirtualDatabase(VirtualDatabase vdb)
      throws ControllerException
  {
    this.addVirtualDatabase(vdb, ControllerConstants.AUTO_ENABLE_FALSE, null);
  }

  /**
   * Add the virtual database with the specified options
   * 
   * @param vdb the <code>VirtualDatabase</code> object to add
   * @param autoLoad specified if backends should be enabled
   * @param checkPoint specified the checkPoint to recover from, leave null if
   *          no recovery speficied
   * @throws ControllerException if database already exists on the specified
   *           <code>Controller</code> object
   */
  public synchronized void addVirtualDatabase(VirtualDatabase vdb,
      int autoLoad, String checkPoint) throws ControllerException
  {
    // Add the database or retrieve it if it already exists
    if (hasVirtualDatabase(vdb.getDatabaseName()))
    {
      String msg = Translate.get(
          "controller.add.virtualdatabase.already.used", vdb.getDatabaseName());
      logger.warn(msg);
      throw new ControllerException(msg);
    }
    else
    {
      vdb.start();
      virtualDatabases.put(vdb.getDatabaseName(), vdb);
    }

    // Enable backends with the proper states
    try
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("controller.database.autoenable", autoLoad));

      switch (autoLoad)
      {
        case ControllerConstants.AUTO_ENABLE_TRUE :
          vdb.enableAllBackendsFromCheckpoint();
          break;
        case ControllerConstants.AUTO_ENABLE_FALSE :
          break;
        case ControllerConstants.AUTO_ENABLE_FORCE :
          logger.warn("Backends enabled in force mode from checkpoint "
              + checkPoint);
          vdb.forceEnableAllBackendsFromCheckpoint(checkPoint);
          break;
        case ControllerConstants.AUTO_ENABLE_INIT :
        case ControllerConstants.AUTO_ENABLE_FORCE_LOAD :
          break;
        default :
          logger
              .error("Unsupported autoEnabledBackends mode in controller configuration");
          break;
      }
    }
    catch (VirtualDatabaseException e)
    {
      logger
          .warn("Failed to automatically enable backends, manual resynchronization is probably needed");
    }

    logger.info(Translate.get("controller.add.virtualdatabase", vdb
        .getDatabaseName()));
    endUserLogger.info(Translate.get(
        "controller.add.virtualdatabase.success", vdb.getDatabaseName()));
    sendJmxNotification(
        SequoiaNotificationList.CONTROLLER_VIRTUALDATABASE_ADDED,
          Translate.get("notification.virtualdatabase.added", vdb
            .getDatabaseName()));
  }

  /**
   * Gets the <code>VirtualDatabase</code> object corresponding to a virtual
   * database name.
   * 
   * @param virtualDatabaseName the virtual database name
   * @return a <code>VirtualDatabase</code> object or null if not found
   */
  public VirtualDatabase getVirtualDatabase(String virtualDatabaseName)
  {
    return (VirtualDatabase) virtualDatabases.get(virtualDatabaseName);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getVirtualDatabaseNames()
   */
  public ArrayList getVirtualDatabaseNames()
  {
    ArrayList result = new ArrayList();
    for (Iterator iter = virtualDatabases.values().iterator(); iter.hasNext();)
      result.add(((VirtualDatabase) iter.next()).getVirtualDatabaseName());
    return result;
  }

  /**
   * Returns information about the available virtual databases.
   * 
   * @return ArrayList of information about virtual databases.
   */
  public ArrayList getVirtualDatabases()
  {
    return new ArrayList(virtualDatabases.values());
  }

  /**
   * Tests if a <code>VirtualDatabase</code> of a given name exists in this
   * controller.
   * 
   * @param name the virtual database name
   * @return <code>true</code> if the virtual database exists
   */
  public boolean hasVirtualDatabase(String name)
  {
    return virtualDatabases.containsKey(name);
  }

  /**
   * Removes the virtual database with the given name (if any)
   * 
   * @param virtualname name of the virtual database
   * @return a translated string for success message
   */
  public String removeVirtualDatabase(String virtualname)
  {
    if (hasVirtualDatabase(virtualname))
    {
      if (this.virtualDatabases.remove(virtualname) == null)
      {
        logger.warn("Unexpected missing virtual database named " + virtualname
            + " while removing from virtual database list");
      }

      // Send notification
      sendJmxNotification(
          SequoiaNotificationList.CONTROLLER_VIRTUALDATABASE_REMOVED,
          Translate.get("notification.virtualdatabase.shutdown",
              virtualname));
    }
    return Translate.get("controller.removeVirtualDatabase.success",
        virtualname);
  }

  //
  // Controller operations
  //

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#addDriver(byte[])
   */
  public void addDriver(byte[] bytes) throws Exception
  {
    // Try to find drivers directory in the classpath
    File driversDirectory = null;
    URL url = Controller.class
        .getResource(ControllerConstants.SEQUOIA_DRIVER_JAR_FILE);
    boolean error = false;
    if (url != null)
    {
      driversDirectory = (new File(URLDecoder.decode(url.getFile())))
          .getParentFile();
      error = (driversDirectory == null) || !driversDirectory.exists();
    }

    if (error)
    {
      String msg = Translate.get("controller.driver.dir.not.found",
          driversDirectory.toString());
      logger.error(msg);
      endUserLogger.error(Translate.get("controller.add.driver.failed",
          msg));
      throw new ControllerException(msg);
    }

    // Read the array of bytes to a file
    File temp = null;
    try
    {
      temp = File.createTempFile("driver", "zip", driversDirectory);
      FileOutputStream output = new FileOutputStream(temp);
      output.write(bytes);
      output.close();
    }
    catch (IOException e)
    {
      String msg = Translate.get("controller.add.jar.write.failed", e);
      logger.error(msg);
      endUserLogger.error(Translate.get("controller.add.driver.failed",
          msg));
      throw new ControllerException(msg);
    }

    // Unzip the file content
    try
    {
      Enumeration entries;
      ZipFile zipFile = new ZipFile(temp);

      // Read the file
      int lenght;
      InputStream in;
      BufferedOutputStream out;
      byte[] buffer = new byte[1024];

      entries = zipFile.entries();
      while (entries.hasMoreElements())
      {
        ZipEntry entry = (ZipEntry) entries.nextElement();

        if (entry.isDirectory())
        {
          // Create the directory
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("controller.add.jar.extract.dir", entry
                .getName()));

          (new File(driversDirectory, entry.getName())).mkdir();
          continue;
        }

        // Extract the file
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("controller.add.jar.extract.file", entry
              .getName()));

        in = zipFile.getInputStream(entry);
        out = new BufferedOutputStream(new FileOutputStream(driversDirectory
            + System.getProperty("file.separator") + entry.getName()));
        while ((lenght = in.read(buffer)) >= 0)
          out.write(buffer, 0, lenght);

        in.close();
        out.close();
      }

      zipFile.close();
      temp.delete();
      String msg = Translate.get("controller.add.driver.success",
          driversDirectory.toString());
      logger.info(msg);
      endUserLogger.info(msg);
    }
    catch (IOException e)
    {
      String msg = Translate.get("controller.driver.extract.failed",
          new String[]{temp.getCanonicalPath(), e.getMessage()});
      logger.error(msg);
      endUserLogger.error(Translate.get("controller.add.driver.failed",
          msg));
      throw new ControllerException(msg);
    }
  }

  /**
   * Read a XML configuration file and load only the
   * <code>VirtualDatabase</code> specified in the arguments list
   * 
   * @param filename XML configuration file name to take info on
   *          <code>VirtualDatabase</code>
   * @param virtualName the only database to load, null if should load all
   * @param autoLoad specifies if the backends should be enabled automatically
   *          after loading
   * @param checkPoint checkPoint to recover from when enabling backends. Leave
   *          <code>null</code> if no recovery option is needed.
   * @return a diagnostic message (success or error)
   * @throws Exception if an error occurs
   */
  public String loadXmlConfiguration(String filename, String virtualName,
      int autoLoad, String checkPoint) throws Exception
  {
    FileReader fileReader = null;
    try
    {
      filename = filename.trim();
      try
      {
        fileReader = new FileReader(filename);
      }
      catch (FileNotFoundException fnf)
      {
        return Translate.get("controller.file.not.found", filename);
      }

      if (logger.isDebugEnabled())
        logger.debug("Loading virtual database configuration " + filename);
      // Read the file
      BufferedReader in = new BufferedReader(fileReader);
      StringBuffer xml = new StringBuffer();
      String line;
      do
      {
        line = in.readLine();
        if (line != null)
          xml.append(line);
      }
      while (line != null);

      // Send it to the controller
      addVirtualDatabases(xml.toString(), virtualName, autoLoad, checkPoint);
      return Translate.get("controller.file.send", filename);
    }
    catch (Exception e)
    {
      logger.error(Translate.get("controller.loadXml.failed", ControllerConstants.PRODUCT_NAME, e), e);
      throw new ControllerException(Translate.get(
          "controller.loadXml.failed", ControllerConstants.PRODUCT_NAME, e));
    }
    finally
    {
      if (fileReader != null)
        fileReader.close();
    }
  }

  /**
   * Save current configuration of the controller to a default file
   * 
   * @return Status message
   * @throws Exception if an error occurs
   * @see org.continuent.sequoia.controller.core.ControllerConstants#getSaveFile
   */
  public String saveConfiguration() throws Exception
  {
    String msg;
    try
    {
      String configurationFile = ControllerConstants
          .getSaveFile(new SimpleDateFormat("yyyy-MM-dd-HH-mm")
              .format(new Date()));
      DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(configurationFile)));
      StringBuffer xml = new StringBuffer();
      xml.append(XmlTools.prettyXml(getXmlVirtualDatabases()));
      String prettyXml = xml.toString();
      // ugly hack to insert the doctype which has been stripped
      // when prettyfying the xml
      prettyXml = XmlTools.insertDoctype(prettyXml, ControllerConstants.VIRTUAL_DATABASE_DOCTYPE);
      dos.write(prettyXml.getBytes());
      dos.close();
      msg = Translate.get("controller.save.configuration", configurationFile);
      return msg;
    }
    catch (Exception e)
    {
      msg = Translate.get("controller.save.configuration.failed", e);
      logger.error(msg);
      throw new ControllerException(msg, e);
    }
  }

  //
  // Controller shutdown
  //
  /**
   * Create report about fatal error
   * 
   * @param fatal the cause of the fatal error
   */
  public void endOfController(Exception fatal)
  {
    endUserLogger.fatal(Translate.get("fatal.error"));
    logger.fatal(Translate.get("fatal.error"), fatal);
    if (report.isGenerateOnFatal())
    {
      new ReportManager(this).generateAndWriteException(true, fatal);
      logger.info(Translate.get("fatal.report.generated", report
          .getReportLocation()
          + File.separator + ControllerConstants.REPORT_FILE));
    }
    Runtime.getRuntime().exit(1);
  }

  /**
   * Access the connection thread. Need this for shutting down
   * 
   * @return <code>connectionThread</code>
   */
  public ControllerServerThread getConnectionThread()
  {
    return connectionThread;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#isShuttingDown()
   */
  public boolean isShuttingDown()
  {
    return isShuttingDown;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#shutdown()
   */
  public void shutdown() throws ControllerException
  {
    if (virtualDatabases.size() != 0)
    {
      String listOfActiveVdbs = "";
      for (Enumeration e = virtualDatabases.keys(); e.hasMoreElements();)
      {
        if (listOfActiveVdbs.length() != 0)
          listOfActiveVdbs = listOfActiveVdbs + ",";
        listOfActiveVdbs = listOfActiveVdbs + e.nextElement();
      }
      String msg = Translate.get(
          "controller.shutdown.error.existing.virtualdatabases", new String[]{
              getControllerName(), listOfActiveVdbs});
      throw new ControllerException(msg);
    }

    ControllerShutdownThread shutdownThread = null;
    synchronized (this)
    {
      if (isShuttingDown())
      {
        logger.info(Translate.get("controller.already.shutting.down",
            this.getControllerName()));
        return;
      }

      isShuttingDown = true;
      shutdownThread = new ControllerShutdownThread(this);
      logger.info(Translate.get("controller.shutdown", this
          .getControllerName()));
    }

    Thread thread = new Thread(shutdownThread.getShutdownGroup(),
        shutdownThread, "Controller Shutdown Thread");
    thread.start();

    try
    {
      logger.info("Waiting for controller shutdown");
      thread.join();
      logger.info(Translate.get("controller.shutdown.completed", this
          .getControllerName()));
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  /**
   * Launches the Sequoia controller. The available options are:
   * <ul>
   * <li><code>-h</code> or <code>--help</code> <code>&lt;port&gt;</code>:
   * displays usage informations.</li>
   * <li><code>-j</code> or <code>--jmx</code> <code>&lt;port&gt;</code>:
   * optinal JMX server HTTP adaptor port number.</li>
   * <li><code>-n</code> or <code>--name</code> <code>&lt;name&gt;</code>:
   * optional controller name.</li>
   * <li><code>-i</code> or <code>--ip</code> <code>&lt;ip&gt;</code>:
   * optional IP address to beind the controller to.</li>
   * <li><code>-r</code> or <code>--rmi</code> <code>&lt;port&gt;</code>:
   * optional RMI registry port number.</li>
   * <li><code>-v</code> or <code>--version</code>: displays version
   * informations.</li>
   * </ul>
   * <p>
   * The controller starts listening for socket connections on the default port.
   * Jmx is configured, and a virtual database can be added.
   * <p>
   * {@link org.continuent.sequoia.controller.core.ControllerConstants#DEFAULT_PORT}
   * Default Listening port
   * 
   * @param args command line arguments (see above)
   * @throws Exception when everything goes wrong
   */
  public static void main(String[] args) throws Exception
  {
    System.setProperty("javax.management.builder.initial",
        org.continuent.sequoia.controller.jmx.MBeanServerBuilder.class
            .getName());

    if (ControllerConstants.CONTROLLER_FACTORY == null)
    {
      System.err
          .println("Impossible to start Controller with an invalid controller.properties file.");
      System.exit(1);
    }

    // This parses command line arguments and exits for
    // --help and --version
    ControllerConfiguration conf = new ControllerConfiguration(args);

    logger.info(getVersion());

    Controller controller = conf.getController();
    if (controller != null)
    {
      controller.launch();
    }
    else
    {
      endUserLogger.error(Translate.get("controller.configure.failed"));
      throw new Exception(Translate.get("controller.configure.failed"));
    }
  }

  /**
   * Actively launch the <code>controller</code>. Add startup actions here to
   * avoid them in <method>main </method>
   */
  public void launch()
  {
    if (ControllerConstants.ENABLE_SHUTDOWN_HOOK) {
      Runtime.getRuntime().addShutdownHook(new ControllerShutdownHook(this));
      if (logger.isInfoEnabled())
        logger.info("Controller shutdown hook enabled");
    }
    
    // Initialize interceptors. 
    InterceptorManagerAdapter interceptorManager = InterceptorManagerAdapter.getInstance(); 
    try
    {
      InterceptorConfigurator configurator = new ControllerPropertyInterceptorConfigurator();
      interceptorManager.initialize(configurator);
      logger.info("Interceptors initialized");
    }
    catch (InterceptorException e)
    {
      // This is a fatal error. 
      endOfController(e);
    }
    
    connectionThread = new ControllerServerThread(this);
    connectionThread.start();

    SimpleDateFormat formatter = new SimpleDateFormat(
        "yyyy.MM.dd ww 'at' hh:mm:ss a zzz");
    Date day = new Date();
    String date = formatter.format(day);
    logger.info(Translate.get("controller.date", date));
    logger.info(Translate.get("controller.ready", getControllerName()));
    endUserLogger.info(Translate.get("controller.ready",
        getControllerName()));
  }

  //
  // Controller information
  //

  /**
   * Returns the controller name.
   * 
   * @return String
   */
  public String getControllerName()
  {
    return ipAddress + ":" + portNumber;
  }

  /**
   * Get the IP address to bind the controller to
   * 
   * @return the IP address
   */
  public String getIPAddress()
  {
    return ipAddress;
  }

  /**
   * Set the IP address to bind the controller to
   * 
   * @param ipAddress the IP address to use
   */
  public void setIPAddress(String ipAddress)
  {
    this.ipAddress = ipAddress;
  }

  /**
   * Get the controller port number
   * 
   * @return the port number
   */
  public int getPortNumber()
  {
    return portNumber;
  }

  /**
   * Set the controller backlog size.
   * 
   * @param port the port number to set
   */
  public void setPortNumber(int port)
  {
    portNumber = port;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getBacklogSize()
   */
  public int getBacklogSize()
  {
    return backlogSize;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#setBacklogSize(int)
   */
  public void setBacklogSize(int size)
  {
    backlogSize = size;
  }

  /**
   * Returns jmx enable
   * 
   * @return jmxEnabled
   */
  public boolean getJmxEnable()
  {
    return MBeanServerManager.isJmxEnabled();
  }

  /**
   * Return the jmx name of this controller (hostname:rmiport)
   * 
   * @return jmx name
   */
  public String getJmxName()
  {
    if (getJmxEnable())
    {
      List rmiConnectors = RmiConnector.getRmiConnectors();
      if ((rmiConnectors != null) && (rmiConnectors.size() > 0))
      {
        RmiConnector connector = ((RmiConnector) rmiConnectors.get(0));
        return connector.getHostName() + ":" + connector.getPort();
      }
    }
    return getControllerName();
  }

  /**
   * set enable JMX
   * 
   * @param enable true if jmx should be enable.
   */
  public void setJmxEnable(boolean enable)
  {
    configuration.put(ControllerConfiguration.JMX_ENABLE, "" + enable);
  }

  /**
   * Returns Version as a long String
   * 
   * @return version
   */
  public static String getVersion()
  {
    return Translate.get("controller.info", 
        new String[] {ControllerConstants.PRODUCT_NAME, Constants.VERSION});
  }

  /**
   * Get current configuration options
   * 
   * @return configure a <code>Hashtable</code> with controller options
   */
  public Hashtable getConfiguration()
  {
    return configuration;
  }

  /**
   * Check whether security is enabled or not
   * 
   * @return true if there is not null controller security manager
   */
  public boolean isSecurityEnabled()
  {
    return security != null;
  }

  /**
   * @return Returns the security.
   */
  public ControllerSecurityManager getSecurity()
  {
    return security;
  }

  /**
   * @param security The security to set.
   */
  public void setSecurity(ControllerSecurityManager security)
  {
    this.security = security;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#generateReport()
   */
  public void generateReport() throws Exception
  {
    report.generate(true);
  }

  /**
   * Sets the configuration value.
   * 
   * @param configuration The configuration to set.
   */
  public void setConfiguration(Hashtable configuration)
  {
    this.configuration = configuration;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#getVersionNumber()
   */
  public String getVersionNumber()
  {
    return Constants.VERSION;
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    try
    {
      String prettyXml = XmlTools.prettyXml(getXmlController());
      return XmlTools.insertDoctype(prettyXml, ControllerConstants.CONTROLLER_DOCTYPE);
    }
    catch (Exception e)
    {
      logger.error(Translate.get("controller.xml.transformation.failed", e));
      return e.getMessage();
    }
  }

  /**
   * Return the xml version of the controller.xml file without doc type
   * declaration, just data.
   * 
   * @return controller xml data
   */
  public String getXmlController()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + ControllerConstants.CONTROLLER_XML_ROOT_ELEMENT + ">");
    info.append("<" + ControllerXmlTags.ELT_CONTROLLER + " "
        + ControllerXmlTags.ATT_CONTROLLER_IP + "=\"" + this.getIPAddress()
        + "\" " + ControllerXmlTags.ATT_CONTROLLER_PORT + "=\""
        + this.getPortNumber() + "\" " + ">");

    info.append("<" + ControllerXmlTags.ELT_INTERNATIONALIZATION + " "
        + ControllerXmlTags.ATT_LANGUAGE + "=\""
        + Locale.getDefault().getLanguage() + "\"/>");

    if (report.isReportEnabled())
    {
      info.append("<" + ControllerXmlTags.ELT_REPORT + " "
          + ControllerXmlTags.ATT_REPORT_ENABLE_FILE_LOGGING + "=\""
          + report.isEnableFileLogging() + "\" "
          + ControllerXmlTags.ATT_REPORT_HIDE_SENSITIVE_DATA + "=\""
          + report.isHideSensitiveData() + "\" "
          + ControllerXmlTags.ATT_REPORT_GENERATE_ON_FATAL + "=\""
          + report.isGenerateOnFatal() + "\" "
          + ControllerXmlTags.ATT_REPORT_GENERATE_ON_SHUTDOWN + "=\""
          + report.isGenerateOnShutdown() + "\" "
          + ControllerXmlTags.ATT_REPORT_REPORT_LOCATION + "=\""
          + report.getReportLocation() + "\" />");
    }

    if (getJmxEnable())
    {
      info.append("<" + ControllerXmlTags.ELT_JMX + ">");
      if (configuration.containsKey(JmxConstants.ADAPTOR_TYPE_HTTP))
      {
        info.append("<" + ControllerXmlTags.ELT_HTTP_JMX_ADAPTOR + " "
            + ControllerXmlTags.ATT_JMX_ADAPTOR_PORT + "=\""
            + configuration.get(JmxConstants.ADAPTOR_TYPE_HTTP) + "\" />");
      }
      if (configuration.containsKey(JmxConstants.ADAPTOR_TYPE_RMI))
      {
        info.append("<" + ControllerXmlTags.ELT_RMI_JMX_ADAPTOR + " "
            + ControllerXmlTags.ATT_JMX_ADAPTOR_PORT + "=\""
            + configuration.get(JmxConstants.ADAPTOR_TYPE_RMI) + "\" />");
      }

      info.append("</" + ControllerXmlTags.ELT_JMX + ">");
    }

    if (this.isSecurityEnabled())
      info.append(this.getSecurity().getXml());
    info.append("</" + ControllerXmlTags.ELT_CONTROLLER + ">");
    info.append("</" + ControllerConstants.CONTROLLER_XML_ROOT_ELEMENT + ">");
    return info.toString();
  }

  /**
   * Same as above but for the virtual databases.
   * 
   * @return xml virtual databases data.
   */
  public String getXmlVirtualDatabases()
  {
    try
    {
      StringBuffer info = new StringBuffer();
      info.append(XML_VERSION);
      info.append("\n");
      info.append("<" + ControllerConstants.VIRTUAL_DATABASE_XML_ROOT_ELEMENT
          + ">");
      List vdbs = this.getVirtualDatabases();
      for (int i = 0; i < vdbs.size(); i++)
      {
        info.append(((XmlComponent) vdbs.get(i)).getXml());
      }
      info.append("</" + ControllerConstants.VIRTUAL_DATABASE_XML_ROOT_ELEMENT
          + ">");
      return info.toString();
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      return e.getMessage();
    }
  }

  // 
  // Logging system
  //

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#refreshLogConfiguration()
   */
  public void refreshLogConfiguration() throws ControllerException
  {
    try
    {
      LogManager.configure(URLDecoder.decode(this.getClass().getResource(
          ControllerConstants.LOG4J_RESOURCE).getFile()));
      if (logger.isDebugEnabled())
        logger.info(Translate.get("controller.refresh.log.success"));
    }
    catch (Exception e)
    {
      throw new ControllerException(Translate
          .get("controller.logconfigfile.not.found"));
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#updateLogConfigurationFile(java.lang.String)
   */
  public void updateLogConfigurationFile(String newConfiguration)
      throws IOException, ControllerException
  {
    File logFile = new File(URLDecoder.decode(getClass().getResource(
        ControllerConstants.LOG4J_RESOURCE).getFile()));
    BufferedWriter writer = new BufferedWriter(new FileWriter(logFile));
    writer.write(newConfiguration);
    writer.flush();
    writer.close();
    refreshLogConfiguration();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.ControllerMBean#viewLogConfigurationFile()
   */
  public String viewLogConfigurationFile() throws IOException
  {
    File logFile = new File(URLDecoder.decode(getClass().getResource(
        ControllerConstants.LOG4J_RESOURCE).getFile()));
    BufferedReader reader = new BufferedReader(new FileReader(logFile));
    StringBuffer buffer = new StringBuffer();
    String line;
    while ((line = reader.readLine()) != null)
      buffer.append(line + System.getProperty("line.separator"));
    reader.close();
    return buffer.toString();
  }

  /**
   * Returns the report value.
   * 
   * @return Returns the report.
   */
  public ReportManager getReport()
  {
    return report;
  }

  /**
   * Sets the report value.
   * 
   * @param report The report to set.
   */
  public void setReport(ReportManager report)
  {
    this.report = report;
  }

}