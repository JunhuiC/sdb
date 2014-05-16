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
 * Initial developer(s): Mathieu Peltier. 
 * Contributor(s): Nicolas Modrzyk, Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.core;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.common.xml.ControllerXmlTags;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * Constants relative to Sequoia controller.
 * 
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:duncan@mightybot.com">Duncan Smith </a>
 * @version 1.0
 */
public class ControllerConstants
{
  private static final String           PROP_FILENAME_RADIX                         = "controller_default.properties";
  /**
   * Controller properties
   */
  private static final Properties       CONTROLLER_PROPERTIES                       = new Properties();

  /** Controller factory to use */
  public static final ControllerFactory CONTROLLER_FACTORY;

  static
  {
    try
    {
      CONTROLLER_PROPERTIES.load(ClassLoader
          .getSystemResourceAsStream(PROP_FILENAME_RADIX));
      CONTROLLER_FACTORY = (ControllerFactory) Class.forName(
          CONTROLLER_PROPERTIES.getProperty("controller.factory"))
          .newInstance();
    }
    catch (Exception e)
    {
      throw new ExceptionInInitializerError("Invalid controller factory ("
          + CONTROLLER_PROPERTIES.getProperty("controller.factory")
          + "), fix your " + PROP_FILENAME_RADIX + " file");
    }
  }

  /** Default controller port number value. */
  public static final String            PRODUCT_NAME                                = CONTROLLER_PROPERTIES
                                                                                        .getProperty("product.name");

  /** Default controller port number value. */
  public static final int               DEFAULT_PORT                                = Integer
                                                                                        .parseInt(CONTROLLER_PROPERTIES
                                                                                            .getProperty(
                                                                                                "controller.default.port",
                                                                                                "25322"));

  /**
   * Default IP address to bind controller to.
   * {@link java.net.InetAddress#anyLocalAddress()}.getHostAddress() would
   * probably be better.
   */
  public static final String            DEFAULT_IP                                  = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "controller.default.ip",
                                                                                            "0.0.0.0");

  /** Default backlog size for driver connections. */
  public static final int               DEFAULT_BACKLOG_SIZE                        = Integer
                                                                                        .parseInt(CONTROLLER_PROPERTIES
                                                                                            .getProperty(
                                                                                                "controller.default.backlog.size",
                                                                                                "10"));

  /**
   * Maximum number of characters to display when a SQL statement is logged into
   * an Exception.
   */
  public static final int               SQL_SHORT_FORM_LENGTH                       = Integer
                                                                                        .parseInt(CONTROLLER_PROPERTIES
                                                                                            .getProperty(
                                                                                                "sql.short.form.length",
                                                                                                "40"));

  /**
   * Doctype for the virtual database config file
   */
  public static final String            VIRTUAL_DATABASE_DOCTYPE                    = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "virtual.database.doctype",
                                                                                            "<!DOCTYPE SEQUOIA PUBLIC \"-//Continuent//DTD SEQUOIA "
                                                                                                + Constants.VERSION
                                                                                                + "//EN\" \"http://sequoia.continuent.org/dtds/sequoia-"
                                                                                                + Constants.VERSION
                                                                                                + ".dtd\">");
  /**
   * Doctype for the controller config file
   */
  public static final String            CONTROLLER_DOCTYPE                          = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "virtual.database.doctype",
                                                                                            "<!DOCTYPE SEQUOIA-CONTROLLER PUBLIC \"-//Continuent//DTD SEQUOIA-CONTROLLER "
                                                                                                + Constants.VERSION
                                                                                                + "//EN\" \"http://sequoia.continuent.org/dtds/sequoia-controller-"
                                                                                                + Constants.VERSION
                                                                                                + ".dtd\">");

  /** Sequoia DTD file name (must be found in classpath). */
  public static final String            SEQUOIA_DTD_FILE                            = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "virtual.database.dtd",
                                                                                            "sequoia.dtd");

  /** Virtual database XML config file rool element name */
  public static final String            VIRTUAL_DATABASE_XML_ROOT_ELEMENT           = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "virtual.database.xml.root.element",
                                                                                            DatabasesXmlTags.ELT_SEQUOIA);

  /** SEQUOIA-CONTROLLER DTD file name (must be found in classpath). */
  public static final String            SEQUOIA_CONTROLLER_DTD_FILE                 = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "controller.dtd",
                                                                                            "sequoia-controller.dtd");

  /** Sequoia Controller XML config file root element name */
  public static final String            CONTROLLER_XML_ROOT_ELEMENT                 = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "controller.xml.root.element",
                                                                                            ControllerXmlTags.ELT_SEQUOIA_CONTROLLER);
  /**
   * Default sleep time in ms for a controller worker thread. If no job is ready
   * after this time, the thread dies.
   */
  public static final int               DEFAULT_CONTROLLER_WORKER_THREAD_SLEEP_TIME = Integer
                                                                                        .parseInt(CONTROLLER_PROPERTIES
                                                                                            .getProperty(
                                                                                                "controller.default.worker.thread.sleep.time",
                                                                                                "15000"));

  /** JMX Enable by default */
  public static final boolean           JMX_ENABLE                                  = Boolean
                                                                                        .valueOf(
                                                                                            CONTROLLER_PROPERTIES
                                                                                                .getProperty(
                                                                                                    "controller.jmx.enabled",
                                                                                                    "true"))
                                                                                        .booleanValue();

  /** Add Driver enable by default */
  public static final boolean           ADD_DRIVER_ENABLE                           = Boolean
                                                                                        .valueOf(
                                                                                            CONTROLLER_PROPERTIES
                                                                                                .getProperty(
                                                                                                    "controller.add.driver.enabled",
                                                                                                    "false"))
                                                                                        .booleanValue();

  /**
   * Name of the Sequoia driver JAR file (must be found in classpath). This
   * information is used to find the drivers directory.
   */
  public static final String            SEQUOIA_DRIVER_JAR_FILE                     = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "controller.driver.jar.file",
                                                                                            "/sequoia-driver.jar");

  /** Default configuration file */
  public static final String            DEFAULT_CONFIG_FILE                         = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "controller.default.config.file",
                                                                                            "controller.xml");

  /** Log4j property file resource (must be found in classpath). */
  public static final String            LOG4J_RESOURCE                              = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "log4j.properties.file",
                                                                                            "/log4j.properties");

  /** Report file */
  public static final String            REPORT_FILE                                 = CONTROLLER_PROPERTIES
                                                                                        .getProperty(
                                                                                            "controller.report.file",
                                                                                            "sequoia.report");

  /**
   * Ping interval of idle persistent connections. A value of 0 will mean that
   * it will never be checked.
   */
  public static final int               IDLE_PERSISTENT_CONNECTION_PING_INTERVAL    = Integer
                                                                                        .parseInt(CONTROLLER_PROPERTIES
                                                                                            .getProperty(
                                                                                                "default.timeout.for.idle.persistent.connection",
                                                                                                "0"));
  /** Enable/disable schema refresh on create table statements. */
  public static final boolean           FORCE_SCHEMA_REFRESH_ON_CREATE_STATEMENT    = Boolean
                                                                                        .valueOf(
                                                                                            CONTROLLER_PROPERTIES
                                                                                                .getProperty(
                                                                                                    "schema.refresh.on.create.statement",
                                                                                                    "true"))
                                                                                        .booleanValue();

  /** Enable/disable schema refresh on create table statements. */
  public static final boolean           FORCE_BACKEND_EARLY_ROLLBACK_ON_FAILURE     = Boolean
                                                                                        .valueOf(
                                                                                            CONTROLLER_PROPERTIES
                                                                                                .getProperty(
                                                                                                    "force.backend.early.rollback.on.failure",
                                                                                                    "false"))
                                                                                        .booleanValue();

  /** Enable/disable controller shutdown hook. */
  public static final boolean           ENABLE_SHUTDOWN_HOOK                        = Boolean
                                                                                        .valueOf(
                                                                                            CONTROLLER_PROPERTIES
                                                                                                .getProperty(
                                                                                                    "controller.enableShutdownHook",
                                                                                                    "false"))
                                                                                        .booleanValue();

  /**
   * Declare a list of front end interceptors, which are class names separated
   * by semi-colons or a null if there are no interceptors.
   */
  public static final String            FRONTEND_INTERCEPTORS                       = CONTROLLER_PROPERTIES
                                                                                        .getProperty("frontend.interceptors");

  /**
   * Declare a list of backend end interceptors, which are class names separated
   * by semi-colons or a null if there are no interceptors.
   */
  public static final String            BACKEND_INTERCEPTORS                        = CONTROLLER_PROPERTIES
                                                                                        .getProperty("backend.interceptors");

  /**
   * Return default path and name for saving of configuration file
   * 
   * @param resource name of the resource to get save file for
   * @return path
   */
  public static final String getSaveFile(String resource)
  {
    URL url = ControllerConstants.class.getResource("/" + DEFAULT_CONFIG_FILE);
    File dir = (new File(url.getFile())).getParentFile();
    return dir.getPath() + File.separator + resource + "-saved.xml";
  }

  //
  // Backend auto-enable constants
  //

  /** Enable all backend from their last known state at controller start */
  public static final int AUTO_ENABLE_TRUE       = 0;
  /** Do not enable any backend when starting controller */
  public static final int AUTO_ENABLE_FALSE      = 1;
  /** Restore the state from an existing recovery log */
  public static final int AUTO_ENABLE_FORCE      = 2;

  //
  // VDB load options (implemented using auto-enable backdoor)
  //
  /** Initialize state to empty recovery log */
  public static final int AUTO_ENABLE_INIT       = 3;
  /** Bypass last-man-down checks * */
  public static final int AUTO_ENABLE_FORCE_LOAD = 4;

  /** Auto Enable Backend default */
  public static final int AUTO_ENABLE_BACKEND    = AUTO_ENABLE_FALSE;

}