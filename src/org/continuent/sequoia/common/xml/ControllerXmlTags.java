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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.xml;

/**
 * List of the xml tags recognized to read and write the controller
 * configuration with.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */

public final class ControllerXmlTags
{
  /**
   * XML Tag and attributes to work with on the DTD and the xml file
   */

  /** Root object element. */
  public static final String ELT_SEQUOIA_CONTROLLER           = "SEQUOIA-CONTROLLER";

  /** Controller tag */
  public static final String ELT_CONTROLLER                   = "Controller";
  /** Controller name */
  public static final String ATT_CONTROLLER_NAME              = "name";
  /** Controller rmi port */
  public static final String ATT_CONTROLLER_PORT              = "port";
  /** <code>backlogSize</code> attribute in <code>Controller</code>. */
  public static final String ATT_backlogSize                  = "backlogSize";
  /** Controller IP address */
  public static final String ATT_CONTROLLER_IP                = "ipAddress";

  /** Internationalization */
  public static final String ELT_INTERNATIONALIZATION         = "Internationalization";
  /** Language */
  public static final String ATT_LANGUAGE                     = "language";

  /** Report Tag */
  public static final String ELT_REPORT                       = "Report";
  /** Enabled */
  public static final String ATT_REPORT_ENABLED               = "enabled";
  /** Hide data */
  public static final String ATT_REPORT_HIDE_SENSITIVE_DATA   = "hideSensitiveData";
  /** Generate on shutdown */
  public static final String ATT_REPORT_GENERATE_ON_SHUTDOWN  = "generateOnShutdown";
  /** Generate on fatal */
  public static final String ATT_REPORT_GENERATE_ON_FATAL     = "generateOnFatal";
  /** Enable file loggin */
  public static final String ATT_REPORT_ENABLE_FILE_LOGGING   = "enableFileLogging";
  /** Report Location */
  public static final String ATT_REPORT_REPORT_LOCATION       = "reportLocation";
  /** Delete on shutdown */
  public static final String ATT_REPORT_DELETE_ON_SHUTDOWN    = "deleteOnShutdown";

  /** Virtual Database tag */
  public static final String ELT_VIRTUAL_DATABASE             = "VirtualDatabase";
  /** Virtual Database name */
  public static final String ATT_VIRTUAL_DATABASE_NAME        = "virtualDatabaseName";
  /** Config file attribute */
  public static final String ATT_VIRTUAL_DATABASE_FILE        = "configFile";
  /** auto-enable backend attribute */
  public static final String ATT_VIRTUAL_DATABASE_AUTO_ENABLE = "autoEnableBackends";
  /** checkpoint when autoEnable is set to force */
  public static final String ATT_VIRTUAL_DATABASE_CHECKPOINT  = "checkpointName";
  /** True value for restoring backend */
  public static final String VAL_true                         = "true";
  /** False value for restoring backend */
  public static final String VAL_false                        = "false";
  /** Force value for restoring backend */
  public static final String VAL_force                        = "force";

  /** Jmx Settings tag */
  public static final String ELT_JMX                          = "JmxSettings";
  /** Jmx enable attribute */
  public static final String ATT_JMX_ENABLE                   = "enabled";
  /** http Jmx adaptor */
  public static final String ELT_HTTP_JMX_ADAPTOR             = "HttpJmxAdaptor";
  /** Rmi Jmx adaptor */
  public static final String ELT_RMI_JMX_ADAPTOR              = "RmiJmxAdaptor";
  /** Registry Port of the adaptor */
  public static final String ATT_JMX_ADAPTOR_PORT             = "port";
  /** Server Port of the adaptor */
  public static final String ATT_JMX_ADAPTOR_SERVER_PORT      = "serverPort";
  /** username of the adaptor */
  public static final String ATT_JMX_CONNECTOR_USERNAME       = "username";
  /** password of the adaptor */
  public static final String ATT_JMX_CONNECTOR_PASSWORD       = "password";

  /** ssl configuration */
  public static final String ELT_SSL                          = "SSL";
  /** kestore file */
  public static final String ATT_SSL_KEYSTORE                 = "keyStore";
  /** keystore password */
  public static final String ATT_SSL_KEYSTORE_PASSWORD        = "keyStorePassword";
  /** key password */
  public static final String ATT_SSL_KEYSTORE_KEYPASSWORD     = "keyStoreKeyPassword";
  /** need client authentication */
  public static final String ATT_SSL_NEED_CLIENT_AUTH         = "isClientAuthNeeded";
  /** truststore file */
  public static final String ATT_SSL_TRUSTSTORE               = "trustStore";
  /** truststore password */
  public static final String ATT_SSL_TRUSTSTORE_PASSWORD      = "trustStorePassword";

  /** Security tag */
  public static final String ELT_SECURITY                     = "SecuritySettings";
  /** Default Accept Connect */
  public static final String ATT_DEFAULT_CONNECT              = "defaultConnect";

  /** jar tag */
  public static final String ELT_JAR                          = "jar";
  /** allow driver attribute */
  public static final String ATT_JAR_ALLOW_DRIVER             = "allowAdditionalDriver";

  /** should we backup */
  public static final String ATT_BACKUP_ON_SHUDOWN            = "backupOnShutdown";
  /** Allow attribute */
  public static final String ATT_ALLOW                        = "allow";
  /** Configuration for the console shutdown */
  public static final String ELT_CONSOLE                      = "Console";
  /** accept attribute */
  public static final String ELT_ACCEPT                       = "Accept";
  /** block attribute */
  public static final String ELT_BLOCK                        = "Block";
  /** ipaddress attribute */
  public static final String ELT_IPADDRESS                    = "IpAddress";
  /** iprange attribute */
  public static final String ELT_IPRANGE                      = "IpRange";
  /** Hostname */
  public static final String ELT_HOSTNAME                     = "Hostname";
  /** Value */
  public static final String ATT_VALUE                        = "value";
}
