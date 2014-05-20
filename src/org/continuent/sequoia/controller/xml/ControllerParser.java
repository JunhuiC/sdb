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
 * Contributor(s): Mathieu Peltier, Sara Bouchenak.
 */

package org.continuent.sequoia.controller.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Locale;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.JmxException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.net.SSLConfiguration;
import org.continuent.sequoia.common.xml.ControllerXmlTags;
import org.continuent.sequoia.common.xml.XmlValidator;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.core.ControllerConfiguration;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.core.ReportManager;
import org.continuent.sequoia.controller.core.security.ControllerSecurityManager;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * Allows to parse an XML content containing the description of the controller
 * confirming to sequoia-controller.dtd.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ControllerParser extends DefaultHandler
{
  /** Logger instance. */
  static Trace                      logger          = Trace
                                                        .getLogger(ControllerParser.class
                                                            .getName());
  static Trace                      endUserLogger   = Trace
                                                        .getLogger("org.continuent.sequoia.enduser");

  /** XML parser. */
  private XMLReader                 parser;

  /** Sequoia controller to setup. */
  private ControllerConfiguration   config;
  private ControllerSecurityManager security;
  private Controller                controller;
  private boolean                   parseAccept     = false;
  private SSLConfiguration          ssl;
  private String                    controllerIP;
  private ReportManager             manager;

  /**
   * Creates a new <code>ControllerParser</code> instance. This method
   * Instanciates also a new <code>ControllerHandler</code>.
   * 
   * @param configure a <code>ControllerConfiguration</code> object that
   *          contains the configuration to update with values from xml parsing
   * @throws Exception i private String doctype; f an error occurs
   */
  public ControllerParser(ControllerConfiguration configure) throws Exception
  {
    this.config = configure;
    this.controller = configure.getController();

    // Instantiate a new parser
    parser = XMLReaderFactory.createXMLReader();

    // Activate validation
    parser.setFeature("http://xml.org/sax/features/validation", true);

    // Install error handler
    parser.setErrorHandler(this);

    // Install document handler
    parser.setContentHandler(this);

    // Install local entity resolver
    parser.setEntityResolver(this);
  }

  /**
   * Parses an XML content according to SEQUOIA-CONTROLLER DTD.
   * 
   * @param xml a <code>String</code> containing the XML content to parse
   * @exception SAXException if an error occurs
   * @exception IOException if an error occurs
   */
  public void readXML(String xml) throws IOException, SAXException
  {
    if (xml != null)
    {
      InputSource input = new InputSource(new StringReader(xml));
      parser.parse(input);
    }
    else
      throw new IOException("Input was null in input source.");
  }

  /**
   * Parses an XML formatted string according to SEQUOIA-CONTROLLER DTD.
   * 
   * @param xml a <code>String</code> reference to the xml to parse
   * @param validateBeforeParsing if validation should be checked before parsing
   * @exception SAXException if an error occurs
   * @exception IOException if an error occurs
   */
  public void readXML(String xml, boolean validateBeforeParsing)
      throws IOException, SAXException
  {
    if (validateBeforeParsing)
    {
      XmlValidator validator = new XmlValidator(
          ControllerConstants.SEQUOIA_CONTROLLER_DTD_FILE, xml.toString());
      if (logger.isDebugEnabled())
      {
        if (validator.isDtdValid())
          logger.debug(Translate.get("controller.xml.dtd.validated"));
        if (validator.isXmlValid())
          logger.debug(Translate.get("controller.xml.document.validated"));
      }

      if (validator.getWarnings().size() > 0)
      {
        ArrayList<?> warnings = validator.getWarnings();
        for (int i = 0; i < warnings.size(); i++)
          logger.warn(Translate.get("virtualdatabase.xml.parsing.warning",
              warnings.get(i)));
      }

      if (!validator.isDtdValid())
        logger.error(Translate.get("controller.xml.dtd.not.validated"));
      if (!validator.isXmlValid())
        logger.error(Translate.get("controller.xml.document.not.validated"));

      ArrayList<?> errors = validator.getExceptions();
      for (int i = 0; i < errors.size(); i++)
        logger.error(((Exception) errors.get(i)).getMessage());

      if (!validator.isValid())
        throw new SAXException(Translate
            .get("controller.xml.document.not.valid"));
    }
    readXML(xml);
  }

  /**
   * Parses an XML formatted file according to SEQUOIA-CONTROLLER DTD.
   * 
   * @param fileReader a <code>FileReader</code> reference to the xml to parse
   * @param validateBeforeParsing if validation should be checked before parsing
   * @exception SAXException if an error occurs
   * @exception IOException if an error occurs
   */
  public void readXML(FileReader fileReader, boolean validateBeforeParsing)
      throws IOException, SAXException
  {
    if (fileReader != null)
    {

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

      readXML(xml.toString(), validateBeforeParsing);
    }
    else
    {
      throw new IOException("Input was null in input source.");
    }
  }

  /**
   * Handles notification of a non-recoverable parser error.
   * 
   * @param e the warning information encoded as an exception.
   * @exception SAXException any SAX exception, possibly wrapping another
   *              exception.
   */
  public void fatalError(SAXParseException e) throws SAXException
  {
    String msg = Translate.get("controller.xml.parsing.fatal",
        new String[]{e.getPublicId(), String.valueOf(e.getLineNumber()),
            String.valueOf(e.getColumnNumber()), e.getMessage()});
    logger.error(msg);
    endUserLogger.fatal(msg);
    throw e;
  }

  /**
   * Handles notification of a recoverable parser error.
   * 
   * @param e the warning information encoded as an exception.
   * @exception SAXException any SAX exception, possibly wrapping another
   *              exception
   */
  public void error(SAXParseException e) throws SAXException
  {
    logger.error(Translate.get("controller.xml.parsing.error", new String[]{
        e.getPublicId(), String.valueOf(e.getLineNumber()),
        String.valueOf(e.getColumnNumber()), e.getMessage()}));
    throw e;
  }

  /**
   * Allows to parse the document with a local copy of the DTD whatever the
   * original <code>DOCTYPE</code> found. Warning, this method is called only
   * if the XML document contains a <code>DOCTYPE</code>.
   * 
   * @see org.xml.sax.EntityResolver#resolveEntity(java.lang.String,
   *      java.lang.String)
   */
  public InputSource resolveEntity(String publicId, String systemId)
      throws SAXException
  {
    logger.debug(Translate.get("controller.xml.dtd.using",
        ControllerConstants.SEQUOIA_CONTROLLER_DTD_FILE));
    InputStream stream = ControllerParser.class.getResourceAsStream("/"
        + ControllerConstants.SEQUOIA_CONTROLLER_DTD_FILE);
    if (stream == null)
    {
      throw new SAXException(Translate.get(
          "controller.xml.dtd.not.found",
          ControllerConstants.PRODUCT_NAME,
          ControllerConstants.SEQUOIA_CONTROLLER_DTD_FILE));
    }

    return new InputSource(stream);
  }

  /**
   * Initializes parsing of a document.
   * 
   * @exception SAXException unspecialized error
   */
  public void startDocument() throws SAXException
  {
    logger.debug(Translate.get("controller.xml.parsing.document"));
  }

  /**
   * Finalizes parsing of a document.
   * 
   * @exception SAXException unspecialized error
   */
  public void endDocument() throws SAXException
  {
    logger.info(Translate.get("controller.xml.done"));
  }

  /**
   * Analyzes an element first line.
   * 
   * @param uri name space URI
   * @param localName local name
   * @param name element raw name
   * @param atts element attributes
   * @exception SAXException if an error occurs
   */
  public void startElement(String uri, String localName, String name,
      Attributes atts) throws SAXException
  {
    logger.debug(Translate.get("controller.xml.parsing.start", name));
    if (name.equalsIgnoreCase(ControllerXmlTags.ELT_CONTROLLER))
      configureController(atts);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_INTERNATIONALIZATION))
    {
      Locale.setDefault(new Locale(atts
          .getValue(ControllerXmlTags.ATT_LANGUAGE), ""));
    }
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_REPORT))
      configureReport(atts);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_JMX))
    {
      config.put(ControllerConfiguration.JMX_ENABLE, "true");
    }
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_HTTP_JMX_ADAPTOR))
      configureHttpJmxAdaptor(atts);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_RMI_JMX_ADAPTOR))
      configureRmiJmxAdaptor(atts);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_SSL))
      configureSSL(atts);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_VIRTUAL_DATABASE))
      configureVirtualDatabase(atts);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_SECURITY))
    {
      security = new ControllerSecurityManager();
      boolean connect = new Boolean(atts
          .getValue(ControllerXmlTags.ATT_DEFAULT_CONNECT)).booleanValue();
      security.setDefaultConnect(connect);
    }
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_JAR))
    {
      boolean allow = new Boolean(atts
          .getValue(ControllerXmlTags.ATT_JAR_ALLOW_DRIVER)).booleanValue();
      security.setAllowAdditionalDriver(allow);
    }
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_ACCEPT))
      parseAccept = true;
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_BLOCK))
      parseAccept = false;
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_HOSTNAME))
      security.addHostToSecureList(atts.getValue(ControllerXmlTags.ATT_VALUE),
          parseAccept);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_IPADDRESS))
      security.addHostToSecureList(atts.getValue(ControllerXmlTags.ATT_VALUE),
          parseAccept);
    else if (name.equalsIgnoreCase(ControllerXmlTags.ELT_IPRANGE))
      configureIpRange(atts);
  }

  /**
   * DatabasesParser for end of element.
   * 
   * @param uri name space URI
   * @param localName local name
   * @param name element raw name
   * @exception SAXException if an error occurs
   */
  public void endElement(String uri, String localName, String name)
      throws SAXException
  {
    // We need information on what configuration are for jmx
    if (name.equalsIgnoreCase(ControllerXmlTags.ELT_JMX))
    {
      try
      {
        config.setUpJmx();
      }
      catch (JmxException jmxEx)
      {
        logger.error(Translate.get("controller.xml.jmx.setup.failed", jmxEx
            .getMessage()), jmxEx);
      }
    }
    if (name.equalsIgnoreCase(ControllerXmlTags.ELT_SECURITY))
    {
      security.setSslConfig(ssl);
      ssl = null;
      config.setUpSecurity(security);
    }
    if (name.equalsIgnoreCase(ControllerXmlTags.ELT_RMI_JMX_ADAPTOR))
    {
      if (ssl != null)
      {
        config.put(JmxConstants.CONNECTOR_RMI_SSL, ssl);
        ssl = null;
      }
    }
    if (name.equalsIgnoreCase(ControllerXmlTags.ELT_CONTROLLER))
    {
      // Be sure no settings are used for report
      if (manager == null)
      {
        manager = new ReportManager(controller);
        manager.setSettings(null);
        controller.setReport(manager);
      }
    }
    logger.debug(Translate.get("controller.xml.parsing.end", name));
  }

  /**
   * Configure a <code>ControllerXmlTags.ELT_CONTROLLER</code> element.
   * 
   * @param atts the parser attributes
   * @throws SAXException if an error occurs
   */
  private void configureController(Attributes atts) throws SAXException
  {
    try
    {
      String controllerPort = atts
          .getValue(ControllerXmlTags.ATT_CONTROLLER_PORT);
      if (controllerPort == null)
        config.put(ControllerConfiguration.CONTROLLER_PORT, String
            .valueOf(ControllerConstants.DEFAULT_PORT));
      else
        config.put(ControllerConfiguration.CONTROLLER_PORT, controllerPort);
      config.getController().setPortNumber(
          Integer.parseInt((String) config
              .get(ControllerConfiguration.CONTROLLER_PORT)));

      controllerIP = atts.getValue(ControllerXmlTags.ATT_CONTROLLER_IP);
      if (controllerIP == null)
      {
        try
        {
          /**
           * leaving "null" would be ok for
           * 
           * @see org.continuent.sequoia.controller.core.ControllerServerThread#ControllerServerThread(Controller)
           *      but JMX/RMI naming scheme would not like it, so we use
           *      getLocalHost().getHostAddress() as a pseudo-empty/default
           *      value. See SEQUOIA-804
           */
          String localIP = InetAddress.getLocalHost().getHostAddress();
          config.put(ControllerConfiguration.CONTROLLER_IP, localIP);
        }
        catch (RuntimeException e1)
        {
          logger
              .warn("Unable to obtain IP address of controller, setting default address: "
                  + ControllerConstants.DEFAULT_IP);
          config.put(ControllerConfiguration.CONTROLLER_IP,
              ControllerConstants.DEFAULT_IP);
        }
      }
      else
        config.put(ControllerConfiguration.CONTROLLER_IP, controllerIP);
      config.getController().setIPAddress(
          (String) config.get(ControllerConfiguration.CONTROLLER_IP));

      String controllerBacklog = atts
          .getValue(ControllerXmlTags.ATT_backlogSize);
      if (controllerBacklog == null)
        config.put(ControllerConfiguration.CONTROLLER_BACKLOG, String
            .valueOf(ControllerConstants.DEFAULT_BACKLOG_SIZE));
      else
        config.put(ControllerConfiguration.CONTROLLER_BACKLOG,
            controllerBacklog);
      config.getController().setBacklogSize(
          Integer.parseInt((String) config
              .get(ControllerConfiguration.CONTROLLER_BACKLOG)));
    }
    catch (Exception e)
    {
      logger.warn("Error while configuring controller", e);
      throw new SAXException(e.getMessage());
    }
  }

  /**
   * Configure a <code>ControllerXmlTags.ELT_HTTP_JMX_ADAPTOR</code> element.
   * 
   * @param atts the parser attributes
   */
  private void configureHttpJmxAdaptor(Attributes atts)
  {
    String adaptorPort = atts.getValue(ControllerXmlTags.ATT_JMX_ADAPTOR_PORT);
    if (config.get(JmxConstants.ADAPTOR_TYPE_HTTP) == null)
      config.put(JmxConstants.ADAPTOR_TYPE_HTTP, String.valueOf(adaptorPort));
  }

  /**
   * Configure a <code>ControllerXmlTags.ELT_IPRANGE</code> element.
   * 
   * @param atts the parser attributes
   */
  private void configureIpRange(Attributes atts)
  {
    String iprange = atts.getValue(ControllerXmlTags.ATT_VALUE);
    try
    {
      security.addToSecureList(iprange, parseAccept);
    }
    catch (Exception e)
    {
      logger.warn(Translate
          .get("controller.configure.invalid.iprange", iprange));
    }
  }

  /**
   * Configure a <code>ControllerXmlTags.ELT_REPORT</code> element.
   * 
   * @param atts the parser attributes
   */
  private void configureReport(Attributes atts)
  {
    config.put(ControllerXmlTags.ATT_REPORT_ENABLED, "true");
    config.put(ControllerXmlTags.ATT_REPORT_HIDE_SENSITIVE_DATA, atts
        .getValue(ControllerXmlTags.ATT_REPORT_HIDE_SENSITIVE_DATA));
    config.put(ControllerXmlTags.ATT_REPORT_GENERATE_ON_SHUTDOWN, atts
        .getValue(ControllerXmlTags.ATT_REPORT_GENERATE_ON_SHUTDOWN));
    config.put(ControllerXmlTags.ATT_REPORT_GENERATE_ON_FATAL, atts
        .getValue(ControllerXmlTags.ATT_REPORT_GENERATE_ON_FATAL));
    config.put(ControllerXmlTags.ATT_REPORT_DELETE_ON_SHUTDOWN, atts
        .getValue(ControllerXmlTags.ATT_REPORT_DELETE_ON_SHUTDOWN));
    String reportLocation = atts
        .getValue(ControllerXmlTags.ATT_REPORT_REPORT_LOCATION);

    if ((reportLocation == null) || reportLocation.equals(""))
    {
      reportLocation = System.getProperty("sequoia.log");
      if (reportLocation == null)
        reportLocation = ".";
    }
    config.put(ControllerXmlTags.ATT_REPORT_REPORT_LOCATION, reportLocation);

    config.put(ControllerXmlTags.ATT_REPORT_ENABLE_FILE_LOGGING, atts
        .getValue(ControllerXmlTags.ATT_REPORT_ENABLE_FILE_LOGGING));
    manager = new ReportManager(controller);
    manager.setSettings(config);
    controller.setReport(manager);
  }

  /**
   * Configure a <code>ControllerXmlTags.ELT_RMI_JMX_ADAPTOR</code> element.
   * 
   * @param atts the parser attributes
   */
  private void configureRmiJmxAdaptor(Attributes atts)
  {
    String adaptorPort = atts.getValue(ControllerXmlTags.ATT_JMX_ADAPTOR_PORT);
    if (config.get(JmxConstants.ADAPTOR_TYPE_RMI) == null)
      config.put(JmxConstants.ADAPTOR_TYPE_RMI, String.valueOf(adaptorPort));

    String adaptorServerPort = atts.getValue(ControllerXmlTags.ATT_JMX_ADAPTOR_SERVER_PORT);
    if (config.get(JmxConstants.ADAPTOR_TYPE_RMI_SERVER) == null)
      config.put(JmxConstants.ADAPTOR_TYPE_RMI_SERVER, String.valueOf(adaptorServerPort));
    
    String username = atts
        .getValue(ControllerXmlTags.ATT_JMX_CONNECTOR_USERNAME);
    String password = atts
        .getValue(ControllerXmlTags.ATT_JMX_CONNECTOR_PASSWORD);
    if (username != null)
      config.put(JmxConstants.CONNECTOR_AUTH_USERNAME, username);
    if (password != null)
      config.put(JmxConstants.CONNECTOR_AUTH_PASSWORD, password);
  }

  /**
   * Configure a <code>ControllerXmlTags.ELT_SSL</code> element.
   * 
   * @param atts the parser attributes
   */
  private void configureSSL(Attributes atts)
  {
    ssl = new SSLConfiguration();
    String keyStore = atts.getValue(ControllerXmlTags.ATT_SSL_KEYSTORE);
    String keyStorePassword = atts
        .getValue(ControllerXmlTags.ATT_SSL_KEYSTORE_PASSWORD);
    String keyStoreKeyPassword = atts
        .getValue(ControllerXmlTags.ATT_SSL_KEYSTORE_KEYPASSWORD);
    String trustStore = atts.getValue(ControllerXmlTags.ATT_SSL_TRUSTSTORE);
    String trustStorePassword = atts
        .getValue(ControllerXmlTags.ATT_SSL_TRUSTSTORE_PASSWORD);
    ssl.setKeyStore(new File(keyStore));

    // Sanity checks, default to SSL_KEYSTORE values
    if (keyStoreKeyPassword == null)
      keyStoreKeyPassword = keyStorePassword;
    if (trustStore == null)
      trustStore = keyStore;
    if (trustStorePassword == null)
      trustStorePassword = keyStorePassword;

    ssl.setKeyStorePassword(keyStorePassword);
    ssl.setKeyStoreKeyPassword(keyStoreKeyPassword);
    ssl.setClientAuthenticationRequired("true".equals(atts
        .getValue(ControllerXmlTags.ATT_SSL_NEED_CLIENT_AUTH)));
    ssl.setTrustStore(new File(trustStore));
    ssl.setTrustStorePassword(trustStorePassword);
  }

  /**
   * Configure a <code>ControllerXmlTags.ELT_VIRTUAL_DATABASE</code> element.
   * 
   * @param atts the parser attributes
   * @throws SAXException if an error occurs
   */
  @SuppressWarnings("deprecation")
private void configureVirtualDatabase(Attributes atts) throws SAXException
  {
    String checkPoint = atts
        .getValue(ControllerXmlTags.ATT_VIRTUAL_DATABASE_CHECKPOINT);
    String virtualName = atts
        .getValue(ControllerXmlTags.ATT_VIRTUAL_DATABASE_NAME);
    String file = atts.getValue(ControllerXmlTags.ATT_VIRTUAL_DATABASE_FILE);

    // Try to find the file on the path (usually config directory) if no file
    // separator is found
    if (file.indexOf(File.separator) == -1)
    {
      try
      {
        URL url = this.getClass().getResource("/" + file);
        file = url.getFile();
        logger.info(Translate.get("controller.configure.using", file));
      }
      catch (Exception e)
      {
        throw new SAXException(Translate.get(
            "controller.configure.file.not.found", file));
      }
    }

    file = URLDecoder.decode(file);

    File checkExist = new File(file);
    if (checkExist.exists() == false)
      throw new SAXException(Translate.get(
          "controller.configure.file.not.found", file));

    int autoLoad = -1;
    String autoLoadString = atts
        .getValue(ControllerXmlTags.ATT_VIRTUAL_DATABASE_AUTO_ENABLE);

    if (autoLoadString.equalsIgnoreCase(ControllerXmlTags.VAL_true))
      autoLoad = ControllerConstants.AUTO_ENABLE_TRUE;
    else if (autoLoadString.equalsIgnoreCase(ControllerXmlTags.VAL_force))
      autoLoad = ControllerConstants.AUTO_ENABLE_FORCE;
    else
      autoLoad = ControllerConstants.AUTO_ENABLE_FALSE;

    logger.info(Translate.get("controller.configure.setup", new String[]{
        virtualName, String.valueOf(autoLoad), checkPoint}));
    config.setUpVirtualDatabase(file, virtualName, autoLoad, checkPoint);
  }
}