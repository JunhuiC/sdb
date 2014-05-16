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
 * Contributor(s): Nicolas Modrzyk, Mathieu Peltier.
 */

package org.continuent.sequoia.controller.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.common.util.ReadWrite;
import org.continuent.sequoia.common.xml.ControllerXmlTags;
import org.continuent.sequoia.common.xml.XmlTools;

/**
 * Class to create report from Controller
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 */
public class ReportManager
{
  private static final String FILE_APPENDER_NAME = "Filetrace";

  private static final String LINE_SEPARATOR     = System
                                                     .getProperty("line.separator");

  /** Logger instance. */
  static Trace                logger             = Trace
                                                     .getLogger(ReportManager.class
                                                         .getName());

  /** Settings */
  boolean                     reportEnabled      = false;
  boolean                     hideSensitiveData  = true;
  boolean                     generateOnShutdown = true;
  boolean                     generateOnFatal    = true;
  boolean                     enableFileLogging  = true;
  boolean                     showLogsOnly       = false;
  String                      reportLocation     = System
                                                     .getProperty("sequoia.log") == null
                                                     ? "."
                                                     : System
                                                         .getProperty("sequoia.log");

  private Controller          controller;
  private BufferedWriter      fos;

  /**
   * Call above and write controller xml information and information
   * 
   * @param controller to report
   */
  public ReportManager(Controller controller)
  {
    this.controller = controller;
  }

  /**
   * Creates a new <code>ReportManager.java</code> object Report only logs
   * 
   * @param controller the controller to report logs from
   * @param showLogsOnly show logs
   */
  public ReportManager(Controller controller, boolean showLogsOnly)
  {
    this(controller);
    this.showLogsOnly = showLogsOnly;
  }

  /**
   * Create the report file, write the environment settings if asked for, the
   * log contents (calling writeLogs()) and close the file. The method is
   * synchronized so that only one report can be generated at a time.
   * 
   * @param displaySettingsInfo true if the report should include information
   *          about the java properties, controller settings and configuration.
   */
  public synchronized void generate(boolean displaySettingsInfo)
  {
    try
    {
      createFileAndWriteSettings(displaySettingsInfo);

      // Append the logs
      writeLogs();
    }
    catch (Exception e)
    {
      logger.warn("Error while generating the report", e);
    }
    finally
    {
      try
      {
        fos.close();
      }
      catch (IOException ignore)
      {
      }
    }
  }

  /**
   * Create the report file, write the environment settings if asked to, append
   * the given exception stack trace and close the file. The method is
   * synchronized so that only one report can be generated at a time.
   * 
   * @param displaySettingsInfo true if the report should include information
   *          about the java properties, controller settings and configutation.
   * @param e exception to write about
   */
  public synchronized void generateAndWriteException(
      boolean displaySettingsInfo, Exception e)
  {
    try
    {
      createFileAndWriteSettings(displaySettingsInfo);

      writeException(e);
    }
    catch (Exception ex)
    {
      logger.warn("Error while generating the report", ex);
    }
    finally
    {
      try
      {
        fos.close();
      }
      catch (IOException ignore)
      {
      }
    }
  }

  private void createFileAndWriteSettings(boolean displaySettingsInfo)
      throws IOException
  {
    File reportFile = new File(reportLocation + File.separator
        + ControllerConstants.REPORT_FILE);
    reportFile.getParentFile().mkdirs();
    fos = new BufferedWriter(new FileWriter(reportFile));

    if (displaySettingsInfo)
    { // Settings/environment info
      writeTitle("Sequoia (version:" + Constants.VERSION
          + ") REPORT generated on " + new Date().toString());
      writeJavaProperties();
      writeControllerSettings();
      writeControllerInfo();
    }
  }

  /**
   * Write Controller info as return by <code>getInformation()</code>
   */
  private void writeControllerInfo()
  {
    try
    {
      writeHeader("CONTROLLER INFO XML");
      write(controller.getXml());
      writeHeader("DATABASE INFO XML");
      write(XmlTools.prettyXml(controller.getXmlVirtualDatabases()));
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /** Write all parameters from <code>ControllerConfiguration</code> */
  private void writeControllerSettings()
  {
    writeHeader("CONTROLLER SETTINGS");
    write(controller.getConfiguration());
  }

  /**
   * Write Details of the exception
   * 
   * @param e exception to write
   */
  private void writeException(Exception e)
  {
    writeHeader("EXCEPTION DESCRIPTION");
    write(e.getClass().toString());
    write(e.getMessage());
    write(e.toString());
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    write(sw.toString());
  }

  /** Write All Java Properties */
  private void writeJavaProperties()
  {
    writeHeader("JAVA SETTINGS");
    write(System.getProperties());
  }

  /**
   * Write Log4j log files that go to the Filetrace appender
   */
  private void writeLogs()
  {
    writeHeader("LOG CONFIGURATION");
    String s = this.getClass().getResource(ControllerConstants.LOG4J_RESOURCE)
        .getFile();
    writeFile(s);
    writeHeader("LOGS");
    if (isEnableFileLogging())
    {
      Logger log = Logger.getRootLogger();
      FileAppender appender = (FileAppender) log
          .getAppender(FILE_APPENDER_NAME);
      s = appender.getFile();
      writeFile(s);
    }
  }

  private void write(String string)
  {
    try
    {
      fos.write(string);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private void write(Hashtable table)
  {
    write(ReadWrite.write(table, true));
  }

  private void writeFile(String filename)
  {
    try
    {
      File f = new File(filename);
      BufferedReader input = new BufferedReader(new FileReader(f));
      String line = null;
      while ((line = input.readLine()) != null)
        write(line + LINE_SEPARATOR);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private void writeHeader(String header)
  {
    write(LINE_SEPARATOR);
    write("############################################################"
        + LINE_SEPARATOR);
    write("####\t\t" + header + LINE_SEPARATOR);
    write("############################################################"
        + LINE_SEPARATOR);
  }

  private void writeTitle(String title)
  {
    write("==========================================================================="
        + LINE_SEPARATOR);
    write("==========================================================================="
        + LINE_SEPARATOR);
    write("==== " + title + LINE_SEPARATOR);
    write("==========================================================================="
        + LINE_SEPARATOR);
    write("==========================================================================="
        + LINE_SEPARATOR);
  }

  private void setLogsDeleteOnExit()
  {
    try
    {
      Logger log = Logger.getRootLogger();
      RollingFileAppender appender = (RollingFileAppender) log
          .getAppender(FILE_APPENDER_NAME);
      File logFile = new File(appender.getFile());
      logFile.deleteOnExit();
    }
    catch (Exception e)
    {
      // appender has been removed or is not defined.
      logger.debug("Failed to set deleteOnExit on log file", e);
    }
  }

  /**
   * @param settings hashtable of settings
   */
  public final void setSettings(Hashtable settings)
  {
    // listLoggers();
    if (settings == null)
    {
      reportEnabled = false;
    }
    else if (settings.containsKey(ControllerXmlTags.ATT_REPORT_ENABLED))
    {
      reportEnabled = new Boolean((String) settings
          .get(ControllerXmlTags.ATT_REPORT_ENABLED)).booleanValue();
    }
    if (!reportEnabled)
    {
      hideSensitiveData = false;
      generateOnShutdown = false;
      generateOnFatal = false;
      enableFileLogging = false;
      // removeFileTraceAppender();
      return;
    }
    else
    {
      if ("true".equals(settings
          .get(ControllerXmlTags.ATT_REPORT_DELETE_ON_SHUTDOWN)))
      {
        setLogsDeleteOnExit();
      }
      if (settings
          .containsKey(ControllerXmlTags.ATT_REPORT_ENABLE_FILE_LOGGING))
      {
        enableFileLogging = new Boolean((String) settings
            .get(ControllerXmlTags.ATT_REPORT_ENABLE_FILE_LOGGING))
            .booleanValue();
        if (!enableFileLogging)
        {
          // removeFileTraceAppender();
        }
      }
      if (settings.containsKey(ControllerXmlTags.ATT_REPORT_GENERATE_ON_FATAL))
      {
        generateOnFatal = new Boolean((String) settings
            .get(ControllerXmlTags.ATT_REPORT_GENERATE_ON_FATAL))
            .booleanValue();
      }
      if (settings
          .containsKey(ControllerXmlTags.ATT_REPORT_GENERATE_ON_SHUTDOWN))
      {
        generateOnShutdown = new Boolean((String) settings
            .get(ControllerXmlTags.ATT_REPORT_GENERATE_ON_SHUTDOWN))
            .booleanValue();
      }
      if (settings
          .containsKey(ControllerXmlTags.ATT_REPORT_HIDE_SENSITIVE_DATA))
      {
        hideSensitiveData = new Boolean((String) settings
            .get(ControllerXmlTags.ATT_REPORT_HIDE_SENSITIVE_DATA))
            .booleanValue();
      }
      if (settings.containsKey(ControllerXmlTags.ATT_REPORT_REPORT_LOCATION))
      {
        reportLocation = (String) settings
            .get(ControllerXmlTags.ATT_REPORT_REPORT_LOCATION);
      }
    }
  }

  void listLoggers()
  {
    Logger log = Logger.getRootLogger();
    if (!log.isDebugEnabled())
      return;
    Enumeration loggers = Logger.getDefaultHierarchy().getCurrentLoggers();
    while (loggers.hasMoreElements())
    {
      Logger l = (Logger) loggers.nextElement();
      log.debug("Found logger:" + l.getName());
    }
  }

  /**
   * @return Returns the enableFileLogging.
   */
  public boolean isEnableFileLogging()
  {
    return enableFileLogging;
  }

  /**
   * @return Returns the generateOnFatal.
   */
  public boolean isGenerateOnFatal()
  {
    return reportEnabled && generateOnFatal;
  }

  /**
   * @return Returns the generateOnShutdown.
   */
  public boolean isGenerateOnShutdown()
  {
    return reportEnabled && generateOnShutdown;
  }

  /**
   * @return Returns the hideSensitiveData.
   */
  public boolean isHideSensitiveData()
  {
    return hideSensitiveData;
  }

  /**
   * @return Returns the reportEnabled.
   */
  public boolean isReportEnabled()
  {
    return reportEnabled;
  }

  /**
   * @return Returns the reportLocation.
   */
  public String getReportLocation()
  {
    return reportLocation;
  }

  /**
   * Sets the enableFileLogging value.
   * 
   * @param enableFileLogging The enableFileLogging to set.
   */
  public void setEnableFileLogging(boolean enableFileLogging)
  {
    this.enableFileLogging = enableFileLogging;
  }

  /**
   * Sets the generateOnFatal value.
   * 
   * @param generateOnFatal The generateOnFatal to set.
   */
  public void setGenerateOnFatal(boolean generateOnFatal)
  {
    this.generateOnFatal = generateOnFatal;
  }

  /**
   * Sets the generateOnShutdown value.
   * 
   * @param generateOnShutdown The generateOnShutdown to set.
   */
  public void setGenerateOnShutdown(boolean generateOnShutdown)
  {
    this.generateOnShutdown = generateOnShutdown;
  }

  /**
   * Sets the hideSensitiveData value.
   * 
   * @param hideSensitiveData The hideSensitiveData to set.
   */
  public void setHideSensitiveData(boolean hideSensitiveData)
  {
    this.hideSensitiveData = hideSensitiveData;
  }

  /**
   * Sets the reportEnabled value.
   * 
   * @param reportEnabled The reportEnabled to set.
   */
  public void setReportEnabled(boolean reportEnabled)
  {
    this.reportEnabled = reportEnabled;
  }

  /**
   * Sets the reportLocation value.
   * 
   * @param reportLocation The reportLocation to set.
   */
  public void setReportLocation(String reportLocation)
  {
    this.reportLocation = reportLocation;
  }
}