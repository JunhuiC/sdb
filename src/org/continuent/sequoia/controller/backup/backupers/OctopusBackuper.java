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
 * Contributor(s): Nicolas Modrzyk.
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.log4j.Category;
import org.apache.log4j.Priority;
import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.exceptions.OctopusException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.util.FileManagement;
import org.continuent.sequoia.common.util.LoggingOutputStream;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backup.BackupManager;
import org.continuent.sequoia.controller.backup.Backuper;
import org.continuent.sequoia.controller.backup.DumpTransferInfo;
import org.webdocwf.util.loader.Loader;
import org.webdocwf.util.loader.generator.LoaderGenerator;

/**
 * This class defines a Backuper based on Octopus v3.4.1.
 * <p>
 * The options supported by this Backuper must be separated by commas (default
 * is 'zip=true,redirectOutput=false'). The options are defined as follows:
 * <p>
 * zip=[true,false]: defines if the dump directory must be compressed in a zip
 * file. Default is true.
 * <p>
 * redirectOutput=[true,false]: redirect Octopus output directly in the logger
 * instead of dumping everything on the standard console (may not work on some
 * systems). Default is false.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class OctopusBackuper implements Backuper
{
  static Trace                  logger         = Trace
                                                   .getLogger(OctopusBackuper.class
                                                       .getName());
  private boolean               redirectOutput = false;
  private boolean               zipBackupFiles = true;

  static
  {
    String sequoiaHome = System.getProperty("sequoia.home");
    if (sequoiaHome != null)
      System.setProperty("OCTOPUS_HOME", sequoiaHome + File.separator + "lib"
          + File.separator + "octopus" + File.separator + "xml");
  }

  //
  // Octopus constants
  //

  private static final int      DB_NAME        = 0;
  private static final int      DRIVER         = 1;
  private static final int      FULL_NAME      = 2;
  private static final int      PREFIX_URL     = 3;

  // four values ... this is REALLY dirty but I see no other way to deal with
  // octopus constants ...
  // 1. If we parse the url, what can describe the db we're dealing with
  // 2. In octopus specific conf file, <Driver name="hsql">
  // 3. In octopus general vendors file, what driver are we using ...
  // 4. the part of the url, octopus adds up automatically, from conf file

  private static final String[] HSQL           = {"hsqldb", "hsql",
      "HypersonicSQL", "jdbc:hsqldb:"          };
  private static final String[] CSV            = {"csv", "csv", "Csv",
      "jdbc:relique:csv:"                      };
  private static final String[] MYSQL          = {"mysql", "mm", "MySQL",
      "jdbc:mysql://"                          };
  private static final String[] POSTGRESQL     = {"postgresql", "postgresql",
      "PostgreSQL", "jdbc:postgresql://"       };
  private static final String[] ORACLE         = {"oracle", "oracle", "Oracle",
      "jdbc:oracle:thin:"                      };
  private static final String[] JTURBO         = {"jTurbo", "jTurbo", "MSQL",
      "jdbc:JTurbo://"                         };
  private static final String[] MSSQL          = {"microsoft", "microsoft",
      "MSQL", "jdbc:microsoft:sqlserver://"    };

  static final Hashtable<String, String[]>        TYPES;
  static
  {
    TYPES = new Hashtable<String, String[]>();
    TYPES.put(HSQL[DB_NAME], HSQL);
    TYPES.put(CSV[DB_NAME], CSV);
    TYPES.put(MYSQL[DB_NAME], MYSQL);
    TYPES.put(ORACLE[DB_NAME], ORACLE);
    TYPES.put(POSTGRESQL[DB_NAME], POSTGRESQL);
    TYPES.put(JTURBO[DB_NAME], JTURBO);
    TYPES.put(MSSQL[DB_NAME], MSSQL);
  }

  //
  // OctopusBackuper interface implementation
  //

  /**
   * Creates a new <code>OctopusBackuper</code> object
   */
  public OctopusBackuper()
  {
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getDumpFormat()
   */
  public String getDumpFormat()
  {
    if (zipBackupFiles)
      return "Octopus v3.4.1 database neutral dump compressed";
    else
      return "Octopus v3.4.1 database neutral dump";
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#backup(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    logger.info(Translate.get("backup.manager.backuping.backend", new String[]{
        backend.getName(), dumpName}));

    if (logger.isDebugEnabled())
      logger.debug("Creating directory for backup");

    // The dump will be located in a sub-directory with the same name
    String octopusDir = createOctopusDir(path, dumpName);

    String type = getDatabaseType(backend.getURL());
    String sourceType = getOctopusType(type);
    String sourceUrl = backend.getURL().substring(getUrlPrefix(type).length());
    String sourceDriver = getOctopusDriver(type);
    String targetType = getOctopusType(TYPE_CSV);
    String targetDriver = getOctopusDriver(TYPE_CSV);
    String targetUrl = createCsvDir(octopusDir);
    String targetUser = "";
    String targetPassword = "";

    PrintStream oldStream = null;
    if (redirectOutput)
    {
      if (logger.isDebugEnabled())
        logger.debug("Redirecting Octopus output streams");

      // Prevent Octopus from dumping everything on the standard output
      oldStream = redirectOutputStream();
    }

    try
    {
      // Generate metadata
      if (logger.isDebugEnabled())
        logger.debug("### Generating Octopus metadata ###");
      callOctopusLoader(sourceType, sourceUrl, sourceDriver, login, password,
          targetType, targetDriver, targetUrl, targetUser, targetPassword,
          true, true, octopusDir);

      // Generate loader job
      if (logger.isDebugEnabled())
        logger.debug("### Generating loader job ###");
      callOctopusLoader(sourceType, sourceUrl, sourceDriver, login, password,
          targetType, targetDriver, targetUrl, targetUser, targetPassword,
          true, false, octopusDir);

      if (logger.isDebugEnabled())
      {
        logger.debug("=======================================");
        logger.debug("Using the following Octopus settings:");
        logger.debug("Octopus dump directory=" + octopusDir);
        logger.debug("Target URL=" + targetUrl);
        logger.debug("Loader job file=" + getLoaderJobFile(octopusDir));
        logger.debug("Compress backup=" + zipBackupFiles);
        logger.debug("OCTOPUS HOME:" + System.getProperty("OCTOPUS_HOME"));
        logger.debug("=======================================");
      }

      // Perform the backup
      launchOctopus(octopusDir, dumpName, tables);

      if (redirectOutput)
      {
        // Restore previous output stream
        System.setOut(oldStream);
      }
    }
    catch (Exception e)
    {
      if (redirectOutput)
      {
        // Restore previous output stream
        System.setOut(oldStream);
      }
      String msg = "Error while performing backup for backend "
          + backend.getName();
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }

    // Check if we need to compress the backup
    if (zipBackupFiles)
    {
      try
      {
        if (logger.isDebugEnabled())
          logger.debug("Compressing dump");
        Zipper.zip(path + File.separator + dumpName + Zipper.ZIP_EXT,
            octopusDir, Zipper.STORE_PATH_FROM_ZIP_ROOT);
        if (logger.isDebugEnabled())
          logger.debug("Cleaning uncompressed dump files");
        cleanUp(octopusDir);
      }
      catch (Exception e)
      {
        String msg = "Error while compressing dump";
        logger.error(msg, e);
        throw new BackupException(msg, e);
      }
    }

    return new Date(System.currentTimeMillis());
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#restore(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException
  {
    logger.info(Translate.get("backup.manager.restoring.backend", new String[]{
        backend.getName(), dumpName}));

    // The dump will be located in a sub-directory with the same name
    String octopusDir = createOctopusDir(path, dumpName);

    if (zipBackupFiles)
    {
      try
      {
        if (logger.isDebugEnabled())
          logger.debug("Uncompressing dump");
        Zipper.unzip(path + File.separator + dumpName + Zipper.ZIP_EXT,
            octopusDir);
      }
      catch (Exception e)
      {
        String msg = "Error while uncompressing dump";
        logger.error(msg, e);
        throw new BackupException(msg, e);
      }
    }

    String type = getDatabaseType(backend.getURL());
    String targetType = getOctopusType(type);
    String targetUrl = backend.getURL().substring(getUrlPrefix(type).length());
    String targetDriver = getOctopusDriver(type);
    String sourceType = getOctopusType(TYPE_CSV);
    String sourceDriver = getOctopusDriver(TYPE_CSV);
    String sourceUrl = createCsvDir(octopusDir);
    String sourceUser = "";
    String sourcePassword = "";

    PrintStream oldStream = null;
    if (redirectOutput)
    {
      if (logger.isDebugEnabled())
        logger.debug("Redirecting Octopus output streams");

      // Prevent Octopus from dumping everything on the standard output
      oldStream = redirectOutputStream();
    }

    try
    {
      // Generate loader job
      if (logger.isDebugEnabled())
        logger.debug("### Generating loader job ###");
      callOctopusLoader(sourceType, sourceUrl, sourceDriver, sourceUser,
          sourcePassword, targetType, targetDriver, targetUrl, login, password,
          false, false, octopusDir);

      setOctopusLoaderJob(octopusDir, sourceType);

      if (logger.isDebugEnabled())
      {
        logger.debug("=======================================");
        logger.debug("Using the following Octopus settings:");
        logger.debug("Octopus dump directory=" + octopusDir);
        logger.debug("Source URL=" + sourceUrl);
        logger.debug("Target URL=" + targetUrl);
        logger.debug("Loader job file=" + getLoaderJobFile(octopusDir));
        logger.debug("Compress backup=" + zipBackupFiles);
        logger.debug("OCTOPUS HOME:" + System.getProperty("OCTOPUS_HOME"));
        logger.debug("=======================================");
      }

      // Perform the backup
      launchOctopus(octopusDir, dumpName, tables);

      if (zipBackupFiles)
      {
        if (logger.isDebugEnabled())
          logger.debug("Cleaning backup files");
        cleanUp(octopusDir);
      }

      if (redirectOutput)
      {
        // Restore previous output stream
        System.setOut(oldStream);
      }
    }
    catch (Exception e)
    {
      if (redirectOutput)
      {
        // Restore previous output stream
        System.setOut(oldStream);
      }
      String msg = "Error while performing restore operation on backend "
          + backend.getName();
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#deleteDump(java.lang.String,
   *      java.lang.String)
   */
  public void deleteDump(String path, String dumpName) throws BackupException
  {
    if (zipBackupFiles)
    {
      File toRemove = new File(path + File.separator + dumpName
          + Zipper.ZIP_EXT);
      if (logger.isDebugEnabled())
        logger.debug("Deleting compressed dump " + toRemove);
      toRemove.delete();
    }
    else
    {
      if (logger.isDebugEnabled())
        logger.debug("Deleting dump directory " + path + File.separator
            + dumpName);
      cleanUp(path + File.separator + dumpName);
    }
  }

  //
  // Octopus wrappers
  //

  private static final String TYPE_CSV             = "csv";
  private static final String COPY_MODE            = "copy";
  private static final String OCTOPUS_INCLUDE_HREF = "<include href=\"sql/";

  private void callOctopusLoader(String sourceType, String sourceUrl,
      String sourceDriver, String sourceUser, String sourcePassword,
      String targetType, String targetDriver, String targetUrl,
      String targetUser, String targetPassword, boolean backup,
      boolean generateAllVendors, String octopusDir) throws OctopusException
  {
    try
    {
      if (logger.isDebugEnabled())
      {
        logger.debug("Source Type:" + sourceType);
        logger.debug("Source Driver:" + sourceDriver);
        logger.debug("Source URL :" + sourceUrl);
        logger.debug("Source User :" + sourceUser);
        logger.debug("Target Type:" + targetType);
        logger.debug("Target Driver:" + targetDriver);
        logger.debug("Target URL:" + targetUrl);
        logger.debug("Target User :" + targetUser);
        logger.debug("Generate SQL for all vendors :" + generateAllVendors);
      }
      LoaderGenerator loader = new LoaderGenerator(sourceType, // sourceType
          sourceUrl, // sourceDatabase Url?
          COPY_MODE, // valueMode
          octopusDir, // generatorOutput
          sourceDriver, // sourceDriverName
          targetDriver, // TargetDriverName
          targetUrl, // targetDataBase
          targetType, // TargetType
          sourceUser, // sourceUser
          sourcePassword, // sourcePassword
          targetUser, // targetUser
          targetPassword, // targetPassword
          "", // domlPath
          "org.webdoc.util.loader", // package name
          "true", // generate drop table stmt
          "true", // generate drop integrity statement
          "true", // generate create table stmt
          "true", // generate create pk statement
          "true", // generate create fk statement
          "true", // generate create index stmt
          String.valueOf(generateAllVendors), // generate sql for all vendors
          String.valueOf(!generateAllVendors), // generate xml
          "false", // generate doml
          String.valueOf(!generateAllVendors), // full mode ??
          String.valueOf(!backup), // restore mode
          null, // convertTablesToSemicolonSeparatedList(database.getTables()),
          // tables list
          null // Jar file structure
      );
      loader.generate();
    }
    catch (Exception e)
    {
      throw new OctopusException(e);
    }
  }

  /**
   * This start octopus with previously generated LoaderJob file
   * 
   * @param octopusDir the working directory
   * @param tables the list of tables to backup, null means all tables
   * @throws OctopusException if octopus fails
   */
  private void launchOctopus(String octopusDir, String dumpName,
      ArrayList<?> tables) throws OctopusException
  {
    try
    {
      Loader myOctopus;
      String loaderLogging;
      if (logger.isDebugEnabled())
        loaderLogging = Loader.LOGMODE_FULL;
      else if (!logger.isFatalEnabled()) // Logger is OFF
        loaderLogging = Loader.LOGMODE_NONE;
      else
        loaderLogging = Loader.LOGMODE_NORMAL;

      if (tables == null)
      {
        // Copy everything
        myOctopus = new Loader(getLoaderJobFile(octopusDir), loaderLogging,
            "sequoia", octopusDir, "Octopus" + dumpName + ".log", true, null,
            null, true, null, 0, 100);
      }
      else
      {
        // Copy only the tables we want
        myOctopus = new Loader(getLoaderJobFile(octopusDir), loaderLogging,
            "sequoia", octopusDir, "Octopus" + dumpName + ".log", true, null,
            null, true, null, 0, 100, convertTablesToArray(tables));
      }
      try
      {
        myOctopus.load();
      }
      catch (Exception e)
      {
        logger.error("Failed to load octopus", e);
        throw new OctopusException(Translate.get(
            "controller.octopus.load.failed", e));
      }
    }
    // I am doing this because Octopus throws NullPointerException
    // all the time so it is impossible to know which failed
    catch (OctopusException oe)
    {
      // This is thrown only by the above.
      throw oe;
    }
    catch (Exception e)
    {
      throw new OctopusException(Translate
          .get("controller.octopus.instance.failed"));
    }
  }

  private void cleanUp(String octopusDir)
  {
    if (logger.isDebugEnabled())
      logger.debug("Cleaning up temporary backup files...");
    File toRemove = new File(octopusDir);
    FileManagement.deleteDir(toRemove);
  }

  private String[] convertTablesToArray(ArrayList<?> tablesList)
  {
    int length = tablesList.size();
    String[] result = new String[length];
    for (int i = 0; i < length; i++)
      result[i] = ((DatabaseTable) tablesList.get(i)).getName();
    return result;
  }

  private String createOctopusDir(String path, String dumpName)
      throws BackupException
  {
    // Create main octopus directory
    String octopusDir = path + File.separator + dumpName;

    File octopusd = new File(octopusDir);
    octopusd.mkdirs();
    octopusd.mkdir();

    if (!octopusd.exists())
      throw new BackupException("backup.directory.cannot.be.created");

    return octopusDir;
  }

  private String createCsvDir(String octopusDir) throws BackupException
  {
    // Create Csv directory
    String csvDir = TYPE_CSV;
    File csvd = new File(octopusDir + File.separator + csvDir);
    csvDir = csvd.getAbsolutePath();
    csvd.mkdirs();
    csvd.mkdir();

    if (!csvd.exists())
      throw new BackupException("backup.directory.cannot.be.created");

    return csvDir;
  }

  private String getDatabaseType(String url) throws BackupException
  {
    if (url == null)
      throw new BackupException("Invalid null source url");
    int index = url.indexOf(':');
    int index2 = url.indexOf(':', index + 1);
    if (index == -1 || index2 == -1 || index > index2)
      throw new BackupException("Invalid source url format");
    String type = url.substring(index + 1, index2);
    return type;
  }

  private String getLoaderJobFile(String octopusDir)
  {
    return octopusDir + File.separator + "LoaderJob.olj";
  }

  @SuppressWarnings("deprecation")
private PrintStream redirectOutputStream()
  {
    PrintStream previousOut = System.out;
    System.setOut(new PrintStream(new LoggingOutputStream(Category
        .getInstance(this.getClass().getName()), Priority.DEBUG), true));
    return previousOut;
  }

  private void setOctopusLoaderJob(String octopusDir, String sourceType)
      throws OctopusException
  {
    String onErrorContinueEqualFalse = "onErrorContinue=\"false\"";
    String onErrorContinueEqualTrue = "onErrorContinue=\"true\"";
    BufferedReader br = null;
    BufferedWriter bw = null;

    try
    {
      br = new BufferedReader(new FileReader(getLoaderJobFile(octopusDir)));
      String line = "";
      StringBuffer buffer = new StringBuffer();

      while ((line = br.readLine()) != null)
      {
        /* Give the metadata location */
        int idx = line.indexOf(OCTOPUS_INCLUDE_HREF);
        if (idx != -1)
        {
          idx += OCTOPUS_INCLUDE_HREF.length();
          // -4 = Skip "sql/"
          line = line.substring(0, idx - 4) + ".." + File.separator
              + octopusDir + File.separator + "SQLForAllVendors"
              + File.separator + sourceType + File.separator + "sql"
              + File.separator + line.substring(idx);
        }

        /* Force on error continue */
        int index7 = line.indexOf(onErrorContinueEqualFalse);
        if (index7 != -1)
        {
          line = line.substring(0, index7) + onErrorContinueEqualTrue
              + line.substring(index7 + onErrorContinueEqualFalse.length());
        }
        buffer.append(line + System.getProperty("line.separator"));
      }
      br.close();
      if (logger.isDebugEnabled())
      {
        logger.debug("Octopus file updated with success");
      }

      bw = new BufferedWriter(new FileWriter(getLoaderJobFile(octopusDir)));
      bw.write(buffer.toString());
      bw.close();
    }
    catch (FileNotFoundException fie)
    {
      // loader job was not generated properly
      logger.warn(Translate.get("controller.octopus.loader.job.not.found"));
      throw new OctopusException(fie.getMessage());
    }
    catch (IOException e)
    {
      // Error while reading file
      logger.warn(Translate.get("controller.octopus.loader.io.problem"));
    }
    finally
    {
      // close the open streams
      if (bw != null)
        try
        {
          bw.close();
        }
        catch (IOException e1)
        {

        }
      if (br != null)
        try
        {
          br.close();
        }
        catch (IOException e2)
        {
        }
    }
  }

  /**
   * Get octopus type.
   * 
   * @param type from url
   * @return value from hashtable or null
   * @throws BackupException if the type is not supported
   */
  private String getOctopusType(String type) throws BackupException
  {
    if (type == null)
      return null;

    // SEQUOIA-700 fix
    if (!TYPES.containsKey(type))
      throw new BackupException(
          "OctopusBackuper does not support this database type.");

    return ((String[]) TYPES.get(type))[OctopusBackuper.FULL_NAME];
  }

  /**
   * Get octopus driver.
   * 
   * @param type from url
   * @return value from hashtable or null
   */
  private String getOctopusDriver(String type)
  {
    if (type == null)
      return null;
    return ((String[]) TYPES.get(type))[OctopusBackuper.DRIVER];
  }

  /**
   * Get Octopus url prefix
   * 
   * @param type from url
   * @return value from hashtable or null
   */
  private String getUrlPrefix(String type)
  {
    if (type == null)
      return null;
    return ((String[]) TYPES.get(type))[OctopusBackuper.PREFIX_URL];
  }

  //
  // Octopus Backuper options
  //

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getOptions()
   */
  public String getOptions()
  {
    return "zip=" + String.valueOf(zipBackupFiles);
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#setOptions(java.lang.String)
   */
  public void setOptions(String options)
  {
    if (options != null)
    {
      StringTokenizer strTok = new StringTokenizer(options, ",");
      String option = null;
      String name = null;
      String value = null;

      // Parse the string of options, add them to the HashMap
      while (strTok.hasMoreTokens())
      {
        option = strTok.nextToken();
        name = option.substring(0, option.indexOf("="));
        value = option.substring(option.indexOf("=") + 1, option.length());

        if ("zip".equals(name))
        {
          try
          {
            zipBackupFiles = !"false".equals(value);
          }
          catch (RuntimeException e)
          {
            zipBackupFiles = true;
            logger
                .warn("Invalid zip value for OctopusBackuper, available option is 'zip=[true,false]' ("
                    + option + ")");
          }
        }
        else if ("redirectOutput".equals(name))
        {
          try
          {
            redirectOutput = "true".equals(value);
          }
          catch (RuntimeException e)
          {
            redirectOutput = false;
            logger
                .warn("Invalid redirectOutput value for OctopusBackuper, available option is 'redirectOutput=[true,false]' ("
                    + option + ")");
          }
        }
        else
        {
          logger
              .warn("Unsupported option '" + option + "' for OctopusBackuper");
        }
      }
    }
    logger.info("OctopusBackuper backup compression is set to "
        + zipBackupFiles);
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#fetchDump(org.continuent.sequoia.controller.backup.DumpTransferInfo,
   *      java.lang.String, java.lang.String)
   */
  public void fetchDump(DumpTransferInfo dumpTransferInfo, String path,
      String dumpName) throws BackupException, IOException
  {
    BackupManager.fetchDumpFile(dumpTransferInfo, path, dumpName
        + Zipper.ZIP_EXT);
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#setupDumpServer()
   */
  public DumpTransferInfo setupDumpServer() throws IOException
  {
    return BackupManager.setupDumpFileServer();
  }
}