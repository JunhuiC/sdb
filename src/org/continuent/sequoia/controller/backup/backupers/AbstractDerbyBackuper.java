/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2008 Emmanuel Cecchet.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.util.FileManagement;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backup.BackupManager;
import org.continuent.sequoia.controller.backup.Backuper;
import org.continuent.sequoia.controller.backup.DumpTransferInfo;

/**
 * This class defines an AbstractDerbyBackuper that factorizes code for Derby
 * backuper implementations.
 * 
 * @author <a href="mailto:emmanuel.cecchet@epfl.ch">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class AbstractDerbyBackuper implements Backuper
{
  static Trace logger = Trace
                          .getLogger("org.continuent.sequoia.controller.backup.backupers.DerbyBackuper");

  /**
   * Get the dump physical path from its logical name
   * 
   * @param path the path where the dump is stored
   * @param dumpName dump logical name
   * @return path to zip file
   */
  protected abstract String getDumpPhysicalPath(String path, String dumpName);

  /**
   * Extract the path where the Derby database is stored by parsing the backend
   * JDBC URL.
   * 
   * @param backend the Derby backend
   * @param checkPath if true we check if the path is a valid directory
   * @return path to the Derby database
   * @throws BackupException if the URL is not valid or the path not valid
   */
  protected abstract String getDerbyPath(DatabaseBackend backend,
      boolean checkPath) throws BackupException;

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getOptions()
   */
  public String getOptions()
  {
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#setOptions(java.lang.String)
   */
  public void setOptions(String options)
  {
    // Ignored, no options
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#backup(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList tables) throws BackupException
  {
    String derbyPath = getDerbyPath(backend, true);

    try
    {
      File pathDir = new File(path);
      if (!pathDir.exists())
      {
        pathDir.mkdirs();
        pathDir.mkdir();
      }

      if (logger.isDebugEnabled())
        logger.debug("Archiving " + derbyPath + " in " + path + File.separator
            + dumpName + Zipper.ZIP_EXT);

      Zipper.zip(getDumpPhysicalPath(path, dumpName), derbyPath,
          Zipper.STORE_PATH_FROM_ZIP_ROOT);
    }
    catch (Exception e)
    {
      String msg = "Error while performing backup";
      logger.error(msg, e);
      throw new BackupException(msg, e);
    }

    return new Date(System.currentTimeMillis());
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#restore(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.util.ArrayList)
   */
  public void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList tables) throws BackupException
  {
    String derbyPath = getDerbyPath(backend, false);

    File derbyDir = new File(derbyPath);

    // First delete any existing directory
    if (FileManagement.deleteDir(derbyDir))
      logger.info("Existing Derby directory " + derbyPath
          + " has been deleted.");

    // Now create the dir
    derbyDir.mkdirs();
    derbyDir.mkdir();

    // Unzip the dump
    try
    {
      if (logger.isDebugEnabled())
        logger.debug("Uncompressing dump");
      Zipper.unzip(getDumpPhysicalPath(path, dumpName), derbyPath);
    }
    catch (Exception e)
    {
      String msg = "Error while uncompressing dump";
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
    File toRemove = new File(getDumpPhysicalPath(path, dumpName));
    if (logger.isDebugEnabled())
      logger.debug("Deleting compressed dump " + toRemove);
    toRemove.delete();
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