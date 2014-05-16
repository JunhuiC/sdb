/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2008 Emmanuel Cecchet
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

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backup.Backuper;

/**
 * This class defines a Backuper for Apache Derby databases.
 * <p>
 * Supported URLs are jdbc:derby://host:port/PathToDerbyDatabase[;options]
 * <p>
 * The Backuper itself does not take any option. It simply dumps the Derby
 * directory into a zip file.
 * 
 * @author <a href="mailto:emmanuel.cecchet@epfl.ch">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DerbyLocalNetBackuper extends AbstractDerbyBackuper
    implements
      Backuper
{

  /**
   * Creates a new <code>DerbyEmbeddedBackuper</code> object
   */
  public DerbyLocalNetBackuper()
  {
  }

  /**
   * @see org.continuent.sequoia.controller.backup.Backuper#getDumpFormat()
   */
  public String getDumpFormat()
  {
    return "Derby local network server compressed dump";
  }

  /**
   * @see org.continuent.sequoia.controller.backup.backupers.AbstractDerbyBackuper#getDumpPhysicalPath(java.lang.String,
   *      java.lang.String)
   */
  protected String getDumpPhysicalPath(String path, String dumpName)
  {
    return path + File.separator + dumpName + Zipper.ZIP_EXT;
  }

  /**
   * @see org.continuent.sequoia.controller.backup.backupers.AbstractDerbyBackuper#getDerbyPath(org.continuent.sequoia.controller.backend.DatabaseBackend,
   *      boolean)
   */
  protected String getDerbyPath(DatabaseBackend backend, boolean checkPath)
      throws BackupException
  {
    String url = backend.getURL();
    if (!url.startsWith("jdbc:derby://"))
      throw new BackupException("Unsupported url " + url
          + " expecting jdbc:derby://hostname[:port]/pathToDb[;options]");

    // Strip 'jdbc:derby://'
    // 11 = "jdbc:derby://".length()
    String derbyPath = url.substring(13);

    // Strip 'hostname[:port]/'
    int nextSlash = derbyPath.indexOf("/");
    if (nextSlash == -1)
      throw new BackupException("Unsupported url " + url
          + " expecting jdbc:derby://hostname[:port]/pathToDb[;options]");
    derbyPath = derbyPath.substring(nextSlash);

    // If path starts with "//" to force an absolute path, strip first slash
    if (derbyPath.startsWith("//"))
      derbyPath = derbyPath.substring(1);

    // Remove all options that are after the first semicolon
    int semicolon = derbyPath.indexOf(';');
    if (semicolon > -1)
      derbyPath = derbyPath.substring(0, semicolon);

    if (checkPath)
    {
      File checkDerbyPath = new File(derbyPath);
      if (!checkDerbyPath.isDirectory())
        throw new BackupException(
            "Directory "
                + derbyPath
                + " does not exist. This might be due to an unsupported URL format (expectin jdbc:derby:pathToDb)");
    }

    return derbyPath;
  }

}
