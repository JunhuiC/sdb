/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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

package org.continuent.sequoia.common.util;

import java.io.File;

/**
 * This class defines a FileManagement that provides tools to manage files and
 * directories.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class FileManagement
{

  /**
   * Delete a directory by deleting recursively all sub files.
   * 
   * @param dir directoty to delete
   * @return true if success, false otherwise
   */
  public static final boolean deleteDir(File dir)
  {
    if (dir.isDirectory())
    {
      String[] children = dir.list();
      for (int i = 0; i < children.length; i++)
      {
        boolean success = deleteDir(new File(dir, children[i]));
        if (!success)
        {
          return false;
        }
      }
    }
    // The directory is now empty so delete it
    return dir.delete();
  }

}
