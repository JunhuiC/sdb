/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent Inc.
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
 */

package org.continuent.sequoia.controller.backup.backupers;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.controller.backup.BackupManager;
import org.continuent.sequoia.controller.backup.Backuper;
import org.continuent.sequoia.controller.backup.DumpTransferInfo;

/**
 * This class defines a AbstractBackuper, which can be used as a base class for
 * most backupers. This class provides a default implementation for options
 * parsing/setting and an implementation for dump serving/fetching that
 * understands the dumpServer option.
 */
public abstract class AbstractBackuper implements Backuper
{
  protected HashMap<String, String> optionsMap    = new HashMap<String, String>();
  protected String  optionsString = null;

  /**
   * @see Backuper#getOptions()
   */
  public String getOptions()
  {
    return optionsString;
  }

  /**
   * @see Backuper#setOptions(java.lang.String)
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
        optionsMap.put(name, value);
      }

      optionsString = options;
    }
  }

  /**
   * Returns the value of 'ignoreStdErrOutput' option. This option tells whether
   * command success should be altered by any output on stderr. Default is
   * false.
   * 
   * @return true if ignoreStdErrOutput option is set to true, false otherwise
   */
  public boolean getIgnoreStdErrOutput()
  {
    try
    {
      return Boolean.valueOf((String) optionsMap.get("ignoreStdErrOutput"))
          .booleanValue();
    }
    catch (Exception e)
    { // Invalid or non-existing value for ignoreStdErrOutput
      return false;
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.continuent.sequoia.controller.backup.Backuper#setupDumpServer()
   */
  public DumpTransferInfo setupDumpServer() throws IOException
  {
    if (optionsMap.containsKey("dumpServer"))
      return BackupManager.setupDumpFileServer((String) optionsMap
          .get("dumpServer"));

    return BackupManager.setupDumpFileServer();
  }

  /**
   * @see Backuper#fetchDump(org.continuent.sequoia.controller.backup.DumpTransferInfo,
   *      java.lang.String, java.lang.String)
   */
  public void fetchDump(DumpTransferInfo dumpTransferInfo, String path,
      String dumpName) throws BackupException, IOException
  {
    BackupManager.fetchDumpFile(dumpTransferInfo, path, dumpName);
  }

}
