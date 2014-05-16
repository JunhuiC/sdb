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

package org.continuent.sequoia.common.jmx.management;

import java.io.Serializable;
import java.util.Date;

/**
 * This class defines a DumpInfo which carries dump metadata information that is
 * mapped on a row in the dump table of the recovery log.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DumpInfo implements Serializable
{
  private static final long serialVersionUID = -5627995243952765938L;

  private String            dumpName;
  private Date              dumpDate;
  private String            dumpPath;
  private String            dumpFormat;
  private String            checkpointName;
  private String            backendName;
  private String            tables;

  /**
   * Creates a new <code>DumpInfo</code> object
   * 
   * @param dumpName the dump logical name
   * @param dumpDate the date at which the dump was started
   * @param dumpPath the path where the dump can be found
   * @param dumpFormat the format of the dump
   * @param checkpointName the checkpoint name associated to this dump
   * @param backendName the name of the backend that was dumped
   * @param tables the list of tables contained in the dump ('*' means all
   *          tables)
   */
  public DumpInfo(String dumpName, Date dumpDate, String dumpPath,
      String dumpFormat, String checkpointName, String backendName,
      String tables)
  {
    this.dumpName = dumpName;
    this.dumpDate = dumpDate;
    this.dumpPath = dumpPath;
    this.dumpFormat = dumpFormat;
    this.checkpointName = checkpointName;
    this.backendName = backendName;
    this.tables = tables;
  }

  /**
   * Returns the backendName value.
   * 
   * @return Returns the backendName.
   */
  public String getBackendName()
  {
    return backendName;
  }

  /**
   * Sets the backendName value.
   * 
   * @param backendName The backendName to set.
   */
  public void setBackendName(String backendName)
  {
    this.backendName = backendName;
  }

  /**
   * Returns the checkpointName value.
   * 
   * @return Returns the checkpointName.
   */
  public String getCheckpointName()
  {
    return checkpointName;
  }

  /**
   * Sets the checkpointName value.
   * 
   * @param checkpointName The checkpointName to set.
   */
  public void setCheckpointName(String checkpointName)
  {
    this.checkpointName = checkpointName;
  }

  /**
   * Returns the dumpDate value.
   * 
   * @return Returns the dumpDate.
   */
  public Date getDumpDate()
  {
    return dumpDate;
  }

  /**
   * Sets the dumpDate value.
   * 
   * @param dumpDate The dumpDate to set.
   */
  public void setDumpDate(Date dumpDate)
  {
    this.dumpDate = dumpDate;
  }

  /**
   * Returns the dumpName value.
   * 
   * @return Returns the dumpName.
   */
  public String getDumpName()
  {
    return dumpName;
  }

  /**
   * Sets the dumpName value.
   * 
   * @param dumpName The dumpName to set.
   */
  public void setDumpName(String dumpName)
  {
    this.dumpName = dumpName;
  }

  /**
   * Returns the dumpPath value.
   * 
   * @return Returns the dumpPath.
   */
  public String getDumpPath()
  {
    return dumpPath;
  }

  /**
   * Sets the dumpPath value.
   * 
   * @param dumpPath The dumpPath to set.
   */
  public void setDumpPath(String dumpPath)
  {
    this.dumpPath = dumpPath;
  }

  /**
   * Returns the dumpFormat value.
   * 
   * @return Returns the dumpFormat.
   */
  public String getDumpFormat()
  {
    return dumpFormat;
  }

  /**
   * Sets the dumpFormat value.
   * 
   * @param dumpFormat The dumpFormat to set.
   */
  public void setDumpFormat(String dumpFormat)
  {
    this.dumpFormat = dumpFormat;
  }

  /**
   * Returns the tables value.
   * 
   * @return Returns the tables.
   */
  public String getTables()
  {
    return tables;
  }

  /**
   * Sets the tables value.
   * 
   * @param tables The tables to set.
   */
  public void setTables(String tables)
  {
    this.tables = tables;
  }

}
