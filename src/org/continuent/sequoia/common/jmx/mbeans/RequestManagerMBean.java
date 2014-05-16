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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.jmx.mbeans;

/**
 * This class defines a RequestManagerMBean
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 * @version 1.0
 */
public interface RequestManagerMBean
{
  /**
   * Sets the parsing case sensitivity. If true the request are parsed in a case
   * sensitive way (table/column name must match exactly the case of the names
   * fetched from the database or enforced by a static schema).
   * 
   * @param isCaseSensitiveParsing true if parsing is case sensitive
   */
  void setCaseSensitiveParsing(boolean isCaseSensitiveParsing);

  /**
   * Returns the beginTimeout value.
   * 
   * @return Returns the beginTimeout.
   */
  long getBeginTimeout();

  /**
   * Returns the cacheParsingranularity value.
   * 
   * @return Returns the cacheParsingranularity.
   */
  int getCacheParsingranularity();

  /**
   * Sets the cacheParsingranularity value.
   * 
   * @param cacheParsingranularity The cacheParsingranularity to set.
   */
  void setCacheParsingranularity(int cacheParsingranularity);

  /**
   * Returns the commitTimeout value.
   * 
   * @return Returns the commitTimeout.
   */
  long getCommitTimeout();

  /**
   * Returns the requiredParsingGranularity value.
   * 
   * @return Returns the requiredParsingGranularity.
   */
  int getRequiredParsingGranularity();

  /**
   * Returns the rollbackTimeout value.
   * 
   * @return Returns the rollbackTimeout.
   */
  long getRollbackTimeout();

  /**
   * Returns the schedulerParsingranularity value.
   * 
   * @return Returns the schedulerParsingranularity.
   */
  int getSchedulerParsingranularity();

  /**
   * Sets the schedulerParsingranularity value.
   * 
   * @param schedulerParsingranularity The schedulerParsingranularity to set.
   */
  void setSchedulerParsingranularity(int schedulerParsingranularity);

  /**
   * Returns the schemaIsStatic value.
   * 
   * @return Returns the schemaIsStatic.
   */
  boolean isStaticSchema();

  /**
   * Sets the schemaIsStatic value.
   * 
   * @param schemaIsStatic The schemaIsStatic to set.
   */
  void setStaticSchema(boolean schemaIsStatic);

  /**
   * Returns the isCaseSensitiveParsing value.
   * 
   * @return Returns the isCaseSensitiveParsing.
   */
  boolean isCaseSensitiveParsing();
  
  /**
   * Use request factory to parse the given sql request and returns result of
   * the newly created request parsing
   * 
   * @param sqlRequest sql query to parse
   * @param lineSeparator request line separator used on client side
   * @return result of the parsing
   */
  String parseSqlRequest(String sqlRequest, String lineSeparator);

}