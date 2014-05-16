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
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.controller.monitoring;

import java.util.regex.Pattern;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This class implements a SQL monitoring rule.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class SQLMonitoringRule
{
  private Pattern queryPattern;
  private boolean isCaseSentive;
  private boolean applyToSkeleton;
  private boolean monitoring;

  /**
   * Creates a new SQL Monitoring rule
   * 
   * @param queryPattern the query pattern to match
   * @param isCaseSentive true if matching is case sensitive
   * @param applyToSkeleton true if matching applies to the query skeleton
   * @param monitoring true if the request must be monitored
   */
  public SQLMonitoringRule(String queryPattern, boolean isCaseSentive,
      boolean applyToSkeleton, boolean monitoring)
  {
    this.isCaseSentive = isCaseSentive;
    if (isCaseSentive)
      this.queryPattern = Pattern.compile(queryPattern);
    else
      this.queryPattern = Pattern.compile(queryPattern,
          Pattern.CASE_INSENSITIVE);
    this.applyToSkeleton = applyToSkeleton;
    this.monitoring = monitoring;
  }

  /**
   * If matching is case sensitive or not
   * 
   * @return true if the matching is case sensitive
   */
  public boolean isCaseSentive()
  {
    return isCaseSentive;
  }

  /**
   * If monitoring is activated or not.
   * 
   * @return true if monitoring is activated for this pattern
   */
  public boolean isMonitoring()
  {
    return monitoring;
  }

  /**
   * Get query pattern
   * 
   * @return the query pattern
   */
  public String getQueryPattern()
  {
    return queryPattern.toString();
  }

  /**
   * Set the matching case sensitiveness
   * 
   * @param b true if matching is case sensitive
   */
  public void setCaseSentive(boolean b)
  {
    isCaseSentive = b;
  }

  /**
   * Set the monitoring on or off
   * 
   * @param b true if monitoring must be activated for this rule
   */
  public void setMonitoring(boolean b)
  {
    monitoring = b;
  }

  /**
   * Sets the query pattern
   * 
   * @param queryPattern the queryPattern
   */
  public void setQueryPattern(String queryPattern)
  {
    this.queryPattern = Pattern.compile(queryPattern);
  }

  /**
   * If the pattern apply to the skeleton ot the instanciated query.
   * 
   * @return true if the pattern apply to the query skeleton
   */
  public boolean isApplyToSkeleton()
  {
    return applyToSkeleton;
  }

  /**
   * Set to true if the pattern apply to the query skeleton
   * 
   * @param b true if the pattern apply to the query skeleton
   */
  public void setApplyToSkeleton(boolean b)
  {
    applyToSkeleton = b;
  }

  /**
   * Returns true if the given query matches the pattern of this rule. This
   * function applies the applytoSkeleton rule.
   * 
   * @param request the query
   * @return the SQL that matches the rule or null if it does not match
   */
  public String matches(AbstractRequest request)
  {
    if (queryPattern.matcher(request.getSqlOrTemplate()).matches())
      return request.getSqlOrTemplate();
    else
      return null;
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    String info = "<" + DatabasesXmlTags.ELT_SQLMonitoringRule + " "
        + DatabasesXmlTags.ATT_queryPattern + "=\"" + getQueryPattern() + "\" "
        + DatabasesXmlTags.ATT_caseSensitive + "=\"" + isCaseSentive() + "\" "
        + DatabasesXmlTags.ATT_applyToSkeleton + "=\"" + isApplyToSkeleton()
        + "\" " + DatabasesXmlTags.ATT_monitoring + "=\"";
    if (isMonitoring())
      info += "on";
    else
      info += "off";
    info += "\"/>";
    return info;
  }

}
