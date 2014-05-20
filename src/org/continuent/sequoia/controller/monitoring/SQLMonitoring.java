/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 Continuent, Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.util.Stats;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This class implements a SQL monitoring module.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class SQLMonitoring extends Monitoring
{
  private Hashtable<String, Stats>    statList;     // SQL query -> Stat
  private ArrayList<SQLMonitoringRule>    ruleList;
  private boolean      defaultRule;

  private static Trace logger = null;

  /**
   * Create a SQLMonitoring object.
   * 
   * @param vdbName name of the virtual database to be used by the logger
   */
  public SQLMonitoring(String vdbName)
  {
    statList = new Hashtable<String, Stats>();
    ruleList = new ArrayList<SQLMonitoringRule>();
    logger = Trace.getLogger("org.continuent.sequoia.controller.monitoring."
        + vdbName);
    setActive(true);
  }

  /**
   * @see org.continuent.sequoia.controller.monitoring.Monitoring#cleanStats()
   */
  public void cleanStats()
  {
    statList.clear();
  }

  /**
   * Log the time elapsed to execute the given request. The time is computed
   * from the request start and end time stored in the object.
   * 
   * @param request the request executed
   */
  public final void logRequestTime(AbstractRequest request)
  {
    Stats stat = getStatForRequest(request);
    if (stat == null)
      return;
    stat.incrementCount();
    long time = request.getEndTime() - request.getStartTime();
    stat.updateTime(time);
    if (logger.isDebugEnabled())
      logger.debug(time + "ms for " + stat.getName());
  }

  /**
   * Log an error for the given request.
   * 
   * @param request the request that failed to execute
   */
  public final void logError(AbstractRequest request)
  {
    Stats stat = getStatForRequest(request);
    if (stat == null)
      return;
    stat.incrementError();
    if (logger.isDebugEnabled())
      logger.debug("ERROR " + stat.getName());
  }

  /**
   * Log a cache hit for the given request.
   * 
   * @param request the request that failed to execute
   */
  public final void logCacheHit(AbstractRequest request)
  {
    Stats stat = getStatForRequest(request);
    if (stat == null)
      return;
    stat.incrementCacheHit();
    if (logger.isDebugEnabled())
      logger.debug("Cache hit " + stat.getName());
  }

  /**
   * Reset the stats associated to a request.
   * 
   * @param request the request to reset
   */
  public final void resetRequestStat(AbstractRequest request)
  {
    Stats stat = getStatForRequest(request);
    if (stat == null)
      return;
    stat.reset();
  }

  /**
   * Retrieve the stat corresponding to a request and create it if it does not
   * exist.
   * 
   * @param request the request to look for
   * @return corresponding stat or null if a rule does not authorize this
   *         request to be monitored
   */
  public final Stats getStatForRequest(AbstractRequest request)
  {
    String sql = monitorRequestRule(request);
    if (sql == null)
      return null;

    // Note that the Hashtable is synchronized
    Stats stat = (Stats) statList.get(sql);
    if (stat == null)
    { // No entry for this query, create a new Stats entry
      stat = new Stats(sql);
      statList.put(sql, stat);
    }
    return stat;
  }

  /**
   * Return all stats information in the form of a String
   * 
   * @return stats information
   */
  public String[][] getAllStatsInformation()
  {
    Collection<Stats> values = statList.values();
    String[][] result = new String[values.size()][];
    int i = 0;
    for (Iterator<Stats> iter = values.iterator(); iter.hasNext(); i++)
    {
      Stats stat = (Stats) iter.next();
      result[i] = stat.toStringTable();
    }
    return result;
  }

  /**
   * Dump all stats using the current logger (INFO level).
   */
  public void dumpAllStatsInformation()
  {
    if (logger.isInfoEnabled())
    {
      for (Iterator<Stats> iter = statList.values().iterator(); iter.hasNext();)
      {
        Stats stat = (Stats) iter.next();
        logger.info(stat.singleLineDisplay());
      }
    }
  }

  /*
   * Rules Management
   */

  /**
   * Get the default monitoring rule
   * 
   * @return true if default is monitoring enabled
   */
  public boolean getDefaultRule()
  {
    return defaultRule;
  }

  /**
   * Defines the default rule
   * 
   * @param monitoring true if on, false is off
   */
  public void setDefaultRule(boolean monitoring)
  {
    this.defaultRule = monitoring;
  }

  /**
   * Add a rule to the list.
   * 
   * @param rule the rule to add
   */
  public void addRule(SQLMonitoringRule rule)
  {
    this.ruleList.add(rule);
  }

  /**
   * Check the rule list to check if this request should be monitored or not.
   * 
   * @param request the query to look for
   * @return the SQL query to monitor or null if monitoring is off for this
   *         request
   */
  private String monitorRequestRule(AbstractRequest request)
  {
    for (int i = 0; i < ruleList.size(); i++)
    {
      SQLMonitoringRule rule = (SQLMonitoringRule) ruleList.get(i);
      String sql = rule.matches(request);
      if (sql != null)
      { // This rule matches
        if (rule.isMonitoring())
          return sql;
        else
          return null;
      }
    }

    // No rule matched, use the default rule
    if (defaultRule)
      return request.getSqlOrTemplate();
    else
      return null;
  }

  /**
   * @return Returns the ruleList.
   */
  public ArrayList<SQLMonitoringRule> getRuleList()
  {
    return ruleList;
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXmlImpl()
  {
    String info = "<" + DatabasesXmlTags.ELT_SQLMonitoring + " "
        + DatabasesXmlTags.ATT_defaultMonitoring + "=\"";
    String defaultMonitoring = getDefaultRule() ? "on" : "off";
    info += defaultMonitoring;
    info += "\">";
    for (int i = 0; i < ruleList.size(); i++)
    {
      SQLMonitoringRule rule = (SQLMonitoringRule) ruleList.get(i);
      info += rule.getXml();
    }
    info += "</" + DatabasesXmlTags.ELT_SQLMonitoring + ">";
    return info;
  }

}