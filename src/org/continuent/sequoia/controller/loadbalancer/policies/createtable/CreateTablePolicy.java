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
 * Contributor(s): _______________________
 */

package org.continuent.sequoia.controller.loadbalancer.policies.createtable;

import java.util.HashMap;
import java.util.Iterator;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;

/**
 * Defines the policy to adopt when creating a new table.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class CreateTablePolicy implements XmlComponent
{
  /** Pickup a backend name randomly in the backend list. */
  public static final int RANDOM      = 0;

  /** Backends are chosen using a round-robin algorithm. */
  public static final int ROUND_ROBIN = 1;

  /** Table is created on all backends in the backend list. */
  public static final int ALL         = 2;

  /** List of backends to wait for. */
  private HashMap         ruleList    = new HashMap();

  /**
   * Adds a rule to this policy. <br>
   * If the rule's table name is <code>null</code>, the rule is considered as
   * the default rule
   * 
   * @param rule rule to add
   */
  public void addRule(CreateTableRule rule)
  {
    ruleList.put(rule.getTableName(), rule);
  }

  /**
   * Returns the rule Hashmap(table name,rule).
   * 
   * @return Hashmap
   */
  public HashMap getRuleList()
  {
    return ruleList;
  }

  /**
   * Gets the rule corresponding to a table name.
   * 
   * @param tableName table name of the rule
   * @return the rule or <code>null</code> if no specific rule has been
   *         defined for this table
   */
  public CreateTableRule getTableRule(String tableName)
  {
    return (CreateTableRule) ruleList.get(tableName);
  }

  /**
   * Returns the default rule or <code>null</code> if no default rule has been
   * defined.
   * 
   * @return a <code>CreateTableRule</code>
   */
  public CreateTableRule getDefaultRule()
  {
    return (CreateTableRule) ruleList.get(null);
  }

  /**
   * Returns the xml attribute value for the given policy
   * 
   * @param policy the policy to convert
   * @return xml attribute value or "" if not found
   */
  public static final String getXmlValue(int policy)
  {
    switch (policy)
    {
      case RANDOM :
        return DatabasesXmlTags.VAL_random;
      case ROUND_ROBIN :
        return DatabasesXmlTags.VAL_roundRobin;
      case ALL :
        return DatabasesXmlTags.VAL_all;
      default :
        return "";
    }
  }

  /**
   * Returns xml formatted string containing information on all rules of the
   * system
   * 
   * @return xml formatted string.
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    for (Iterator iterator = ruleList.keySet().iterator(); iterator.hasNext();)
      info.append(((CreateTableRule) ruleList.get(iterator.next())).getXml());
    return info.toString();
  }

}
