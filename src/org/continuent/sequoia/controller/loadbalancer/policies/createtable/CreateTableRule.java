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
 * Contributor(s): Jean-Bernard van Zuylen
 */

package org.continuent.sequoia.controller.loadbalancer.policies.createtable;

import java.util.ArrayList;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * Defines the policy to adopt when creating a new table.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @version 1.0
 */
public abstract class CreateTableRule
{
  /** List of backend names to wait for. */
  protected ArrayList<String> backendList;

  /** Number of nodes that must create the table. */
  protected int       nbOfNodes = 0;

  /**
   * Table name pattern to which this rule apply (null means it is the default
   * rule).
   */
  protected String    tableName = null;

  protected int       policy;

  /**
   * Constructor for CreateTableRule.
   * 
   * @param policy the implemented policy
   */
  public CreateTableRule(int policy)
  {
    this.policy = policy;
    backendList = new ArrayList<String>();
  }

  /**
   * Creates a new <code>CreateTableRule</code> instance.
   * 
   * @param policy the implemented policy
   * @param backendList the backend list to use
   */
  public CreateTableRule(int policy, ArrayList<String> backendList)
  {
    if (backendList == null)
      throw new IllegalArgumentException(
          "Null backendList in CreateTableRule constructor");

    this.policy = policy;
    this.backendList = backendList;
  }

  /**
   * Add a backend name to the list of backends to wait for.
   * 
   * @param name backend name
   */
  public void addBackendName(String name)
  {
    backendList.add(name);
  }

  /**
   * Returns the backendList.
   * 
   * @return ArrayList
   */
  public ArrayList<String> getBackendList()
  {
    return backendList;
  }

  /**
   * Returns the number of nodes.
   * 
   * @return an <code>int</code> value
   */
  public int getNumberOfNodes()
  {
    return nbOfNodes;
  }

  /**
   * Sets the number of nodes.
   * 
   * @param numberOfNodes the number of nodes to set
   */
  public void setNumberOfNodes(int numberOfNodes)
  {
    this.nbOfNodes = numberOfNodes;
  }

  /**
   * Returns the table name.
   * 
   * @return a <code>String</code> value
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Sets the table name.
   * 
   * @param tableName the table name to set
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * Returns the policy.
   * 
   * @return an <code>int</code> value
   */
  public int getPolicy()
  {
    return policy;
  }

  /**
   * Sets the policy.
   * 
   * @param policy the policy to set
   */
  public void setPolicy(int policy)
  {
    this.policy = policy;
  }

  /**
   * Returns <code>true</code> if this rule is the default rule.
   * 
   * @return <code>boolean</code>
   */
  public boolean isDefaultRule()
  {
    return this.tableName == null;
  }

  /**
   * Pickups backends from the given backends arraylist according to the current
   * rule policy.
   * 
   * @param backends backends to choose from
   * @return <code>Arraylist</code> of choosen <code>DatabaseBackend</code>
   * @throws CreateTableException in some specific implementations (not this
   *           one)
   */
  public ArrayList<DatabaseBackend> getBackends(ArrayList<?> backends) throws CreateTableException
  {
    ArrayList<DatabaseBackend> clonedList;

    int size = backends.size();

    if (backendList.size() > 0)
    { // Keep only the backends that are affected by this rule
      clonedList = new ArrayList<DatabaseBackend>(size);
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend db = (DatabaseBackend) backends.get(i);
        if (db.isWriteEnabled() && backendList.contains(db.getName()))
          clonedList.add(db);
      }
    }
    else
    { // Take all enabled backends
      clonedList = new ArrayList<DatabaseBackend>(size);
      for (int i = 0; i < size; i++)
      {
        DatabaseBackend db = (DatabaseBackend) backends.get(i);
        if (db.isWriteEnabled())
          clonedList.add(db);
      }
    }

    return clonedList;
  }

  /**
   * Gives information about the current policy.
   * 
   * @return a <code>String</code> value
   */
  public abstract String getInformation();

  /**
   * Gives information about the current policy in xml
   * 
   * @return a <code>String</code> value in xml
   */
  public String getXml()

  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_CreateTable + " "
        + DatabasesXmlTags.ATT_tableName + "=\"" + tableName + "\" "
        + DatabasesXmlTags.ATT_policy + "=\""
        + CreateTablePolicy.getXmlValue(policy) + "\" "
        + DatabasesXmlTags.ATT_numberOfNodes + "=\"" + nbOfNodes + "\">");
    ArrayList<String> list = this.getBackendList();
    int count = list.size();
    for (int i = 0; i < count; i++)
    {
      info
          .append("<" + DatabasesXmlTags.ELT_BackendName + " "
              + DatabasesXmlTags.ATT_name + "=\"" + ((String) list.get(i))
              + "\"/>");
    }
    info.append("</" + DatabasesXmlTags.ELT_CreateTable + ">");
    return info.toString();
  }

}
