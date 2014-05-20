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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.jmx.management;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * This class defines a BackendInfo. We cannot use DatabaseBackend as a
 * serializable object because it is used as an MBean interface. We use this
 * class to share configuration information on backends between distributed
 * virtual database.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class BackendInfo implements Serializable
{
  private static final long serialVersionUID   = -9143159288188992488L;

  private String            name;
  private String            url;
  private String            driverPath;
  private String            driverClassName;
  private String            virtualDatabaseName;
  private String            connectionTestStatement;
  private int               nbOfWorkerThreads;
  private int               dynamicPrecision;
  private boolean           gatherSystemTables = false;
  private String            schemaName;
  private String            xml;

  /**
   * Creates a new <code>BackendInfo</code> object. Extract configuration
   * information from the original backend object
   * 
   * @param backend DatabaseBackend to extract information from
   */
  public BackendInfo(DatabaseBackend backend)
  {
    this.url = backend.getURL();
    this.name = backend.getName();
    this.driverPath = backend.getDriverPath();
    this.driverClassName = backend.getDriverClassName();
    this.virtualDatabaseName = backend.getVirtualDatabaseName();
    this.connectionTestStatement = backend.getConnectionTestStatement();
    this.nbOfWorkerThreads = backend.getNbOfWorkerThreads();
    this.dynamicPrecision = backend.getDynamicPrecision();
    this.gatherSystemTables = backend.isGatherSystemTables();
    this.schemaName = backend.getSchemaName();
    this.xml = backend.getXml();
  }

  /**
   * Create a corresponding DatabaseBackend object from the information stored
   * in this object.
   * 
   * @return a <code>DatabaseBackend</code>
   */
  public DatabaseBackend getDatabaseBackend()
  {
    return new DatabaseBackend(name, driverPath, driverClassName, url,
        virtualDatabaseName, true, connectionTestStatement, nbOfWorkerThreads);
  }

  /**
   * Returns the xml value.
   * 
   * @return Returns the xml.
   */
  public String getXml()
  {
    return xml;
  }

  /**
   * Returns the connectionTestStatement value.
   * 
   * @return Returns the connectionTestStatement.
   */
  public String getConnectionTestStatement()
  {
    return connectionTestStatement;
  }

  /**
   * Returns the driverClassName value.
   * 
   * @return Returns the driverClassName.
   */
  public String getDriverClassName()
  {
    return driverClassName;
  }

  /**
   * Returns the driverPath value.
   * 
   * @return Returns the driverPath.
   */
  public String getDriverPath()
  {
    return driverPath;
  }

  /**
   * Returns the dynamicPrecision value.
   * 
   * @return Returns the dynamicPrecision.
   */
  public int getDynamicPrecision()
  {
    return dynamicPrecision;
  }

  /**
   * Returns the name value.
   * 
   * @return Returns the name.
   */
  public String getName()
  {
    return name;
  }

  /**
   * Returns the nbOfWorkerThreads value.
   * 
   * @return Returns the nbOfWorkerThreads.
   */
  public final int getNbOfWorkerThreads()
  {
    return nbOfWorkerThreads;
  }

  /**
   * Returns the schemaName value.
   * 
   * @return Returns the schemaName.
   */
  public String getSchemaName()
  {
    return schemaName;
  }

  /**
   * Returns the url value.
   * 
   * @return Returns the url.
   */
  public String getUrl()
  {
    return url;
  }

  /**
   * Returns the virtualDatabaseName value.
   * 
   * @return Returns the virtualDatabaseName.
   */
  public String getVirtualDatabaseName()
  {
    return virtualDatabaseName;
  }

  /**
   * Returns the gatherSystemTables value.
   * 
   * @return Returns the gatherSystemTables.
   */
  public boolean isGatherSystemTables()
  {
    return gatherSystemTables;
  }

  /**
   * Set the xml information on that BackendInfo object
   * 
   * @param xml new XML to set
   */
  public void setXml(String xml)
  {
    this.xml = null;
  }

  /**
   * Convert a <code>List&lt;BackendInfo&gt;</code> to a
   * <code>List&lt;DatabaseBackend&gt;</code>.
   * <em>The DatabaseBackends returned by this method does
   * to reflect the state of the backends in the cluster. To
   * get the "real" DatabaseBackends, use 
   * {@link org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase#getAndCheckBackend(String, int)}.</em> 
   * 
   * @param backendInfos a <code>List</code> of <code>BackendInfo</code>
   * @return a <code>List</code> of <code>DatabaseBackend</code> (possibly
   *         empty if the list of backendInfos was <code>null</code>
   * @see DatabaseBackend#toBackendInfos(List)        
   */
  public static List /*<DatabaseBackend>*/<DatabaseBackend> toDatabaseBackends(
  List /*<BackendInfo>*/<BackendInfo> backendInfos)
  {
    if (backendInfos == null)
    {
      return new ArrayList<DatabaseBackend>();
    }
    // Convert BackendInfo arraylist to real DatabaseBackend objects
    List<DatabaseBackend> backends = new ArrayList<DatabaseBackend>(backendInfos.size());
    for (Iterator<BackendInfo> iter = backendInfos.iterator(); iter.hasNext();)
    {
      BackendInfo info = (BackendInfo) iter.next();
      backends.add(info.getDatabaseBackend());
    }
    return backends;
  }

}