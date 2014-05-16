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
 * Contributor(s): _______________________.
 */

package org.continuent.sequoia.common.jmx;

import java.text.MessageFormat;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * This class contains static information on the jmx services.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public final class JmxConstants
{

  /** RMI Adaptor */
  public static final String  ADAPTOR_TYPE_RMI                     = "rmiAdaptor";                                                                     //$NON-NLS-1$
  public static final String  ADAPTOR_TYPE_RMI_SERVER              = "rmiAdaptorServerPort"; 
  /** ssl config for rmi */
  public static final String  CONNECTOR_RMI_SSL                    = "jmx.rmi.ssl";                                                                    //$NON-NLS-1$

  /** Http adaptor */
  public static final String  ADAPTOR_TYPE_HTTP                    = "httpAdaptor";                                                                    //$NON-NLS-1$

  /** jmx authenticator username */
  public static final String  CONNECTOR_AUTH_USERNAME              = "jmx.auth.username";                                                              //$NON-NLS-1$
  /** jmx authenticator password */
  public static final String  CONNECTOR_AUTH_PASSWORD              = "jmx.auth.password";                                                              //$NON-NLS-1$

  /** Default RMI port number value. */
  public static final int     DEFAULT_JMX_RMI_PORT                 = 1090;

  /** Default JMX server HTTP adaptor port value. */
  public static final int     DEFAULT_JMX_HTTP_PORT                = 8090;

  /**
   * This is in the xsl transformation file, so we should leave as is. Other
   * domain are filtered.
   */
  private static final String SEQUOIA_DOMAIN_NAME                  = "sequoia";                                                                        //$NON-NLS-1$

  /** the controller mbean type */
  private static final String CONTROLLER_TYPE_VALUE                = "Controller";                                                                     //$NON-NLS-1$
  /** the virtual database mbean type */
  public static final String  VIRTUALDATABASE_TYPE_VALUE           = "VirtualDataBase";                                                                //$NON-NLS-1$
  /** the data collector mbean type */
  public static final String  DATABASEBACKEND_TYPE_VALUE           = "VirtualDataBase.Backend";                                                        //$NON-NLS-1$

  private static final String DATACOLLECTOR_TYPE_VALUE             = "datacollector";                                                                  //$NON-NLS-1$

  private static final String DATACOLLECTOR                        = "datacollector";                                                                  //$NON-NLS-1$
  /** the cache mbean type */
  private static final String SEQUOIA_TYPE_CACHE                   = "cache";                                                                          //$NON-NLS-1$

  private static final String CONTROLLER_OBJECTNAME                = "sequoia:type=Controller";                                                        //$NON-NLS-1$
  // 0 => virtual database name
  private static final String VIRTUALDATABASE_OBJECTNAME_PATTERN   = "sequoia:type=VirtualDataBase,name={0}";                                          //$NON-NLS-1$
  // 0 => virtual database name, 1 => backend name
  private static final String DATABASEBACKEND_OBJECTNAME_PATTERN   = "sequoia:type=VirtualDataBase.Backend,VirtualDataBase={0},name={1}";              //$NON-NLS-1$
  // 0 => vdb name, 1 => backend name
  private static final String BACKENDTASKQUEUES_OBJECTNAME_PATTERN = "sequoia:type=VirtualDataBase.Backend.TaskQueues,VirtualDataBase={0},Backend={1}"; //$NON-NLS-1$
  // 0 => vdb name
  private static final String RECOVERYLOG_OBJECTNAME_PATTERN       = "sequoia:type=VirtualDataBase.RecoveryLog,VirtualDataBase={0}";                   //$NON-NLS-1$
  // 0 => vdb name
  private static final String LOADBALANCER_OBJECTNAME_PATTERN      = "sequoia:type=VirtualDataBase.LoadBalancer,VirtualDataBase={0}";                  //$NON-NLS-1$
  // 0 => vdb name
  private static final String REQUESTMANAGER_OBJECTNAME_PATTERN    = "sequoia:type=VirtualDataBase.RequestManager,VirtualDataBase={0}";                //$NON-NLS-1$
  // 0 => vdb name
  private static final String ABSTRACTSCHEDULER_OBJECTNAME_PATTERN = "sequoia:type=VirtualDataBase.AbstractScheduler,VirtualDataBase={0}";             //$NON-NLS-1$
  // 0 => vdb name
  private static final String PARSINGCACHE_OBJECTNAME_PATTERN      = "sequoia:type=VirtualDataBase.RequestManager.ParsingCache,VirtualDataBase={0}";   //$NON-NLS-1$

  /** Virtual database property name */
  public static final String  VIRTUALDATABASE_PROPERTY             = "VirtualDataBase";                                                                //$NON-NLS-1$

  /**
   * Get the associated jmx object name
   * 
   * @param name the name of the mbean
   * @param type the sequoia type of the mbean
   * @return the associated object name, no exception is thrown as the object
   *         name calculated is always valid ex;
   *         sequoia:type:=&lt;type&gt;:name:=&lt;name&gt;
   */
  private static ObjectName getJmxObjectName(String name, String type)
  {
    try
    {
      return new ObjectName(SEQUOIA_DOMAIN_NAME + ":type=" + type + ",name="
          + name);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      // impossible?
      return null;
    }
  }

  /**
   * Get the associated controller object name
   * 
   * @return the ObjectName associated to a controller
   * @throws MalformedObjectNameException if the provided name is not correct
   * @see JmxConstants#CONTROLLER_OBJECTNAME
   */
  public static ObjectName getControllerObjectName()
      throws MalformedObjectNameException
  {
    return new ObjectName(CONTROLLER_OBJECTNAME);
  }

  /**
   * Get the objectname associated with a virtualdatabase
   * 
   * @param virtualDataBaseName the name of the virtualdatabase
   * @return sequoia:type:=&lt;virtualdatabase&gt;:name:=&lt;name&gt;
   * @throws MalformedObjectNameException if the provided name is not correct
   */
  public static ObjectName getVirtualDataBaseObjectName(
      String virtualDataBaseName) throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(
        VIRTUALDATABASE_OBJECTNAME_PATTERN, new String[]{virtualDataBaseName}));
  }

  /**
   * Get the associated data collector object name
   * 
   * @return sequoia:type:=&lt;datacollector&gt;:name:=&lt;name&gt;
   */
  public static ObjectName getDataCollectorObjectName()
  {
    return getJmxObjectName(DATACOLLECTOR, DATACOLLECTOR_TYPE_VALUE);
  }

  /**
   * Get the object name associated with a backend on a given virtual database
   * 
   * @param virtualDataBaseName name of the virtual database
   * @param name name of the backend
   * @return the object name associated with a backend on a given virtual
   *         database
   * @throws MalformedObjectNameException if the provided name is not correct
   */
  public static ObjectName getDatabaseBackendObjectName(
      String virtualDataBaseName, String name)
      throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(
        DATABASEBACKEND_OBJECTNAME_PATTERN, new String[]{
            virtualDataBaseName,
            name}));
  }

  /**
   * Get the object name associated with the recovery log on a given virtual
   * database
   * 
   * @param vdbName name of the virtual database
   * @return the object name associated with the recovery log on a given virtual
   *         database
   * @throws MalformedObjectNameException if the provided name is not correct
   */
  public static ObjectName getRecoveryLogObjectName(String vdbName)
      throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(RECOVERYLOG_OBJECTNAME_PATTERN,
        new String[]{vdbName}));
  }

  /**
   * Get the associated cache object name
   * 
   * @param vdbName name of the virtual database
   * @return sequoia:type:=&lt;cache&gt;:name:=&lt;name&gt;
   */
  public static ObjectName getCacheObjectName(String vdbName)
  {
    return getJmxObjectName(vdbName + "--cache", SEQUOIA_TYPE_CACHE);
  }

  /**
   * Return the object name associated to the request manager of a given virtual
   * database.
   * 
   * @param vdbName name of the virtual database
   * @return return the object name associated to the request manager of a given
   *         virtual database
   * @throws MalformedObjectNameException if the object name is malformed
   * @see #REQUESTMANAGER_OBJECTNAME_PATTERN
   */
  public static ObjectName getRequestManagerObjectName(String vdbName)
      throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(
        REQUESTMANAGER_OBJECTNAME_PATTERN, new String[]{vdbName}));
  }

  /**
   * Return the object name associated to the load balancer of a given virtual
   * database.
   * 
   * @param vdbName name of the virtual database
   * @return return the object name associated to the load balancer of a given
   *         virtual database
   * @throws MalformedObjectNameException if the object name is malformed
   * @see #LOADBALANCER_OBJECTNAME_PATTERN
   */
  public static ObjectName getLoadBalancerObjectName(String vdbName)
      throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(LOADBALANCER_OBJECTNAME_PATTERN,
        new String[]{vdbName}));
  }

  /**
   * Return the object name associated to the task queues of a given backend.
   * 
   * @param vdbName name of the virtual database managing the backend
   * @param backendName name of the backend
   * @return return the object name associated to the task queues of a given
   *         backend
   * @throws MalformedObjectNameException if the object name is malformed
   * @see #BACKENDTASKQUEUES_OBJECTNAME_PATTERN
   */
  public static ObjectName getBackendTaskQueuesObjectName(String vdbName,
      String backendName) throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(
        BACKENDTASKQUEUES_OBJECTNAME_PATTERN,
        new String[]{vdbName, backendName}));
  }

  /**
   * Gets the object name of the given vdb scheduler
   * 
   * @param vdbName name of the virtual database
   * @return object name associated to the given virtual database scheduler
   * @throws MalformedObjectNameException if the provided vdb name is incorrect
   */
  public static ObjectName getAbstractSchedulerObjectName(String vdbName)
      throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(
        ABSTRACTSCHEDULER_OBJECTNAME_PATTERN, new String[]{vdbName}));
  }

  /**
   * Gets the object name of the given vdb request manager parsing cache
   * 
   * @param vdbName name of the virtual database
   * @return object name associated to the given virtual database scheduler
   * @throws MalformedObjectNameException if the provided vdb name is incorrect
   */
  public static ObjectName getParsingCacheObjectName(String vdbName)
      throws MalformedObjectNameException
  {
    return new ObjectName(MessageFormat.format(PARSINGCACHE_OBJECTNAME_PATTERN,
        new String[]{vdbName}));
  }

  /**
   * Sequoia rules to determine if a mbean need authentication or not. By
   * default all the mbeans need authentication apart from the controller mbean
   * and the data collector mbean
   * 
   * @param mbean <tt>ObjectName</tt> of the mbean to test
   * @return <tt>true</tt> if the call to the mbean should have a user and a
   *         password attribute attached to it.
   */
  public static boolean mbeanNeedAuthentication(ObjectName mbean)
  {
    String type = mbean.getKeyProperty("type"); //$NON-NLS-1$
    if (type == null)
    {
      return false;
    }
    if (type.equalsIgnoreCase(CONTROLLER_TYPE_VALUE)
        || type.equalsIgnoreCase(DATACOLLECTOR_TYPE_VALUE))
    {
      return false;
    }
    else
    {
      return true;
    }
  }
}