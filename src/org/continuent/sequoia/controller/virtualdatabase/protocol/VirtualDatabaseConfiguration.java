/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2006 Continuent, Inc.
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

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.management.BackendInfo;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.jmx.RmiConnector;
import org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;

/**
 * Transports the configuration of a virtual database to remote controllers so
 * that compatibility checking can be performed.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class VirtualDatabaseConfiguration
    extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = -4753828540599620782L;

  private String            controllerName;
  private String            controllerJmxName;
  private String            vdbName;
  private String            groupName        = null;
  private ArrayList         vLogins;
  private int               schedulerRAIDbLevel;
  private int               loadBalancerRAIDbLevel;
  private List             /* <BackendInfo> */backendInfos;

  // Jmx Information
  private String            rmiHostname;
  private String            rmiPort;

  /**
   * @return Returns the controllerName.
   */
  public String getControllerName()
  {
    return controllerName;
  }

  /**
   * Returns the controllerJmxName value.
   * 
   * @return Returns the controllerJmxName.
   */
  public String getControllerJmxName()
  {
    return controllerJmxName;
  }

  /**
   * Constructs a new <code>VirtualDatabaseConfiguration</code> object from a
   * <code>DistributedVirtualDatabase</code>.
   * 
   * @param dvdb The distributed virtual database to get configuration from.
   */
  public VirtualDatabaseConfiguration(DistributedVirtualDatabase dvdb)
  {
    this.controllerName = dvdb.getControllerName();
    this.controllerJmxName = dvdb.viewOwningController();
    this.vdbName = dvdb.getVirtualDatabaseName();
    this.groupName = dvdb.getGroupName();
    this.vLogins = dvdb.getAuthenticationManager().getVirtualLogins();
    this.schedulerRAIDbLevel = dvdb.getRequestManager().getScheduler()
        .getRAIDbLevel();
    this.loadBalancerRAIDbLevel = dvdb.getRequestManager().getLoadBalancer()
        .getRAIDbLevel();
    this.backendInfos = DatabaseBackend.toBackendInfos(dvdb.getBackends());

    List connectors = RmiConnector.getRmiConnectors();
    if (connectors.size() > 0)
    {
      RmiConnector rmi = (RmiConnector) connectors.get(0);
      rmiHostname = rmi.getHostName();
      rmiPort = String.valueOf(rmi.getPort());
    }
    else
    {
      rmiHostname = controllerName.substring(0, controllerName.indexOf(":"));
      rmiPort = String.valueOf(JmxConstants.DEFAULT_JMX_RMI_PORT);
    }
  }

  /**
   * @return Returns the rmiHostname.
   */
  public String getRmiHostname()
  {
    return rmiHostname;
  }

  /**
   * @return Returns the rmiPort.
   */
  public String getRmiPort()
  {
    return rmiPort;
  }

  /**
   * Check if the local distributed virtual database is compatible with this
   * virtual database configuration.
   * 
   * @param localDvdb The local distributed virtual database
   * @return an array of objects containing:
   *         <ul>
   *         <li>Boolean.TRUE if both configurations are compatible,
   *         Boolean.FALSE otherwise
   *         <li>a List of VirtualDatabaseUser objects if there are missing vdb
   *         users, null otherwise
   *         </ul>
   */
  private Object[] isCompatible(DistributedVirtualDatabase localDvdb)
  {
    Object[] ret = new Object[2];
    ret[0] = Boolean.FALSE;
    ret[1] = null;
    try
    {
      if (controllerName.equals(localDvdb.getControllerName()))
      {
        localDvdb
            .getLogger()
            .warn(
                Translate
                    .get("virtualdatabase.distributed.configuration.checking.duplicate.controller.name"));
        return ret;
      }

      // Sanity checks for virtual database name and group name
      if (!vdbName.equals(localDvdb.getVirtualDatabaseName()))
      {
        localDvdb
            .getLogger()
            .warn(
                Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.name"));
        return ret;
      }
      if (!groupName.equals(localDvdb.getGroupName()))
      {
        localDvdb
            .getLogger()
            .warn(
                Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.groupname"));
        return ret;
      }

      // Scheduler and Load Balancer checking
      if (schedulerRAIDbLevel != localDvdb.getRequestManager().getScheduler()
          .getRAIDbLevel())
      {
        localDvdb
            .getLogger()
            .warn(
                Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.scheduler"));
        return ret;
      }

      if (loadBalancerRAIDbLevel != localDvdb.getRequestManager()
          .getLoadBalancer().getRAIDbLevel())
      {
        localDvdb
            .getLogger()
            .warn(
                Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.loadbalancer"));
        return ret;
      }

      // Checking backendInfos
      int size = backendInfos.size();
      for (int i = 0; i < size; i++)
      {
        BackendInfo b = (BackendInfo) backendInfos.get(i);
        if (!localDvdb.isCompatibleBackend(b))
        {
          localDvdb
              .getLogger()
              .warn(
                  Translate
                      .get(
                          "virtualdatabase.distributed.configuration.checking.mismatch.backend.shared",
                          b.getName()));
          return ret;
        }
      }

      // Authentication managers must contains the same set of elements but
      // possibly in different orders (equals require the element to be in the
      // same order).
      if (!localDvdb.getAuthenticationManager().getVirtualLogins().containsAll(
          vLogins))
      {
        localDvdb
            .getLogger()
            .warn(
                Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.vlogins"));
        return ret;
      }

      // Special case: the joining vdb will try to become compatible by
      // dynamically creating the missing vd users.
      // WARNING: don't move this test around, it does expect to be the last
      // test performed, meaning that if missing vdb users can be added the
      // configurations will effectively become compatible.
      if (!vLogins.containsAll(localDvdb.getAuthenticationManager()
          .getVirtualLogins()))
      {
        localDvdb
            .getLogger()
            .warn(
                Translate
                    .get("virtualdatabase.distributed.configuration.checking.mismatch.vlogins"));
        List additionalVdbUsers = new ArrayList(localDvdb
            .getAuthenticationManager().getVirtualLogins());
        additionalVdbUsers.removeAll(vLogins);
        ret[0] = Boolean.TRUE;
        ret[1] = additionalVdbUsers;
        return ret;
      }

      // Ok, all tests succeeded, configuration is compatible
      ret[0] = Boolean.TRUE;
      return ret;
    }
    catch (Exception e)
    {
      localDvdb.getLogger().error(
          Translate
              .get("virtualdatabase.distributed.configuration.checking.error"),
          e);
      return ret;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {
    return null;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    Trace logger = dvdb.getLogger();
    if (logger.isInfoEnabled())
      logger.info("Checking virtual database configuration from "
          + getControllerName());

    Object[] ret = isCompatible(dvdb);
    if (!dvdb.isLocalSender(sender) && !((Boolean) ret[0]).booleanValue())
    {
      // Failure
      return new VirtualDatabaseConfigurationResponse(
          DistributedVirtualDatabase.INCOMPATIBLE_CONFIGURATION, (List) ret[1]);
    }

    // Configuration is compatible, add it to our controller list
    dvdb.addRemoteControllerJmxName(sender, controllerJmxName);

    // Send back our local configuration
    try
    {
      dvdb.sendLocalConfiguration(sender);
    }
    catch (Exception e)
    {
      return e;
    }

    if (logger.isInfoEnabled())
      logger.info("Controller " + getControllerName()
          + " is compatible with the local configuration");
    return new VirtualDatabaseConfigurationResponse(
        ((DistributedRequestManager) dvdb.getRequestManager())
            .getControllerId(), (List) ret[1]);
  }

}