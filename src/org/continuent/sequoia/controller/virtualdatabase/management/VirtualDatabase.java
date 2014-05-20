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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): Stephane Giron.
 */

package org.continuent.sequoia.controller.virtualdatabase.management;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.TabularData;

import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.jmx.management.DumpInfoSupport;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.common.jmx.monitoring.backend.BackendStatistics;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.common.xml.XmlTools;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;

/**
 * This class defines a VirtualDatabaseMBean implementation. It delegates
 * management method call to a managed VirtualDatabase
 * 
 * @see org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase
 */
public class VirtualDatabase extends AbstractStandardMBean
    implements
      VirtualDatabaseMBean
{

  private org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase managedVirtualDatabase;

  /* end user logger */
  static Trace                                                              endUserLogger = Trace
                                                                                              .getLogger("org.continuent.sequoia.enduser");

  /**
   * Creates a new <code>VirtualDatabase</code> object from a managed
   * VirtualDatabase
   * 
   * @param managedVirtualDatabase the VirtualDatabase to manage
   * @throws NotCompliantMBeanException if this instance in not a compliant
   *             MBean
   */
  public VirtualDatabase(
      org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase managedVirtualDatabase)
      throws NotCompliantMBeanException
  {
    super(VirtualDatabaseMBean.class);
    this.managedVirtualDatabase = managedVirtualDatabase;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#initializeFromBackend(java.lang.String,
   *      boolean)
   */
  public void initializeFromBackend(String databaseBackendName, boolean force)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.initializing",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName}));
      managedVirtualDatabase.initializeFromBackend(databaseBackendName, force);
      endUserLogger.info(Translate.get("virtualdatabase.initialize.success",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get("virtualdatabase.initialize.failed",
          new String[]{this.getVirtualDatabaseName(), e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#initializeFromBackend(java.lang.String)
   */
  public void initializeFromBackend(String databaseBackendName)
      throws VirtualDatabaseException
  {
    initializeFromBackend(databaseBackendName, false);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#forceEnableBackend(java.lang.String)
   */
  public void forceEnableBackend(String databaseBackendName)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.force.enabling",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName}));
      managedVirtualDatabase.forceEnableBackend(databaseBackendName);
      endUserLogger.info(Translate.get("virtualdatabase.force.enable.success",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get("virtualdatabase.force.enable.failed",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName,
              e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#enableBackendFromCheckpoint(java.lang.String)
   */
  public void enableBackendFromCheckpoint(String backendName, boolean isWrite)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get(
          "virtualdatabase.enabling.from.checkpoint", new String[]{
              this.getVirtualDatabaseName(), backendName}));
      managedVirtualDatabase.enableBackendFromCheckpoint(backendName, isWrite);
      endUserLogger.info(Translate.get(
          "virtualdatabase.enable.from.checkpoint.success", new String[]{
              this.getVirtualDatabaseName(), backendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.enable.from.checkpoint.failed", new String[]{
              this.getVirtualDatabaseName(), backendName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#forceDisableBackend(java.lang.String)
   */
  public void forceDisableBackend(String databaseBackendName)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.force.disabling",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName}));
      managedVirtualDatabase.forceDisableBackend(databaseBackendName);
      endUserLogger.info(Translate.get("virtualdatabase.force.disable.success",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get("virtualdatabase.force.disable.failed",
          new String[]{this.getVirtualDatabaseName(), databaseBackendName,
              e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#disableBackendWithCheckpoint(java.lang.String)
   */
  public void disableBackendWithCheckpoint(String databaseBackendName)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get(
          "virtualdatabase.disabling.with.checkpoint", new String[]{
              this.getVirtualDatabaseName(), databaseBackendName}));
      managedVirtualDatabase.disableBackendWithCheckpoint(databaseBackendName);
      endUserLogger.info(Translate.get(
          "virtualdatabase.disable.with.checkpoint.success", new String[]{
              this.getVirtualDatabaseName(), databaseBackendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.disable.with.checkpoint.failed", new String[]{
              this.getVirtualDatabaseName(), databaseBackendName,
              e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getAllBackendNames()
   */
  public List<?> getAllBackendNames() throws VirtualDatabaseException
  {
    return managedVirtualDatabase.getAllBackendNames();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#replicateBackend(java.lang.String,
   *      java.lang.String, java.util.Map)
   */
  public void replicateBackend(String backendName, String newBackendName,
      Map<?, ?> parameters) throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.replicating.backend",
          new String[]{this.getVirtualDatabaseName(), backendName}));
      managedVirtualDatabase.replicateBackend(backendName, newBackendName,
          parameters);
      endUserLogger.info(Translate.get(
          "virtualdatabase.replicate.backend.success", new String[]{
              this.getVirtualDatabaseName(), backendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.replicate.backend.failed", new String[]{
              this.getVirtualDatabaseName(), backendName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#transferBackend(java.lang.String,
   *      java.lang.String)
   */
  public void transferBackend(String backend, String controllerDestination)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.transferring.backend",
          new String[]{backend, this.getVirtualDatabaseName(),
              controllerDestination}));
      managedVirtualDatabase.transferBackend(backend, controllerDestination);
      endUserLogger.info(Translate.get(
          "virtualdatabase.transfer.backend.success", new String[]{backend,
              this.getVirtualDatabaseName(), controllerDestination}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.transfer.backend.failed", new String[]{backend,
              this.getVirtualDatabaseName(), controllerDestination,
              e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#copyLogFromCheckpoint(java.lang.String,
   *      java.lang.String)
   */
  public void copyLogFromCheckpoint(String dumpName, String controllerName)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get(
          "virtualdatabase.copying.log.fromCheckpoint", new String[]{
              this.getVirtualDatabaseName(), dumpName}));
      managedVirtualDatabase.copyLogFromCheckpoint(dumpName, controllerName);
      endUserLogger.info(Translate.get(
          "virtualdatabase.copy.log.fromCheckpoint.success", new String[]{
              this.getVirtualDatabaseName(), dumpName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.copy.log.fromCheckpoint.failed", new String[]{
              this.getVirtualDatabaseName(), dumpName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#deleteLogUpToCheckpoint(java.lang.String)
   */
  public void deleteLogUpToCheckpoint(String checkpointName)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get(
          "virtualdatabase.deleting.log.upToCheckpoint", new String[]{
              this.getVirtualDatabaseName(), checkpointName}));
      managedVirtualDatabase.deleteLogUpToCheckpoint(checkpointName);
      endUserLogger.info(Translate.get(
          "virtualdatabase.delete.log.upToCheckpoint.success", new String[]{
              this.getVirtualDatabaseName(), checkpointName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.delete.log.upToCheckpoint.failed", new String[]{
              this.getVirtualDatabaseName(), checkpointName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#setBackendLastKnownCheckpoint(java.lang.String,
   *      java.lang.String)
   */
  public void setBackendLastKnownCheckpoint(String backendName,
      String checkpoint) throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get(
          "virtualdatabase.setting.lastKnownCheckpoint", new String[]{
              this.getVirtualDatabaseName(), checkpoint, backendName}));
      managedVirtualDatabase.setBackendLastKnownCheckpoint(backendName,
          checkpoint);
      endUserLogger.info(Translate.get(
          "virtualdatabase.set.lastKnownCheckpoint.success", new String[]{
              this.getVirtualDatabaseName(), checkpoint, backendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.set.lastKnownCheckpoint.failed", new String[]{
              this.getVirtualDatabaseName(), checkpoint, backendName,
              e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackuperNames()
   */
  public String[] getBackuperNames()
  {
    return managedVirtualDatabase.getBackuperNames();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getDumpFormatForBackuper(java.lang.String)
   */
  public String getDumpFormatForBackuper(String backuperName)
  {
    return managedVirtualDatabase.getDumpFormatForBackuper(backuperName);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#backupBackend(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.lang.String, java.util.ArrayList)
   */
  public void backupBackend(String backendName, String login, String password,
      String dumpName, String backuperName, String path, ArrayList<?> tables)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.backuping.backend",
          new String[]{this.getVirtualDatabaseName(), backendName, dumpName}));
      managedVirtualDatabase.backupBackend(backendName, login, password,
          dumpName, backuperName, path, true, tables);
      endUserLogger.info(Translate.get(
          "virtualdatabase.backup.backend.success", new String[]{
              this.getVirtualDatabaseName(), backendName, dumpName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.backup.backend.failed", new String[]{
              this.getVirtualDatabaseName(), backendName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#backupBackend(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String, java.lang.String, boolean, java.util.ArrayList)
   */
  public void backupBackend(String backendName, String login, String password,
      String dumpName, String backuperName, String path, boolean force,
      ArrayList<?> tables) throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.backuping.backend",
          new String[]{this.getVirtualDatabaseName(), backendName, dumpName}));
      managedVirtualDatabase.backupBackend(backendName, login, password,
          dumpName, backuperName, path, force, tables);
      endUserLogger.info(Translate.get(
          "virtualdatabase.backup.backend.success", new String[]{
              this.getVirtualDatabaseName(), backendName, dumpName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.backup.backend.failed", new String[]{
              this.getVirtualDatabaseName(), backendName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getAvailableDumps()
   */
  public DumpInfo[] getAvailableDumps() throws VirtualDatabaseException
  {
    return managedVirtualDatabase.getAvailableDumps();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getDumps()
   */
  public TabularData getDumps() throws Exception
  {
    TabularData ret = DumpInfoSupport.newTabularData();
    DumpInfo info[] = getAvailableDumps();
    for (int i = 0; i < info.length; i++)
      ret.put(DumpInfoSupport.newCompositeData(info[i]));
    return ret;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#updateDumpPath(java.lang.String,
   *      java.lang.String)
   */
  public void updateDumpPath(String dumpName, String newPath)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.updating.dump.path",
          new String[]{this.getVirtualDatabaseName(), dumpName, newPath}));
      managedVirtualDatabase.updateDumpPath(dumpName, newPath);
      endUserLogger.info(Translate.get(
          "virtualdatabase.update.dump.path.success", new String[]{
              this.getVirtualDatabaseName(), dumpName, newPath}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate.get(
          "virtualdatabase.update.dump.path.failed", new String[]{
              this.getVirtualDatabaseName(), dumpName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#deleteDump(java.lang.String,
   *      boolean)
   */
  public void deleteDump(String dumpName, boolean keepsFile)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.deleting.dump",
          new String[]{this.getVirtualDatabaseName(), dumpName}));
      managedVirtualDatabase.deleteDump(dumpName, keepsFile);
      endUserLogger.info(Translate.get("virtualdatabase.delete.dump.success",
          new String[]{this.getVirtualDatabaseName(), dumpName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate
          .get("virtualdatabase.delete.dump.failed", new String[]{
              this.getVirtualDatabaseName(), dumpName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#restoreDumpOnBackend(java.lang.String,
   *      java.lang.String, java.lang.String, java.lang.String,
   *      java.util.ArrayList)
   */
  public void restoreDumpOnBackend(String databaseBackendName, String login,
      String password, String dumpName, ArrayList<?> tables)
      throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.restoring.dump",
          new String[]{this.getVirtualDatabaseName(), dumpName,
              databaseBackendName}));
      managedVirtualDatabase.restoreDumpOnBackend(databaseBackendName, login,
          password, dumpName, tables);
      endUserLogger.info(Translate.get("virtualdatabase.restore.dump.success",
          new String[]{this.getVirtualDatabaseName(), dumpName,
              databaseBackendName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate
          .get("virtualdatabase.restore.dump.failed", new String[]{
              this.getVirtualDatabaseName(), dumpName, e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#transferDump(java.lang.String,
   *      java.lang.String, boolean)
   */
  public void transferDump(String dumpName, String remoteControllerName,
      boolean noCopy) throws VirtualDatabaseException
  {
    try
    {
      endUserLogger.info(Translate.get("virtualdatabase.transferring.dump",
          new String[]{dumpName, this.getVirtualDatabaseName(),
              remoteControllerName}));
      managedVirtualDatabase.transferDump(dumpName, remoteControllerName,
          noCopy);
      endUserLogger.info(Translate.get("virtualdatabase.transfer.dump.success",
          new String[]{dumpName, this.getVirtualDatabaseName(),
              remoteControllerName}));
    }
    catch (VirtualDatabaseException e)
    {
      endUserLogger.error(Translate
          .get("virtualdatabase.transfer.dump.failed", new String[]{dumpName,
              this.getVirtualDatabaseName(), e.getMessage()}));
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackendInformation(java.lang.String)
   */
  public String getBackendInformation(String backendName)
      throws VirtualDatabaseException
  {
    return managedVirtualDatabase.getBackendInformation(backendName);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackendSchema(java.lang.String)
   */
  public String getBackendSchema(String backendName)
      throws VirtualDatabaseException
  {
    return managedVirtualDatabase.getBackendSchema(backendName);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getXml()
   */
  public String getXml()
  {
    StringBuffer xml = new StringBuffer();
    xml.append(XmlComponent.XML_VERSION);
    xml.append("<" + ControllerConstants.VIRTUAL_DATABASE_XML_ROOT_ELEMENT
        + ">\n");
    xml.append(managedVirtualDatabase.getXml());
    xml.append("\n</" + ControllerConstants.VIRTUAL_DATABASE_XML_ROOT_ELEMENT
        + ">");
    try
    {
      String prettyXml = XmlTools.prettyXml(xml.toString());
      // ugly hack to insert the doctype which has been stripped
      // when prettyfying the xml
      prettyXml = XmlTools.insertDoctype(prettyXml,
          ControllerConstants.VIRTUAL_DATABASE_DOCTYPE);
      return prettyXml;
    }
    catch (Exception e)
    {
      return "XML unavailable for Virtual Database " + getVirtualDatabaseName();
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#checkAdminAuthentication(java.lang.String,
   *      java.lang.String)
   */
  public boolean checkAdminAuthentication(String adminLogin,
      String adminPassword) throws VirtualDatabaseException
  {
    return managedVirtualDatabase.checkAdminAuthentication(adminLogin,
        adminPassword);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getVirtualDatabaseName()
   */
  public String getVirtualDatabaseName()
  {
    return managedVirtualDatabase.getVirtualDatabaseName();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#hasRecoveryLog()
   */
  public boolean hasRecoveryLog()
  {
    return managedVirtualDatabase.hasRecoveryLog();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#hasResultCache()
   */
  public boolean hasResultCache()
  {
    return managedVirtualDatabase.hasResultCache();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#isDistributed()
   */
  public boolean isDistributed()
  {
    return managedVirtualDatabase.isDistributed();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#shutdown(int)
   */
  public void shutdown(int level) throws VirtualDatabaseException
  {
    managedVirtualDatabase.shutdown(level);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getOwningController()
   */
  public String getOwningController()
  {
    return managedVirtualDatabase.viewOwningController();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getBackendStatistics(java.lang.String)
   */
  public BackendStatistics getBackendStatistics(String backendName)
      throws Exception
  {
    return managedVirtualDatabase.getBackendStatistics(backendName);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getControllers()
   */
  public String[] getControllers()
  {
    return managedVirtualDatabase.viewControllerList();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getCurrentNbOfThreads()
   */
  public int getCurrentNbOfThreads()
  {
    return managedVirtualDatabase.getCurrentNbOfThreads();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#setMonitoringToActive(boolean)
   */
  public void setMonitoringToActive(boolean active)
      throws VirtualDatabaseException
  {
    managedVirtualDatabase.setMonitoringToActive(active);
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "virtualdatabase"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#abort(long,
   *      boolean, boolean)
   */
  public void abort(long transactionId, boolean logAbort, boolean forceAbort)
      throws SQLException
  {
    managedVirtualDatabase.abort(transactionId, logAbort, forceAbort);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#closePersistentConnection(java.lang.String,
   *      long)
   */
  public void closePersistentConnection(String login,
      long persistentConnectionId)
  {
    managedVirtualDatabase.closePersistentConnection(login,
        persistentConnectionId);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#resumeActivity()
   */
  public void resumeActivity() throws VirtualDatabaseException
  {
    managedVirtualDatabase.resumeActivity();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean#getActivityStatus()
   */
  public String getActivityStatus()
  {
    switch (managedVirtualDatabase.getActivityStatus())
    {
      case org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase.SUSPENDING :
        return "SUSPENDING";

      case org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase.SUSPENDED :
        return "SUSPENDED";

      case org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase.RESUMING :
        return "RESUMING";

      case org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase.RUNNING :
        return "RUNNING";

      default :
        return "UNKNOWN";
    }
  }
}
