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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.ArrayList;
import java.util.List;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.common.jmx.monitoring.backend.BackendStatistics;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a ShowBackend
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ShowBackend extends AbstractAdminCommand
{

  /**
   * Creates a new <code>ShowBackend.java</code> object
   * 
   * @param module the commands is attached to
   */
  public ShowBackend(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    if (commandText.trim().length() == 0)
    {
      console.printError(getUsage());
      return;
    }

    VirtualDatabaseMBean db = jmxClient.getVirtualDatabaseProxy(dbName, user,
        password);
    String[] backendNames;
    if (("*").equals(commandText.trim())) //$NON-NLS-1$
    {
      List backendNamesList = db.getAllBackendNames();
      backendNames = (String[]) backendNamesList
          .toArray(new String[backendNamesList.size()]);
    }
    else
    {
      String backendName = commandText.trim();
      backendNames = new String[]{backendName};
    }

    ArrayList stats = new ArrayList();
    if (backendNames.length == 0)
    {
      console.printInfo(ConsoleTranslate
          .get("admin.command.show.backend.no.backends")); //$NON-NLS-1$
      return;
    }
    for (int i = 0; i < backendNames.length; i++)
    {
      String backendName = backendNames[i];
      BackendStatistics stat = db.getBackendStatistics(backendName);
      if (stat == null)
      {
        continue;
      }
      stats.add(stat);
    }
    if (stats.size() == 0)
    {
      console.printInfo(ConsoleTranslate.get(
          "admin.command.show.backend.no.stats", commandText)); //$NON-NLS-1$
      return;
    }
    String formattedBackends = TableFormatter.format(
        getBackendStatisticsDescriptions(),
        getBackendStatisticsAsStrings(stats), false);
    console.println(formattedBackends);
  }

  private String[][] getBackendStatisticsAsStrings(ArrayList stats)
  {
    String[][] statsStr = new String[stats.size()][15];
    for (int i = 0; i < statsStr.length; i++)
    {
      BackendStatistics stat = (BackendStatistics) stats.get(i);
      statsStr[i][0] = stat.getBackendName();
      statsStr[i][1] = stat.getDriverClassName();
      statsStr[i][2] = stat.getUrl();
      statsStr[i][3] = Integer.toString(stat.getNumberOfActiveTransactions());
      statsStr[i][4] = Integer.toString(stat.getNumberOfPendingRequests());
      statsStr[i][5] = Boolean.toString(stat.isReadEnabled());
      statsStr[i][6] = Boolean.toString(stat.isWriteEnabled());
      statsStr[i][7] = stat.getInitializationStatus();
      statsStr[i][8] = Boolean.toString(stat.isSchemaStatic());
      statsStr[i][9] = Integer.toString(stat.getNumberOfConnectionManagers());
      statsStr[i][10] = Long.toString(stat.getNumberOfTotalActiveConnections());
      statsStr[i][11] = Integer.toString(stat
          .getNumberOfPersistentConnections());
      statsStr[i][12] = Integer.toString(stat.getNumberOfTotalRequests());
      statsStr[i][13] = Integer.toString(stat.getNumberOfTotalTransactions());
      statsStr[i][14] = stat.getLastKnownCheckpoint();
    }
    return statsStr;
  }

  private String[] getBackendStatisticsDescriptions()
  {
    String[] descriptions = new String[15];
    descriptions[0] = Translate.get("console.infoviewer.backend.name"); //$NON-NLS-1$
    descriptions[1] = Translate.get("console.infoviewer.backend.driver"); //$NON-NLS-1$
    descriptions[2] = Translate.get("console.infoviewer.backend.url"); //$NON-NLS-1$
    descriptions[3] = Translate
        .get("console.infoviewer.backend.active.transactions"); //$NON-NLS-1$
    descriptions[4] = Translate
        .get("console.infoviewer.backend.pending.requests"); //$NON-NLS-1$
    descriptions[5] = Translate.get("console.infoviewer.backend.read.enabled"); //$NON-NLS-1$
    descriptions[6] = Translate.get("console.infoviewer.backend.write.enabled"); //$NON-NLS-1$
    descriptions[7] = Translate.get("console.infoviewer.backend.init.status"); //$NON-NLS-1$
    descriptions[8] = Translate.get("console.infoviewer.backend.static.schema"); //$NON-NLS-1$
    descriptions[9] = Translate
        .get("console.infoviewer.backend.connection.managers"); //$NON-NLS-1$
    descriptions[10] = Translate
        .get("console.infoviewer.backend.total.active.connections"); //$NON-NLS-1$
    descriptions[11] = Translate
        .get("console.infoviewer.backend.persistent.connections"); //$NON-NLS-1$
    descriptions[12] = Translate
        .get("console.infoviewer.backend.total.requests"); //$NON-NLS-1$
    descriptions[13] = Translate
        .get("console.infoviewer.backend.total.transactions"); //$NON-NLS-1$
    descriptions[14] = Translate
        .get("console.infoviewer.backend.lastknown.checkpoint"); //$NON-NLS-1$
    return descriptions;
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {

    return "show backend"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandParameters()
   */
  public String getCommandParameters()
  {
    return ConsoleTranslate.get("admin.command.show.backend.params"); //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("admin.command.show.backend.description"); //$NON-NLS-1$
  }

}
