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

package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.Iterator;
import java.util.Map;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean;
import org.continuent.sequoia.console.text.formatter.TableFormatter;
import org.continuent.sequoia.console.text.module.VirtualDatabaseAdmin;

/**
 * This class defines a ViewCheckpointNames
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ViewCheckpointNames extends AbstractAdminCommand
{

  /**
   * Creates a new <code>ViewCheckpointNames</code> object
   * 
   * @param module module that owns this commands
   */
  public ViewCheckpointNames(VirtualDatabaseAdmin module)
  {
    super(module);
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#parse(java.lang.String)
   */
  public void parse(String commandText) throws Exception
  {
    RecoveryLogControlMBean recoveryLog = jmxClient.getRecoveryLog(dbName,
        user,
        password);
    Map checkpoints = recoveryLog.getCheckpoints();
    if (checkpoints.size() == 0)
    {
      console.printInfo(ConsoleTranslate.get("ViewCheckpointNames.nocheckpoints")); //$NON-NLS-1$
      return;
    }
    // iterate over checkpoints
    String[][] cp = getCheckpointsAsStrings(checkpoints);
    String[] headers = new String[]{"id", "name"}; //$NON-NLS-1$//$NON-NLS-2$
    String formattedCheckpoints = TableFormatter.format(headers, cp, true);
    console.println(formattedCheckpoints);

  }

  private String[][] getCheckpointsAsStrings(Map checkpoints)
  {
    String[][] cp = new String[checkpoints.entrySet().size()][2];
    Iterator iter = checkpoints.entrySet().iterator();
    int i = 0;
    while (iter.hasNext())
    {
      // we display first the log ID and then the checkpoint names
      Map.Entry checkpoint = (Map.Entry) iter.next();
      cp[i][1] = (String) checkpoint.getKey();
      cp[i][0] = (String) checkpoint.getValue();
      i++;
    }
    return cp;
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandName()
   */
  public String getCommandName()
  {
    return "show checkpoints"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.console.text.commands.ConsoleCommand#getCommandDescription()
   */
  public String getCommandDescription()
  {
    return ConsoleTranslate.get("ViewCheckpointNames.description"); //$NON-NLS-1$
  }

}