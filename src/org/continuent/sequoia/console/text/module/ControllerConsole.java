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

package org.continuent.sequoia.console.text.module;

import org.continuent.sequoia.common.i18n.ConsoleTranslate;
import org.continuent.sequoia.console.jmx.RmiJmxClient;
import org.continuent.sequoia.console.text.Console;
import org.continuent.sequoia.console.text.ConsoleLauncher;

/**
 * This class defines a ControllerConsole
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ControllerConsole extends AbstractConsoleModule
{

  /**
   * Creates a new <code>ControllerConsole.java</code> object
   * 
   * @param console the controller console is attached to
   */
  public ControllerConsole(Console console)
  {
    super(console);
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#quit()
   */
  public void quit()
  {
    super.quit();
    console.storeHistory();
    console.printInfo(ConsoleTranslate.get("console.byebye", ConsoleLauncher.PRODUCT_NAME));
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getPromptString()
   */
  public String getPromptString()
  {
    RmiJmxClient jmxClient = console.getJmxClient();
    if (jmxClient == null)
      return "not connected";
    return jmxClient.getRemoteName();
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#login(String[])
   */
  public void login(String[] params)
  {
    // do nothing
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getDescriptionString()
   */
  public String getDescriptionString()
  {
    return "Controller";
  }

  /**
   * @see org.continuent.sequoia.console.text.module.AbstractConsoleModule#getModuleID()
   */
  protected String getModuleID()
  {
    return "controller"; //$NON-NLS-1$
  }

}
