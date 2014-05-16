/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.driver;

import java.io.IOException;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.continuent.sequoia.driver.connectpolicy.AbstractControllerConnectPolicy;

public class SocketKillerCallBack implements ControllerStateChangedCallback
{
  /** The list of controllers with their set of sockets */
  Map                             controllersAndSockets;
  /** Policy that created us, to tell about controller state changes */
  AbstractControllerConnectPolicy policy;
  /** Level of logging (logs are printed to stdout, see {@link SequoiaUrl}) */
  int                             logLevel;

  public SocketKillerCallBack(AbstractControllerConnectPolicy policy,
      int logLevel)
  {
    controllersAndSockets = new HashMap();
    this.policy = policy;
    this.logLevel = logLevel;
  }

  public void registerSocket(ControllerInfo c, Socket s)
  {
    Set sockets = null;
    synchronized (controllersAndSockets)
    {
      if (controllersAndSockets.containsKey(c))
        sockets = (HashSet) controllersAndSockets.get(c);
      else
        sockets = new HashSet();
      sockets.add(s);
      controllersAndSockets.put(c, sockets);
    }
  }

  public void onControllerDown(ControllerInfo ctrl)
  {
    // tell policy asap
    if (policy != null)
      policy.controllerDown(ctrl);
    synchronized (controllersAndSockets)
    {
      if (controllersAndSockets.containsKey(ctrl))
      {
        if (logLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
        {
          System.out.println(new Date() + " Controller " + ctrl
              + " down - shutting down connected sockets");
        }
        Set sockets = (HashSet) controllersAndSockets.get(ctrl);
        for (Iterator iter = sockets.iterator(); iter.hasNext();)
        {
          Socket s = (Socket) iter.next();
          if (s != null)
          {
            try
            {
              s.shutdownInput();
              s.shutdownOutput();
            }
            catch (IOException ignored)
            { // ignore errors
            }
            // close inside its own try/catch to make sure it is called
            try
            {
              s.close();
            }
            catch (IOException ignored)
            { // ignore errors
            }
          }
        }
        // all sockets have been killed, remove them
        sockets.clear();
      }
    }
  }

  public void onControllerUp(ControllerInfo ctrl)
  {
    // tell policy asap
    if (policy != null)
      policy.controllerUp(ctrl);
    if (logLevel >= SequoiaUrl.DEBUG_LEVEL_DEBUG)
    {
      System.out.println(new Date() + " Controller " + ctrl + " is up again");
    }
  }

}
