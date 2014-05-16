/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2007 Continuent, Inc.
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
 * Initial developer(s): Landon Fuller.
 * Contributor(s): 
 */

package org.continuent.sequoia.controller.core.shutdown;

import java.util.ArrayList;
import java.util.Iterator;

import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;


/**
 * Class to handle externally induced controller shutdown (ie, delivery
 * of signals, such as SIGINT).
 * 
 * @author <a href="mailto:landonf@threerings.net">Landon Fuller</a>
 *
 */
public class ControllerShutdownHook extends Thread {
  /** Managed controller. */
  private Controller controller;

  Trace logger = Trace.getLogger("org.continuent.sequoia.controller.shutdown");

  Trace endUserLogger = Trace.getLogger("org.continuent.sequoia.enduser");

  /**
   * On shutdown (ie, SIGINT delivered), shutdown all virtual databases
   * and the controller.
   * 
   * @param controller The controller to shut down.
   */
  public ControllerShutdownHook(Controller controller) {
    this.controller = controller;
  }
  
  public void run () {
    String msg = "Controller shutting down on SIGINT";
    endUserLogger.info(msg);
    logger.info(msg);
	  
    // Shut down all virtual databases, safely.
    ArrayList databases = controller.getVirtualDatabases();
    for (Iterator i = databases.iterator(); i.hasNext();) {
      VirtualDatabase db = (VirtualDatabase)i.next();
      if (!db.isShuttingDown())
        db.shutdown(Constants.SHUTDOWN_SAFE);
    }
    
    // If we're not already shutting down, shut down the Controller
    if (!controller.isShuttingDown()) {
      try {
        controller.shutdown();
      } catch (ControllerException e) {
        // This is a best effort ...
        logger.error("Error while shutting down controller", e);
      }
    }
  }
}
