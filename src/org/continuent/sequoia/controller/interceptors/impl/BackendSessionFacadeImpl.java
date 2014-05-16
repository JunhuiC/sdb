/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Robert Hodges.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.interceptors.impl;

import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.interceptors.SessionFacade;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;

/**
 * Implements a session facade for Sequoia based on data available to 
 * backends. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BackendSessionFacadeImpl implements SessionFacade
{
  DatabaseBackend backend; 
  AbstractRequest request;
  
  public BackendSessionFacadeImpl(DatabaseBackend backend, AbstractRequest request)
  {
    this.backend = backend; 
    this.request = request;
  }
  
  public String getLogin()
  {
    return request.getLogin();
  }

  public String getDatabase()
  {
    return backend.getVirtualDatabaseName();
  }
}