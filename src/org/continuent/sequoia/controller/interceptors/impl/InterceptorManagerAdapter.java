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

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.interceptors.InterceptorConfigurator;
import org.continuent.sequoia.controller.interceptors.InterceptorException;
import org.continuent.sequoia.controller.interceptors.InterceptorManager;
import org.continuent.sequoia.controller.interceptors.RequestFacade;
import org.continuent.sequoia.controller.interceptors.SessionFacade;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread;

/**
 * Adapts InterceptorManager to specific datatypes used by Sequoia. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class InterceptorManagerAdapter
{
  private static Trace logger = Trace.getLogger(InterceptorManagerAdapter.class.getName());
  private static InterceptorManagerAdapter managerAdapter; 

  private InterceptorManager interceptorManager;

  /** Creates a new instance. */
  private InterceptorManagerAdapter(InterceptorManager manager)
  {
    this.interceptorManager = manager;
  }
  
  /** 
   * Returns an instance, which may or may not be a singleton.  Isn't 
   * encapsulation glorious? 
   */
  public static synchronized InterceptorManagerAdapter getInstance()
  {
    if (managerAdapter == null)
    {
      managerAdapter = new InterceptorManagerAdapter(InterceptorManager.getInstance());
    }
    return managerAdapter;
  }

  /** Wrapper for InterceptorManager.initialize(). */
  public void initialize(InterceptorConfigurator configurator) throws InterceptorException
  {
    interceptorManager.initialize(configurator);
  }

  /** Invokesfront-end request interceptors. */
  public void invokeFrontendRequestInterceptors(VirtualDatabase vdb, 
      AbstractRequest request)
  {
    SessionFacade sessionFacade = new FrontendSessionFacadeImpl(
        (VirtualDatabaseWorkerThread) Thread.currentThread(), vdb);
    RequestFacade requestFacade = new RequestFacadeImpl(request);
    
    try
    {
      // Synchronize to ensure that request does not change while we are working on it. 
      synchronized (request)
      {
        interceptorManager.invokeFrontendRequestInterceptors(sessionFacade, requestFacade);
      }
    }
    catch (InterceptorException e)
    {
      // For now log and eat exceptions. 
      logger.error("Frontend request processing failure! " + request.toShortDebugString(), e);
    }
  }
  
  /** Invokes back-end request interceptors. */
  public void invokeBackendRequestInterceptors(DatabaseBackend backend, AbstractRequest request)
  {
    SessionFacade sessionFacade = new BackendSessionFacadeImpl(backend, request);
    RequestFacade requestFacade = new RequestFacadeImpl(request);
    
    try
    {
      // Synchronize to ensure that request does not change while we are working on it. 
      synchronized (request)
      {
        interceptorManager.invokeBackendRequestInterceptors(sessionFacade, requestFacade);
      }
    }
    catch (InterceptorException e)
    {
      // For now log and eat exceptions. 
      logger.error("Backend request processing failure! " + request.toShortDebugString(), e);
    }
  }

  public void terminate() throws InterceptorException
  {
    interceptorManager.terminate();
  }
}