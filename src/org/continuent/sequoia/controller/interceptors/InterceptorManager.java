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

package org.continuent.sequoia.controller.interceptors;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * The InterceptorManager class encapsulates lifecycle management and invocation
 * of interceptors.  To ensure that interceptor data are correctly published 
 * between threads, the manager should be initialized fully by a single thread
 * before any other thread attempts to call getInstance().  The getInstance() 
 * method is synchronized on the class, and will ensure that all changes 
 * during initialization are safely published to other threads.  <p>
 * 
 * See RequestInterceptor for more information on concurrency requirements
 * that apply to interceptor implementations. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class InterceptorManager
{
  /** Singleton manager instance. */
  private static InterceptorManager manager; 
  
  private static RequestInterceptor[] frontendRequestInterceptors; 
  private static RequestInterceptor[] backendRequestInterceptors; 

  private InterceptorManager()
  {
  }
  
  /**
   * Returns manager singleton.  
   */
  public static synchronized InterceptorManager getInstance()
  {
    if (manager == null)
      manager = new InterceptorManager();
    return manager;
  }
  
  /** 
   * Configures and initializes interceptors.  This happens once and only 
   * once in the lifecycle of the manager and hence the interceptors it 
   * controls.  
   */
  public void initialize(InterceptorConfigurator configurator)
    throws InterceptorException
  {
    // Synchronize on class to force publication of changes to all threads. 
    synchronized(InterceptorManager.class)
    {
      // Instantiate and configure interceptor instances. 
      configurator.configure();

      // Initialize and add frontend request interceptors. 
      List<?> frontendList = configurator.getFrontendRequestInterceptors();
      frontendRequestInterceptors = initializeInterceptors(frontendList);
      
      // Initialize and add backend request interceptors. 
      List<?> backendList = configurator.getBackendRequestInterceptors();
      backendRequestInterceptors = initializeInterceptors(backendList);
    }
  }

  /**
   * Initializes a list of interceptors. 
   * 
   * @param interceptorList List from configurator
   * @return Array of initialized interceptors
   * @throws InterceptorException Thrown if initialization fails
   */
  protected RequestInterceptor[] initializeInterceptors(List<?> interceptorList) 
    throws InterceptorException
  {
    Iterator<?> iter = interceptorList.iterator();
    Vector<RequestInterceptor> requestInterceptorList = new Vector<RequestInterceptor>();
    while (iter.hasNext())
    {
      RequestInterceptor interceptor = (RequestInterceptor) iter.next();
      interceptor.initialize();
      requestInterceptorList.add(interceptor);
    }
    
    // Load vector into request interceptor array. 
    RequestInterceptor[] interceptorArray = 
      new RequestInterceptor[requestInterceptorList.size()];
    for (int i = 0; i < interceptorArray.length; i++)
    {
      interceptorArray[i] = (RequestInterceptor) requestInterceptorList.get(i);
    }
    return interceptorArray;
  }

  /**
   * Invokes each frontend request interceptor in succession.  We do not
   * synchronize because interceptors are assumed by default to be essentially
   * immutable. 
   * 
   * @see RequestInterceptor
   * @param session SessionFacade instance with session data
   * @param request RequestFacade instance holding request information
   * @throws InterceptorException Thrown if request processing fails
   */
  public void invokeFrontendRequestInterceptors(SessionFacade session, 
      RequestFacade request) throws InterceptorException
  {
    for (int i = 0; i < frontendRequestInterceptors.length; i++)
    {
      frontendRequestInterceptors[i].intercept(session, request);
    }
  }

  /**
   * Invokes each backend request interceptor in succession.  This call is
   * essentiall identical to invoking backend request interceptors. 
   * 
   * @see RequestInterceptor
   * @param session SessionFacade instance with session data
   * @param request RequestFacade instance holding request information
   * @throws InterceptorException Thrown if request processing fails
   */
  public void invokeBackendRequestInterceptors(SessionFacade session, 
      RequestFacade request) throws InterceptorException
  {
    for (int i = 0; i < backendRequestInterceptors.length; i++)
    {
      backendRequestInterceptors[i].intercept(session, request);
    }
  }

  /**
   * Terminates the manager and all interceptors it manages. 
   * 
   * @throws InterceptorException Thrown if termination cannot be completed
   */
  public void terminate() throws InterceptorException
  {
    synchronized(InterceptorManager.class)
    {
      terminate(frontendRequestInterceptors);
      frontendRequestInterceptors = null;

      terminate(backendRequestInterceptors);
      backendRequestInterceptors = null;
    }
  }
  
  protected void terminate(RequestInterceptor[] interceptorArray) 
      throws InterceptorException
  {
    for (int i = 0; i < interceptorArray.length; i++)
    {
      interceptorArray[i].terminate();
    }
  }
}