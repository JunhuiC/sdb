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

/**
 * Denotes a request interceptor, which is invoked on completed SQL requests
 * prior to passing them to the VDB for execution.  Implementations must provide
 * a null constructor as well as property setters for any properties that 
 * are to be set at initialization time. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface RequestInterceptor
{
  /**
   * Initializes the interceptor.  Any properties that need to be set will be 
   * set prior to this call. 
   * 
   * @throws InterceptorException Thrown if the requestor cannot be 
   *    initialized for use
   */
  public void initialize() throws InterceptorException;

  /**
   * Intercepts a request.  The interceptor may modify the request but may 
   * not make changes that would cause its type to change.  For example, 
   * implementations may not change a read into an update. 
   * 
   * @param request A wrapped request object
   * @param connection Wrapped connection information
   * @throws InterceptorException Thrown if intercept processing fails; 
   *    implies that the request should not be used. 
   */
  public void intercept(SessionFacade connection, RequestFacade request)
    throws InterceptorException;
  
  /**
   * Shuts down the interceptor, which should perform clean-up required for 
   * a nice termination. 
   * 
   * @throws InterceptorException Thrown if clean 
   */
  public void terminate() throws InterceptorException;
}