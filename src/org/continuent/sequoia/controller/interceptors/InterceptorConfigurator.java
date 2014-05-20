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

import java.util.List;

/**
 * Denotes a class that instantiates and sets properties of interceptors. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface InterceptorConfigurator
{
  /**
   * Invokes procedure to instantiate and configure interceptors. 
   *  
   * @throws InterceptorException Thrown if configuration fails
   */
  public void configure() throws InterceptorException;

  /**
   * @return a list of frontend request interceptors in invocation order
   */
  public List<?> getFrontendRequestInterceptors();
  
  /**
   * @return a list of backend request interceptors in invocation order
   */
  public List<?> getBackendRequestInterceptors();
}