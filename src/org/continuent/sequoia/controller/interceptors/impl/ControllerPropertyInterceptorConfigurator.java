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

import java.util.List;
import java.util.Vector;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.interceptors.InterceptorConfigurator;
import org.continuent.sequoia.controller.interceptors.InterceptorException;
import org.continuent.sequoia.controller.interceptors.RequestInterceptor;
import org.omg.PortableInterceptor.Interceptor;

/**
 * This class defines an InterceptorConfigurator that loads interceptors
 * based on controller property values.  The following controller properties
 * are included: <p>
 * <table>
 * <theader>
 * <tr> 
 * <td>Property Name</td><td>Description</td><td>Specification</td>
 * </tr>
 * </theader>
 * <tbody>
 * <tr>
 *    <td>backend.interceptors</td>
 *    <td>Interceptor classes to before SQL is submitted to backend</td>
 *    <td>A list of fully qualified class names separated by semi-colons</td>
 * </tr>
 * <tr>
 *    <td>frontend.interceptors</td>
 *    <td>Interceptor classes to run on fronends, i.e., in VirtualDatabaseWorkerThread</td>
 *    <td>A list of fully qualified class names separated by semi-colons</td>
 * </tr>
 * <tbody>
 * </table>
 * 
 * Interceptors are loaded and run in the order in which they are listed in 
 * the properties.  By default no interceptors are provided.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ControllerPropertyInterceptorConfigurator
    implements
      InterceptorConfigurator
{
  /** Logger instances. */
  static Trace logger = Trace.getLogger("org.continuent.sequoia.controller.core.Controller");
  private Vector backendRequestInterceptors = new Vector();
  private Vector frontendRequestInterceptors = new Vector();

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.InterceptorConfigurator#configure()
   */
  public void configure() throws InterceptorException
  {
    if (logger.isInfoEnabled())
      logger.info("Loading backend interceptor classes");
    String backendClassNames = clean(ControllerConstants.BACKEND_INTERCEPTORS);
    backendRequestInterceptors = loadInterceptorClasses(backendClassNames);
    
    if (logger.isInfoEnabled())
      logger.info("Loading frontend interceptor classes");
    String frontendClassNames = clean(ControllerConstants.FRONTEND_INTERCEPTORS);
    frontendRequestInterceptors = loadInterceptorClasses(frontendClassNames);
  }

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.InterceptorConfigurator#getFrontendRequestInterceptors()
   */
  public List getFrontendRequestInterceptors()
  {
    return frontendRequestInterceptors;
  }

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.InterceptorConfigurator#getBackendRequestInterceptors()
   */
  public List getBackendRequestInterceptors()
  {
    return backendRequestInterceptors;
  }
  
  /**
   * Returns a cleaned up list of interceptors with whitespace removed or empty
   * string if the list does not contain any names. 
   */
  private String clean(String interceptorList)
  {
    if (interceptorList == null)
      return ""; 
    else 
    {
      StringBuffer cleanListBuffer = new StringBuffer();
      for (int i = 0; i < interceptorList.length(); i++)
      {
        char c = interceptorList.charAt(i);
        if (! Character.isWhitespace(c))
          cleanListBuffer.append(c);
      }
      String cleanList = cleanListBuffer.toString();
      return cleanList;
    }
  }
  
  private Vector loadInterceptorClasses(String interceptorList)
      throws InterceptorException
  {
    // Create vector and see if we have work to do. 
    Vector interceptors = new Vector();
    if (! "".equals(interceptorList))
    {
      String[] classNames = interceptorList.split(";");
      for (int i = 0; i < classNames.length; i++)
      {
        String className = classNames[i];
        try
        {
          if (logger.isInfoEnabled())
            logger.info("Loading interceptor class: " + className);
          Class interceptorClass = Class.forName(className);
          RequestInterceptor interceptor = (RequestInterceptor) interceptorClass.newInstance();
          interceptors.add(interceptor);
        }
        catch (Exception e)
        {
          throw new InterceptorException("Unable to load interceptor class: " 
              + className, e);
        }
      }
    }
    
    return interceptors;
  }
}
