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
 * Initial developer(s): Marc Wick.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.authentication;

import javax.management.remote.JMXAuthenticator;
import javax.security.auth.Subject;

import org.continuent.sequoia.common.log.Trace;

/**
 * This class defines a PasswordAuthenticator
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class PasswordAuthenticator implements JMXAuthenticator

{

  /**
   * to enable subject delegation we use a dummy authentication even if none is
   * configured
   */
  public static final PasswordAuthenticator NO_AUTHENICATION = new PasswordAuthenticator(
                                                                 null, null);

  static Trace                              logger           = Trace
                                                                 .getLogger("org.continuent.sequoia.common.authentication");

  private String                            username;
  private String                            password;

  /**
   * Creates a new <code>PasswordAuthenticator.java</code> object
   * 
   * @param username username/loginname
   * @param password password
   */
  public PasswordAuthenticator(String username, String password)
  {
    this.username = username;
    this.password = password;
  }

  /**
   * create a credentials object with the supplied username and password
   * 
   * @param username username
   * @param password password
   * @return credentials Object to be used for authentication,
   */
  public static Object createCredentials(String username, String password)
  {
    return new String[]{username, password};
  }

  /**
   * @see javax.management.remote.JMXAuthenticator#authenticate(java.lang.Object)
   */
  public Subject authenticate(Object credentials) throws SecurityException
  {
    try
    {
      if (username == null && password == null)
      {
        // no authentication is required we return
        return new Subject();
      }

      if (credentials == null)
      {
        throw new SecurityException("credentials are required");
      }

      try
      {
        String[] credentialsArray = (String[]) credentials;
        if (username.equals(credentialsArray[0])
            && password.equals(credentialsArray[1]))
        {
          // username and password are ok
          if (logger.isDebugEnabled())
          {
            logger.debug("successfully authenitcated ");
          }
          return new Subject();
        }
      }
      catch (Exception e)
      {
        // the credentials object makes problems, is was probably not created
        // with the createCredentials method
        throw new SecurityException("problems with credentials object : "
            + e.getMessage());
      }

      // username and password do not match
      throw new SecurityException("invalid credentials");
    }
    catch (SecurityException e)
    {
      logger.error(e.getMessage());
      try
      {
        String clientId = java.rmi.server.RemoteServer.getClientHost();
        logger.warn("refused unauthorized access for client " + clientId);
      }
      catch (Exception ex)
      {

      }
      throw e;
    }
  }
}
