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

package org.continuent.sequoia.controller.jmx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.security.auth.Subject;

import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.users.AbstractDatabaseUser;

/**
 * An MBeanServer authenticating all invoke() requests.
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @author <a href="mailto:nicolas.modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class AuthenticatingMBeanServer extends ChainedMBeanServer
{

  /** Logger instance */
  static Trace logger = Trace
                          .getLogger("org.continuent.sequoia.controller.jmx.AuthenticatingMBeanServer");

  /**
   * Overridden just to make it public
   * <p>
   * 
   * @see org.continuent.sequoia.controller.jmx.ChainedMBeanServer#setMBeanServer(javax.management.MBeanServer)
   */
  public void setMBeanServer(MBeanServer server)
  {
    super.setMBeanServer(server);
  }

  /**
   * @see javax.management.MBeanServerConnection#invoke(javax.management.ObjectName,
   *             java.lang.String, java.lang.Object[], java.lang.String[])
   */
  public Object invoke(ObjectName name, String operationName, Object[] params,
      String[] signature) throws InstanceNotFoundException, MBeanException,
      ReflectionException
  {
    if (JmxConstants.mbeanNeedAuthentication(name)
        && (operationName.equalsIgnoreCase("checkAdminAuthentication") == false))
    {
      // we have to check all methods that access a virtual database
      // except
      // authentication
      boolean authenticationOk = false;
      String username = null;
      String password = null;

      Subject subject = Subject.getSubject(java.security.AccessController
          .getContext());
      if (subject == null || subject.getPrincipals().size() == 0)
      {
        username = (String) params[0];
        password = (String) params[1];
        authenticationOk = authenticate(name, username, password);
        if (!authenticationOk)
          throw new MBeanException(new Exception(
              "Authentication failed (username,password) invalid"));

        if (logger.isDebugEnabled())
          logger
              .debug("Authentication with username and password was successfull");

        // we have to strip the username and password from the params
        // and args
        return super.invoke(name, operationName, cleanO(params),
            cleanS(signature));
      }
      else
      {
        Set principals = subject.getPrincipals(AbstractDatabaseUser.class);
        for (Iterator it = principals.iterator(); it.hasNext();)
        {
          AbstractDatabaseUser user = (AbstractDatabaseUser) it.next();
          username = user.getName();
          password = user.getPassword();
          authenticationOk = authenticate(name, username, password);
          if (authenticationOk)
            break;
        }

        if (principals.size() == 0 && logger.isDebugEnabled())
          throw new MBeanException(new Exception(
              "Authentication failed : no principal"));

        if (!authenticationOk)
          throw new MBeanException(new Exception(
              "Authentication failed : principal invalid"));
        if (logger.isDebugEnabled())
          logger.debug("Authentication with principal was successfull");
        return super.invoke(name, operationName, params, signature);
      }
    }
    else
    {
      if (logger.isDebugEnabled())
        logger.debug("no authentication required");

      return super.invoke(name, operationName, params, signature);
    }
  }

  private boolean authenticate(ObjectName name, String username, String password)
  {
    try
    {
      String type = name.getKeyProperty("type");
      boolean vdb = JmxConstants.VIRTUALDATABASE_TYPE_VALUE.equals(type);
      if (vdb)
        return ((Boolean) invoke(name, "checkAdminAuthentication",
            new Object[]{username, password}, new String[]{"java.lang.String",
                "java.lang.String"})).booleanValue();
      else
      {
        boolean backend = JmxConstants.DATABASEBACKEND_TYPE_VALUE.equals(type);
        if (backend)
        {
          String virtualDataBaseName = name.getKeyProperty(JmxConstants.VIRTUALDATABASE_PROPERTY);
          if (virtualDataBaseName == null)
          {
            return false;
          }
          // Check with the owning database if the password is right
          ObjectName vdbName = JmxConstants
              .getVirtualDataBaseObjectName(virtualDataBaseName);
          return ((Boolean) invoke(vdbName, "checkAdminAuthentication",
              new Object[]{username, password}, new String[]{
                  "java.lang.String", "java.lang.String"})).booleanValue();
        }
        else
          // No further check ...
          return true;
      }
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled())
      {
        logger.debug("authentication failed with exception ", e);
      }
      return false;
    }
  }

  private static Object[] cleanO(Object[] params)
  {
    List o = Arrays.asList(params);
    o = o.subList(2, o.size());
    return (new ArrayList(o).toArray());
  }

  private static String[] cleanS(String[] params)
  {
    List o = Arrays.asList(params);
    o = o.subList(2, o.size());
    String[] s = new String[o.size()];
    return (String[]) new ArrayList(o).toArray(s);
  }
}