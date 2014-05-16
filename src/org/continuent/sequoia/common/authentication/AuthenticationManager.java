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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.common.authentication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.users.AdminUser;
import org.continuent.sequoia.common.users.DatabaseBackendUser;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * The <code>AuthenticationManager</code> manages the mapping between virtual
 * login/password (to the <code>VirtualDatabase</code>) and the real
 * login/password for each backend.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class AuthenticationManager
{
  /*
   * How the code is organized ? 1. Member variables 2. Constructor(s) 3.
   * Login/Password checking functions 4. Getter/Setter (possibly in
   * alphabetical order) 5. Xml
   */

  /** <code>ArrayList</code> of <code>VirtualDatabaseUser</code> objects. */
  private ArrayList virtualLogins;

  /** <code>ArrayList</code> of <code>AdminUser</code> objects. */
  private ArrayList adminUsers;

  /**
   * <code>HashMap</code> of <code>HashMap</code> of
   * <code>DatabaseBackendUser</code> objects hashed by the backend name,
   * hashed by their virtual database login. A virtual user can have several
   * real logins, but has only one real login for a given backend.
   */
  private HashMap   realLogins;

  /** Controls whether the transparent login feature is enabled. */
  private boolean   transparentLogin;

  /*
   * Constructor(s)
   */

  /**
   * Creates a new <code>AuthenticationManager</code> instance.
   */
  public AuthenticationManager()
  {
    this(true);
  }

  /**
   * Creates a new <code>AuthenticationManager</code> instance.
   * 
   * @param transparentLogin enable/disable the transparent login feature.
   */
  public AuthenticationManager(boolean transparentLogin)
  {
    virtualLogins = new ArrayList();
    adminUsers = new ArrayList();
    realLogins = new HashMap();
    this.transparentLogin = transparentLogin;
  }

  /*
   * Login/Password checking functions
   */

  /**
   * Checks whether this administrator user has been registered to this
   * <code>AuthenticationManager</code> or not. Returns <code>false</code>
   * if no admin user has been set.
   * 
   * @param user administrator user login/password to check.
   * @return <code>true</code> if it matches the registered admin user.
   */
  public boolean isValidAdminUser(AdminUser user)
  {
    return adminUsers.contains(user);
  }

  /**
   * Checks whether a given virtual database user has been registered to this
   * <code>AuthenticationManager</code> or not.
   * 
   * @param vUser the virtual database user.
   * @return <code>true</code> if the user login/password is valid.
   */
  public boolean isValidVirtualUser(VirtualDatabaseUser vUser)
  {
    return virtualLogins.contains(vUser);
  }

  /**
   * Checks whether a given virtual login has been registered to this
   * <code>AuthenticationManager</code> or not.
   * 
   * @param vLogin the virtual database login.
   * @return <code>true</code> if the virtual database login is valid.
   */
  public boolean isValidVirtualLogin(String vLogin)
  {
    Iterator iter = virtualLogins.iterator();
    VirtualDatabaseUser u;
    while (iter.hasNext())
    {
      u = (VirtualDatabaseUser) iter.next();
      if (u.getLogin().equals(vLogin))
      {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether transparent login feature is enabled.
   * 
   * @return true if enabled, false otherwise.
   */
  public boolean isTransparentLoginEnabled()
  {
    return transparentLogin;
  }

  /**
   * Sets the administrator user.
   * 
   * @param adminUser the administor user to set.
   */
  // public void setAdminUser(VirtualDatabaseUser adminUser)
  // {
  // this.adminUser = adminUser;
  // }
  /**
   * Registers a new virtual database user.
   * 
   * @param vUser the <code>VirtualDatabaseUser</code> to register.
   */
  public synchronized void addVirtualUser(VirtualDatabaseUser vUser)
  {
    virtualLogins.add(vUser);
  }

  /**
   * Unregisters a new virtual database user.
   * 
   * @param vUser the <code>VirtualDatabaseUser</code> to unregister.
   */
  public void removeVirtualUser(VirtualDatabaseUser vUser)
  {
    virtualLogins.remove(vUser);
  }

  /**
   * Associates a new database backend user to a virtual database login.
   * 
   * @param vLogin the virtual database login.
   * @param rUser the database backend user to add.
   * @exception AuthenticationManagerException if a real user already exists for
   *              this backend.
   */
  public void addRealUser(String vLogin, DatabaseBackendUser rUser)
      throws AuthenticationManagerException
  {
    HashMap list = (HashMap) realLogins.get(vLogin);
    if (list == null)
    {
      list = new HashMap();
      list.put(rUser.getBackendName(), rUser);
      realLogins.put(vLogin, list);
    }
    else
    {
      DatabaseBackendUser u = (DatabaseBackendUser) list.get(rUser
          .getBackendName());
      if (u != null)
        throw new AuthenticationManagerException(
            Translate.get("authentication.failed.add.user.already.exists",
                new String[]{rUser.getLogin(), vLogin, rUser.getBackendName(),
                    u.getLogin()}));
      list.put(rUser.getBackendName(), rUser);
    }
  }

  /**
   * Gets the <code>DatabaseBackendUser</code> given a virtual database login
   * and a database backend logical name.
   * 
   * @param vLogin virtual database login.
   * @param backendName database backend logical name.
   * @return a <code>DatabaseBackendUser</code> value or <code>null</code>
   *         if not found.
   */
  public DatabaseBackendUser getDatabaseBackendUser(String vLogin,
      String backendName)
  {
    Object list = realLogins.get(vLogin);
    if (list == null)
      return null;
    else
      return (DatabaseBackendUser) ((HashMap) list).get(backendName);
  }

  /**
   * @return Returns the realLogins.
   */
  public HashMap getRealLogins()
  {
    return realLogins;
  }

  /**
   * @return Returns the virtualLogins.
   */
  public ArrayList getVirtualLogins()
  {
    return virtualLogins;
  }

  /**
   * Return the virtual password corresponding to the given virtual login
   * 
   * @param vLogin the virtual login
   * @return the virtual password if found, null otherwise
   */
  public String getVirtualPassword(String vLogin)
  {
    Iterator iter = virtualLogins.iterator();
    VirtualDatabaseUser u;
    while (iter.hasNext())
    {
      u = (VirtualDatabaseUser) iter.next();
      if (u.getLogin().equals(vLogin))
      {
        return u.getPassword();
      }
    }
    return null;
  }

  /*
   * 5. Xml
   */
  /**
   * Format to xml
   * 
   * @return xml formatted representation
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_AuthenticationManager + ">");
    info.append("<" + DatabasesXmlTags.ELT_Admin + ">");
    for (int i = 0; i < adminUsers.size(); i++)
    {
      AdminUser vu = (AdminUser) adminUsers.get(i);
      info.append(vu.getXml());
    }
    info.append("</" + DatabasesXmlTags.ELT_Admin + ">");

    info.append("<" + DatabasesXmlTags.ELT_VirtualUsers + ">");
    for (int i = 0; i < virtualLogins.size(); i++)
    {
      VirtualDatabaseUser vu = (VirtualDatabaseUser) virtualLogins.get(i);
      info.append(vu.getXml());
    }
    info.append("</" + DatabasesXmlTags.ELT_VirtualUsers + ">");
    info.append("</" + DatabasesXmlTags.ELT_AuthenticationManager + ">");
    return info.toString();
  }

  /**
   * Add an admin user for this authentication manager.
   * 
   * @param user the <code>AdminUser</code> to add to this
   *          <code>AuthenticationManager</code>
   */
  public void addAdminUser(AdminUser user)
  {
    adminUsers.add(user);
  }

  /**
   * Remove an admin user from the admin list
   * 
   * @param user the admin to remove
   * @return <code>true</code> if was removed.
   */
  public boolean removeAdminUser(AdminUser user)
  {
    return adminUsers.remove(user);
  }

  /**
   * @return Returns the adminUsers.
   */
  public ArrayList getAdminUsers()
  {
    return adminUsers;
  }

}
