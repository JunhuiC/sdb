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
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.common.users;

import java.io.Serializable;
import java.security.Principal;

/**
 * An <code>AbstractDatabaseUser</code> is just a login/password combination
 * to represent an abstract database user.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public abstract class AbstractDatabaseUser implements Serializable, Principal
{
  /** Login name. */
  protected String login;

  /** Password. */
  protected String password;

  /**
   * Creates a new <code>AbstractDatabaseUser</code> instance. The caller must
   * ensure that the parameters are not <code>null</code>.
   * 
   * @param login the user name.
   * @param password the password.
   */
  protected AbstractDatabaseUser(String login, String password)
  {
    this.login = login;
    this.password = password;
  }

  /**
   * Gets the login name.
   * 
   * @return the login name.
   */
  public String getLogin()
  {
    return login;
  }

  /**
   * Gets the login name.
   * 
   * @return the login name.
   */
  public String getName()
  {
    return getLogin();
  }

  /**
   * Gets the password.
   * 
   * @return the password.
   */
  public String getPassword()
  {
    return password;
  }

  /**
   * Tests if the login and password provided matches the login/password of this
   * object.
   * 
   * @param login a user name.
   * @param password a password.
   * @return <code>true</code> if it matches this object's login/password.
   */
  public boolean matches(String login, String password)
  {
    return (this.login.equals(login) && this.password.equals(password));
  }

  /**
   * Two <code>AbstractDatabaseUser</code> are equals if both objects have
   * same login & password.
   * 
   * @param other the object to compare with.
   * @return <code>true</code> if both objects have same login & password.
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof AbstractDatabaseUser))
      return false;

    AbstractDatabaseUser user = (AbstractDatabaseUser) other;
    return matches(user.login, user.password);
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public abstract String getXml();
}
