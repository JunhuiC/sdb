/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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

package org.continuent.sequoia.common.users;

/**
 * A <code>DatabaseBackendUser</code> is a login/password combination to
 * represent a database backend user.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public class DatabaseBackendUser extends AbstractDatabaseUser
{
  private static final long serialVersionUID = 92260597820622650L;

  /** Backend logical name. */
  private String            backendName;

  /**
   * Creates a new <code>DatabaseBackendUser</code> instance. The caller must
   * ensure that the parameters are not <code>null</code>.
   * 
   * @param backendName the backend logical name.
   * @param login the user name.
   * @param password the password.
   */
  public DatabaseBackendUser(String backendName, String login, String password)
  {
    super(login, password);
    this.backendName = backendName;
  }

  /**
   * Returns the backend logical name.
   * 
   * @return the backend logical name.
   */
  public String getBackendName()
  {
    return backendName;
  }

  /**
   * Tests if the login and password provided matches the login/password of this
   * object.
   * 
   * @param backendName backend logical name
   * @param login a user name
   * @param password a password
   * @return <code>true</code> if it matches this object's login/password
   */
  public boolean matches(String backendName, String login, String password)
  {
    return (super.matches(login, password) && this.backendName
        .equals(backendName));
  }

  /**
   * Two <code>DatabaseBackendUser</code> are equals if both objects have the
   * same login & password.
   * 
   * @param other the object to compare with.
   * @return <code>true</code> if both objects have the same login & password.
   */
  public boolean equals(Object other)
  {
    if ((other == null) || !(other instanceof DatabaseBackendUser))
      return false;

    DatabaseBackendUser user = (DatabaseBackendUser) other;
    return (super.matches(user.login, user.password) && backendName
        .equals(user.backendName));
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    return "";
    // return "<"
    // + DatabasesXmlTags.ELT_RealLogin
    // + " "
    // + DatabasesXmlTags.ATT_backendName
    // + "=\""
    // + getBackendName()
    // + "\" "
    // + DatabasesXmlTags.ATT_rLogin
    // + "=\""
    // + getLogin()
    // + "\" "
    // + DatabasesXmlTags.ATT_rPassword
    // + "=\""
    // + getPassword()
    // + "\"/>";
  }
}
