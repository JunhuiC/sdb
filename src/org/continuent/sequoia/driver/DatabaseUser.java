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
 * Initial developer(s): Julie Marguerite.
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.driver;

import java.io.Serializable;

/**
 * A <code>DatabaseUser</code> is just a login/password combination to
 * represent database user.
 * 
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite</a>
 * @author <a href="Mathieu.Peltier@inrialpes.fr">Mathieu Peltier</a>
 * @version 1.0
 */
public class DatabaseUser implements Serializable
{
  private static final long serialVersionUID = 4733183236454586066L;

  /** Virtual database name. */
  private String            dbName;

  /** User name. */
  private String            login;

  /** Password. */
  private String            password;

  /**
   * Creates a new <code>DatabaseUser</code> instance.
   * 
   * @param dbName The virtual database name
   * @param login User name
   * @param password Password
   */
  public DatabaseUser(String dbName, String login, String password)
  {
    this.dbName = dbName;
    this.login = login;
    this.password = password;
  }

  /**
   * Tests if the virtual database name login and password provided matches the
   * virtual database name/login/password of this object.
   * 
   * @param dbName virtual database name
   * @param login a user name
   * @param password a password
   * @return <code>true</code> if it matches this object's virtual database
   *         name/login/password
   */
  public boolean matches(String dbName, String login, String password)
  {
    return (this.dbName.equals(dbName) && this.login.equals(login) && this.password
        .equals(password));
  }

  /**
   * Compares an object with this object.
   * 
   * @param other an <code>Object</code>
   * @return <code>true</code> if both objects have same virtual database
   *         name, login and password
   */
  public boolean equals(Object other)
  {
    if (!(other instanceof DatabaseUser))
      return false;

    DatabaseUser castOther = (DatabaseUser) other;
    return matches(castOther.dbName, castOther.login, castOther.password);
  }

  /**
   * Returns the virtual database name.
   * 
   * @return database name
   */
  public String getDbName()
  {
    return dbName;
  }

  /**
   * Gets the login name.
   * 
   * @return login name
   */
  public String getLogin()
  {
    return login;
  }

  /**
   * Gets the password.
   * 
   * @return password
   */
  public String getPassword()
  {
    return password;
  }
}
