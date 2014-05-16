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
 * Initial developer(s): Marek Prochazka. 
 * Contributor(s):
 */

package org.continuent.sequoia.driver;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

/**
 * An implementation of the JDBC 2.0 optional package <code>DataSource</code>
 * interface. It allows to set the URL, user name, and password to its
 * properties. It can be bound via JNDI so that the properties can be set by an
 * "application server" and a "ready-to-use" reference to
 * <code>DataSource</code> can be retrieved via JNDI.
 * 
 * @author <a href="mailto:Marek.Prochazka@inrialpes.fr">Marek Prochazka </a>
 * @version 1.0
 */
public class DataSource
    implements
      javax.sql.DataSource,
      Referenceable,
      Serializable
{
  private static final long serialVersionUID = 7103360001817804513L;
  
  /** DataSource properties */
  protected static final String URL_PROPERTY         = "url";
  protected static final String USER_PROPERTY        = Driver.USER_PROPERTY;
  protected static final String PASSWORD_PROPERTY    = Driver.PASSWORD_PROPERTY;
  protected static final String DRIVER_CLASSNAME     = "org.continuent.sequoia.driver.Driver";
  protected static final String FACTORY_CLASSNAME    = "org.continuent.sequoia.driver.DataSourceFactory";
  protected static final String DESCRIPTION_PROPERTY = "description";

  /** Wrapped driver for to get connections. */
  protected static Driver       driver               = null;
  static
  {
    try
    {
      driver = (Driver) Class.forName(DRIVER_CLASSNAME).newInstance();
    }
    catch (Exception e)
    {
      throw new RuntimeException("Can't load " + DRIVER_CLASSNAME);
    }
  }

  /** DataSource properties */
  protected String              url                  = null;
  protected String              user                 = null;
  protected String              password             = null;
  protected PrintWriter         logWriter            = null;

  /**
   * Default constructor.
   */
  public DataSource()
  {
  }

  // ---------------------------------
  // --- DataSource methods
  // ---------------------------------
  /**
   * Gets connection. Retrieves a new connection using the user name and
   * password that have been already set.
   * 
   * @throws SQLException if an error occurs.
   * @return a new connection.
   */
  public java.sql.Connection getConnection() throws SQLException
  {
    return getConnection(user, password);
  }

  /**
   * Gets connection. Retrieves a new connection using the user name and
   * password specified.
   * 
   * @param user user name.
   * @param password password.
   * @return a new connection.
   * @throws SQLException if an error occurs.
   */
  public java.sql.Connection getConnection(String user, String password)
      throws SQLException
  {
    if (user == null)
    {
      user = "";
    }
    if (password == null)
    {
      password = "";
    }
    Properties props = new Properties();
    props.put(USER_PROPERTY, user);
    props.put(PASSWORD_PROPERTY, password);

    return getConnection(props);
  }

  /**
   * Sets the log writer for this data source.
   * 
   * @param output print writer.
   * @throws SQLException in case of an error occurs.
   */
  public void setLogWriter(PrintWriter output) throws SQLException
  {
    logWriter = output;
  }

  /**
   * Gets the log writer.
   * 
   * @return log writer.
   */
  public java.io.PrintWriter getLogWriter()
  {
    return logWriter;
  }

  /**
   * Sets the timeout. Actually does nothing.
   * 
   * @param seconds timeout in seconds.
   * @throws SQLException in case of an error occurs.
   */
  public void setLoginTimeout(int seconds) throws SQLException
  {
  }

  /**
   * Gets the login timeout.
   * 
   * @return login timeout
   * @throws SQLException in case of an error occurs.
   */
  public int getLoginTimeout() throws SQLException
  {
    return 0;
  }

  // ---------------------------------
  // --- Referenceable methods
  // ---------------------------------
  /**
   * Gets a reference to this. The factory used for this class is the
   * {@link DataSourceFactory}class.
   * 
   * @return a reference to this.
   * @throws NamingException if <code>DataSourceFactory</code> not found.
   */
  public Reference getReference() throws NamingException
  {
    Reference ref = new Reference(getClass().getName(), FACTORY_CLASSNAME, null);
    ref.add(new StringRefAddr(DESCRIPTION_PROPERTY, getDescription()));
    ref.add(new StringRefAddr(USER_PROPERTY, getUser()));
    ref.add(new StringRefAddr(PASSWORD_PROPERTY, password));
    ref.add(new StringRefAddr(URL_PROPERTY, getUrl()));
    return ref;
  }

  // ---------------------------------
  // --- Properties methods
  // ---------------------------------

  /**
   * Return the description of this Datasource with the Driver version number.
   * 
   * @return Datasource description
   */
  public String getDescription()
  {
    return "Sequoia " + driver.getMajorVersion() + "."
        + driver.getMinorVersion() + " Datasource";
  }

  /**
   * Sets url of the Sequoia controller(s) to connect. The method is used by the
   * "application server" to set the URL (potentially according a deployment
   * descriptor). The url is stored in the {@link #URL_PROPERTY}property.
   * 
   * @param url URL to be used to connect Sequoia controller(s)
   */
  public void setUrl(String url)
  {
    this.url = url;
  }

  /**
   * Sets URL of the Sequoia controller(s) to connect. The method is used by the
   * "application server" to set the URL (potentially according a deployment
   * descriptor). The URL is stored in the "url" property.
   * 
   * @param url URL to be used to connect Sequoia controller(s).
   */
  public void setURL(String url)
  {
    setUrl(url);
  }

  /**
   * Gets url of the Sequoia controller(s) to connect. The URL is stored in the
   * {@link #URL_PROPERTY}property.
   * 
   * @return URL to be used to connect Sequoia controller(s).
   */
  public String getUrl()
  {
    return url;
  }

  /**
   * Gets URL of the Sequoia controller(s) to connect. The URL is stored in the
   * {@link #URL_PROPERTY}property.
   * 
   * @return URL to be used to connect Sequoia controller(s).
   */
  public String getURL()
  {
    return getUrl();
  }

  /**
   * Sets user name to be used to connect the Sequoia controller(s). The method
   * can be used by the "application server" to set the user name (potentially
   * according a deployment descriptor). The user name is stored in the
   * {@link #USER_PROPERTY}property.
   * 
   * @param userName user name to be used to connect Sequoia controller(s).
   */
  public void setUser(String userName)
  {
    user = userName;
  }

  /**
   * Gets user name to be used to connect the Sequoia controller(s). The user
   * name is stored in the {@link #USER_PROPERTY}property.
   * 
   * @return user name to be used to connect Sequoia controller(s).
   */
  public String getUser()
  {
    return user;
  }

  /**
   * Sets password to be used to connect the Sequoia controller(s). The method
   * can be used by the "application server" to set the password (potentially
   * according a deployment descriptor). The password is stored in the
   * {@link #PASSWORD_PROPERTY}property. Note that there is not a
   * <code>getPassword</code> method.
   * 
   * @param pwd password to be used to connect Sequoia controller(s).
   */
  public void setPassword(String pwd)
  {
    password = pwd;
  }

  // ---------------------------------
  // --- Protected methods
  // ---------------------------------
  /**
   * Creates a connection using the specified properties.
   * 
   * @param props connection properties.
   * @throws SQLException if an error occurs.
   * @return a new connection.
   */
  protected java.sql.Connection getConnection(Properties props)
      throws SQLException
  {
    return driver.connect(url, props);
  }

@Override
public Logger getParentLogger() throws SQLFeatureNotSupportedException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public <T> T unwrap(Class<T> iface) throws SQLException {
	// TODO Auto-generated method stub
	return null;
}

@Override
public boolean isWrapperFor(Class<?> iface) throws SQLException {
	// TODO Auto-generated method stub
	return false;
}

}
