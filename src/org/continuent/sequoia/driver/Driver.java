/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Julie Marguerite, Mathieu Peltier, Marek Prochazka, Sara
 * Bouchenak, Jaco Swart.
 */

package org.continuent.sequoia.driver;

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Logger;

import javax.net.SocketFactory;

import org.continuent.sequoia.common.exceptions.AuthenticationException;
import org.continuent.sequoia.common.exceptions.NoMoreControllerException;
import org.continuent.sequoia.common.exceptions.driver.DriverSQLException;
import org.continuent.sequoia.common.exceptions.driver.VirtualDatabaseUnavailableException;
import org.continuent.sequoia.common.net.SSLConfiguration;
import org.continuent.sequoia.common.net.SocketFactoryFactory;
import org.continuent.sequoia.common.protocol.Commands;
import org.continuent.sequoia.common.protocol.Field;
import org.continuent.sequoia.common.protocol.SQLDataSerialization;
import org.continuent.sequoia.common.protocol.TypeTag;
import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;
import org.continuent.sequoia.common.util.Constants;
import org.continuent.sequoia.driver.connectpolicy.AbstractControllerConnectPolicy;

/**
 * Sequoia Driver for client side. This driver is a generic driver that is
 * designed to replace any specific JDBC driver that could be used by a client.
 * The client only has to know the node where the Sequoia controller is running
 * and the database he wants to access (the RDBMS could be PostgreSQL, Oracle,
 * DB2, Sybase, MySQL or whatever, we only need the name of the database and the
 * Sequoia controller will be responsible for finding the RDBMs hosting this
 * database).
 * <p>
 * The Sequoia driver can be loaded from the client with:
 * <code>Class.forName("org.continuent.sequoia.driver.Driver");</code>
 * <p>
 * The URL expected for the use with Sequoia is:
 * <code>jdbc:sequoia://host1:port1,host2:port2/database</code>.
 * <p>
 * At least one host must be specified. If several hosts are given, one is
 * picked up randomly from the list. If the currently selected controller fails,
 * another one is automatically picked up from the list.
 * <p>
 * Default port number is 25322 if omitted.
 * <p>
 * Those 2 examples are equivalent:
 * 
 * <pre>
 * DriverManager.getConnection(&quot;jdbc:sequoia://localhost:/tpcw&quot;);
 * DriverManager.getConnection(&quot;jdbc:sequoia://localhost:25322/tpcw&quot;);
 * </pre>
 * 
 * <p>
 * Examples using 2 controllers for fault tolerance:
 * 
 * <pre>
 * DriverManager
 *     .getConnection(&quot;jdbc:sequoia://cluster1.continuent.org:25322,cluster2.continuent.org:25322/tpcw&quot;);
 * DriverManager
 *     .getConnection(&quot;jdbc:sequoia://localhost:25322,remote.continuent.org:25322/tpcw&quot;);
 * DriverManager
 *     .getConnection(&quot;jdbc:sequoia://smpnode.com:25322,smpnode.com:1098/tpcw&quot;);
 * </pre>
 * 
 * <p>
 * The driver accepts a number of options that starts after a ? sign and are
 * separated by an & sign. Each option is a name=value pair. Example:
 * jdbc:sequoia://host/db?option1=value1;option2=value2.
 * <p>
 * Currently supported options are:
 * 
 * <pre>
 * user: user login
 * password: user password
 * escapeBackslash: set this to true to escape backslashes when performing escape processing of PreparedStatements
 * escapeSingleQuote: set this to true to escape single quotes (') when performing escape processing of PreparedStatements
 * escapeCharacter: use this character to prepend and append to the values when performing escape processing of PreparedStatements
 * connectionPooling: set this to false if you do not want the driver to perform transparent connection pooling
 * preferredController: defines the strategy to use to choose a preferred controller to connect to
 *  - jdbc:sequoia://node1,node2,node3/myDB?preferredController=ordered 
 *      Always connect to node1, and if not available then try to node2 and
 *      finally if none are available try node3.
 *  - jdbc:sequoia://node1,node2,node3/myDB?preferredController=random
 *      Pickup a controller node randomly (default strategy)
 *  - jdbc:sequoia://node1,node2:25343,node3/myDB?preferredController=node2:25343,node3 
 *      Round-robin between node2 and node3, fallback to node1 if none of node2
 *      and node3 is available.
 *  - jdbc:sequoia://node1,node2,node3/myDB?preferredController=roundRobin
 *      Round robin starting with first node in URL.
 * pingDelayInMs Interval in milliseconds between two pings of a controller. The
 * default is 1000 (1 second).
 * controllerTimeoutInMs timeout in milliseconds after which a controller is
 * considered as dead if it did not respond to pings. Default is 25000 (25
 * seconds).
 * persistentConnection: defines if a connection should remain persistent on 
 *   cluster backends between connection opening and closing (bypasses any 
 *   connection pooling and preserve all information relative to the connection
 *   context. Default is false.
 * retrieveSQLWarnings: set this to true if you want the controller to retrieve
 * SQL warnings. Default is false, which means that (Connection|Statement|ResultSet).getWarnings()
 * will always return null.
 * allowCommitWithAutoCommit: When set to true, trying to call commit/rollback
 *   on a connection in autoCommit will not throw an exception. If set to false
 *   (default) an SQLException will be thrown when commit is called on a 
 *   connection in autoCommit mode.
 * alwaysGetGeneratedKeys: when set to true, always fetch generated keys even if 
 *   not requested with Statement.RETURN_GENERATED_KEYS.
 * </pre>
 * 
 * <p>
 * This original code has been inspired from the PostgreSQL JDBC driver by Peter
 * T. Mount <peter@retep.org.uk>and the MM MySQL JDBC Drivers from Mark Matthews
 * <mmatthew@worldserver.com>.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Marek.Prochazka@inrialpes.fr">Marek Prochazka </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:jaco.swart@iblocks.co.uk">Jaco Swart </a>
 * @author <a href="mailto:Gilles.Rayrat@continuent.com">Gilles Rayrat </a>
 * @version 1.0
 */

public class Driver implements java.sql.Driver
{
  /** Sequoia URL header. */
  protected String               sequoiaUrlHeader                                  = "jdbc:sequoia://";

  /** Sequoia URL header length. */
  protected int                  sequoiaUrlHeaderLength                            = sequoiaUrlHeader
                                                                                       .length();
  /**
   * Default interval in milliseconds between two pings of a controller. The
   * default is 1000 (1 second)
   */
  public static final int        DEFAULT_PING_DELAY_IN_MS                          = 1000;
  /**
   * Default timeout in milliseconds after which a controller is considered as
   * dead if it did not respond to pings. Default is 25000 (25 seconds).
   */
  public static final int        DEFAULT_CONTROLLER_TIMEOUT_IN_MS                  = 25000;

  /**
   * List of driver properties initialized in the static class initializer
   * <p>
   * !!! Static intializer needs to be udpated when new properties are added !!!
   */
  protected static ArrayList     driverPropertiesNames;

  /** Sequoia driver property name (if you add one, read driverProperties above). */
  protected static final String  HOST_PROPERTY                                     = "HOST";
  protected static final String  PORT_PROPERTY                                     = "PORT";
  protected static final String  DATABASE_PROPERTY                                 = "DATABASE";
  protected static final String  USER_PROPERTY                                     = "user";
  protected static final String  PASSWORD_PROPERTY                                 = "password";

  protected static final String  ESCAPE_BACKSLASH_PROPERTY                         = "escapeBackslash";
  protected static final String  ESCAPE_SINGLE_QUOTE_PROPERTY                      = "escapeSingleQuote";
  protected static final String  ESCAPE_CHARACTER_PROPERTY                         = "escapeCharacter";
  protected static final String  CONNECTION_POOLING_PROPERTY                       = "connectionPooling";
  protected static final String  PREFERRED_CONTROLLER_PROPERTY                     = "preferredController";
  protected static final String  PING_DELAY_IN_MS_PROPERTY                         = "pingDelayInMs";
  protected static final String  CONTROLLER_TIMEOUT_IN_MS_PROPERTY                 = "controllerTimeoutInMs";
  protected static final String  DEBUG_PROPERTY                                    = "debugLevel";
  protected static final String  PERSISTENT_CONNECTION_PROPERTY                    = "persistentConnection";
  protected static final String  RETRIEVE_SQL_WARNINGS_PROPERTY                    = "retrieveSQLWarnings";
  protected static final String  ALLOW_COMMIT_WITH_AUTOCOMMIT_PROPERTY             = "allowCommitWithAutoCommit";
  protected static final String  ALWAYS_RETRIEVE_GENERATED_KEYS_PROPERTY           = "alwaysGetGeneratedKeys";
  protected static final String  DEFAULT_FETCH_SIZE_PROPERTY                       = "defaultFetchSize";

  /** Sequoia driver property description. */
  private static final String    HOST_PROPERTY_DESCRIPTION                         = "Hostname of Sequoia controller";
  private static final String    PORT_PROPERTY_DESCRIPTION                         = "Port number of Sequoia controller";
  private static final String    DATABASE_PROPERTY_DESCRIPTION                     = "Database name";
  private static final String    USER_PROPERTY_DESCRIPTION                         = "Username to authenticate as";
  private static final String    PASSWORD_PROPERTY_DESCRIPTION                     = "Password to use for authentication";
  private static final String    ESCAPE_BACKSLASH_PROPERTY_DESCRIPTION             = "Set this to true to escape backslashes when performing escape processing of PreparedStatements";
  private static final String    ESCAPE_SINGLE_QUOTE_PROPERTY_DESCRIPTION          = "Set this to true to escape single quotes (') when performing escape processing of PreparedStatements";
  private static final String    ESCAPE_CHARACTER_PROPERTY_DESCRIPTION             = "Use this character to prepend and append to the values when performing escape processing of PreparedStatements";
  protected static final String  CONNECTION_POOLING_PROPERTY_DESCRIPTION           = "Set this to false if you do not want the driver to perform transparent connection pooling";
  protected static final String  PREFERRED_CONTROLLER_PROPERTY_DESCRIPTION         = "Defines the strategy to use to choose a preferred controller to connect to";
  protected static final String  PING_DELAY_IN_MS_DESCRIPTION                      = "Interval in milliseconds between two pings of a controller";
  protected static final String  CONTROLLER_TIMEOUT_IN_MS_DESCRIPTION              = "Timeout in milliseconds after which a controller is considered as dead if it did not respond to pings";
  protected static final String  DEBUG_PROPERTY_DESCRIPTION                        = "Debug level that can be set to 'debug', 'info' or 'off'";
  protected static final String  PERSISTENT_CONNECTION_PROPERTY_DESCRIPTION        = "Defines if a connection in autoCommit mode should remain persistent on cluster backends";
  protected static final String  RETRIEVE_SQL_WARNINGS_PROPERTY_DESCRIPTION        = "Set this to true to allow retrieval of SQL warnings. A value set to false will make *.getWarnings() always return null";
  protected static final String  ALLOW_COMMIT_WITH_AUTOCOMMIT_PROPERTY_DESCRIPTION = "Indicates whether or not commit can be called while autocommit is enabled";
  protected static final String  ALWAYS_RETRIEVE_GENERATED_KEYS_DESCRIPTION        = "Indicates whether or not every INSERT should make generated keys available, if any";
  protected static final String  DEFAULT_FETCH_SIZE_PROPERTY_DESCRIPTION           = "Set this to a non-zero value to override fetch size default setting of 0";

  /** Driver major version. */
  public static final int        MAJOR_VERSION                                     = Constants
                                                                                       .getMajorVersion();

  /** Driver minor version. */
  public static final int        MINOR_VERSION                                     = Constants
                                                                                       .getMinorVersion();
  /** Get the sequoia.ssl.enabled system property to check if SSL is enabled */
  protected static final boolean SSL_ENABLED_PROPERTY                              = "true"
                                                                                       .equalsIgnoreCase(System
                                                                                           .getProperty("sequoia.ssl.enabled"));

  /**
   * Cache of parsed URLs used to connect to the controller. It always grows and
   * is never purged: we don't yet handle the unlikely case of a long-lived
   * driver using zillions of different URLs. Warning: this caches urls BOTH
   * with AND without properties.
   * <p>
   * Hashmap is URL=> <code>SequoiaUrl</code>
   */
  private HashMap                parsedUrlsCache                                   = new HashMap();

  /** List of connections that are ready to be closed. */
  protected ArrayList            pendingConnectionClosing                          = new ArrayList();
  protected boolean              connectionClosingThreadisAlive                    = false;

  private JDBCRegExp             jdbcRegExp                                        = new SequoiaJDBCRegExp();

  // optimization: non-lazy class loading
  private static Class           c1                                                = Field.class;
  private static Class           c2                                                = SQLDataSerialization.class;
  private static Class           c3                                                = TypeTag.class;
  private static Class           c4                                                = DriverResultSet.class;

  // The static initializer registers ourselves with the DriverManager
  // and try to bind the Sequoia Controller
  static
  {
    // Register with the DriverManager (see JDBC API Tutorial and Reference,
    // Second Edition p. 941)
    try
    {
      java.sql.DriverManager.registerDriver(new Driver());
    }
    catch (SQLException e)
    {
      throw new RuntimeException("Unable to register Sequoia driver");
    }

    // Build the static list of driver properties
    driverPropertiesNames = new ArrayList();
    driverPropertiesNames.add(Driver.HOST_PROPERTY);
    driverPropertiesNames.add(Driver.PORT_PROPERTY);
    driverPropertiesNames.add(Driver.DATABASE_PROPERTY);
    driverPropertiesNames.add(Driver.USER_PROPERTY);
    driverPropertiesNames.add(Driver.PASSWORD_PROPERTY);

    driverPropertiesNames.add(Driver.ESCAPE_BACKSLASH_PROPERTY);
    driverPropertiesNames.add(Driver.ESCAPE_SINGLE_QUOTE_PROPERTY);
    driverPropertiesNames.add(Driver.ESCAPE_CHARACTER_PROPERTY);
    driverPropertiesNames.add(Driver.CONNECTION_POOLING_PROPERTY);
    driverPropertiesNames.add(Driver.PREFERRED_CONTROLLER_PROPERTY);
    driverPropertiesNames.add(Driver.PING_DELAY_IN_MS_PROPERTY);
    driverPropertiesNames.add(Driver.CONTROLLER_TIMEOUT_IN_MS_PROPERTY);
    driverPropertiesNames.add(Driver.DEBUG_PROPERTY);
    driverPropertiesNames.add(Driver.PERSISTENT_CONNECTION_PROPERTY);
    driverPropertiesNames.add(Driver.ALLOW_COMMIT_WITH_AUTOCOMMIT_PROPERTY);
    driverPropertiesNames.add(Driver.ALWAYS_RETRIEVE_GENERATED_KEYS_PROPERTY);
    driverPropertiesNames.add(Driver.DEFAULT_FETCH_SIZE_PROPERTY);
  }

  /**
   * Creates a new <code>Driver</code>. Only an actual instance can be
   * registered to the DriverManager, see call to
   * {@link java.sql.DriverManager#registerDriver(java.sql.Driver)} in static
   * initializer block just above.
   */
  public Driver()
  {
    // see javadoc above
  }

  /**
   * Asks the Sequoia controller if the requested database can be accessed with
   * the provided user name and password. If the Sequoia controller can't access
   * the requested database, an <code>SQLException</code> is thrown, else a
   * "fake" <code>Connection</code> is returned to the user so that he or she
   * can create <code>Statements</code>.
   * 
   * @param url the URL of the Sequoia controller to which to connect.
   * @param clientProperties a list of arbitrary string tag/value pairs as
   *          connection arguments (usually at least a "user" and "password").
   *          In case of conflict, this list overrides settings from the url;
   *          see SEQUOIA-105
   * @return a <code>Connection</code> object that represents a connection to
   *         the database through the Sequoia Controller.
   * @exception SQLException if an error occurs.
   */
  public java.sql.Connection connect(String url, Properties clientProperties)
      throws SQLException, VirtualDatabaseUnavailableException,
      NoMoreControllerException
  {
    if (url == null)
      throw new SQLException("Invalid null URL in connect");

    /**
     * If the URL is for another driver we must return null according to the
     * javadoc of the implemented interface. This is likely to happen as the
     * DriverManager tries every driver until one succeeds.
     */
    if (!url.startsWith(sequoiaUrlHeader))
      return null;

    Properties filteredProperties = filterProperties(clientProperties);

    String urlCacheKey = url + filteredProperties.toString();

    // In the common case, we do not synchronize
    SequoiaUrl sequoiaUrl = (SequoiaUrl) parsedUrlsCache.get(urlCacheKey);
    if (sequoiaUrl == null) // Not in the cache
    {
      synchronized (this)
      {
        // Recheck here in case someone updated before we entered the
        // synchronized block
        sequoiaUrl = (SequoiaUrl) parsedUrlsCache.get(urlCacheKey);
        if (sequoiaUrl == null)
        {
          sequoiaUrl = new SequoiaUrl(this, url, filteredProperties);
          parsedUrlsCache.put(urlCacheKey, sequoiaUrl);
        }
      }
    }

    ControllerInfo controller = null;
    try
    {
      Connection newConn = getConnectionToNewController(sequoiaUrl);
      if (newConn != null)
        controller = newConn.getControllerInfo();
      return newConn;
    }
    // Exceptions thrown directly to client:
    // VirtualDatabaseUnavailableException
    // NoMoreControllerException
    catch (AuthenticationException e)
    {
      throw (SQLException) new SQLException(e.getMessage()).initCause(e);
    }
    catch (GeneralSecurityException e)
    {
      e.printStackTrace();
      throw (SQLException) new SQLException(
          "Fatal General Security Exception received while trying to connect")
          .initCause(e);
    }
    catch (RuntimeException e)
    {
      e.printStackTrace();
      throw (SQLException) new SQLException(
          "Fatal Runtime Exception received while trying to connect to" + e
              + ")").initCause(e);
    }
  }

  /**
   * This function should be used to establish a new connection to another
   * controller in the list. It monitors "virtualdatabase not available"
   * failures by calling {@link #connectToNextController(SequoiaUrl)} only a
   * limited number of times (number = number of available controllers).
   * 
   * @param sequoiaUrl Sequoia URL object including parameters
   * @return connection to the next available controller
   * @throws AuthenticationException if the authentication has failed
   * @throws NoMoreControllerException if all controllers in the list are down
   * @throws DriverSQLException if the connection cannot be established with the
   *           controller
   * @throws VirtualDatabaseUnavailableException if none of the remaining
   *           controllers has the desired vdb
   */
  protected Connection getConnectionToNewController(SequoiaUrl sequoiaUrl)
      throws AuthenticationException, NoMoreControllerException,
      VirtualDatabaseUnavailableException, GeneralSecurityException
  {
    // This will count the number of controllers left to connect to upon
    // VDB not available exceptions
    // No need to synchronize with getController(): at worth, we will not try
    // one controller that just reappeared, or we will try two time the same
    // controller. This is harmless, we just want to prevent endless retry loops
    int numberOfCtrlsLeft = sequoiaUrl.getControllerConnectPolicy()
        .numberOfAliveControllers();
    while (numberOfCtrlsLeft > 0)
    {
      try
      {
        return connectToNextController(sequoiaUrl);
      }
      catch (VirtualDatabaseUnavailableException vdbue)
      {
        numberOfCtrlsLeft--;
      }
    }
    // at this point, we have tried all controllers
    throw new VirtualDatabaseUnavailableException("Virtual database "
        + sequoiaUrl.getDatabaseName() + " not found on any of the controllers");
  }

  /**
   * Connects with the specified parameters to the next controller (next
   * according to the current connection policy).<br>
   * Retrieves a new controller by asking the connect policy, creates and
   * connects a socket to this new controller, and registers the new socket to
   * the policy. Then, tries to authenticate to the controller. Finally, creates
   * the connection with the given parameters.<br>
   * Upon IOException during connection, this function will mark the new
   * controller as failing (by calling
   * {@link AbstractControllerConnectPolicy#forceControllerDown(ControllerInfo)}
   * and try the next controller until NoMoreControllerException (which will be
   * forwarded to caller)<br>
   * If a VirtualDatabaseUnavailableException is thrown, the controller's vdb
   * will be considered as down by calling
   * {@link AbstractControllerConnectPolicy#setVdbDownOnController(ControllerInfo)}
   * and the exception will be forwarded to caller Note that newly connected
   * controller can be retrieved by calling
   * {@link Connection#getControllerInfo()}
   * 
   * @param sequoiaUrl Sequoia URL object including parameters
   * @return connection to the next available controller
   * @throws VirtualDatabaseUnavailableException if the given vdb is not
   *           available on the new controller we are trying to connect to
   * @throws AuthenticationException if the authentication has failed or the
   *           database name is wrong
   * @throws NoMoreControllerException if all controllers in the list are down
   */
  private Connection connectToNextController(SequoiaUrl sequoiaUrl)
      throws AuthenticationException, NoMoreControllerException,
      VirtualDatabaseUnavailableException, GeneralSecurityException
  {
    // TODO: methods should be extracted to reduce the size of this one

    ControllerInfo newController = null;

    // Check the user
    String user = (String) sequoiaUrl.getParameters().get(USER_PROPERTY);
    if (user == null || user.equals(""))
      throw new AuthenticationException("Invalid user name in connect");
    // Check the password
    String password = (String) sequoiaUrl.getParameters()
        .get(PASSWORD_PROPERTY);
    if (password == null)
      password = "";

    // Let's go for a new connection

    // This is actually a connection constructor,
    // we should try to move most of it below.
    AbstractControllerConnectPolicy policy = sequoiaUrl
        .getControllerConnectPolicy();
    try
    {
      Socket socket = null;
      // This synchronized block prevents other connections from doing a
      // forceControllerDown between our getController and registerSocket
      synchronized (policy)
      {
        // Choose a controller according to the policy
        newController = policy.getController();

        // Try to retrieve a reusable connection
        if (!"false".equals(sequoiaUrl.getParameters().get(
            CONNECTION_POOLING_PROPERTY)))
        { // Connection pooling is activated
          Connection c = retrievePendingClosingConnection(sequoiaUrl,
              newController, user, password);
          if (c != null)
          {
            if (sequoiaUrl.isDebugEnabled())
              System.out.println("Reusing connection from pool");
            return c; // Re-use this one
          }
        }

        // Create the socket
        // SSL enabled ?
        if (SSL_ENABLED_PROPERTY)
        {
          SocketFactory sslFact = SocketFactoryFactory
              .createFactory(SSLConfiguration.getDefaultConfig());
          socket = sslFact.createSocket();
        }
        else
        {
          // no ssl - we use ordinary socket
          socket = new Socket();
        }
        // Register asap the socket to the policy callback so it can kill it
        // (even when connecting)
        // synchronized is reentrant => we can call a policy synchronized method
        // inside this synchronized(policy) block
        policy.registerSocket(newController, socket);

      }
      socket.connect(newController);

      // Disable Nagle algorithm else small messages are not sent
      // (at least under Linux) even if we flush the output stream.
      socket.setTcpNoDelay(true);

      if (sequoiaUrl.isInfoEnabled())
        System.out.println("Authenticating with controller " + newController);

      DriverBufferedOutputStream out = new DriverBufferedOutputStream(socket,
          sequoiaUrl.getDebugLevel());
      // Send protocol version and database name
      out.writeInt(Commands.ProtocolVersion);
      out.writeLongUTF(sequoiaUrl.getDatabaseName());
      out.flush();

      // Send user information
      out.writeLongUTF(user);
      out.writeLongUTF(password);
      out.flush();

      // Create input stream only here else it will block
      DriverBufferedInputStream in = new DriverBufferedInputStream(socket,
          sequoiaUrl.getDebugLevel());

      return new Connection(this, socket, in, out, sequoiaUrl, newController,
          user, password);
    } // try connect to the controller/connection constructor
    catch (IOException ioe)
    {
      policy.forceControllerDown(newController);
      return connectToNextController(sequoiaUrl);
    }
    catch (VirtualDatabaseUnavailableException vdbue)
    {
      // mark vdb as down. Caller will retry if appropriate
      policy.setVdbDownOnController(newController);
      throw vdbue;
    }
    // Other exceptions are forwarded to caller
  }

  /**
   * This extracts from the (too complex) client Properties a leaner and cleaner
   * HashMap with is: - maybe empty but never null, - holding only the keys we
   * are interested in, - its values are guaranteed to be strings, - no complex
   * and hidden layered "defaults". See SEQUOIA-105 and SEQUOIA-440
   * 
   * @param props to filter
   * @return filtered properties
   * @throws SQLException
   */
  protected Properties filterProperties(Properties props)
  {
    Properties filtered = new Properties();

    if (props == null)
      return filtered;

    // extract only the keys we know
    Iterator iter = driverPropertiesNames.iterator();
    while (iter.hasNext())
    {
      String name = (String) iter.next();
      String val = props.getProperty(name);
      if (val == null)
        continue;
      filtered.setProperty(name, val);
    }

    return filtered;
  }

  /**
   * This method is used to implement the transparent connection pooling and try
   * to retrieve a connection that was recently closed to the given controller
   * with the provided login/password information.
   * 
   * @param url Sequoia URL object including parameters
   * @param controllerInfo the controller to connect to
   * @param user user name used for connection
   * @param password password used for connection
   * @return a connection that could be reuse or null if none
   */
  private Connection retrievePendingClosingConnection(SequoiaUrl url,
      ControllerInfo controllerInfo, String user, String password)
  {
    // Check if there is a connection that is about to be closed that could
    // be reused. We take the bet that if a connection has been released by
    // a client, in the general case, it will reuse the same connection.
    // As we need to keep the work in the synchronized block as minimal as
    // possible, we have to extract the string comparison
    // (url,name,password,controller)from the sync block. This way, we cannot
    // just read/compare/take the connection without synchronizing the whole
    // thing. A solution is to systematically extract the first available
    // connection in the sync block, and do the checkings outside the block. If
    // we fail, we re-sync to put the connection back but in practice it is
    // almost always a success and we don't really care to pay this extra cost
    // once in a while.
    try
    {
      Connection c;
      synchronized (pendingConnectionClosing)
      {
        // Take the last one to prevent shifting all elements
        c = (Connection) pendingConnectionClosing
            .remove(pendingConnectionClosing.size() - 1);
      }
      if (url.equals(c.getSequoiaUrl()) // This compares all the Connection
          // properties
          && controllerInfo.equals(c.getControllerInfo())
          && user.equals(c.getUserName()) && password.equals(c.getPassword()))
      { // Great! Take this one.
        c.isClosed = false;
        return c;
      }
      else
      {
        // Put this connection back, it is not good for us
        synchronized (pendingConnectionClosing)
        {
          pendingConnectionClosing.add(c);
          // Now scan the list for a suitable connection
          for (Iterator iter = pendingConnectionClosing.iterator(); iter
              .hasNext();)
          {
            Connection conn = (Connection) iter.next();
            if (url.equals(conn.getSequoiaUrl()) // This compares all the
                // Connection
                // properties
                && controllerInfo.equals(conn.getControllerInfo())
                && user.equals(conn.getUserName())
                && password.equals(conn.getPassword()))
            { // Great! Take this one.
              iter.remove();
              conn.isClosed = false;
              return conn;
            }
          }
        }
      }
    }
    catch (IndexOutOfBoundsException ignore)
    {
      // No connection available
    }
    return null;
  }

  /**
   * Tests if the URL is understood by the driver. Simply tries to construct a
   * parsed URLs and catch the failure.
   * 
   * @param url the JDBC URL.
   * @return <code>true</code> if the URL is correct, otherwise an exception
   *         with extensive error message is thrown.
   * @exception SQLException if the URL is incorrect an explicit error message
   *              is given.
   */
  public synchronized boolean acceptsURL(String url) throws SQLException
  {
    if (url == null)
      return false;

    try
    {
      SequoiaUrl sequoiaUrl = (SequoiaUrl) parsedUrlsCache.get(url);
      if (sequoiaUrl == null) // Not in the cache
      {
        synchronized (this)
        {
          // Recheck here in case someone updated before we entered the
          // synchronized block
          sequoiaUrl = (SequoiaUrl) parsedUrlsCache.get(url);
          if (sequoiaUrl == null)
          {
            // URL parsed here.
            sequoiaUrl = new SequoiaUrl(this, url, new Properties());
            // Update the cache anyway that can be useful later on
            parsedUrlsCache.put(url, sequoiaUrl);
          }
        }
      }
      return true;
    }
    catch (SQLException e)
    {
      return false;
    }
  }

  /**
   * Change the database name in the provided URL.
   * 
   * @param url URL to parse
   * @param newDbName new database name to insert
   * @return the updated URL
   * @throws SQLException if an error occurs while parsing the url
   */
  public String changeDatabaseName(String url, String newDbName)
      throws SQLException
  {
    StringBuffer sb = new StringBuffer();
    sb.append(sequoiaUrlHeader);

    SequoiaUrl sequoiaUrl = (SequoiaUrl) parsedUrlsCache.get(url);
    if (sequoiaUrl == null)
    {
      acceptsURL(url); // parse and put in cache
      sequoiaUrl = (SequoiaUrl) parsedUrlsCache.get(url);
    }

    // append controller list
    ControllerInfo[] controllerList = sequoiaUrl.getControllerList();
    for (int i = 0; i < controllerList.length; i++)
    {
      if (i == 0)
        sb.append(controllerList[i].toString());
      else
        sb.append("," + controllerList[i].toString());
    }
    sb.append("/" + newDbName);

    // append parameters parsed above
    HashMap params = sequoiaUrl.getParameters();
    if (params != null)
    {
      Iterator paramsKeys = params.keySet().iterator();
      String element = null;
      while (paramsKeys.hasNext())
      {
        if (element == null)
          sb.append("?");
        else
          sb.append("&");
        element = (String) paramsKeys.next();
        sb.append(element + "=" + params.get(paramsKeys));
      }
    }
    return sb.toString();
  }

  /**
   * Get the default transaction isolation level to use for this driver.
   * 
   * @return java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
   */
  protected int getDefaultTransactionIsolationLevel()
  {
    return java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
  }

  /**
   * Returns the SequoiaJDBCRegExp value.
   * 
   * @return Returns the SequoiaJDBCRegExp.
   */
  public JDBCRegExp getJDBCRegExp()
  {
    return jdbcRegExp;
  }

  /**
   * This method is intended to allow a generic GUI tool to discover what
   * properties it should prompt a human for in order to get enough information
   * to connect to a database.
   * <p>
   * The only properties supported by Sequoia are:
   * <ul>
   * <li>HOST_PROPERTY</li>
   * <li>PORT_PROPERTY</li>
   * <li>DATABASE_PROPERTY</li>
   * <li>USER_PROPERTY</li>
   * <li>PASSWORD_PROPERTY</li>
   * <li>ESCAPE_BACKSLASH_PROPERTY</li>
   * <li>ESCAPE_SINGLE_QUOTE_PROPERTY</li>
   * <li>ESCAPE_CHARACTER_PROPERTY</li>
   * <li>CONNECTION_POOLING_PROPERTY</li>
   * <li>PREFERRED_CONTROLLER_PROPERTY</li>
   * <li>PING_DELAY_IN_MS_PROPERTY</li>
   * <li>CONTROLLER_TIMEOUT_IN_MS_PROPERTY</li>
   * <li>DEBUG_PROPERTY</li>
   * <li>PERSISTENT_CONNECTION_PROPERTY</li>
   * <li>RETRIEVE_SQL_WARNINGS_PROPERTY</li>
   * <li>ALLOW_COMMIT_WITH_AUTOCOMMIT_PROPERTY</li>
   * <li>DEFAULT_FETCH_SIZE_PROPERTY</li>
   * </ul>
   * 
   * @param url the URL of the database to connect to
   * @param info a proposed list of tag/value pairs that will be sent on connect
   *          open.
   * @return an array of <code>DriverPropertyInfo</code> objects describing
   *         possible properties. This array may be an empty array if no
   *         properties are required (note that this override any setting that
   *         might be set in the URL).
   * @exception SQLException if the url is not valid
   * @see java.sql.Driver#getPropertyInfo
   */
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException
  {
    if (!acceptsURL(url))
      throw new SQLException("Invalid url " + url);

    SequoiaUrl sequoiaUrl;
    synchronized (this)
    {
      sequoiaUrl = (SequoiaUrl) parsedUrlsCache.get(url);
      if (sequoiaUrl == null)
        throw new SQLException("Error while retrieving URL information");
    }
    HashMap params = sequoiaUrl.getParameters();

    String host = info.getProperty(HOST_PROPERTY);
    if (host == null)
    {
      ControllerInfo[] controllerList = sequoiaUrl.getControllerList();
      for (int i = 0; i < controllerList.length; i++)
      {
        ControllerInfo controller = controllerList[i];
        if (i == 0)
          host = controller.toString();
        else
          host += "," + controller.toString();
      }
    }
    DriverPropertyInfo hostProp = new DriverPropertyInfo(HOST_PROPERTY, host);
    hostProp.required = true;
    hostProp.description = HOST_PROPERTY_DESCRIPTION;

    DriverPropertyInfo portProp = new DriverPropertyInfo(PORT_PROPERTY, info
        .getProperty(PORT_PROPERTY, Integer
            .toString(SequoiaUrl.DEFAULT_CONTROLLER_PORT)));
    portProp.required = false;
    portProp.description = PORT_PROPERTY_DESCRIPTION;

    String database = info.getProperty(DATABASE_PROPERTY);
    if (database == null)
      database = sequoiaUrl.getDatabaseName();
    DriverPropertyInfo databaseProp = new DriverPropertyInfo(DATABASE_PROPERTY,
        database);
    databaseProp.required = true;
    databaseProp.description = DATABASE_PROPERTY_DESCRIPTION;

    String user = info.getProperty(USER_PROPERTY);
    if (user == null)
      user = (String) params.get(USER_PROPERTY);
    DriverPropertyInfo userProp = new DriverPropertyInfo(USER_PROPERTY, user);
    userProp.required = true;
    userProp.description = USER_PROPERTY_DESCRIPTION;

    String password = info.getProperty(PASSWORD_PROPERTY);
    if (password == null)
      password = (String) params.get(PASSWORD_PROPERTY);
    DriverPropertyInfo passwordProp = new DriverPropertyInfo(PASSWORD_PROPERTY,
        password);
    passwordProp.required = true;
    passwordProp.description = PASSWORD_PROPERTY_DESCRIPTION;

    String escapeChar = info.getProperty(ESCAPE_CHARACTER_PROPERTY);
    if (escapeChar == null)
      escapeChar = (String) params.get(ESCAPE_CHARACTER_PROPERTY);
    DriverPropertyInfo escapeCharProp = new DriverPropertyInfo(
        ESCAPE_CHARACTER_PROPERTY, escapeChar);
    escapeCharProp.required = false;
    escapeCharProp.description = ESCAPE_CHARACTER_PROPERTY_DESCRIPTION;

    String escapeBackslash = info.getProperty(ESCAPE_BACKSLASH_PROPERTY);
    if (escapeBackslash == null)
      escapeBackslash = (String) params.get(ESCAPE_BACKSLASH_PROPERTY);
    DriverPropertyInfo escapeBackProp = new DriverPropertyInfo(
        ESCAPE_BACKSLASH_PROPERTY, escapeBackslash);
    escapeBackProp.required = false;
    escapeBackProp.description = ESCAPE_BACKSLASH_PROPERTY_DESCRIPTION;

    String escapeSingleQuote = info.getProperty(ESCAPE_SINGLE_QUOTE_PROPERTY);
    if (escapeSingleQuote == null)
      escapeSingleQuote = (String) params.get(ESCAPE_SINGLE_QUOTE_PROPERTY);
    DriverPropertyInfo escapeSingleProp = new DriverPropertyInfo(
        ESCAPE_SINGLE_QUOTE_PROPERTY, escapeSingleQuote);
    escapeSingleProp.required = false;
    escapeSingleProp.description = ESCAPE_SINGLE_QUOTE_PROPERTY_DESCRIPTION;

    String connectionPooling = info.getProperty(CONNECTION_POOLING_PROPERTY);
    if (connectionPooling == null)
      connectionPooling = (String) params.get(CONNECTION_POOLING_PROPERTY);
    DriverPropertyInfo connectionPoolingProp = new DriverPropertyInfo(
        CONNECTION_POOLING_PROPERTY, connectionPooling);
    connectionPoolingProp.required = false;
    connectionPoolingProp.description = CONNECTION_POOLING_PROPERTY_DESCRIPTION;

    String preferredController = info
        .getProperty(PREFERRED_CONTROLLER_PROPERTY);
    if (preferredController == null)
      preferredController = (String) params.get(PREFERRED_CONTROLLER_PROPERTY);
    DriverPropertyInfo preferredControllerProp = new DriverPropertyInfo(
        PREFERRED_CONTROLLER_PROPERTY, preferredController);
    preferredControllerProp.required = false;
    preferredControllerProp.description = PREFERRED_CONTROLLER_PROPERTY_DESCRIPTION;

    String pingDelayInMs = info.getProperty(PING_DELAY_IN_MS_PROPERTY);
    if (pingDelayInMs == null)
      pingDelayInMs = (String) params.get(PING_DELAY_IN_MS_PROPERTY);
    DriverPropertyInfo pingDelayInMsProp = new DriverPropertyInfo(
        PING_DELAY_IN_MS_PROPERTY, pingDelayInMs);
    pingDelayInMsProp.required = false;
    pingDelayInMsProp.description = PING_DELAY_IN_MS_DESCRIPTION;

    String controllerTimeoutInMs = info
        .getProperty(CONTROLLER_TIMEOUT_IN_MS_PROPERTY);
    if (controllerTimeoutInMs == null)
      controllerTimeoutInMs = (String) params
          .get(CONTROLLER_TIMEOUT_IN_MS_PROPERTY);
    DriverPropertyInfo controllerTimeoutInMsProp = new DriverPropertyInfo(
        CONTROLLER_TIMEOUT_IN_MS_PROPERTY, controllerTimeoutInMs);
    controllerTimeoutInMsProp.required = false;
    controllerTimeoutInMsProp.description = CONTROLLER_TIMEOUT_IN_MS_DESCRIPTION;

    String persistentConnection = info
        .getProperty(PERSISTENT_CONNECTION_PROPERTY);
    if (persistentConnection == null)
      persistentConnection = (String) params
          .get(PERSISTENT_CONNECTION_PROPERTY);
    DriverPropertyInfo persistentConnectionProp = new DriverPropertyInfo(
        PERSISTENT_CONNECTION_PROPERTY, persistentConnection);
    persistentConnectionProp.required = false;
    persistentConnectionProp.description = PERSISTENT_CONNECTION_PROPERTY_DESCRIPTION;

    String retrieveSQLWarnings = info
        .getProperty(RETRIEVE_SQL_WARNINGS_PROPERTY);
    if (retrieveSQLWarnings == null)
      retrieveSQLWarnings = (String) params.get(RETRIEVE_SQL_WARNINGS_PROPERTY);
    DriverPropertyInfo retrieveSQLWarningsProp = new DriverPropertyInfo(
        RETRIEVE_SQL_WARNINGS_PROPERTY, retrieveSQLWarnings);
    retrieveSQLWarningsProp.required = false;
    retrieveSQLWarningsProp.description = RETRIEVE_SQL_WARNINGS_PROPERTY_DESCRIPTION;

    String getGeneratedKeys = info
        .getProperty(ALWAYS_RETRIEVE_GENERATED_KEYS_PROPERTY);
    if (getGeneratedKeys == null)
      getGeneratedKeys = (String) params
          .get(ALWAYS_RETRIEVE_GENERATED_KEYS_PROPERTY);
    DriverPropertyInfo getGeneratedKeysProp = new DriverPropertyInfo(
        ALWAYS_RETRIEVE_GENERATED_KEYS_PROPERTY, getGeneratedKeys);
    getGeneratedKeysProp.required = false;
    getGeneratedKeysProp.description = ALWAYS_RETRIEVE_GENERATED_KEYS_DESCRIPTION;

    String defaultFetchSize = info.getProperty(DEFAULT_FETCH_SIZE_PROPERTY);
    if (defaultFetchSize == null)
      defaultFetchSize = (String) params.get(DEFAULT_FETCH_SIZE_PROPERTY);
    DriverPropertyInfo defaultFetchSizeProp = new DriverPropertyInfo(
        DEFAULT_FETCH_SIZE_PROPERTY, defaultFetchSize);
    defaultFetchSizeProp.required = false;
    defaultFetchSizeProp.description = DEFAULT_FETCH_SIZE_PROPERTY_DESCRIPTION;

    return new DriverPropertyInfo[]{hostProp, portProp, databaseProp, userProp,
        passwordProp, escapeCharProp, escapeBackProp, escapeSingleProp,
        connectionPoolingProp, preferredControllerProp,
        persistentConnectionProp, retrieveSQLWarningsProp,
        getGeneratedKeysProp, defaultFetchSizeProp};
  }

  /**
   * Gets the driver's major version number
   * 
   * @return the driver's major version number
   */
  public int getMajorVersion()
  {
    return MAJOR_VERSION;
  }

  /**
   * Gets the driver's minor version number
   * 
   * @return the driver's minor version number
   */
  public int getMinorVersion()
  {
    return MINOR_VERSION;
  }

  /**
   * Reports whether the driver is a genuine JDBC compliant driver. A driver may
   * only report <code>true</code> here if it passes the JDBC compliance
   * tests, otherwise it is required to return <code>false</code>. JDBC
   * compliance requires full support for the JDBC API and full support for SQL
   * 92 Entry Level. We cannot ensure that the underlying JDBC drivers will be
   * JDBC compliant, so it is safer to return <code>false</code>.
   * 
   * @return always <code>false</code>
   */
  public boolean jdbcCompliant()
  {
    return false;
  }

  /**
   * @return True, escape processing of backslash is ON by default
   */
  public boolean getEscapeBackslash()
  {
    return true;
  }

  /**
   * @return the default escape character
   */
  public String getEscapeChar()
  {
    return "\'";
  }

  /**
   * @return True, escape processing of single quote is ON by default
   */
  public boolean getEscapeSingleQuote()
  {
    return true;
  }

  /**
   * @return True, as connection pooling is activated by default
   */
  public boolean getConnectionPooling()
  {
    return true;
  }

  /**
   * @return False, as connection are not persistent by default.
   */
  public boolean getPersistentConnection()
  {
    return false;
  }

  /**
   * @return False, as retrieval of SQL warnings is disabled by default.
   */
  public boolean getRetrieveSQLWarnings()
  {
    return false;
  }

  /**
   * @return False, as retrieval of generated keys is not forced by default.
   */
  public boolean getRetrieveGeneratedKeys()
  {
    return false;
  }

@Override
public Logger getParentLogger() throws SQLFeatureNotSupportedException {
	// TODO Auto-generated method stub
	return null;
}
}