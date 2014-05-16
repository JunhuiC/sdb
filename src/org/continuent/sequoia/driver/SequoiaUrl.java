/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.driver;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

import org.continuent.sequoia.driver.connectpolicy.AbstractControllerConnectPolicy;
import org.continuent.sequoia.driver.connectpolicy.OrderedConnectPolicy;
import org.continuent.sequoia.driver.connectpolicy.PreferredListConnectPolicy;
import org.continuent.sequoia.driver.connectpolicy.RandomConnectPolicy;
import org.continuent.sequoia.driver.connectpolicy.RoundRobinConnectPolicy;
import org.continuent.sequoia.driver.connectpolicy.SingleConnectPolicy;

/**
 * This class defines a Sequoia url with parsed Metadata and so on. The
 * connection policy is interpreted while reading the URL. We could rename it to
 * ParsedURL.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class SequoiaUrl
{
  private Driver                          driver;
  private String                          url;
  private String                          databaseName;
  private ControllerInfo[]                controllerList;
  /** This includes parameters parsed from the url */
  private final HashMap                   parameters;
  private AbstractControllerConnectPolicy controllerConnectPolicy;

  /**
   * Default port number to connect to the Sequoia controller
   */
  public static final int                 DEFAULT_CONTROLLER_PORT = 25322;

  // Debug information
  private int                             debugLevel;
  /** Most verbose level of debug */
  public static final int                 DEBUG_LEVEL_DEBUG       = 2;
  /** Informational level of debug */
  public static final int                 DEBUG_LEVEL_INFO        = 1;
  /** No debug messages */
  public static final int                 DEBUG_LEVEL_OFF         = 0;

  /**
   * Creates a new <code>SequoiaUrl</code> object, parse it and instantiate
   * the connection creation policy.
   * 
   * @param driver driver used to create the connections
   * @param url the URL to parse
   * @param props optional filtered connection properties. Must not be null.
   * @throws SQLException if an error occurs while parsing the url
   */
  public SequoiaUrl(Driver driver, String url, Properties props)
      throws SQLException
  {
    this(driver, url, props, null);
  }

  private SequoiaUrl(Driver driver, String url, Properties props,
      AbstractControllerConnectPolicy policy) throws SQLException
  {
    this.driver = driver;
    this.url = url;

    parameters = new HashMap();
    // Initialize parameters
    parameters.putAll(props);
    // Parse URL now, will override any parameter defined as a property
    // by the previous line. Fixes SEQUOIA-105.
    parseUrl();

    String debugProperty = (String) parameters.get(Driver.DEBUG_PROPERTY);
    debugLevel = DEBUG_LEVEL_OFF;
    if (debugProperty != null)
    {
      if ("debug".equals(debugProperty))
        debugLevel = DEBUG_LEVEL_DEBUG;
      else if ("info".equals(debugProperty))
        debugLevel = DEBUG_LEVEL_INFO;
    }

    controllerConnectPolicy = (policy != null)
        ? policy
        : createConnectionPolicy();
  }

  /**
   * Returns the debugLevel value.
   * 
   * @return Returns the debugLevel.
   */
  public int getDebugLevel()
  {
    return debugLevel;
  }

  /**
   * Returns true if debugging is set to debug level 'debug'
   * 
   * @return true if debugging is enabled
   */
  public boolean isDebugEnabled()
  {
    return debugLevel == DEBUG_LEVEL_DEBUG;
  }

  /**
   * Returns true if debug level is 'info' or greater
   * 
   * @return true if debug level is 'info' or greater
   */
  public boolean isInfoEnabled()
  {
    return debugLevel >= DEBUG_LEVEL_INFO;
  }

  /**
   * Returns the controllerConnectPolicy value.
   * 
   * @return Returns the controllerConnectPolicy.
   */
  public AbstractControllerConnectPolicy getControllerConnectPolicy()
  {
    return controllerConnectPolicy;
  }

  /**
   * Returns the controllerList value.
   * 
   * @return Returns the controllerList.
   */
  public ControllerInfo[] getControllerList()
  {
    return controllerList;
  }

  /**
   * Returns the database name.
   * 
   * @return Returns the database name.
   */
  public String getDatabaseName()
  {
    return databaseName;
  }

  /**
   * Returns the URL parameters/value in a HashMap (warning this is not a
   * clone). Parameters come from both the parsed URL and the properties
   * argument if any.
   * <p>
   * The HashMap is 'parameter name'=>'value'
   * 
   * @return Returns the parameters and their value in a Hasmap.
   */
  public HashMap getParameters()
  {
    return parameters;
  }

  /**
   * Returns the url value.
   * 
   * @return Returns the url.
   */
  public String getUrl()
  {
    return url;
  }

  /**
   * Sets the url value.
   * 
   * @param url The url to set.
   */
  public void setUrl(String url)
  {
    this.url = url;
  }

  //
  // Private methods (mainly parsing)
  //

  /**
   * Create the corresponding controller connect policy according to what is
   * found in the URL. If no policy was specified then a
   * <code>RandomConnectPolicy</code> is returned.
   * 
   * @return an <code>AbstractControllerConnectPolicy</code>
   */
  private AbstractControllerConnectPolicy createConnectionPolicy()
  {
    int pingDelayInMs = Driver.DEFAULT_PING_DELAY_IN_MS;
    String pingDelayStr = (String) parameters
        .get(Driver.PING_DELAY_IN_MS_PROPERTY);
    if (pingDelayStr != null)
      pingDelayInMs = Integer.parseInt(pingDelayStr);

    int controllerTimeoutInMs = Driver.DEFAULT_CONTROLLER_TIMEOUT_IN_MS;
    String controllerTimeoutStr = (String) parameters
        .get(Driver.CONTROLLER_TIMEOUT_IN_MS_PROPERTY);
    if (controllerTimeoutStr != null)
      controllerTimeoutInMs = Integer.parseInt(controllerTimeoutStr);

    if (controllerList.length == 1)
      return new SingleConnectPolicy(controllerList, pingDelayInMs,
          controllerTimeoutInMs, debugLevel);

    String policy = (String) parameters
        .get(Driver.PREFERRED_CONTROLLER_PROPERTY);

    // preferredController: defines the strategy to use to choose a preferred
    // controller to connect to
    // - jdbc:sequoia://node1,node2,node3/myDB?preferredController=roundRobin
    // round robin starting with first node in URL
    // This is the default policy.
    if ((policy == null) || policy.equals("roundRobin"))
      return new RoundRobinConnectPolicy(controllerList, pingDelayInMs,
          controllerTimeoutInMs, debugLevel);

    // - jdbc:sequoia://node1,node2,node3/myDB?preferredController=ordered
    // Always connect to node1, and if not available then try to node2 and
    // finally if none are available try node3.
    if (policy.equals("ordered"))
      return new OrderedConnectPolicy(controllerList, pingDelayInMs,
          controllerTimeoutInMs, debugLevel);

    // - jdbc:sequoia://node1,node2,node3/myDB?preferredController=random
    // Pick a node randomly amongst active controllers.
    if (policy.equals("random"))
      return new RandomConnectPolicy(controllerList, pingDelayInMs,
          controllerTimeoutInMs, debugLevel);

    // - jdbc:sequoia://node1,node2,node3/myDB?preferredController=node2,node3
    // same as above but round-robin (or random?) between 2 and 3
    return new PreferredListConnectPolicy(controllerList, policy, pingDelayInMs,
        controllerTimeoutInMs, debugLevel);
  }

  /**
   * Checks for URL correctness and extract database name, controller list and
   * parameters into the map "this.parameters".
   * 
   * @exception SQLException if an error occurs.
   */
  private void parseUrl() throws SQLException
  {
    // Find the hostname and check for URL correctness
    if (url == null)
    {
      throw new IllegalArgumentException(
          "Illegal null URL in parseURL(String) method");
    }

    if (!url.toLowerCase().startsWith(driver.sequoiaUrlHeader))
      throw new SQLException("Malformed header from URL '" + url
          + "' (expected '" + driver.sequoiaUrlHeader + "')");
    else
    {
      // Get the controllers list
      int nextSlash = url.indexOf('/', driver.sequoiaUrlHeaderLength);
      if (nextSlash == -1)
        // Missing '/' between hostname and database name.
        throw new SQLException("Malformed URL '" + url + "' (expected '"
            + driver.sequoiaUrlHeader + "<hostname>/<database>')");

      // Found end of database name
      int questionMark = url.indexOf('?', nextSlash);
      questionMark = (questionMark == -1)
          ? url.indexOf(';', nextSlash)
          : questionMark;

      String controllerURLs = url.substring(driver.sequoiaUrlHeaderLength,
          nextSlash);
      // Check the validity of each controller in the list
      // empty tokens (when successive delims) are ignored
      StringTokenizer controllers = new StringTokenizer(controllerURLs, ",",
          false);
      int tokenNumber = controllers.countTokens();
      if (tokenNumber == 0)
      {
        throw new SQLException("Empty controller name in '" + controllerURLs
            + "' in URL '" + url + "'");
      }
      controllerList = new ControllerInfo[tokenNumber];
      int i = 0;
      String token;
      // TODO: the following code does not recognize the following buggy urls:
      // jdbc:sequoia://,localhost:/tpcw or jdbc:sequoia://host1,,host2:/tpcw
      while (controllers.hasMoreTokens())
      {
        token = controllers.nextToken().trim();
        if (token.equals("")) // whitespace tokens
        {
          throw new SQLException("Empty controller name in '" + controllerURLs
              + "' in URL '" + url + "'");
        }
        controllerList[i] = parseController(token);
        i++;
      }

      // Check database name validity
      databaseName = (questionMark == -1) ? url.substring(nextSlash + 1, url
          .length()) : url.substring(nextSlash + 1, questionMark);
      Character c = validDatabaseName(databaseName);
      if (c != null)
        throw new SQLException(
            "Unable to validate database name (unacceptable character '" + c
                + "' in database '" + databaseName + "' from URL '" + url
                + "')");

      // Get the parameters from the url
      parameters.putAll(parseUrlParams(url));
    }
  }

  /**
   * Parse the given URL and returns the parameters in a HashMap containing
   * ParamaterName=>Value.
   * 
   * @param urlString the URL to parse
   * @return a Hashmap of param name=>value possibly empty
   * @throws SQLException if an error occurs
   */
  private HashMap parseUrlParams(String urlString) throws SQLException
  {
    HashMap props = parseUrlParams(urlString, '?', "&", "=");
    if (props == null)
      props = parseUrlParams(urlString, ';', ";", "=");
    if (props == null)
      props = new HashMap();

    return props;
  }

  /**
   * Parse the given URL looking for parameters starting after the beginMarker,
   * using parameterSeparator as the separator between parameters and equal as
   * the delimiter between a parameter and its value.
   * 
   * @param urlString the URL to parse
   * @param beginMarker delimiter for beginning of parameters
   * @param parameterSeparator delimiter between parameters
   * @param equal delimiter between parameter and its value
   * @return HashMap of ParameterName=>Value
   * @throws SQLException if an error occurs
   */
  private HashMap parseUrlParams(String urlString, char beginMarker,
      String parameterSeparator, String equal) throws SQLException
  {
    int questionMark = urlString.indexOf(beginMarker);
    if (questionMark == -1)
      return null;
    else
    {
      HashMap props = new HashMap();
      String params = urlString.substring(questionMark + 1);
      StringTokenizer st1 = new StringTokenizer(params, parameterSeparator);
      while (st1.hasMoreTokens())
      {
        String param = st1.nextToken();
        StringTokenizer st2 = new StringTokenizer(param, equal);
        if (st2.hasMoreTokens())
        {
          try
          {
            String paramName = st2.nextToken();
            String paramValue = (st2.hasMoreTokens()) ? st2.nextToken() : "";
            props.put(paramName, paramValue);
          }
          catch (Exception e) // TODOC: what are we supposed to catch here?
          {
            throw new SQLException("Invalid parameter in URL: " + urlString);
          }
        }
      }
      return props;
    }
  }

  /**
   * Checks the validity of the hostname, port number and controller name given
   * in the URL and build the full URL used to lookup a controller.
   * 
   * @param controller information regarding a controller.
   * @return a <code>ControllerInfo</code> object
   * @exception SQLException if an error occurs.
   */
  public static ControllerInfo parseController(String controller)
      throws SQLException
  {
    String hostname = null;
    int hostport = -1;

    // Check controller syntax
    StringTokenizer controllerURL = new StringTokenizer(controller, ":", true);

    // Get hostname
    hostname = controllerURL.nextToken();
    Character c = validHostname(hostname);
    if (c != null)
      throw new SQLException(
          "Unable to validate hostname (unacceptable character '" + c
              + "' in hostname '" + hostname + "' from the URL part '"
              + controller + "')");

    if (!controllerURL.hasMoreTokens())
      hostport = DEFAULT_CONTROLLER_PORT;
    else
    {
      controllerURL.nextToken(); // should be ':'
      if (!controllerURL.hasMoreTokens())
        hostport = DEFAULT_CONTROLLER_PORT;
      else
      { // Get the port number
        String port = controllerURL.nextToken();
        if (controllerURL.hasMoreTokens())
          throw new SQLException(
              "Invalid controller definition with more than one semicolon in URL part '"
                  + controller + "'");

        // Check the port number validity
        try
        {
          hostport = Integer.parseInt(port);
        }
        catch (NumberFormatException ne)
        {
          throw new SQLException(
              "Unable to validate port number (unacceptable port number '"
                  + port + "' in this URL part '" + controller + "')");
        }
      }
    }
    try
    {
      return new ControllerInfo(hostname, hostport);
    }
    catch (UnknownHostException e)
    {
      SQLException err = new SQLException(
          "Unable to determine address for host " + hostname);
      err.initCause(e);
      throw err;
    }
  }

  /**
   * Checks that the given name contains acceptable characters for a hostname
   * name ([0-9][A-Z][a-z][["-_."]).
   * 
   * @param hostname name to check (caller must check that it is not
   *          <code>null</code>).
   * @return <code>null</code> if the hostname is acceptable, else the
   *         character that causes the fault.
   */
  private static Character validHostname(String hostname)
  {
    char[] name = hostname.toCharArray();
    int size = hostname.length();
    char c;
    // boolean lastCharWasPoint = false; // used to avoid '..' in hostname
    char lastChar = ' ';

    for (int i = 0; i < size; i++)
    {
      c = name[i];

      if (c == '.' || c == '-')
      {
        if (lastChar == '.' || lastChar == '-' || (i == size - 1) || (i == 0))
        {
          // . or - cannot be the first or the last char of hostname
          // hostname cannot contain '..' or '.-' or '-.' or '--'
          return new Character(c);
        }
      }
      else
      {
        if (((c < '0') || (c > 'z') || ((c > '9') && (c < 'A'))
            || ((c > 'Z') && (c < '_')) || (c == '`')))
        {
          return new Character(c);
        }
      }
      lastChar = c;
    }
    return null;
  }

  /**
   * Checks that the given name contains acceptable characters for a database
   * name ([0-9][A-Z][a-z]["-_"]).
   * 
   * @param databaseName name to check (caller must check that it is not
   *          <code>null</code>).
   * @return <code>null</code> if the name is acceptable, else the character
   *         that causes the fault.
   */
  private static Character validDatabaseName(String databaseName)
  {
    char[] name = databaseName.toCharArray();
    int size = databaseName.length();
    char c;

    for (int i = 0; i < size; i++)
    {
      c = name[i];
      if ((c < '-') || (c > 'z') || (c == '/') || (c == '.') || (c == '`')
          || ((c > '9') && (c < 'A')) || ((c > 'Z') && (c < '_')))
        return new Character(c);
    }
    return null;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object other)
  {
    if (!(other instanceof SequoiaUrl))
      return false;
    SequoiaUrl castedOther = (SequoiaUrl) other;
    return (url.equals(castedOther.url) && parameters
        .equals(castedOther.parameters));
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode()
  {
    return toString().hashCode();
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return (url + parameters);
  }

  /**
   * This clone has the following "differences" with the original:
   * <ul>
   * <li>not persistent
   * <li>but sharing the same controller policy anyway
   * <ul>
   */
  public SequoiaUrl getTemporaryCloneForReconnection(String user,
      String password) throws SQLException
  {
    Properties properties = new Properties();
    for (Iterator iter = getParameters().keySet().iterator(); iter.hasNext();)
    {
      String key = (String) iter.next();
      properties.setProperty(key, (String) getParameters().get(key));
    }
    // The SequoiaUrl does not carry the login/password info so we have to
    // re-create these properties to reconnect
    properties.put(Driver.USER_PROPERTY, user);
    properties.put(Driver.PASSWORD_PROPERTY, password);

    // Reuse the same controller policy so we do not fork new pingers with
    // different controller states
    SequoiaUrl tempUrl = new SequoiaUrl(this.driver, getUrl(), properties,
        this.controllerConnectPolicy);

    // Fixes SEQUOIA-568
    tempUrl.getParameters().put(Driver.PERSISTENT_CONNECTION_PROPERTY, "false");

    return tempUrl;
  }
}
