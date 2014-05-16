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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.util;

import java.io.IOException;
import java.util.Properties;

/**
 * Constants that are common to the console, driver and controller modules
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 */
public class Constants
{
  static Properties getVersionProperties()
  {
    Properties versionProperties = new Properties();
    try
    {
      versionProperties.load(Constants.class
          .getResourceAsStream("version.properties"));
    }
    catch (IOException e)
    { // ignored
    }
    return versionProperties;
  }

  // the following default value "@VERSION@" is patched by build.xml
  // target 'init-compile'.

  /** Sequoia version. */
  public static final String VERSION = getVersionProperties().getProperty(
                                         "sequoia.version", "@VERSION@");

  /**
   * Sequoia major version
   * 
   * @return major version
   */
  public static final int getMajorVersion()
  {
    int ind = VERSION.indexOf('.');
    if (ind > 0)
      return Integer.parseInt(VERSION.substring(0, ind));
    else
      return 1;
  }

  /**
   * Sequoia minor version
   * 
   * @return minor version
   */
  public static final int getMinorVersion()
  {
    int ind = VERSION.indexOf('.');
    if (ind > 0)
      return Integer.parseInt(VERSION.substring(ind + 1, ind + 2));
    else
      return 0;
  }

  //
  // Shutdown constants
  //

  /** Shutdown Mode Wait: Wait for all clients to disconnect */
  public static final int  SHUTDOWN_WAIT           = 1;

  /**
   * Shutdown Mode Safe: Wait for all current transactions to complete before
   * shutdown
   */
  public static final int  SHUTDOWN_SAFE           = 2;

  /**
   * Shutdown Mode Force: Does not wait for the end of the current transactions
   * and kill all connections. Recovery will be needed on restart.
   */
  public static final int  SHUTDOWN_FORCE          = 3;

  //
  // Ping constant
  /** Controller ping protocol version = byte that is sent/received as ping/pong */
  public static final byte CONTROLLER_PING_VERSION = 0x01;
}
