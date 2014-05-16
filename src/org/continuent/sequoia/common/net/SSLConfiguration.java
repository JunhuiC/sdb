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
 * Initial developer(s): Marc Wick.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.net;

import java.io.File;
import java.io.Serializable;

/**
 * This class defines a SSLConfiguration
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class SSLConfiguration implements Serializable
{
  private static final long serialVersionUID               = -7030030045041996566L;

  /** kestore file */
  private File              keyStore;
  /** keystore password */
  private String            keyStorePassword;
  /** key password */
  private String            keyStoreKeyPassword;

  // TODO : provide support for naming aliases

  /** need client authentication */
  private boolean           isClientAuthenticationRequired = false;

  /** truststore file */
  private File              trustStore;
  /** truststore password */
  private String            trustStorePassword;

  /**
   * Returns the isClientAuthenticationRequired value.
   * 
   * @return Returns the isClientAuthenticationRequired.
   */
  public boolean isClientAuthenticationRequired()
  {
    return isClientAuthenticationRequired;
  }

  /**
   * Sets the isClientAuthenticationRequired value.
   * 
   * @param isClientAuthenticationRequired The isClientAuthenticationRequired to
   *          set.
   */
  public void setClientAuthenticationRequired(
      boolean isClientAuthenticationRequired)
  {
    this.isClientAuthenticationRequired = isClientAuthenticationRequired;
  }

  /**
   * Returns the keyStore value.
   * 
   * @return Returns the keyStore.
   */
  public File getKeyStore()
  {
    return keyStore;
  }

  /**
   * Sets the keyStore value.
   * 
   * @param keyStore The keyStore to set.
   */
  public void setKeyStore(File keyStore)
  {
    this.keyStore = keyStore;
  }

  /**
   * Returns the keyStoreKeyPassword value.
   * 
   * @return Returns the keyStoreKeyPassword.
   */
  public String getKeyStoreKeyPassword()
  {
    if (keyStoreKeyPassword != null)
      return keyStoreKeyPassword;
    return getKeyStorePassword();
  }

  /**
   * Sets the keyStoreKeyPassword value.
   * 
   * @param keyStoreKeyPassword The keyStoreKeyPassword to set.
   */
  public void setKeyStoreKeyPassword(String keyStoreKeyPassword)
  {
    this.keyStoreKeyPassword = keyStoreKeyPassword;
  }

  /**
   * Returns the keyStorePassword value.
   * 
   * @return Returns the keyStorePassword.
   */
  public String getKeyStorePassword()
  {
    return keyStorePassword;
  }

  /**
   * Sets the keyStorePassword value.
   * 
   * @param keyStorePassword The keyStorePassword to set.
   */
  public void setKeyStorePassword(String keyStorePassword)
  {
    this.keyStorePassword = keyStorePassword;
  }

  /**
   * Returns the trustStore value.
   * 
   * @return Returns the trustStore.
   */
  public File getTrustStore()
  {
    if (trustStore != null)
      return trustStore;

    return getKeyStore();
  }

  /**
   * Sets the trustStore value.
   * 
   * @param trustStore The trustStore to set.
   */
  public void setTrustStore(File trustStore)
  {
    this.trustStore = trustStore;
  }

  /**
   * Returns the trustStorePassword value.
   * 
   * @return Returns the trustStorePassword.
   */
  public String getTrustStorePassword()
  {
    if (trustStorePassword != null)
      return trustStorePassword;

    return getKeyStorePassword();
  }

  /**
   * Sets the trustStorePassword value.
   * 
   * @param trustStorePassword The trustStorePassword to set.
   */
  public void setTrustStorePassword(String trustStorePassword)
  {
    this.trustStorePassword = trustStorePassword;
  }

  /**
   * create a SSLConfiguration with the java default behaviour (using System
   * properties)
   * 
   * @return config
   */
  public static SSLConfiguration getDefaultConfig()
  {
    SSLConfiguration config = new SSLConfiguration();
    String keyStoreProperty = System.getProperty("javax.net.ssl.keyStore");
    if (keyStoreProperty == null)
      throw new RuntimeException(
          "javax.net.ssl.keyStore has not been properly defined");
    config.keyStore = new File(keyStoreProperty);

    config.keyStorePassword = System
        .getProperty("javax.net.ssl.keyStorePassword");
    if (config.keyStorePassword == null)
      throw new RuntimeException(
          "javax.net.ssl.keyStorePassword has not been properly defined");

    config.keyStoreKeyPassword = System
        .getProperty("javax.net.ssl.keyStoreKeyPassword");
    if (config.keyStoreKeyPassword == null)
      config.keyStoreKeyPassword = config.keyStorePassword;

    String trustStoreProperty = System.getProperty("javax.net.ssl.trustStore");
    if (trustStoreProperty == null)
      trustStoreProperty = keyStoreProperty;
    config.trustStore = new File(trustStoreProperty);

    config.trustStorePassword = System
        .getProperty("javax.net.ssl.trustStorePassword");
    if (config.trustStorePassword == null)
      config.trustStorePassword = config.keyStorePassword;
    return config;
  }

}
