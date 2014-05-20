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

package org.continuent.sequoia.common.net;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;

import com.sun.net.ssl.KeyManager;
import com.sun.net.ssl.KeyManagerFactory;
import com.sun.net.ssl.SSLContext;
import com.sun.net.ssl.TrustManager;
import com.sun.net.ssl.TrustManagerFactory;

/**
 * This class defines a SocketFactory
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
@SuppressWarnings("deprecation")
public class SocketFactoryFactory
{

  /**
   * create a server socket factory with the specified configuration
   * 
   * @param config - the ssl configuration
   * @return - the socket factory
   * @throws SSLException - could not create factory
   */
  public static ServerSocketFactory createServerFactory(SSLConfiguration config)
      throws SSLException
  {
    try
    {

      if (config == null)
        // nothing todo return default SocketFactory
        return ServerSocketFactory.getDefault();

      SSLContext context = createSSLContext(config);
      // Finally, we get a SocketFactory
      SSLServerSocketFactory ssf = context.getServerSocketFactory();

      if (!config.isClientAuthenticationRequired())
        return ssf;

      return new AuthenticatedServerSocketFactory(ssf);
    }
    catch (Exception e)
    {
      throw new SSLException(e);
    }
  }

  /**
   * create a socket factory with the specified configuration
   * 
   * @param config - the ssl configuration
   * @return - the socket factory
   * @throws IOException if the SSL keystore file could not be opened
   * @throws GeneralSecurityException if the SSL keystore file access is denied
   */
  public static SocketFactory createFactory(SSLConfiguration config)
      throws IOException, GeneralSecurityException
  {
    if (config == null)
      // nothing todo return default SocketFactory
      return SocketFactory.getDefault();

    SSLContext context = createSSLContext(config);

    // Finally, we get a SocketFactory
    SSLSocketFactory ssf = context.getSocketFactory();

    if (!config.isClientAuthenticationRequired())
      return ssf;

    return new AuthenticatedSocketFactory(ssf);
  }

  /**
   * create a ssl context
   * 
   * @param config - ssl config
   * @return - the ssl context
   * @throws IOException if the SSL keystore file could not be opened
   * @throws GeneralSecurityException if the SSL keystore file access is denied
   */
  public static SSLContext createSSLContext(SSLConfiguration config)
      throws IOException, GeneralSecurityException
  {

    KeyManager[] kms = getKeyManagers(config.getKeyStore(), config
        .getKeyStorePassword(), config.getKeyStoreKeyPassword());

    TrustManager[] tms = getTrustManagers(config.getTrustStore(), config
        .getTrustStorePassword());

    // Now construct a SSLContext using these KeyManagers. We
    // specify a null SecureRandom, indicating that the
    // defaults should be used.
    SSLContext context = SSLContext.getInstance("SSL");
    context.init(kms, tms, null);
    return context;
  }

  protected static KeyManager[] getKeyManagers(File keyStore,
      String keyStorePassword, String keyPassword) throws IOException,
      GeneralSecurityException
  {
    // First, get the default KeyManagerFactory.
    String alg = KeyManagerFactory.getDefaultAlgorithm();
    KeyManagerFactory kmFact = KeyManagerFactory.getInstance(alg);

    // Next, set up the KeyStore to use. We need to load the file into
    // a KeyStore instance.
    FileInputStream fis = new FileInputStream(keyStore);
    KeyStore ks = KeyStore.getInstance("jks");

    char[] passwd = null;
    if (keyStorePassword != null)
    {
      passwd = keyStorePassword.toCharArray();
    }
    ks.load(fis, passwd);
    fis.close();

    // Now we initialize the TrustManagerFactory with this KeyStore
    kmFact.init(ks, keyPassword.toCharArray());

    // And now get the TrustManagers
    KeyManager[] kms = kmFact.getKeyManagers();
    return kms;
  }

  protected static TrustManager[] getTrustManagers(File trustStore,
      String trustStorePassword) throws IOException, GeneralSecurityException
  {
    // First, get the default TrustManagerFactory.
    String alg = TrustManagerFactory.getDefaultAlgorithm();
    TrustManagerFactory tmFact = TrustManagerFactory.getInstance(alg);

    // Next, set up the TrustStore to use. We need to load the file into
    // a KeyStore instance.
    FileInputStream fis = new FileInputStream(trustStore);
    KeyStore ks = KeyStore.getInstance("jks");
    ks.load(fis, trustStorePassword.toCharArray());
    fis.close();

    // Now we initialize the TrustManagerFactory with this KeyStore
    tmFact.init(ks);

    // And now get the TrustManagers
    TrustManager[] tms = tmFact.getTrustManagers();
    return tms;
  }
}
