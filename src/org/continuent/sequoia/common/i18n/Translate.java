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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s):  Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.i18n;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Class to translate the different messages of Sequoia.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */

public final class Translate
{
  private static final String         BUNDLE_NAME     = "org.continuent.sequoia.common.i18n.messages"; //$NON-NLS-1$

  private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
                                                          .getBundle(BUNDLE_NAME);

  /**
   * Class is not meant to be instantiated.
   */
  private Translate()
  {
  }
  
  /**
   * Returns the translation for the given &lt;code&gt;key&lt;/code&gt;
   * 
   * @param key the translation key
   * @return the translation or &lt;code&gt;'!' + key + '!'&lt;/code&gt; if
   *         there is no translation for the given key
   */
  public static String get(String key)
  {
    try
    {
      return RESOURCE_BUNDLE.getString(key);
    }
    catch (MissingResourceException e)
    {
      return '!' + key + '!';
    }
  }
  
  /**
   * Returns the translation for the given &lt;code&gt;key&lt;/code&gt;
   * 
   * @param key the translation key
   * @param args array of &lt;code&gt;Objects&lt;/code&gt; used by the
   *        translation
   * @return the translation or &lt;code&gt;'!' + key + '!'&lt;/code&gt; if
   *         there is no translation for the given key
   */
  public static String get(String key, Object[] args)
  {
    try
    {
      return MessageFormat.format(RESOURCE_BUNDLE.getString(key), args);
    }
    catch (MissingResourceException e)
    {
      return '!' + key + '!';
    }
  }

  // all methods below are convenience methods which
  // delegate to get(String key, Object[] args)
  
  /**
   * @see #get(String, Object[])
   */
  public static String get(String key, boolean parameter)
  {
    return get(key, new Object[]{Boolean.valueOf(parameter)});
  }

  /**
   * @see #get(String, Object[])
   */
  public static String get(String key, int parameter)
  {
    return get(key, new Object[]{Integer.toString(parameter)});
  }

  /**
   * @see #get(String, Object[])
   */
  public static String get(String key, long parameter)
  {
    return get(key, new Object[]{Long.toString(parameter)});
  }

  /**
   * @see #get(String, Object[])
   */
  public static String get(String key, Object param1, Object param2)
  {
    return get(key, new Object[]{param1, param2});
  }

  /**
   * @see #get(String, Object[])
   */
  public static String get(String key, Object parameter)
  {
    return get(key, new Object[]{parameter});
  }
}