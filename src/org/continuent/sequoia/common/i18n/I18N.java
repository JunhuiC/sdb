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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.i18n;

import java.text.MessageFormat;
import java.util.ResourceBundle;

/**
 * This class defines a I18N
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public abstract class I18N
{
  /**
   * Returns associated sentence to that key
   * 
   * @param key the key to find in the translation file
   * @param bundle then translation bundle to use
   * @return the corresponding sentence of the key if not found
   */
  public static String get(ResourceBundle bundle, String key)
  {
    try
    {
      return bundle.getString(key);
    }
    catch (Exception e)
    {
      return key;
    }
  }

  /**
   * Returns translated key with instanciated parameters
   * 
   * @param bundle then translation bundle to use
   * @param key the key to find in translation file.
   * @param parameter the parameter value
   * @return the corresponding sentence with key and parameters
   */
  public static String get(ResourceBundle bundle, String key, boolean parameter)
  {
    return MessageFormat.format(get(bundle, key), new Object[]{String
        .valueOf(parameter)});
  }

  /**
   * Returns translated key with instanciated parameters
   * 
   * @param bundle then translation bundle to use
   * @param key the key to find in translation file.
   * @param parameter the parameter value
   * @return the corresponding sentence with key and parameters
   */
  public static String get(ResourceBundle bundle, String key, int parameter)
  {
    return MessageFormat.format(get(bundle, key), new Object[]{String
        .valueOf(parameter)});
  }

  /**
   * Returns translated key with instanciated parameters
   * 
   * @param bundle then translation bundle to use
   * @param key the key to find in translation file.
   * @param parameter the parameter value
   * @return the corresponding sentence with key and parameters
   */
  public static String get(ResourceBundle bundle, String key, long parameter)
  {
    return MessageFormat.format(get(bundle, key), new Object[]{String
        .valueOf(parameter)});
  }

  /**
   * Replace <code>REPLACE_CHAR</code> in the translated message with
   * parameters. If you have more parameters than charaters to replace,
   * remaining parameters are appended as a comma separated list at the end of
   * the message.
   * 
   * @param bundle then translation bundle to use
   * @param key the key to find in the translation file
   * @param parameters to put inside square braquets
   * @return the corresponding sentence of the key if not found
   */
  public static String get(ResourceBundle bundle, String key,
      Object[] parameters)
  {
    return MessageFormat.format(get(bundle, key), parameters);
  }

  /**
   * Same as above but implies creation of an array for the parameter
   * 
   * @param bundle then translation bundle to use
   * @param key to translate
   * @param parameter to put in translation
   * @return translated message
   */
  public static String get(ResourceBundle bundle, String key, Object parameter)
  {
    return MessageFormat.format(get(bundle, key), new Object[]{parameter});
  }

}