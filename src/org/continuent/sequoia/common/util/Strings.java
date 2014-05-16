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
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.common.util;

/**
 * This class provides utilities for Strings manipulation.
 * 
 * @author <a href="mailto:mwick@dplanet.ch">Marc Wick</a>
 * @author <a href="mailto:mathieu.peltier@emicnetworks.com">Mathieu Peltier</a>
 * @version 1.0
 */
public class Strings
{

  /**
   * Replaces all occurrences of a String within another String.
   * 
   * @param sourceString source String
   * @param replace text pattern to replace
   * @param with replacement text
   * @return the text with any replacements processed, <code>null</code> if
   *         null String input
   */
  public static String replace(String sourceString, String replace, String with)
  {
    if (sourceString == null || replace == null || with == null
        || "".equals(replace))
    {
      return sourceString;
    }

    StringBuffer buf = new StringBuffer(sourceString.length());
    int start = 0, end = 0;
    while ((end = sourceString.indexOf(replace, start)) != -1)
    {
      buf.append(sourceString.substring(start, end)).append(with);
      start = end + replace.length();
    }
    buf.append(sourceString.substring(start));
    return buf.toString();
  }

  /**
   * Replaces all occurrences of a String within another String. The String to
   * be replaced will be replaced ignoring cases, all other cases are preserved
   * in the returned string
   * 
   * @param sourceString source String
   * @param replace text to replace, case insensitive
   * @param with replacement text
   * @return the text with any replacements processed, <code>null</code> if
   *         null String input
   */
  public static String replaceCasePreserving(String sourceString,
      String replace, String with)
  {
    if (sourceString == null || replace == null || with == null)
    {
      return sourceString;
    }
    String lower = sourceString.toLowerCase();
    int shift = 0;
    int idx = lower.indexOf(replace);
    int length = replace.length();
    StringBuffer resultString = new StringBuffer(sourceString);
    do
    {
      resultString = resultString.replace(idx + shift, idx + shift + length,
          with);
      shift += with.length() - length;
      idx = lower.indexOf(replace, idx + length);
    }
    while (idx > 0);

    return resultString.toString();
  }

}
