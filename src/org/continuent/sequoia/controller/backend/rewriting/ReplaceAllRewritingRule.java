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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend.rewriting;

/**
 * This class defines a ReplaceAllRewritingRule. Replace all instance of a
 * <code>String</code> token by another <code>String</code> token
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ReplaceAllRewritingRule extends AbstractRewritingRule
{

  /**
   * @see org.continuent.sequoia.controller.backend.rewriting.AbstractRewritingRule#rewrite(java.lang.String)
   */
  public String rewrite(String sqlQuery)
  {
    // Check first if it is a match
    int start;
    if (isCaseSensitive)
      start = sqlQuery.indexOf(queryPattern);
    else
      start = sqlQuery.toLowerCase().indexOf(queryPattern.toLowerCase());
    if (start == -1)
    { // No match
      hasMatched = false;
      return sqlQuery;
    }
    // Match, rewrite the query
    hasMatched = true;

    return replace(sqlQuery, queryPattern, rewrite);
  }

  /**
   * Creates a new <code>ReplaceAllRewritingRule.java</code> object
   * 
   * @param queryPattern SQL pattern to match
   * @param rewrite rewritten SQL query
   * @param caseSensitive true if matching is case sensitive
   * @param stopOnMatch true if rewriting must stop after this rule if it
   *          matches.
   */
  public ReplaceAllRewritingRule(String queryPattern, String rewrite,
      boolean caseSensitive, boolean stopOnMatch)
  {
    super(queryPattern, rewrite, caseSensitive, stopOnMatch);
  }

  private static String replace(String s, String oldText, String newText)
  {
    final int oldLength = oldText.length();
    final int newLength = newText.length();

    if (oldLength == 0)
      throw new IllegalArgumentException("cannot replace the empty string");

    if (oldText.equals(newText))
      return s;

    int i = 0;
    int x = 0;

    StringBuffer sb = new StringBuffer(s);

    while ((i = sb.indexOf(oldText, x)) > -1)
    {
      sb.delete(i, i + oldLength);
      sb.insert(i, newText);
      x = i + newLength;
    }

    return sb.toString();
  }
}