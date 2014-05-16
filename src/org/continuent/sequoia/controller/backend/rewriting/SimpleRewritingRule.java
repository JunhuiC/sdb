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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend.rewriting;

/**
 * This class defines a SimpleRewritingRule
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class SimpleRewritingRule extends AbstractRewritingRule
{

  private int queryPatternLength;

  /**
   * Creates a new <code>SimpleRewritingRule.java</code> object
   * 
   * @param queryPattern SQL pattern to match
   * @param rewrite rewritten SQL query
   * @param caseSensitive true if matching is case sensitive
   * @param stopOnMatch true if rewriting must stop after this rule if it
   *          matches.
   */
  public SimpleRewritingRule(String queryPattern, String rewrite,
      boolean caseSensitive, boolean stopOnMatch)
  {
    super(queryPattern, caseSensitive ? rewrite : rewrite.toLowerCase(),
        caseSensitive, stopOnMatch);
    queryPatternLength = queryPattern.length();
  }

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
    if (start == 0)
    {
      if (queryPatternLength < sqlQuery.length())
        // Match at the beginning of the pattern
        return rewrite + sqlQuery.substring(queryPatternLength);
      else
        // The query was exactly the pattern
        return rewrite;
    }
    else
    {
      if (start + queryPatternLength < sqlQuery.length())
        return sqlQuery.substring(0, start) + rewrite
            + sqlQuery.substring(start + queryPatternLength);
      else
        // Match at the end of the pattern
        return sqlQuery.substring(0, start) + rewrite;
    }
  }

}