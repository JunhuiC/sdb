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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend.rewriting;

import java.util.Hashtable;
import java.util.StringTokenizer;

/**
 * This class defines a PatternRewritingRule
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class PatternRewritingRule extends AbstractRewritingRule
{
  private char     tokenDelimiter = '?';
  private String[] patternArray;
  private String[] rewriteArray;

  /**
   * Creates a new <code>PatternRewritingRule.java</code> object
   * 
   * @param queryPattern SQL pattern to match
   * @param rewrite rewritten SQL query
   * @param caseSensitive true if matching is case sensitive
   * @param stopOnMatch true if rewriting must stop after this rule if it
   *          matches.
   */
  public PatternRewritingRule(String queryPattern, String rewrite,
      boolean caseSensitive, boolean stopOnMatch)
  {
    super(queryPattern, rewrite, caseSensitive, stopOnMatch);

    // Parse queryPattern and rewrite to extract the parameters ?1 ?2 ...
    StringTokenizer patternTokenizer = new StringTokenizer(queryPattern, String
        .valueOf(tokenDelimiter), true);
    patternArray = new String[patternTokenizer.countTokens()];
    int i = 0;
    try
    {
      do
      {
        patternArray[i] = patternTokenizer.nextToken();
        if (patternArray[i].charAt(0) == tokenDelimiter)
        { // We found a delimiter (?)
          String nextToken = patternTokenizer.nextToken();
          // Add the parameter number (only works with 1 digit parameters)
          //For example ?124 will be recognized as ?1
          patternArray[i] += nextToken.charAt(0);
          i++;
          if (nextToken.length() > 1)
            // This is the next token
            patternArray[i] = nextToken.substring(1);
        }
        i++;
      }
      while (patternTokenizer.hasMoreTokens());
    }
    catch (RuntimeException e)
    {
      throw new RuntimeException("Malformed query pattern: " + queryPattern);
    }
    StringTokenizer rewriteTokenizer = new StringTokenizer(rewrite, String
        .valueOf(tokenDelimiter), true);
    rewriteArray = new String[rewriteTokenizer.countTokens()];
    i = 0;
    try
    {
      do
      {
        rewriteArray[i] = rewriteTokenizer.nextToken();
        if (rewriteArray[i].charAt(0) == tokenDelimiter)
        { // We found a delimiter (?)
          String nextToken = rewriteTokenizer.nextToken();
          // Add the parameter number (only works with 1 digit parameters)
          //For example ?124 will be recognized as ?1
          rewriteArray[i] += nextToken.charAt(0);
          i++;
          if (nextToken.length() > 1)
            // This is the next token
            rewriteArray[i] = nextToken.substring(1);
        }
        i++;
      }
      while (rewriteTokenizer.hasMoreTokens());
    }
    catch (RuntimeException e1)
    {
      throw new RuntimeException("Malformed rewrite element: " + rewrite);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.backend.rewriting.AbstractRewritingRule#rewrite(java.lang.String)
   */
  public String rewrite(String sqlQuery)
  {
    Hashtable<String, String> tokens = null; // Parameters value in the query
    String lastParameter = null;
    String currentToken;
    int oldIndex = 0;
    int newIndex = 0;

    // Check for match and collect parameters into tokens
    for (int i = 0; i < patternArray.length; i++)
    {
      currentToken = patternArray[i];
      if (currentToken == null)
        break; // Last token was a parameter
      if (currentToken.charAt(0) == tokenDelimiter)
      { // A new parameter is expected
        lastParameter = currentToken;
        continue;
      }
      // Here is the value of the parameter
      newIndex = sqlQuery.indexOf(currentToken, oldIndex);
      if (newIndex == -1)
      { // No match
        hasMatched = false;
        return sqlQuery;
      }

      if (lastParameter != null)
      { // Add the parameter value
        if (tokens == null)
          tokens = new Hashtable<String, String>();
        tokens.put(lastParameter, sqlQuery.substring(oldIndex, newIndex));
      }
      oldIndex = newIndex + currentToken.length();
    }
    // Last parameter
    if (newIndex < sqlQuery.length())
    {
      if (tokens != null)
      {
        if (tokens.containsKey(lastParameter))
        { // No match on the end of the pattern
          hasMatched = false;
          return sqlQuery;
        }
        else
          tokens.put(lastParameter, sqlQuery.substring(oldIndex));
      }
      // Here, we probably had a match without parameters. What's the point?
    }

    hasMatched = true;

    StringBuffer rewrittenQuery = new StringBuffer();
    for (int i = 0; i < rewriteArray.length; i++)
    {
      currentToken = rewriteArray[i];
      if (currentToken == null)
        break; // Last token was a parameter
      if (currentToken.charAt(0) != tokenDelimiter)
        rewrittenQuery.append(currentToken);
      else
        rewrittenQuery.append(tokens.get(currentToken));
    }
    return rewrittenQuery.toString();
  }
}