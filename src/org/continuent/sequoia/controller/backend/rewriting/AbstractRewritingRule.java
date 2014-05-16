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

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This class defines a AbstractRewritingRule to rewrite SQL requests for a
 * specific backend.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class AbstractRewritingRule
{
  protected String  queryPattern;
  protected String  rewrite;
  protected boolean isCaseSensitive;
  protected boolean stopOnMatch;
  protected boolean hasMatched;

  /**
   * Creates a new <code>AbstractRewritingRule</code> object
   * 
   * @param queryPattern SQL pattern to match
   * @param rewrite rewritten SQL query
   * @param caseSensitive true if matching is case sensitive
   * @param stopOnMatch true if rewriting must stop after this rule if it
   *          matches.
   */
  public AbstractRewritingRule(String queryPattern, String rewrite,
      boolean caseSensitive, boolean stopOnMatch)
  {
    this.queryPattern = queryPattern;
    this.rewrite = rewrite;
    this.isCaseSensitive = caseSensitive;
    this.stopOnMatch = stopOnMatch;
    this.hasMatched = false;
  }

  /**
   * Returns true if the query given in the last call to rewrite has matched
   * this rule.
   * <p>1. call rewrite(query)
   * <p>2. call hasMatched() to know if query matched this rule.
   * 
   * @return true if the query matched this rule.
   * @see #rewrite(String)
   */
  public boolean hasMatched()
  {
    return hasMatched;
  }

  /**
   * Rewrite the given query according to the rule. Note that this method does
   * not check if the given query matches the rule or not. You must call
   * matches(String) before calling this method.
   * 
   * @param sqlQuery request to rewrite
   * @return the rewritten SQL query according to the rule.
   * @see AbstractRewritingRule#hasMatched
   */
  public abstract String rewrite(String sqlQuery);

  /**
   * Returns the isCaseSensitive value.
   * 
   * @return Returns the isCaseSensitive.
   */
  public boolean isCaseSensitive()
  {
    return isCaseSensitive;
  }

  /**
   * Returns the queryPattern value.
   * 
   * @return Returns the queryPattern.
   */
  public String getQueryPattern()
  {
    return queryPattern;
  }

  /**
   * Returns the rewrite value.
   * 
   * @return Returns the rewrite.
   */
  public String getRewrite()
  {
    return rewrite;
  }

  /**
   * Returns the stopOnMatch value.
   * 
   * @return Returns the stopOnMatch.
   */
  public boolean isStopOnMatch()
  {
    return stopOnMatch;
  }

  /**
   * Get xml information about this AbstractRewritingRule.
   * 
   * @return xml formatted information on this AbstractRewritingRule.
   */
  public String getXml()
  {
    return "<" + DatabasesXmlTags.ELT_RewritingRule + " "
        + DatabasesXmlTags.ATT_queryPattern + "=\"" + queryPattern + "\" "
        + DatabasesXmlTags.ATT_rewrite + "=\"" + rewrite + "\" "
        + DatabasesXmlTags.ATT_caseSensitive + "=\"" + isCaseSensitive + "\" "
        + DatabasesXmlTags.ATT_stopOnMatch + "=\"" + stopOnMatch + "\"/>";
  }

}
