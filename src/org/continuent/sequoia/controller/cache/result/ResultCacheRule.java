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
 * Contributor(s): Emmanuel Cecchet. 
 */

package org.continuent.sequoia.controller.cache.result;

import java.util.regex.Pattern;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This is the to define cache rules in the cache. A
 * <code>ResultCacheRule</code> is defined by a queryPattern, set to 'default'
 * if default rule, and a <code>CacheBehavior</code>.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ResultCacheRule implements XmlComponent
{
  Trace                 logger = Trace.getLogger(ResultCacheRule.class
                                   .getName());
  private Pattern       queryPattern;
  private String        queryString;
  private boolean       isCaseSensitive;
  private boolean       applyToSkeleton;
  private long          timestampResolution;
  private CacheBehavior behavior;

  /**
   * Creates a new <code>ResultCacheRule</code>
   * 
   * @param queryString for this rule
   * @param caseSensitive true if matching is case sensitive
   * @param applyToSkeleton true if rule apply to query skeleton
   * @param timestampResolution timestamp resolution for NOW() macro
   */
  public ResultCacheRule(String queryString, boolean caseSensitive,
      boolean applyToSkeleton, long timestampResolution)
  {
    this.queryString = queryString;
    queryPattern = Pattern.compile(queryString);
    this.isCaseSensitive = caseSensitive;
    this.applyToSkeleton = applyToSkeleton;
    this.timestampResolution = timestampResolution;
  }

  /**
   * Get the query pattern
   * 
   * @return the queryPattern for this <code>ResultCacheRule</code>
   */
  public Pattern getQueryPattern()
  {
    return this.queryPattern;
  }

  /**
   * Get the cache behavior
   * 
   * @return the <code>CacheBehavior</code> for this
   *         <code>ResultCacheRule</code>
   */
  public CacheBehavior getCacheBehavior()
  {
    return behavior;
  }

  /**
   * Set the cache behavior
   * 
   * @param behavior behavior for this rule
   */
  public void setCacheBehavior(CacheBehavior behavior)
  {
    this.behavior = behavior;
  }

  /**
   * Retrieve the timestamp resolution of this scheduler
   * 
   * @return timestampResolution
   */
  public long getTimestampResolution()
  {
    return this.timestampResolution;
  }

  /**
   * @param request we may want to add to the cache
   * @return the behavior to get the entry
   */
  public CacheBehavior matches(AbstractRequest request)
  {
    if (queryPattern.matcher(request.getSqlOrTemplate()).matches())
      return behavior;
    else
      return null;
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_ResultCacheRule + " "
        + DatabasesXmlTags.ATT_queryPattern + "=\"" + queryString + "\" "
        + DatabasesXmlTags.ATT_caseSensitive + "=\"" + isCaseSensitive + "\" "
        + DatabasesXmlTags.ATT_applyToSkeleton + "=\"" + applyToSkeleton
        + "\" " + DatabasesXmlTags.ATT_timestampResolution + "=\""
        + timestampResolution / 1000 + "\" >");
    info.append(behavior.getXml());
    info.append("</" + DatabasesXmlTags.ELT_ResultCacheRule + ">");
    return info.toString();
  }

}