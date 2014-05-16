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
 */

package org.continuent.sequoia.controller.loadbalancer;

import java.util.HashMap;
import java.util.Iterator;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * To return information, weighted load balancers share the same kind of
 * information on backend configuration.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public abstract class WeightedBalancer
{
  /**
   * get different xml tags of the weights in the system.
   * 
   * @param weights a list ((String)name,(Integer)weight) of weights
   * @return xml formatted string of weighted backends
   */
  public static final String getWeightedXml(HashMap weights)
  {
    if (weights == null)
      return "";
    StringBuffer info = new StringBuffer();
    String nametmp;
    for (Iterator iterator = weights.keySet().iterator(); iterator.hasNext();)
    {
      nametmp = (String) iterator.next();
      info
          .append("<" + DatabasesXmlTags.ELT_BackendWeight + " "
              + DatabasesXmlTags.ATT_name + "=\"" + nametmp + "\" "
              + DatabasesXmlTags.ATT_weight + "=\"" + weights.get(nametmp)
              + "\"/>");
    }
    return info.toString();
  }

  /**
   * Convert raidb weighted balancers into xml because they share common views.
   * 
   * @param weights hashmap of (name,weight)
   * @param xmltag the xml tag to use
   * @return xml formatted string
   */
  public static final String getRaidbXml(HashMap weights, String xmltag)
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + xmltag + ">");
    info.append(WeightedBalancer.getWeightedXml(weights));
    info.append("</" + xmltag + ">");
    return info.toString();
  }
}