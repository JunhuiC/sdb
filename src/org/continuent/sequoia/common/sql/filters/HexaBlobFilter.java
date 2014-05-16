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

package org.continuent.sequoia.common.sql.filters;

import org.continuent.sequoia.common.stream.encoding.HexaEncoding;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This class defines a HexaBlobFilterInterface. It encodes the blobs in hexa
 * values
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class HexaBlobFilter extends AbstractBlobFilter
{

  /**
   * @see org.continuent.sequoia.common.sql.filters.AbstractBlobFilter#decode(java.lang.String)
   */
  public byte[] decode(String data)
  {
    return HexaEncoding.hex2data(data);
  }

  /**
   * @see org.continuent.sequoia.common.sql.filters.AbstractBlobFilter#encode(byte[])
   */
  public String encode(byte[] data)
  {
    return HexaEncoding.data2hex(data);
  }

  /**
   * @see org.continuent.sequoia.common.sql.filters.AbstractBlobFilter#getXml()
   */
  public String getXml()
  {
    return DatabasesXmlTags.VAL_hexa;
  }

}