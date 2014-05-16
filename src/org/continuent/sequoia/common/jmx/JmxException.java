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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.jmx;

import org.continuent.sequoia.common.exceptions.SequoiaException;
/**
 * Generic JMX Exception
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class JmxException extends SequoiaException
{
  private static final long serialVersionUID = 2591479799351433445L;

  /**
   * Creates a new <code>JmxException</code> instance.
   */
  public JmxException()
  {
  }

  /**
   * Creates a new <code>JmxException</code> instance.
   * 
   * @param message the error message
   */
  public JmxException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>JmxException</code> instance.
   * 
   * @param cause the root cause
   */
  public JmxException(Throwable cause)
  {
    super(cause);
  }


}
