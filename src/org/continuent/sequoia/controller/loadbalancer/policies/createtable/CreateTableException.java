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
 * Contributor(s): _______________________
 */

package org.continuent.sequoia.controller.loadbalancer.policies.createtable;

import org.continuent.sequoia.common.exceptions.SequoiaException;

/**
 * A <code>CreateTableException</code> is thrown when a
 * <code>CreateTableRule</code> policy cannot be applied in the
 * rule.getBackends() method.
 * 
 * @see org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableRule#getBackends(java.util.ArrayList)
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class CreateTableException extends SequoiaException
{
  private static final long serialVersionUID = -1818582588221008537L;

  /**
   * Creates a new <code>CreateTableException</code>.
   */
  public CreateTableException()
  {
    super();
  }

  /**
   * Creates a new <code>CreateTableException</code>.
   * 
   * @param message the error message
   */
  public CreateTableException(String message)
  {
    super(message);
  }

  /**
   * Creates a new <code>CreateTableException</code>.
   * 
   * @param cause the root cause
   */
  public CreateTableException(Throwable cause)
  {
    super(cause);
  }

  /**
   * Creates a new <code>CreateTableException</code>.
   * 
   * @param message the error message
   * @param cause the root cause
   */
  public CreateTableException(String message, Throwable cause)
  {
    super(message, cause);
  }
}