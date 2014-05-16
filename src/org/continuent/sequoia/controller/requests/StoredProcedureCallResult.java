/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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

package org.continuent.sequoia.controller.requests;

import java.io.Serializable;

/**
 * This class defines a StoredProcedureCallResult to store the result of a
 * stored procedure call including information about the OUT and named
 * parameters.
 * 
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class StoredProcedureCallResult implements Serializable
{
  private static final long serialVersionUID = -9116817669101507041L;

  private Object            result;
  private StoredProcedure   proc;

  /**
   * Creates a new <code>StoredProcedureCallResult</code> object
   * 
   * @param proc the stored procedure call (including out and named parameters
   *          information)
   * @param result the result of the call
   */
  public StoredProcedureCallResult(StoredProcedure proc, Object result)
  {
    this.proc = proc;
    this.result = result;
  }

  /**
   * Returns the result value.
   * 
   * @return Returns the result.
   */
  public final Object getResult()
  {
    return result;
  }

  /**
   * Returns the stored procedure object containing the parameters information
   * 
   * @return a stored procedure object
   */
  public StoredProcedure getStoredProcedure()
  {
    return proc;
  }

}
