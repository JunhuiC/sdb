/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Continuent, Inc.
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

package org.continuent.sequoia.controller.backend.result;

import java.io.Serializable;

/**
 * Definition of the result returned by an XXXExecuteUpdate query.<br>
 * This result is an update count enriched with the generated Statement warnings
 * inherited from <code>AbstractResult</code>
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 */
public class ExecuteUpdateResult extends AbstractResult implements Serializable
{
  private static final long serialVersionUID = -57478579897478732L;

  private int               updateCount;

  /**
   * Sets the update count
   * 
   * @param uc result of the update
   */
  public ExecuteUpdateResult(int uc)
  {
    updateCount = uc;
  }

  /**
   * Retrieves the update count resulting from the request execution
   * 
   * @return update count value
   */
  public final int getUpdateCount()
  {
    return updateCount;
  }
}
