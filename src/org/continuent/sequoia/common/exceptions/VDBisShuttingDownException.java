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
 * Initial developer(s): Stephane Giron.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions;

import java.sql.SQLException;

public class VDBisShuttingDownException extends SQLException
{
  private static final long serialVersionUID = -8837315120263388191L;

  public VDBisShuttingDownException(String message)
  {
    super(message);
  }
}
