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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): 
 */
package org.continuent.sequoia.common.jmx.monitoring.backend;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * Return total of currently pending requests on this backend
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk</a>
 */
public class PendingRequestsCollector extends AbstractBackendDataCollector
{
  private static final long serialVersionUID = 2088946444040664408L;

  /**
   * @param backendName of the backend to get data from
   * @param virtualDatabaseName database accessed to get data
   */
  public PendingRequestsCollector(String backendName, String virtualDatabaseName)
  {
    super(backendName, virtualDatabaseName);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#collectValue()
   */
  public long getValue(Object backend)
  {
    return ((DatabaseBackend) backend).getPendingRequests().size();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.monitoring.AbstractDataCollector#getDescription()
   */
  public String getDescription()
  {
    return Translate.get("monitoring.backend.pending.requests"); //$NON-NLS-1$
  }
}
