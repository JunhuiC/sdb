/**
 * Sequoia: Database clustering technology.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.core;

import org.continuent.sequoia.controller.backend.BackendObjectConverter;
import org.continuent.sequoia.controller.backend.DriverComplianceFactory;
import org.continuent.sequoia.controller.backend.ResultSetMetaDataFactory;
import org.continuent.sequoia.controller.requests.RequestFactory;
import org.continuent.sequoia.controller.backend.SchemaFactory;

/**
 * This interace defines a ControllerFactory used to get controller specific
 * objects.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface ControllerFactory
{

  /**
   * Get the BackendObjectConverter to use to fetch the backend ResultSet.
   * 
   * @return a BackendObjectConverter
   */
  BackendObjectConverter getBackendObjectConverter();

  /**
   * Request factory to create request objects that are transmitted between
   * driver and controller
   * 
   * @return a request factory
   */
  RequestFactory getRequestFactory();

  /**
   * Return a ResultSetMetaDataFactory used to build fields of ResultSets
   * objects.
   * 
   * @return a <code>ResultSetMetaDataFactory</code> implementation
   */
  ResultSetMetaDataFactory getResultSetMetaDataFactory();
  
  /**
   * Return a SchemaFactory to build a DatabaseSchema
   * 
   * @return a schema factory
   */
  SchemaFactory getSchemaFactory();
  
  /**
   * 
   * Return a DriverComplianceFactory capable of returning DriverCompliance instances. 
   * 
   * @return a driver compliance factory
   */
  DriverComplianceFactory getDriverComplianceFactory();
}
