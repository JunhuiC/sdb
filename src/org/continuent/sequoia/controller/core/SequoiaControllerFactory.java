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
import org.continuent.sequoia.controller.backend.SequoiaBackendObjectConverter;
import org.continuent.sequoia.controller.backend.SequoiaDriverComplianceFactory;
import org.continuent.sequoia.controller.backend.SequoiaResultSetMetaDataFactory;
import org.continuent.sequoia.controller.backend.SchemaFactory;
import org.continuent.sequoia.controller.backend.SequoiaSchemaFactory;
import org.continuent.sequoia.controller.requests.RequestFactory;
import org.continuent.sequoia.controller.requests.SequoiaRequestFactory;

/**
 * This class defines a SequoiaControllerFactory
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class SequoiaControllerFactory implements ControllerFactory
{
  private static final RequestFactory           REQUEST_FACTORY          = new SequoiaRequestFactory();
  private static final BackendObjectConverter   BACKEND_OBJECT_CONVERTER = new SequoiaBackendObjectConverter();
  private static final ResultSetMetaDataFactory RESULTSET_METADATA       = new SequoiaResultSetMetaDataFactory();
  private static final SchemaFactory            SCHEMA_FACTORY           = new SequoiaSchemaFactory();
  private static final DriverComplianceFactory  DRIVER_COMPLIANCE_FACTORY = new SequoiaDriverComplianceFactory();
  
  /**
   * @see org.continuent.sequoia.controller.core.ControllerFactory#getBackendObjectConverter()
   */
  public BackendObjectConverter getBackendObjectConverter()
  {
    return BACKEND_OBJECT_CONVERTER;
  }

  /**
   * @see org.continuent.sequoia.controller.core.ControllerFactory#getRequestFactory()
   */
  public RequestFactory getRequestFactory()
  {
    return REQUEST_FACTORY;
  }

  /**
   * @see org.continuent.sequoia.controller.core.ControllerFactory#getResultSetMetaDataFactory()
   */
  public ResultSetMetaDataFactory getResultSetMetaDataFactory()
  {
    return RESULTSET_METADATA;
  }

  /**
   * @see org.continuent.sequoia.controller.core.ControllerFactory#getSchemaFactory()
   */
  public SchemaFactory getSchemaFactory()
  {
    return SCHEMA_FACTORY;
  }

  /**
   * @see org.continuent.sequoia.controller.core.ControllerFactory#getDriverComplianceFactory()
   */
  public DriverComplianceFactory getDriverComplianceFactory()
  {
    return DRIVER_COMPLIANCE_FACTORY;
  }
}
