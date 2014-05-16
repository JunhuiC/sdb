/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2007 Continuent, Inc.
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
 * Initial developer(s): Robert Hodges.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.interceptors.impl;

import org.continuent.sequoia.controller.interceptors.RequestFacade;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * Implements a request facade for Sequoia, which wraps an AbstractRequest
 * instance. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RequestFacadeImpl implements RequestFacade
{
  private final AbstractRequest request; 
  
  public RequestFacadeImpl(AbstractRequest request)
  {
    this.request = request; 
  }

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.RequestFacade#getId()
   */
  public long getId()
  {
    return request.getId();
  }

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.RequestFacade#getSql()
   */
  public String getSql()
  {
    return request.getSqlOrTemplate();
  }

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.RequestFacade#getTransactionId()
   */
  public long getTransactionId()
  {
    return request.getTransactionId();
  }

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.RequestFacade#isReadOnly()
   */
  public boolean isReadOnly()
  {
    return request.isReadOnly() || request instanceof SelectRequest;
  }

  /**
   * {@inheritDoc}
   * @see org.continuent.sequoia.controller.interceptors.RequestFacade#setSql(java.lang.String)
   */
  public void setSql(String sql)
  {
    request.setSqlOrTemplate(sql);
  }  
}