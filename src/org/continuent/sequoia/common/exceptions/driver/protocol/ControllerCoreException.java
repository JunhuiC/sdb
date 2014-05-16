/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks
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
 * Initial developer(s): Marc Herbert
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.exceptions.driver.protocol;

import java.io.IOException;
import java.sql.SQLException;

import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoMoreControllerException;
import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.stream.DriverBufferedInputStream;

/**
 * This class is meant for exceptions originated in controller core (i.e.,
 * non-backend) that are serialized to the driver.
 * 
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @version 1.0
 */
public class ControllerCoreException extends SerializableException
{
  private static final long serialVersionUID   = -4526646797888340909L;

  private static final int  UNKNOWN            = 0;
  private static final int  NO_MORE_BACKEND    = 1;
  private static final int  NO_MORE_CONTROLLER = 2;
  private static final int  NOT_IMPLEMENTED    = 3;

  private static int exceptionTypeCode(Throwable ex)
  {
    if (ex instanceof NoMoreBackendException)
      return NO_MORE_BACKEND;
    if (ex instanceof NoMoreControllerException)
      return NO_MORE_CONTROLLER;
    if (ex instanceof NotImplementedException)
      return NOT_IMPLEMENTED;

    return UNKNOWN;
  }

  /**
   * This method returns a wrapper around 'this' ControllerCoreException, in
   * order to stay bug for bug compatible with legacy exception handling code.
   * This method should disappear and be implemented above, directly by the code
   * calling it.
   * 
   * @deprecated
   * @return a wrapper around this object
   */
  public Exception compatibilityWrapperHack()
  {
    Exception wrapper;
    switch (this.getErrorCode())
    {
      case NO_MORE_BACKEND :
        wrapper = new NoMoreBackendException(getMessage(), getSQLState(),
            getErrorCode());
        break;
      case NO_MORE_CONTROLLER :
        wrapper = new NoMoreControllerException(getMessage(), getSQLState(),
            getErrorCode());
        break;
      case NOT_IMPLEMENTED :
        wrapper = new NotImplementedException(getMessage(), getSQLState(),
            getErrorCode());
        break;
      default : // hey, why not ?
        wrapper = new SQLException(getMessage(), getSQLState(), getErrorCode());
    }
    wrapper.initCause(this);
    return wrapper;
  }

  /**
   * @see SerializableException#SerializableException(DriverBufferedInputStream)
   */
  public ControllerCoreException(DriverBufferedInputStream in) throws IOException
  {
    super(in);
  }

  /**
   * Converts a chain of Throwables to a new chain of SerializableExceptions
   * starting with a <code>ControllerCoreException</code>. The returned chain
   * has the same length.
   * 
   * @param ex head of chain to convert
   * @see SerializableException#SerializableException(Throwable)
   */
  public ControllerCoreException(Throwable ex)
  {
    super(ex);

    // This is the place where we could set your own SQL codes
    // super.SQLState = sqlE.getSQLState();

    // hack: let's use vendorCode int as a type
    setErrorCode(exceptionTypeCode(ex));

  }

}
