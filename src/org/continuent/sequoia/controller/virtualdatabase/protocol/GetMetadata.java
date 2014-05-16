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
 * Initial developer(s): Damian Arregui.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.ControllerException;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseDynamicMetaData;

/**
 * This class defines a GetMetadata command.
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damián Arregui</a>
 * @version 1.0
 */
public class GetMetadata extends DistributedVirtualDatabaseMessage
{
  private static final long serialVersionUID = -3691893740718177069L;

  private String            methodName;
  private Class[]           argTypes;
  private Object[]          args;

  /**
   * Creates a new <code>GetMetadata</code> object
   * 
   * @param methodName name of the metdata-related method to be invoked.
   * @param argTypes array describing the types of method arguments.
   * @param args array of method arguments.
   */
  public GetMetadata(String methodName, Class[] argTypes, Object[] args)
  {
    this.methodName = methodName;
    this.argTypes = argTypes;
    this.args = args;
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageSingleThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member)
   */
  public Object handleMessageSingleThreaded(DistributedVirtualDatabase dvdb,
      Member sender)
  {

    try
    {
      return VirtualDatabaseDynamicMetaData.class.getMethod(methodName,
          argTypes).invoke(dvdb.getDynamicMetaData(), args);
    }
    catch (IllegalArgumentException e)
    {
      return new ControllerException(e);
    }
    catch (SecurityException e)
    {
      return new ControllerException(e);
    }
    catch (IllegalAccessException e)
    {
      return new ControllerException(e);
    }
    catch (InvocationTargetException e)
    {
      return new ControllerException(e);
    }
    catch (NoSuchMethodException e)
    {
      return new ControllerException(e);
    }
  }

  /**
   * @see org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedVirtualDatabaseMessage#handleMessageMultiThreaded(org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase,
   *      org.continuent.hedera.common.Member, java.lang.Object)
   */
  public Serializable handleMessageMultiThreaded(
      DistributedVirtualDatabase dvdb, Member sender,
      Object handleMessageSingleThreadedResult)
  {
    return (Serializable) handleMessageSingleThreadedResult;
  }

}
