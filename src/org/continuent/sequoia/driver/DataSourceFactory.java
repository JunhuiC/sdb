/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
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
 * Initial developer(s): Marek Prochazka.
 * Contributor(s): 
 */

package org.continuent.sequoia.driver;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * <code>DataSource</code> factory for to implement <code>Referenceable</code>.
 * The factory serves for the {@link DataSource},<code>PooledDataSource</code>,
 * and <code>XADataSource</code> classes.
 * 
 * @author <a href="mailto:Marek.Prochazka@inrialpes.fr">Marek Prochazka</a>
 * @version 1.0
 */
public class DataSourceFactory implements ObjectFactory
{
  /** DataSource classnames. */
  protected final String dataSourceClassName = "org.continuent.sequoia.driver.DataSource";
  protected final String poolDataSourceName  = "org.continuent.sequoia.driver.PoolDataSource";
  protected final String xaDataSourceName    = "org.continuent.sequoia.driver.XADataSource";

  /**
   * Gets an instance of the requested <code>DataSource</code> object.
   * 
   * @param objRef object containing location or reference information that is
   *          used to create an object instance (could be <code>null</code>).
   * @param name name of this object relative to specified <code>nameCtx</code>
   *          (could be <code>null</code>).
   * @param nameCtx name context (could ne null if the default initial context
   *          is used).
   * @param env environment to use (could be null if default is used)
   * @return a newly created instance of Sequoia DataSource, <code>null</code>
   *         if an error occurred.
   * @throws Exception if an error occurs when creating the object instance.
   */
  public Object getObjectInstance(Object objRef, Name name, Context nameCtx,
      Hashtable env) throws Exception
  {
    // Check the requested object class
    Reference ref = (Reference) objRef;
    String className = ref.getClassName();
    if ((className == null)
        || !(className.equals(dataSourceClassName)
            | className.equals(poolDataSourceName) | className
            .equals(xaDataSourceName)))
    {
      // Wrong class
      return null;
    }
    DataSource ds = null;
    try
    {
      ds = (DataSource) Class.forName(className).newInstance();
    }
    catch (Exception e)
    {
      throw new RuntimeException("Error when creating Sequoia " + className
          + " instance: " + e);
    }

    ds.setUrl((String) ref.get(DataSource.URL_PROPERTY).getContent());
    ds.setUser((String) ref.get(DataSource.USER_PROPERTY).getContent());
    ds.setPassword((String) ref.get(DataSource.PASSWORD_PROPERTY).getContent());
    return ds;
  }
}
