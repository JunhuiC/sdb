/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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
 * Initial developer(s): Jeff Mesnil.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backend.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.NotCompliantMBeanException;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean;
import org.continuent.sequoia.common.locks.TransactionLogicalLock;
import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;

/**
 * This class defines a DatabaseBackend MBean.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DatabaseBackend extends AbstractStandardMBean
    implements
      DatabaseBackendMBean
{
  private org.continuent.sequoia.controller.backend.DatabaseBackend managedBackend;

  /**
   * Creates a new Managed <code>DatabaseBackend</code> object
   * 
   * @param managedBackend the backend that is managed
   * @throws NotCompliantMBeanException if this MBean is not JMX compliant
   */
  public DatabaseBackend(
      org.continuent.sequoia.controller.backend.DatabaseBackend managedBackend)
      throws NotCompliantMBeanException
  {
    super(DatabaseBackendMBean.class);
    this.managedBackend = managedBackend;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#isInitialized()
   */
  public boolean isInitialized() throws Exception
  {
    return managedBackend.isInitialized();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#isReadEnabled()
   */
  public boolean isReadEnabled()
  {
    return managedBackend.isReadEnabled();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#isWriteEnabled()
   */
  public boolean isWriteEnabled()
  {
    return managedBackend.isWriteEnabled();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#isDisabled()
   */
  public boolean isDisabled()
  {
    return managedBackend.isDisabled();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#enableRead()
   */
  public void enableRead()
  {
    managedBackend.enableRead();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#enableWrite()
   */
  public void enableWrite()
  {
    managedBackend.enableWrite();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#disableRead()
   */
  public void disableRead()
  {
    managedBackend.disableRead();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#disableWrite()
   */
  public void disableWrite()
  {
    managedBackend.disableWrite();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#disable()
   */
  public void disable()
  {
    managedBackend.disable();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getConnectionTestStatement()
   */
  public String getConnectionTestStatement()
  {
    return managedBackend.getConnectionTestStatement();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getDriverClassName()
   */
  public String getDriverClassName()
  {
    return managedBackend.getDriverClassName();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getName()
   */
  public String getName()
  {
    return managedBackend.getName();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getState()
   */
  public String getState()
  {
    return managedBackend.getState();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getStateValue()
   */
  public int getStateValue()
  {
    return managedBackend.getStateValue();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getPendingRequestsDescription(int,
   *      boolean, boolean)
   */
  public List getPendingRequestsDescription(int count, boolean fromFirst,
      boolean clone)
  {
    return managedBackend
        .getPendingRequestsDescription(count, fromFirst, clone);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getActiveTransactions()
   */
  public List getActiveTransactions()
  {
    return managedBackend.getActiveTransactions();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getURL()
   */
  public String getURL()
  {
    return managedBackend.getURL();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getTablesNames()
   */
  public String[] getTablesNames()
  {
    Set uniqueValues = new HashSet(managedBackend.getTables());
    Object[] tables = uniqueValues.toArray();
    String[] res = new String[tables.length];
    for (int i = 0; i < tables.length; i++)
    {
      DatabaseTable table = (DatabaseTable) tables[i];
      String fqtn = table.getSchema() + "." + table.getName(); //$NON-NLS-1$
      res[i] = fqtn;
    }
    return res;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getColumnsNames(String)
   */
  public String[] getColumnsNames(String tableName)
  {
    DatabaseTable theTable = getTable(tableName);

    if (theTable == null)
      return null;
    // throw new VirtualDatabaseException("no such table");

    ArrayList l = theTable.getColumns();

    String[] ret = new String[l.size()];

    for (int i = 0; i < ret.length; i++)
    {
      DatabaseColumn column = (DatabaseColumn) l.get(i);
      ret[i] = column.getName();
    }
    return ret;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getLockInfo(String)
   */
  public String getLockInfo(String tableName)
  {
    DatabaseTable theTable = getTable(tableName);
    if (theTable == null)
      return null;
    TransactionLogicalLock tll = theTable.getLock();
    if (tll.isLocked())
    {
      return Translate.get("backend.locker", tll.getLocker()); //$NON-NLS-1$
      // FIXME: print waiting list ?
    }
    else
    {
      return Translate.get("backend.no.lock"); //$NON-NLS-1$
    }
  }

  /**
   * Retrieves a table from its fully qualified name.
   * 
   * @param tableName table name under the for <code><schema>.<table></code>
   * @return the corresponding table in the current backend
   */
  private DatabaseTable getTable(String tableName)
  {
    Set uniqueValues = new HashSet(managedBackend.getTables());
    Object[] tables = uniqueValues.toArray();

    DatabaseTable theTable = null;
    for (int i = 0; i < tables.length; i++)
    {
      DatabaseTable table = (DatabaseTable) tables[i];
      String fqtn = table.getSchema() + "." + table.getName(); //$NON-NLS-1$
      if (fqtn.equals(tableName))
      {
        theTable = table;
        break;
      }
    }
    return theTable;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getStoredProceduresNames()
   */
  public String[] getStoredProceduresNames()
  {
    HashMap hm = managedBackend.getDatabaseSchema().getProcedures();
    Set keys = hm.keySet();
    String[] res = new String[keys.size()];
    int i = 0;
    for (Iterator iter = keys.iterator(); iter.hasNext(); i++)
    {
      res[i] = (String) iter.next();
    }
    return res;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#isSchemaStatic()
   */
  public boolean isSchemaStatic()
  {
    return managedBackend.isSchemaStatic();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getDriverPath()
   */
  public String getDriverPath()
  {
    return managedBackend.getDriverPath();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getLastKnownCheckpoint()
   */
  public String getLastKnownCheckpoint()
  {
    return managedBackend.getLastKnownCheckpoint();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#isJDBCConnected()
   */
  public boolean isJDBCConnected()
  {
    return managedBackend.isJDBCConnected();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getSchemaXml(boolean)
   */
  public String getSchemaXml(boolean expandSchema)
  {
    return managedBackend.getSchemaXml(expandSchema);
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.DatabaseBackendMBean#getXml()
   */
  public String getXml()
  {
    return managedBackend.getXml();
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "backend"; //$NON-NLS-1$
  }

}
