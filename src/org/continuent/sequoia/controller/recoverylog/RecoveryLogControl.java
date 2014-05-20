/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent.
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

package org.continuent.sequoia.controller.recoverylog;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;

/**
 * RecoveryLogControlMBean implemementation.<br />
 * Used to manage a RecoveryLog.
 * 
 * @see org.continuent.sequoia.controller.recoverylog.RecoveryLog
 */
public class RecoveryLogControl extends AbstractStandardMBean
    implements
      RecoveryLogControlMBean
{

  private RecoveryLog             managedRecoveryLog;
  private final Trace             logger               = Trace
                                                           .getLogger("org.continuent.sequoia.controller.recoverylog"); //$NON-NLS-1$

  private static final int        ENTRIES_PER_DUMP     = 100;

  private TabularType             logEntriesType;
  private CompositeType           logEntryType;

  /**
   * Creates a new <code>RecoveryLogControl</code> object
   * 
   * @param recoveryLog the managed RecoveryLog
   * @throws NotCompliantMBeanException if this mbean is not compliant
   */
  public RecoveryLogControl(RecoveryLog recoveryLog)
      throws NotCompliantMBeanException
  {
    super(RecoveryLogControlMBean.class);
    this.managedRecoveryLog = recoveryLog;
    defineTypes();
  }

  /**
   * Defines the OpenType used by this mbean.
   * 
   * @see #dump(int)
   */
  private void defineTypes()
  {
    try
    {
      String[] headers = getHeaders();
      logEntryType = new CompositeType(
          "LogEntry", //$NON-NLS-1$
          Translate.get("RecoveryLogControl.LogEntryDescription"), headers, headers, //$NON-NLS-1$
          getEntryTypes(headers.length));
      logEntriesType = new TabularType(
          "LogEntries", //$NON-NLS-1$
          Translate.get("RecoveryLogControl.LogEntriesDescription"), logEntryType, headers); //$NON-NLS-1$
    }
    catch (OpenDataException e)
    {
      if (logger.isWarnEnabled())
      {
        logger.warn(Translate.get("RecoveryLogControl.openTypeWarning"), e); //$NON-NLS-1$
      }
    }
  }

  private OpenType<?>[] getEntryTypes(int length)
  {
    // all column are returned as String
    OpenType<?>[] types = new OpenType[length];
    for (int i = 0; i < types.length; i++)
    {
      types[i] = SimpleType.STRING;
    }
    return types;
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "recoverylog"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean#getHeaders()
   */
  public String[] getHeaders()
  {
    return managedRecoveryLog.getColumnNames();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean#dump(int)
   */
  public TabularData dump(long from)
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("dumping recovery log from index " + from + " ("
          + ENTRIES_PER_DUMP + " entries per page)");
    }
    String[][] entries = managedRecoveryLog.getLogEntries(from,
        ENTRIES_PER_DUMP);
    TabularData result = toTabularData(entries);
    return result;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean#getEntriesPerDump()
   */
  public int getEntriesPerDump()
  {
    return ENTRIES_PER_DUMP;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean#getIndexes()
   */
  public long[] getIndexes()
  {
    try
    {
      return managedRecoveryLog.getIndexes();
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
      {
        // FIXME we should specify the VBD the managed recovery log belongs to
        logger.warn(
            "unable to get the min and max indexes of the recovery log", e);
      }
      return null;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean#getEntries()
   */
  public long getEntries()
  {
    try
    {
      return managedRecoveryLog.getNumberOfLogEntries();
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
      {
        // FIXME we should specify the VBD the managed recovery log belongs to
        logger.warn(
            "unable to get the number of log entries in the recovery log", e);
      }
      return -1;
    }
  }
  
  public TabularData getRequestsInTransaction(long tid)
  {
    String[][] entriesInTransaction = managedRecoveryLog.getLogEntriesInTransaction(tid);
    TabularData result = toTabularData(entriesInTransaction);
    return result;
  }

  /**
   * Helper method to convert a matrix of String to a TabularData.
   * 
   * @param entries a matrix of String
   * @return a TabularData
   */
  private TabularData toTabularData(String[][] entries)
  {
    TabularData entriesData = new TabularDataSupport(logEntriesType);
    for (int i = 0; i < entries.length; i++)
    {
      String[] entry = entries[i];
      CompositeData entryData = toCompositeData(entry);
      if (entryData != null)
      {
        entriesData.put(entryData);
      }
    }
    return entriesData;
  }

  /**
   * Helper method to convert a array of String to a CompositeData.<br />
   * <strong>this method assumes that all the types of the CompositeData are of
   * SimpleType.STRING.</strong>
   * 
   * @param entry an array of String
   * @return a CompositeData
   * @see SimpleType#STRING
   */
  private CompositeData toCompositeData(String[] entry)
  {
    try
    {
      CompositeData entryData = new CompositeDataSupport(logEntryType,
          getHeaders(), entry);
      return entryData;
    }
    catch (OpenDataException e)
    {
      if (logger.isWarnEnabled())
      {
        logger.warn(Translate.get(
            "RecoveryLogControl.conversionWarning", Arrays.asList(entry)), e); //$NON-NLS-1$
      }
      return null;
    }
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.RecoveryLogControlMBean#getCheckpoints()
   */
  public Map<?, ?> getCheckpoints()
  {
    try
    {
      return managedRecoveryLog.getCheckpoints();
    }
    catch (SQLException e)
    {
      if (logger.isWarnEnabled())
      {
        logger.warn(e.getMessage(), e);
      }
      return new HashMap<Object,Object>();
    }
  }
}
