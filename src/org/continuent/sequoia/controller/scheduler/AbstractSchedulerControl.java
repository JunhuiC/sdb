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

package org.continuent.sequoia.controller.scheduler;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.TabularData;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.management.TransactionDataSupport;
import org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean;
import org.continuent.sequoia.controller.jmx.AbstractStandardMBean;
import org.continuent.sequoia.controller.requestmanager.TransactionMetaData;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * AbstractSchedulerControlMBean implemementation. Used to manage Schedulers
 * 
 * @see org.continuent.sequoia.controller.scheduler.AbstractScheduler
 * @see org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean
 */
public class AbstractSchedulerControl extends AbstractStandardMBean
    implements
      AbstractSchedulerControlMBean
{
  private AbstractScheduler managedScheduler;

  /**
   * Creates a new <code>AbstractSchedulerControl</code> object
   * 
   * @param scheduler the managed AbstractScheduler
   * @throws NotCompliantMBeanException if this mbean is not compliant
   */
  public AbstractSchedulerControl(AbstractScheduler scheduler)
      throws NotCompliantMBeanException
  {
    super(AbstractSchedulerControlMBean.class);
    this.managedScheduler = scheduler;
  }

  /**
   * @see org.continuent.sequoia.controller.jmx.AbstractStandardMBean#getAssociatedString()
   */
  public String getAssociatedString()
  {
    return "abstractscheduler"; //$NON-NLS-1$
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean#listActiveTransactionIds()
   */
  public long[] listActiveTransactionIds()
  {
    List transactions = managedScheduler.getActiveTransactions();
    int sz = transactions.size();
    long[] res = new long[sz];
    for (int i = 0; i < sz; i++)
    {
      TransactionMetaData tmd = (TransactionMetaData) transactions.get(i);
      res[i] = tmd.getTransactionId();
    }
    return res;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean#getActiveTransactions()
   */
  public TabularData getActiveTransactions() throws Exception
  {
    List transactions = managedScheduler.getActiveTransactions();
    TabularData data = TransactionDataSupport.newTabularData();
    long now = System.currentTimeMillis();
    for (int i = 0; i < transactions.size(); i++)
    {
      TransactionMetaData tmd = (TransactionMetaData) transactions.get(i);
      long time = (now - tmd.getTimestamp()) / 1000;
      data.put(TransactionDataSupport.newCompositeData(tmd.getTransactionId(),
          time));
    }
    return data;
  }

  private long[] listPendingRequestIds(Map requests)
  {
    Set reqIds = requests.keySet();
    long[] res = new long[reqIds.size()];
    int i = 0;
    for (Iterator iter = reqIds.iterator(); iter.hasNext(); i++)
    {
      Long l = (Long) iter.next();
      res[i] = l.longValue();
    }
    return res;
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean#listPendingWriteRequestIds()
   */
  public long[] listPendingWriteRequestIds()
  {
    return listPendingRequestIds(managedScheduler.getActiveWriteRequests());
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean#listPendingReadRequestIds()
   */
  public long[] listPendingReadRequestIds()
  {
    return listPendingRequestIds(managedScheduler.getActiveReadRequests());
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean#listOpenPersistentConnections()
   */
  public Hashtable listOpenPersistentConnections()
  {
    return managedScheduler.getOpenPersistentConnections();
  }

  /**
   * @see org.continuent.sequoia.common.jmx.mbeans.AbstractSchedulerControlMBean#dumpRequest(long)
   */
  public String dumpRequest(long requestId)
  {
    AbstractRequest request = null;
    Map writeRequests = managedScheduler.getActiveWriteRequests();
    synchronized (writeRequests)
    {
      request = (AbstractRequest) writeRequests.get(new Long(requestId));
    }
    if (request == null)
    {
      Map readRequests = managedScheduler.getActiveReadRequests();
      synchronized (readRequests)
      {
        request = (AbstractRequest) readRequests.get(new Long(requestId));
      }
    }
    if (request == null)
    {
      return Translate.get("scheduler.request.notActive", requestId);
    }
    return request.toDebugString();
  }
}
