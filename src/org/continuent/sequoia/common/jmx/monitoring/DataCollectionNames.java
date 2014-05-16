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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.jmx.monitoring;

import org.continuent.sequoia.common.i18n.Translate;

/**
 * Name convertions for data collection types.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 */
public final class DataCollectionNames
{
  /**
   * Convert the type reference of a collector into a <code>String</code>
   * 
   * @param dataType see DataCollection
   * @return a <code>String</code> that describes the collector
   */
  public static String get(int dataType)
  {
    switch (dataType)
    {
      /*
       * Controller Collectors
       */
      case DataCollection.CONTROLLER_TOTAL_MEMORY :
        return Translate.get("monitoring.controller.total.memory"); //$NON-NLS-1$
      case DataCollection.CONTROLLER_USED_MEMORY :
        return Translate.get("monitoring.controller.used.memory"); //$NON-NLS-1$
      case DataCollection.CONTROLLER_WORKER_PENDING_QUEUE :
        return Translate.get("monitoring.controller.pending.queue"); //$NON-NLS-1$
      case DataCollection.CONTROLLER_THREADS_NUMBER :
        return Translate.get("monitoring.controller.threads.number"); //$NON-NLS-1$
      case DataCollection.CONTROLLER_IDLE_WORKER_THREADS :
        return Translate.get("monitoring.controller.idle.worker.threads"); //$NON-NLS-1$
      /*
       * Backend collectors
       */
      case DataCollection.BACKEND_ACTIVE_TRANSACTION :
        return Translate.get("monitoring.backend.active.transactions"); //$NON-NLS-1$
      case DataCollection.BACKEND_PENDING_REQUESTS :
        return Translate.get("monitoring.backend.pending.requests"); //$NON-NLS-1$
      case DataCollection.BACKEND_TOTAL_ACTIVE_CONNECTIONS :
        return Translate.get("monitoring.backend.active.connections"); //$NON-NLS-1$
      case DataCollection.BACKEND_TOTAL_REQUEST :
        return Translate.get("monitoring.backend.total.requests"); //$NON-NLS-1$
      case DataCollection.BACKEND_TOTAL_READ_REQUEST :
        return Translate.get("monitoring.backend.total.read.requests"); //$NON-NLS-1$
      case DataCollection.BACKEND_TOTAL_WRITE_REQUEST :
        return Translate.get("monitoring.backend.total.write.requests"); //$NON-NLS-1$
      case DataCollection.BACKEND_TOTAL_TRANSACTIONS :
        return Translate.get("monitoring.backend.total.transactions"); //$NON-NLS-1$
      /*
       * VirtualDatabase collectors
       */
      case DataCollection.DATABASES_ACTIVE_THREADS :
        return Translate.get("monitoring.virtualdatabase.active.threads"); //$NON-NLS-1$
      case DataCollection.DATABASES_PENDING_CONNECTIONS :
        return Translate.get("monitoring.virtualdatabase.pending.connections"); //$NON-NLS-1$
      case DataCollection.DATABASES_NUMBER_OF_THREADS :
        return Translate.get("monitoring.virtualdatabase.threads.count"); //$NON-NLS-1$
      /*
       * Cache stats collectors
       */
      case DataCollection.CACHE_STATS_COUNT_HITS :
        return Translate.get("monitoring.cache.count.hits"); //$NON-NLS-1$
      case DataCollection.CACHE_STATS_COUNT_INSERT :
        return Translate.get("monitoring.cache.count.insert"); //$NON-NLS-1$
      case DataCollection.CACHE_STATS_COUNT_SELECT :
        return Translate.get("monitoring.cache.count.select"); //$NON-NLS-1$
      case DataCollection.CACHE_STATS_HITS_PERCENTAGE :
        return Translate.get("monitoring.cache.hits.ratio"); //$NON-NLS-1$
      case DataCollection.CACHE_STATS_NUMBER_ENTRIES :
        return Translate.get("monitoring.cache.number.entries"); //$NON-NLS-1$
      /*
       * Scheduler collectors
       */
      case DataCollection.SCHEDULER_NUMBER_READ :
        return Translate.get("monitoring.scheduler.number.read"); //$NON-NLS-1$
      case DataCollection.SCHEDULER_NUMBER_REQUESTS :
        return Translate.get("monitoring.scheduler.number.requests"); //$NON-NLS-1$
      case DataCollection.SCHEDULER_NUMBER_WRITES :
        return Translate.get("monitoring.scheduler.number.writes"); //$NON-NLS-1$
      case DataCollection.SCHEDULER_PENDING_TRANSACTIONS :
        return Translate.get("monitoring.scheduler.pending.transactions"); //$NON-NLS-1$
      case DataCollection.SCHEDULER_PENDING_WRITES :
        return Translate.get("monitoring.scheduler.pending.writes"); //$NON-NLS-1$
      /*
       * Client collectors
       */
      case DataCollection.CLIENT_TIME_ACTIVE :
        return Translate.get("monitoring.client.active.time"); //$NON-NLS-1$

      /*
       * Unknown collector
       */
      default :
        return "";
    }
  }

  /**
   * Return the type of the collector corresponding to the command
   * 
   * @param command to get type form
   * @return an <code>int</code>
   */
  public static int getTypeFromCommand(String command)
  {
    command = command.replace('_', ' ');
    /*
     * Controller Collectors
     */
    if (command.equalsIgnoreCase(Translate
        .get("monitoring.controller.total.memory"))) //$NON-NLS-1$
      return DataCollection.CONTROLLER_TOTAL_MEMORY;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.controller.used.memory"))) //$NON-NLS-1$
      return DataCollection.CONTROLLER_USED_MEMORY;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.controller.pending.queue"))) //$NON-NLS-1$
      return DataCollection.CONTROLLER_WORKER_PENDING_QUEUE;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.controller.threads.number"))) //$NON-NLS-1$
      return DataCollection.CONTROLLER_THREADS_NUMBER;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.controller.idle.worker.threads"))) //$NON-NLS-1$
      return DataCollection.CONTROLLER_IDLE_WORKER_THREADS;

    /*
     * Backend collectors
     */
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.backend.active.transactions"))) //$NON-NLS-1$
      return DataCollection.BACKEND_ACTIVE_TRANSACTION;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.backend.pending.requests"))) //$NON-NLS-1$
      return DataCollection.BACKEND_PENDING_REQUESTS;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.backend.active.connections"))) //$NON-NLS-1$
      return DataCollection.BACKEND_TOTAL_ACTIVE_CONNECTIONS;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.backend.total.read.requests"))) //$NON-NLS-1$
      return DataCollection.BACKEND_TOTAL_READ_REQUEST;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.backend.total.write.requests"))) //$NON-NLS-1$
      return DataCollection.BACKEND_TOTAL_WRITE_REQUEST;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.backend.total.requests"))) //$NON-NLS-1$
      return DataCollection.BACKEND_TOTAL_REQUEST;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.backend.total.transactions"))) //$NON-NLS-1$
      return DataCollection.BACKEND_TOTAL_TRANSACTIONS;

    /*
     * VirtualDatabase collectors
     */
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.virtualdatabase.active.threads"))) //$NON-NLS-1$
      return DataCollection.DATABASES_ACTIVE_THREADS;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.virtualdatabase.pending.connections"))) //$NON-NLS-1$
      return DataCollection.DATABASES_PENDING_CONNECTIONS;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.virtualdatabase.threads.count"))) //$NON-NLS-1$
      return DataCollection.DATABASES_NUMBER_OF_THREADS;

    /*
     * Cache stats collectors
     */
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.cache.count.hits"))) //$NON-NLS-1$
      return DataCollection.CACHE_STATS_COUNT_HITS;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.cache.count.insert"))) //$NON-NLS-1$
      return DataCollection.CACHE_STATS_COUNT_INSERT;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.cache.count.select"))) //$NON-NLS-1$
      return DataCollection.CACHE_STATS_COUNT_SELECT;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.cache.hits.ratio"))) //$NON-NLS-1$
      return DataCollection.CACHE_STATS_HITS_PERCENTAGE;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.cache.number.entries"))) //$NON-NLS-1$
      return DataCollection.CACHE_STATS_NUMBER_ENTRIES;

    /*
     * Scheduler collectors
     */
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.scheduler.number.read"))) //$NON-NLS-1$
      return DataCollection.SCHEDULER_NUMBER_READ;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.scheduler.number.requests"))) //$NON-NLS-1$
      return DataCollection.SCHEDULER_NUMBER_REQUESTS;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.scheduler.number.writes"))) //$NON-NLS-1$
      return DataCollection.SCHEDULER_NUMBER_WRITES;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.scheduler.pending.transactions"))) //$NON-NLS-1$
      return DataCollection.SCHEDULER_PENDING_TRANSACTIONS;
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.scheduler.pending.writes"))) //$NON-NLS-1$
      return DataCollection.SCHEDULER_PENDING_WRITES;

    /*
     * Client collectors
     */
    else if (command.equalsIgnoreCase(Translate
        .get("monitoring.client.active.time"))) //$NON-NLS-1$
      return DataCollection.CLIENT_TIME_ACTIVE;

    else
      return 0;
  }
}
