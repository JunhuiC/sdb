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

/**
 * This public static final interface is used as a reference to what kind of
 * information can be collected and monitored with the mbean monitor.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 */
public final class DataCollection
{
  /** Backend active transaction identifier */
  public static final int BACKEND_ACTIVE_TRANSACTION       = 10;
  /** Backend pending request identifier */
  public static final int BACKEND_PENDING_REQUESTS         = 11;
  /** Backend active connections identifier */
  public static final int BACKEND_TOTAL_ACTIVE_CONNECTIONS = 12;
  /** Backend total request identifier */
  public static final int BACKEND_TOTAL_REQUEST            = 13;
  /** Backend total read request */
  public static final int BACKEND_TOTAL_READ_REQUEST       = 14;
  /** Backend total write request */
  public static final int BACKEND_TOTAL_WRITE_REQUEST      = 15;
  /** Backend total transactions identifier */
  public static final int BACKEND_TOTAL_TRANSACTIONS       = 16;

  /** Cache statistics count select identifier */
  public static final int CACHE_STATS_COUNT_SELECT         = 20;
  /** Cache statistics count hits identifier */
  public static final int CACHE_STATS_COUNT_HITS           = 21;
  /** Cache statistics count insert identifier */
  public static final int CACHE_STATS_COUNT_INSERT         = 22;
  /** Cache statistics hit percentage identifier */
  public static final int CACHE_STATS_HITS_PERCENTAGE      = 23;
  /** Cache statistics number of entries identifier */
  public static final int CACHE_STATS_NUMBER_ENTRIES       = 24;

  /** Client time active identifier */
  public static final int CLIENT_TIME_ACTIVE               = 32;

  /** Controller total memory identifier */
  public static final int CONTROLLER_TOTAL_MEMORY          = 40;
  /** Controller used memory identifier */
  public static final int CONTROLLER_USED_MEMORY           = 41;
  /** Controller thread number identifier */
  public static final int CONTROLLER_THREADS_NUMBER        = 42;
  /** Controller worker pending queue identifier */
  public static final int CONTROLLER_WORKER_PENDING_QUEUE  = 43;
  /** Controller idle worker threads identifier */
  public static final int CONTROLLER_IDLE_WORKER_THREADS   = 44;

  /** Database active threads identifier */
  public static final int DATABASES_ACTIVE_THREADS         = 50;
  /** Database pending connection identifier */
  public static final int DATABASES_PENDING_CONNECTIONS    = 51;
  /** Database number threads identifier */
  public static final int DATABASES_NUMBER_OF_THREADS      = 52;

  /** Scheduler number read identifier */
  public static final int SCHEDULER_NUMBER_READ            = 60;
  /** Scheduler number writes identifier */
  public static final int SCHEDULER_NUMBER_WRITES          = 61;
  /** Scheduler pending transactions identifier */
  public static final int SCHEDULER_PENDING_TRANSACTIONS   = 62;
  /** Scheduler pending writes identifier */
  public static final int SCHEDULER_PENDING_WRITES         = 63;
  /** Scheduler number requests identifier */
  public static final int SCHEDULER_NUMBER_REQUESTS        = 64;
}
