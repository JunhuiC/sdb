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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.jmx.notifications;

/**
 * This is the list of the Sequoia notification that can be sent from the mbean
 * server on the controller.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public abstract class SequoiaNotificationList
{

  /**
   * Notification level, notification is not an error
   */
  public static final String NOTIFICATION_LEVEL_INFO                             = "info";

  /**
   * Notification level, notification is an error
   */
  public static final String NOTIFICATION_LEVEL_ERROR                            = "error";

  /**
   * Virtual Database Removed
   */
  public static final String CONTROLLER_VIRTUALDATABASE_REMOVED                  = "sequoia.controller.virtualdatabases.removed";

  /**
   * VirtualDatabase added
   */
  public static final String CONTROLLER_VIRTUALDATABASE_ADDED                    = "sequoia.controller.virtualdatabase.added";

  /**
   * New Dump List for virtual database
   */
  public static final String VIRTUALDATABASE_NEW_DUMP_LIST                       = "sequoia.virtualdatabase.dump.list";

  /**
   * Backend added
   */
  public static final String VIRTUALDATABASE_BACKEND_ADDED                       = "sequoia.virtualdatabase.backend.added";

  /**
   * Controller added for Distributed VirtualDatabase
   */
  public static final String DISTRIBUTED_CONTROLLER_ADDED                        = "sequoia.distributed.controller.added";

  /**
   * Controller removed from Distributed VirtualDatabase
   */
  public static final String DISTRIBUTED_CONTROLLER_REMOVED                      = "sequoia.distributed.controller.removed";

  /**
   * Controller failed for Distributed VirtualDatabase
   */
  public static final String DISTRIBUTED_CONTROLLER_FAILED                       = "sequoia.distributed.controller.failed";

  /**
   * Controller joined for Distributed VirtualDatabase
   */
  public static final String DISTRIBUTED_CONTROLLER_JOINED                       = "sequoia.distributed.controller.joined";

  /**
   * Backend has been disabled
   */
  public static final String VIRTUALDATABASE_BACKEND_DISABLED                    = "sequoia.virtualdatabase.backend.disabled";

  /**
   * Backend has been enabled
   */
  public static final String VIRTUALDATABASE_BACKEND_ENABLED                     = "sequoia.virtualdatabase.backend.enabled";

  /**
   * Backend is recovering
   */
  public static final String VIRTUALDATABASE_BACKEND_RECOVERING                  = "sequoia.virtualdatabase.backend.recovering";

  /**
   * Backend recovery has failed
   */
  public static final String VIRTUALDATABASE_BACKEND_RECOVERY_FAILED             = "sequoia.virtualdatabase.backend.recovery.failed";

  /**
   * Backend replaying has failed
   */
  public static final String VIRTUALDATABASE_BACKEND_REPLAYING_FAILED            = "sequoia.virtualdatabase.backend.replaying.failed";

  /**
   * Backend is backing up
   */
  public static final String VIRTUALDATABASE_BACKEND_BACKINGUP                   = "sequoia.virtualdatabase.backend.backingup";

  /**
   * Backend is read enabled
   */
  public static final String VIRTUALDATABASE_BACKEND_READ_ENABLED_WRITE_ENABLED  = "sequoia.virtualdatabase.backend.enable.read";

  /**
   * Backend is write enabled
   */
  public static final String VIRTUALDATABASE_BACKEND_READ_DISABLED_WRITE_ENABLED = "sequoia.virtualdatabase.backend.enable.write";

  /**
   * Backend has been removed
   */
  public static final String VIRTUALDATABASE_BACKEND_REMOVED                     = "sequoia.virtualdatabase.backend.removed";

  /**
   * Backend is being disabled
   */
  public static final String VIRTUALDATABASE_BACKEND_DISABLING                   = "sequoia.virtualdatabase.backend.disabling";

  /**
   * Backend has been killed by the load balancer. We don't know the state of
   * the backend so we have to restore it
   */
  public static final String VIRTUALDATABASE_BACKEND_UNKNOWN                     = "sequoia.virtualdatabase.backend.unknown";

  /**
   * Replaying content of recovery log
   */
  public static final String VIRTUALDATABASE_BACKEND_REPLAYING                   = "sequoia.virtualdatabase.backend.replaying";

  /**
   * a network partition has been detected on the vdb
   */
  public static final String VDB_NETWORK_PARTITION_DETECTION                     = "vdb.network.partition.detection";                 //$NON-NLS-1$

  /** element that can be found in the data object */
  public static final String DATA_DATABASE                                       = "database";
  /** element that can be found in the data object */
  public static final String DATA_CHECKPOINT                                     = "checkpoint";
  /** element that can be found in the data object */
  public static final String DATA_URL                                            = "url";
  /** element that can be found in the data object */
  public static final String DATA_NAME                                           = "name";
  /** element that can be found in the data object */
  public static final String DATA_DRIVER                                         = "driver";

}