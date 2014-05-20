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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Nicolas Modrzyk, Stephane Giron.
 */

package org.continuent.sequoia.common.jmx.mbeans;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.openmbean.TabularData;

import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.jmx.management.DumpInfo;
import org.continuent.sequoia.common.jmx.monitoring.backend.BackendStatistics;

/**
 * JMX Interface to remotely manage a Virtual Databases.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <A href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <A href="mailto:Stephane.Giron@continuent.com">Stephane Giron </a>
 * @version 1.0
 * @param <E>
 */
public interface VirtualDatabaseMBean
{
  //
  // Methods are organized as follows:
  // 1. Database backends management
  // 2. Checkpoint management
  // 3. Backup management
  // 4. Virtual database management
  //  

  /**
   * Initializes a virtual database from specified Backend. This has to be
   * performed prior to any use of the vdb backends, i.e. there are no active
   * backends in the vdb, and none has a last known checkpoint. This should be
   * immediately followed by a backup operation on the specified backend. This
   * resets the recovery log - if any - to an empty state, i.e. drops logtable,
   * drops checkpoint table, and clears checkpoint names in the dump table.
   * Finally, sets the last known checkpoint of specified backend to
   * Initial_empty_recovery_log.
   * 
   * @param databaseBackendName the name of the backend to use for
   *            initialization.
   * @param force if true, do not perform sanity checks, unconditionally perform
   *            the initialization
   * @throws VirtualDatabaseException if there is an active backend or a backend
   *             which has a last known checkpoint, or if there is no recovery
   *             log for this vdb.
   */
  void initializeFromBackend(String databaseBackendName, boolean force)
      throws VirtualDatabaseException;

  /**
   * Performs initializeFromBackend(databaseBackendName, force=false). This is a
   * convenience method for backward compatibility.
   * 
   * @param databaseBackendName the name of the backend to use for
   *            initialization.
   * @throws VirtualDatabaseException if there is an active backend or a backend
   *             which has a last known checkpoint, or if there is no recovery
   *             log for this vdb.
   * @deprecated use method with explicit force option.
   */
  void initializeFromBackend(String databaseBackendName)
      throws VirtualDatabaseException;

  //
  // Database backends management
  //

  /**
   * Enables a backend that has been previously added to this virtual database
   * and that is in the disabled state. The backend is enabled without further
   * check.
   * 
   * @param databaseBackendName The database backend logical name to enable
   * @exception VirtualDatabaseException in case of communication-related error
   */
  void forceEnableBackend(String databaseBackendName)
      throws VirtualDatabaseException;

  /**
   * Enable the given backend from its last known checkpoint
   * 
   * @param backendName the name of the backend to enable
   * @throws VirtualDatabaseException if enable failed, or if there is no last
   *             known checkpoint
   */
  void enableBackendFromCheckpoint(String backendName, boolean isWrite)
      throws VirtualDatabaseException;

  /**
   * Disables a backend that is currently enabled on this virtual database
   * (without further check).
   * 
   * @param databaseBackendName The database backend logical name to enable
   * @exception VirtualDatabaseException in case of communication-related error
   */
  void forceDisableBackend(String databaseBackendName)
      throws VirtualDatabaseException;

  /**
   * Disables a backend once all the pending write queries are executed. A
   * checkpoint is inserted in the recovery log. The backend must belong to this
   * virtual database. <em>Disabling a disabled backend is a no-operation.</em>
   * 
   * @param databaseBackendName The database backend logical name to disable
   * @exception VirtualDatabaseException in case of communication-related error
   */
  void disableBackendWithCheckpoint(String databaseBackendName)
      throws VirtualDatabaseException;

  /**
   * Get a list of all DatabaseBackend names.
   * 
   * @return List <code>ArrayList</code> of <code>String</code> representing
   *         database backend names
   * @throws VirtualDatabaseException if an error occurs
   */
  List<?> getAllBackendNames() throws VirtualDatabaseException;

  /**
   * Add an additionnal backend to the virtual database with connection managers
   * identical to the backend replicated.
   * 
   * @param backendName the backend to replicate and to use parameters from.
   * @param newBackendName the new backend name.
   * @param parameters parameters to override or modify when replicating to the
   *            new backend
   * @throws VirtualDatabaseException if cannot replicate backend
   */
  void replicateBackend(String backendName, String newBackendName,
      Map<?, ?> parameters) throws VirtualDatabaseException;

  /**
   * Transfer the backend to the destinated controller. Note that this does
   * nothing in a non-distributed environment
   * 
   * @param backend the backend to transfer
   * @param controllerDestination the controller to copy the backend to
   * @throws VirtualDatabaseException if transfer failed
   */
  void transferBackend(String backend, String controllerDestination)
      throws VirtualDatabaseException;

  //
  // Checkpoints management
  //

  /**
   * Copies a chunk of the local virtual database recovery log onto a remote
   * controller's peer virtual database log. The copy is performed from the
   * checkpoint associated to the specified dump uptil 'now' (a new global
   * checkpoint). The copy is sent to the specified remote node.
   * 
   * @param dumpName the name of the dump (which gives associated checkpoint)
   *            from which to perform the copy.
   * @param controllerName the remote controller to send the copy to
   * @throws VirtualDatabaseException if there is no recovery log, or the
   *             virtual database is not distributed, or in case of error.
   */
  void copyLogFromCheckpoint(String dumpName, String controllerName)
      throws VirtualDatabaseException;

  /**
   * Deletes the recovery log (if any) from the begining upto the specified
   * checkpoint.
   * 
   * @param checkpointName the name of the checkpoint upto which to delete the
   *            recovery log.
   * @throws VirtualDatabaseException if there is no recovery log, or in case of
   *             error.
   */
  void deleteLogUpToCheckpoint(String checkpointName)
      throws VirtualDatabaseException;

  /**
   * Sets the last known checkpoint of a backend. This will also update the
   * value in the recovery log
   * 
   * @param backendName backend
   * @param checkpoint checkpoint
   * @throws VirtualDatabaseException if fails
   */
  void setBackendLastKnownCheckpoint(String backendName, String checkpoint)
      throws VirtualDatabaseException;

  // 
  // Backup management
  //

  /**
   * Get the names of the <code>Backupers</code> available from this
   * <code>BackupManager</code>.
   * 
   * @return an (possibly 0-sized) array of <code>String</code> representing
   *         the name of the <code>Backupers</code>
   */
  String[] getBackuperNames();

  /**
   * Get the dump format associated to a given <code>Backuper</code>
   * 
   * @param backuperName name associated to a <code>Backuper</code>
   * @return the dump format associated to a given <code>Backuper</code>
   */
  String getDumpFormatForBackuper(String backuperName);

  /**
   * Create a backup of a specific backend. Note the backend will be disabled if
   * needed during backup, and will be put back to its previous state after
   * backup. If there is only one backend left in the system, this operation
   * will fail when using 'false' as force parameter.
   * 
   * @param backendName the target backend to backup
   * @param login the login to use to connect to the database for the backup
   *            operation
   * @param password the password to use to connect to the database for the
   *            backup operation
   * @param dumpName the name of the dump to create
   * @param backuperName the logical name of the backuper to use
   * @param path the path where to store the dump
   * @param force use true to force the backup even if there is only one backend
   *            left in the system.
   * @param tables the list of tables to backup, null means all tables
   * @throws VirtualDatabaseException if the backup fails
   */
  void backupBackend(String backendName, String login, String password,
      String dumpName, String backuperName, String path, boolean force,
      ArrayList<?> tables) throws VirtualDatabaseException;

  /**
   * This is backupBackend(force=true).
   * 
   * @see backupBackend(String backendName, String login, String password,
   *      String dumpName, String backuperName, String path, boolean force,
   *      ArrayList tables).
   * @deprecated use method with explicit option force.
   */
  void backupBackend(String backendName, String login, String password,
      String dumpName, String backuperName, String path, ArrayList<?> tables)
      throws VirtualDatabaseException;

  /**
   * Get all available dump info for this virtual database
   * 
   * @return an array of <code>DumpInfo</code> containing the available dump
   *         info for this virtual database. Cannot be null but can be empty.
   * @exception VirtualDatabaseException if we can't retrieve dumps
   */
  DumpInfo[] getAvailableDumps() throws VirtualDatabaseException;

  /**
   * Get all available dump info for this virtual database
   * 
   * @return an open-type represenation of the array of <code>DumpInfo</code>
   *         that would be returned by #getAvailableDumps() above.
   * @exception VirtualDatabaseException if we can't retrieve dumps
   */
  TabularData getDumps() throws Exception;

  /**
   * Update the path of the dump for a given dumpName.
   * 
   * @param dumpName name of the dump
   * @param newPath new path for the dump
   * @throws VirtualDatabaseException if cannot update the path
   */
  void updateDumpPath(String dumpName, String newPath)
      throws VirtualDatabaseException;

  /**
   * Delete the dump entry associated to the <code>dumpName</code>.<br />
   * If <code>keepsFile</code> is false, the dump file is also removed from
   * the file system.
   * 
   * @param dumpName name of the dump entry to remove
   * @param keepsFile <code>true</code> if the dump should be also removed
   *            from the file system, <code>false</code> else
   * @throws VirtualDatabaseException if an exception occured while removing the
   *             dump entry or the dump file
   */
  void deleteDump(String dumpName, boolean keepsFile)
      throws VirtualDatabaseException;

  /**
   * Restore a dump on a specific backend. The proper Backuper is retrieved
   * automatically according to the dump format stored in the recovery log dump
   * table.
   * <p>
   * This method disables the backend and leave it disabled after recovery
   * process. The user has to call the <code>enableBackendFromCheckpoint</code>
   * after this.
   * 
   * @param databaseBackendName the name of the backend to restore
   * @param login the login to use to connect to the database for the restore
   *            operation
   * @param password the password to use to connect to the database for the
   *            restore operation
   * @param dumpName the name of the dump to restore
   * @param tables the list of tables to restore, null means all tables
   * @throws VirtualDatabaseException if the restore operation failed
   */
  void restoreDumpOnBackend(String databaseBackendName, String login,
      String password, String dumpName, ArrayList<?> tables)
      throws VirtualDatabaseException;

  /**
   * Transfer specified dump over to specified vdb's controller, making it
   * available for restore operation. The local dump is not deleted and still
   * available for local restore operations. This operation wants a recovery log
   * to be enabled for the vdb (stores dump info, and meaning less otherwize as
   * no restore is possible without a recovery log). It is pointless (and an
   * error) to use this on a non-distributed virtual db.
   * 
   * @param dumpName the name of the dump to copy. Should exist locally, and not
   *            remotely.
   * @param remoteControllerName the remote controller to talk to.
   * @param noCopy specifies whether or not to actually copy the dump. Default:
   *            false. No-copy is a useful option in case of NFS/shared dumps.
   * @throws VirtualDatabaseException in case of error
   */
  void transferDump(String dumpName, String remoteControllerName, boolean noCopy)
      throws VirtualDatabaseException;

  //
  // Administration/Monitoring functions
  //

  /**
   * Return information about the specified backend.
   * 
   * @param backendName the backend logical name
   * @return String the backend information
   * @throws VirtualDatabaseException if an error occurs
   */
  String getBackendInformation(String backendName)
      throws VirtualDatabaseException;

  // TODO: Should return a BackendInfo

  /**
   * The getXml() method does not return the schema if it is not static anymore,
   * to avoid confusion between static and dynamic schema. This method returns a
   * static view of the schema, whatever the dynamic precision is.
   * 
   * @param backendName the name of the backend to get the schema from
   * @return an xml formatted string
   * @throws VirtualDatabaseException if an error occurs while accessing the
   *             backend, or if the backend does not exist.
   */
  String getBackendSchema(String backendName) throws VirtualDatabaseException;

  /**
   * Retrieves this <code>VirtualDatabase</code> object in xml format
   * 
   * @return xml formatted string that conforms to sequoia.dtd
   */
  String getXml();

  //
  // Virtual database management
  //

  // FIXME this method is useless: if a jmx client can connect
  // to the vdb mbean it won't call this method and can still
  // manage the vdb because the other mbean's method do not
  // check that their caller has been authenticated or not...
  /**
   * Authenticate a user for a given virtual database
   * 
   * @param adminLogin username
   * @param adminPassword password
   * @return true if authentication is a success, false otherwise
   * @throws VirtualDatabaseException if database does not exists
   */
  boolean checkAdminAuthentication(String adminLogin, String adminPassword)
      throws VirtualDatabaseException;

  // TODO rename it to getName()
  /**
   * Gets the virtual database name to be used by the client (Sequoia driver)
   * 
   * @return the virtual database name
   */
  String getVirtualDatabaseName();

  // TODO rename it to isRecoveryLogDefined() so that it is considered as an
  // attribute
  // rather than an operation
  /**
   * Indicate if there is a recovery log defined for this virtual database
   * 
   * @return <code>true</code> if the recovery log is defined and can be
   *         accessed, <code>false</code> otherwise
   */
  boolean hasRecoveryLog();

  // TODO rename it to isResultCacheDefined() so that it is considered as an
  // attribute
  // rather than an operation
  /**
   * Indicate if there is a result cache defined for this virtual database
   * 
   * @return <code>true</code> if a request cache is defined and can be
   *         accessed, <code>false</code> otherwise
   */
  boolean hasResultCache();

  /**
   * Tells whether this database is distributed or not
   * 
   * @return <code>true</code> if the database is distributed among multiple
   *         controllers <code>false</code> if it exists on a single
   *         controller only
   */
  boolean isDistributed();

  /**
   * Shutdown this virtual database. Finish all threads and stop connection to
   * backends
   * 
   * @param level Constants.SHUTDOWN_WAIT, Constants.SHUTDOWN_SAFE or
   *            Constants.SHUTDOWN_FORCE
   * @throws VirtualDatabaseException if an error occurs
   */
  void shutdown(int level) throws VirtualDatabaseException;

  /**
   * Name of the controller owning this virtual database
   * 
   * @return url of the controller
   */
  String getOwningController();

  /**
   * Retrieves an array of statistics of the given backend for this virtual
   * database
   * 
   * @param backendName name of the backend
   * @return <code>BackendStatistics[]</code> of formatted data for all
   *         backends or <code>null</code> if the backend does not exist
   * @throws Exception if fails
   */
  BackendStatistics getBackendStatistics(String backendName) throws Exception;

  /**
   * Return the list of controllers defining this virtual database. If the
   * database is not distributed this returns the same as
   * <code>getOwningController</code> otherwise returns an array of controller
   * configuring this <code>DistributedVirtualDatabase</code>
   * 
   * @return <code>String[]</code> of controller names.
   */
  String[] getControllers();

  // TODO rename to getWorkerThreadsSize()
  /**
   * Returns the currentNbOfThreads.
   * 
   * @return int
   */
  int getCurrentNbOfThreads();

  // TODO add a isActiveMonitoring() to know the current state of monitoring
  // activation

  // TODO rename it to setActiveMonitoring() so that it is considered as an
  // attribute
  // rather than an operation
  /**
   * If a monitoring section exists, we can set the monitoring on or off by
   * calling this method. If monitoring is not defined we throw an exception.
   * 
   * @param active should set the monitor to on or off
   * @throws VirtualDatabaseException if there is no monitor.
   */
  void setMonitoringToActive(boolean active) throws VirtualDatabaseException;

  /**
   * Abort a transaction that has been started but in which no query was
   * executed. As we use lazy transaction begin, there is no need to rollback
   * such transaction but just to cleanup the metadata associated with this not
   * effectively started transaction.
   * 
   * @param transactionId id of the transaction to abort
   * @param logAbort true if the abort (in fact rollback) should be logged in
   *            the recovery log
   * @param forceAbort true if the abort will be forced. Actually, abort will do
   *            nothing when a transaction has savepoints (we do not abort the
   *            whole transaction, so that the user can rollback to a previous
   *            savepoint), except when the connection is closed. In this last
   *            case, if the transaction is not aborted, it prevents future
   *            maintenance operations such as shutdowns, enable/disable from
   *            completing, so we have to force this abort operation. It also
   *            applies to the DeadlockDetectionThread and the cleanup of the
   *            VirtualDatabaseWorkerThread.
   * @throws SQLException if an error occurs
   */
  void abort(long transactionId, boolean logAbort, boolean forceAbort)
      throws SQLException;

  /**
   * Close the given persistent connection.
   * 
   * @param login login to use to retrieve the right connection pool
   * @param persistentConnectionId id of the persistent connection to close
   */
  void closePersistentConnection(String login, long persistentConnectionId);

  /**
   * Resume the activity of the virtual database. This command is designed to
   * resume the activity if the recovery process would hang, leaving the virtual
   * database in a suspended state.
   * 
   * @throws VirtualDatabaseException if the command is used while the virtual
   *             database is not in a suspended state or if this command is not
   *             run from the controller where the activity was suspended from
   *             (the controller recovering one of its backend)
   */
  void resumeActivity() throws VirtualDatabaseException;
  
  /**
   * Returns the current activity status for the virtual database.
   * 
   * @return the activity status of the virtual database as a string
   */
  String getActivityStatus();

}