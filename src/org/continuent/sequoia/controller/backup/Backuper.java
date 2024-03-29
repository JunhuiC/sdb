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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * This interface defines a Backuper that is in charge of doing backup/restore
 * operations and manage dumps according to its own internal format. The user
 * will manipulate logical names and the Backuper is responsible to maintain the
 * logical name/physical name mapping.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public interface Backuper
{

  //
  // Backuper information
  //

  /**
   * Returns a String representing the format handled by this Backuper. This
   * field should be human readable and as detailed as possible so that no
   * confusion can be made by the administrator.
   * 
   * @return the Backuper specific format
   */
  String getDumpFormat();

  /**
   * Retrieve the backuper options that were used to initialize the backuper.
   * 
   * @return the backuper options
   * @see #setOptions(String)
   */
  String getOptions();

  /**
   * Options that can be set at backuper initialization. These options are
   * provided in the definition of the Backuper element (see dtd).
   * 
   * @param options Backuper specific options
   */
  void setOptions(String options);

  //
  // Backup/Restore operations
  //

  /**
   * Create a backup from the content of a backend.
   * 
   * @param backend the target backend to backup
   * @param login the login to use to connect to the database for the backup
   *          operation
   * @param password the password to use to connect to the database for the
   *          backup operation
   * @param dumpName the name of the dump to create
   * @param path the path where to store the dump
   * @param tables the list of tables to backup, null means all tables
   * @return the timestamp for the dump if the backup was sucessful, null
   *         otherwise
   * @throws BackupException if the backup operation fails
   */
  Date backup(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException;

  /**
   * Restore a dump on a specific backend.
   * 
   * @param backend the target backend to restore to
   * @param login the login to use to connect to the database for the restore
   *          operation
   * @param password the password to use to connect to the database for the
   *          restore operation
   * @param dumpName the name of the dump to restore
   * @param path the path where to retrieve the dump
   * @param tables the list of tables to restore, null means all tables
   * @throws BackupException if the restore operation failed
   */
  void restore(DatabaseBackend backend, String login, String password,
      String dumpName, String path, ArrayList<?> tables) throws BackupException;

  // 
  // Dump manipulation functions
  //

  /**
   * Delete the specified dump.
   * 
   * @param path the path where to retrieve the dump
   * @param dumpName the dump to delete
   * @throws BackupException if we failed to delete the dump
   */
  void deleteDump(String path, String dumpName) throws BackupException;

  // 
  // Remote dump copy
  //

  /**
   * Client side: Fetch a remote dump from specified dump server.
   * 
   * @param dumpTransferInfo the address and session key of the dump server to
   *          contact for fetching.
   * @param path the path part of the remote dump spec (interpreted by server)
   * @param dumpName the name part of the remote dump spec (interpreted by
   *          server)
   * @throws BackupException in any error case: authentication error, transfer
   *           error, else.
   * @throws IOException if an error occurs during the transfer
   */
  void fetchDump(DumpTransferInfo dumpTransferInfo, String path, String dumpName)
      throws BackupException, IOException;

  /**
   * Server side: setup a server and returns a DumpTransferInfo suitable for
   * authenticated communication by a client using fetchDump().
   * 
   * @return a DumpTransferInfo to be used by a client for authenticated
   *         communication upon fetchDump invocation.
   * @throws IOException if an error occurs during the transfer
   */
  DumpTransferInfo setupDumpServer() throws IOException;

}
