/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Continuent, Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Emmanuel Cecchet
 */

package org.continuent.sequoia.common.sql.schema;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class defines the semantic associated to a stored procedure. This
 * includes the references this stored procedure has to database tables or other
 * stored procedures and semantic properties such as if the stored procedure can
 * be executed out of order or not.
 * 
 * @author <a href="mailto:ed.archibald@continuent.com">Edward Archibald</a>
 * @author <a href="mailto:emmanuel.cecchet@continuent.com">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public class DatabaseProcedureSemantic
{
  /**
   * Sorted list of table names that are referenced by this procedure.
   */
  private SortedSet<String> writeTables          = null;

  /**
   * If this procedure references other procedures, we will set them here.
   */
  private SortedSet<String> proceduresReferenced = null;

  /**
   * The following attributes are meant to communicate some information about
   * how the procedure is used, semantically, by the calling application. Note
   * that the default values are meant to communicate the most restrictive
   * semantic case which would require the stored procedure to be executed in
   * isolation.
   */

  /** procedure has one or more SELECT statements */
  private boolean   hasSelect            = true;

  /** procedure has one or more INSERT statements */
  private boolean   hasInsert            = true;

  /** procedure has one or more UPDATE statements */
  private boolean   hasUpdate            = true;

  /** procedure has one or more DELETE statements */
  private boolean   hasDelete            = true;

  /** procedure has one or more DDL statements */
  private boolean   hasDDLWrite          = true;

  /** procedure has one or more internally managed transactions */
  private boolean   hasTransaction       = true;

  /**
   * procedure executes read operations that may be causally dependent on the
   * completion of a preceeding write statement executed by the same application
   * client but on a different connection
   */
  private boolean   isCausallyDependent  = true;

  /**
   * procedure execution is commutative with other procedures and SQL
   * statements, which affect the same tables, and which are also commutative
   */
  private boolean   isCommutative        = false;

  /**
   * This semantic information is the default semantic information for stored
   * procedures. It is used when no specific
   */
  private boolean   useDefaultSemantic   = false;

  /**
   * Creates a new <code>DatabaseProcedureSemantic</code> object
   * 
   * @param hasSelect true if this procedure has one or more SELECT statements
   * @param hasInsert true if this procedure has one or more INSERT statements
   * @param hasUpdate true if this procedure has one or more UPDATE statements
   * @param hasDelete true if this procedure has one or more DELETE statements
   * @param hasDDLWrite true if this procedure has one or more DDL statements
   * @param hasTransaction true if this procedure has one or more internally
   *          managed transactions
   * @param isCausallyDependent true if this procedure executes read operations
   *          that may be causally dependent on the completion of a preceeding
   *          write statement executed by the same application client but on a
   *          different connection
   * @param isCommutative true if this procedure execution is commutative with
   *          other procedures and SQL statements, which affect the same tables,
   *          and which are also commutative
   */
  public DatabaseProcedureSemantic(boolean hasSelect, boolean hasInsert,
      boolean hasUpdate, boolean hasDelete, boolean hasDDLWrite,
      boolean hasTransaction, boolean isCausallyDependent, boolean isCommutative)
  {
    this.hasSelect = hasSelect;
    this.hasInsert = hasInsert;
    this.hasUpdate = hasUpdate;
    this.hasDelete = hasDelete;
    this.hasDDLWrite = hasDDLWrite;
    this.hasTransaction = hasTransaction;
    this.isCausallyDependent = isCausallyDependent;
    this.isCommutative = isCommutative;
  }

  /**
   * Add the name of a table that is written by this stored procedure.
   * 
   * @param tableName the name of updated table
   */
  public void addWriteTable(String tableName)
  {
    if (writeTables == null)
      writeTables = new TreeSet<String>();

    writeTables.add(tableName);
  }

  /**
   * Add a reference to a stored procedure called by this stored procedure.
   * 
   * @param procKey the unique key of the referenced stored procedure as
   *          returned by DatabaseProcedure.getKey()
   */
  public void addProcedureRef(String procKey)
  {
    if (proceduresReferenced == null)
      proceduresReferenced = new TreeSet<String>();

    proceduresReferenced.add(procKey);
  }

  /**
   * Returns the name of the stored procedures referenced by this stored
   * procedure.
   * 
   * @return Returns the procedures referenced.
   */
  public SortedSet<String> getProceduresReferenced()
  {
    return proceduresReferenced;
  }

  /**
   * Returns the names of the tables that are written by this stored procedure.
   * 
   * @return Returns the name of updated tables.
   */
  public SortedSet<String> getWriteTables()
  {
    return writeTables;
  }

  /**
   * Returns true if this procedure has one or more DDL statements.
   * 
   * @return true if the stored procedure executes DDL statements.
   */
  public boolean hasDDLWrite()
  {
    return hasDDLWrite;
  }

  /**
   * Returns true if this procedure has one or more DELETE statements.
   * 
   * @return true if the stored procedure executes DELETE statements.
   */
  public boolean hasDelete()
  {
    return hasDelete;
  }

  /**
   * Returns true if this procedure has one or more INSERT statements.
   * 
   * @return true if the stored procedure executes INSERT statements.
   */
  public boolean hasInsert()
  {
    return hasInsert;
  }

  /**
   * Returns true if this procedure has one or more SELECT statements.
   * 
   * @return true if the stored procedure executes SELECT statements.
   */
  public boolean hasSelect()
  {
    return hasSelect;
  }

  /**
   * Returns true true if this procedure has one or more internally managed
   * transactions
   * 
   * @return true if the stored procedure executes transactions.
   */
  public boolean hasTransaction()
  {
    return hasTransaction;
  }

  /**
   * Returns true if this procedure has one or more UPDATE statements.
   * 
   * @return true if the stored procedure executes UPDATE statements.
   */
  public boolean hasUpdate()
  {
    return hasUpdate;
  }

  /**
   * Returns true if the procedure execution is commutative with other
   * procedures and SQL statements, which affect the same tables, and which are
   * also commutative
   * 
   * @return Returns true if the stored procedure execution is commutative.
   */
  public boolean isCommutative()
  {
    return isCommutative;
  }

  /**
   * Returns true if the procedure executes read operations that may be causally
   * dependent on the completion of a preceeding write statement executed by the
   * same application client but on a different connection
   * 
   * @return true if the stored procedure is causally dependent with preceeding
   *         write statements
   */
  public boolean isCausallyDependent()
  {
    return isCausallyDependent;
  }

  /**
   * Returns true if this stored procedure has only select statements.
   * 
   * @return true if stored procedure is read-only.
   */
  public boolean isReadOnly()
  {
    return (hasSelect && !(hasInsert || hasUpdate || hasDelete || hasDDLWrite));
  }

  /**
   * Returns if default stored procedure semantic is used or not.
   * 
   * @return true if default stored procedure semantic must be used
   */
  public boolean isUseDefaultSemantic()
  {
    return useDefaultSemantic;
  }

  /**
   * Set the default stored procedure semantic usage.
   * 
   * @param isDefault true if default semantic must be usde
   */
  public void setUseDefaultSemantic(boolean isDefault)
  {
    this.useDefaultSemantic = isDefault;
  }

  /**
   * Returns true if stored procedure executes a statement that updates the
   * database (insert, update, delete or DDL).
   * 
   * @return true if the stored procedure executes write statements
   */
  public boolean isWrite()
  {
    return hasInsert || hasUpdate || hasDelete || hasDDLWrite;
  }

  /**
   * @see java.lang.Object#toString()
   */
  public String toString()
  {
    return "isReadOnly:" + isReadOnly() + " - isWrite:" + isWrite()
        + " - hasDDLWrite:" + hasDDLWrite() + " - isCommutative:"
        + isCommutative + " - hasTransaction:" + hasTransaction
        + " - write tables:" + writeTables + " - procedures referenced:"
        + proceduresReferenced;
  }

}
