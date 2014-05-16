/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Heiko Schramm.
 */

package org.continuent.sequoia.common.sql.schema;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.continuent.sequoia.common.locks.TransactionLogicalLock;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * A <code>DatabaseSchema</code> describes all the tables and columns of a
 * database.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class DatabaseSchema implements Serializable
{
  private static final long                serialVersionUID = 1105453274994163661L;

  /**
   * Virtual database name with a dot at the end (e.g. 'mydb.') that can be used
   * as a prefix in the table names
   */
  private String                           vdbNameWithDot;
  /** <code>HashMap</code> of <code>DatabaseTables</code>. */
  private HashMap                          tables;
  /** <code>HashMap</code> of <code>DatabaseProcedures</code>. */
  private HashMap                          procedures;

  /** Lock for this schema */
  private transient TransactionLogicalLock lock             = new TransactionLogicalLock();

  /**
   * Creates a new <code>DatabaseSchema</code> instance.
   * 
   * @param vdbName the virtual database name this schema represents
   */
  public DatabaseSchema(String vdbName)
  {
    this.vdbNameWithDot = vdbName + ".";
    tables = new HashMap();
    procedures = new HashMap();
  }

  /**
   * Creates a new <code>DatabaseSchema</code> instance with a specified
   * number of tables.
   * 
   * @param vdbName the virtual database name this schema represents
   * @param nbOfTables an <code>int</code> value
   */
  public DatabaseSchema(String vdbName, int nbOfTables)
  {
    this.vdbNameWithDot = vdbName + ".";
    tables = new HashMap(nbOfTables);
    procedures = new HashMap();
  }

  /**
   * Creates a new <code>DatabaseSchema</code> instance from an existing
   * database schema (the schema is cloned).
   * 
   * @param schema the existing database schema
   */
  public DatabaseSchema(DatabaseSchema schema)
  {
    if (schema == null)
      throw new IllegalArgumentException(
          "Illegal null database schema in DatabaseSchema(DatabaseSchema) constructor");

    vdbNameWithDot = schema.getVirtualDatabaseName();
    tables = new HashMap(schema.getTables());
    procedures = new HashMap(schema.getProcedures());
  }

  /**
   * Adds a <code>DatabaseTable</code> describing a table of the database.
   * 
   * @param table the table to add
   */
  public synchronized void addTable(DatabaseTable table)
  {
    if (table == null)
      throw new IllegalArgumentException(
          "Illegal null database table in addTable(DatabaseTable) method");
    tables.put(table.getName(), table);
    if (table.getSchema() != null)
      tables.put(table.getSchema() + "." + table.getName(), table);
  }

  /**
   * Adds a <code>DatabaseProcedure</code> describing a procedure of the
   * database.
   * 
   * @param procedure the procedure to add
   */
  public synchronized void addProcedure(DatabaseProcedure procedure)
  {
    if (procedure == null)
      throw new IllegalArgumentException(
          "Illegal null database table in addTable(DatabaseTable) method");
    String key = procedure.getKey();
    int semicolon = key.indexOf(';');
    if (semicolon > 0)
    { // The procedure includes a version number (like in Sybase). If key is
      // myproc;1.4(2), also add myproc(2) in the list
      String keyWithoutVersionNumber = key.substring(0, semicolon)
          + key.substring(procedure.getName().length());
      procedures.put(keyWithoutVersionNumber, procedure);
    }
    procedures.put(key, procedure);
  }

  /**
   * Returns true if all tables are not locked by anyone.
   * 
   * @param request request trying to execute (to indicate if it is in
   *          autocommit mode and the transaction id)
   * @return true if all tables are unlocked.
   */
  public boolean allTablesAreUnlockedOrLockedByTransaction(
      AbstractRequest request)
  {
    // Optimistic approach where we use an iterator and if the tables are
    // modified concurrently, iter.next() will fail and we will restart the
    // search.
    boolean retry;
    do
    {
      retry = false;
      try
      {
        for (Iterator iter = tables.values().iterator(); iter.hasNext();)
        {
          TransactionLogicalLock l = ((DatabaseTable) iter.next()).getLock();
          if (l.isLocked())
          {
            // If the lock is held by another transaction then this is not ok
            if (request.getTransactionId() != l.getLocker())
              return false;
          }
        }
      }
      catch (ConcurrentModificationException e)
      {
        retry = true;
      }
    }
    while (retry);
    return true;
  }

  /**
   * Lock all tables that are not already locked by this transaction (assumes
   * that locks are free).
   * 
   * @param request request trying to execute (to indicate if it is in
   *          autocommit mode and the transaction id)
   * @return list of locks acquired (excluding locks already acquired before
   *         this method was called)
   */
  public List lockAllTables(AbstractRequest request)
  {
    // Optimistic approach where we use an iterator and if the tables are
    // modified concurrently, iter.next() will fail and we will restart the
    // search.
    boolean retry;
    List acquiredLocks = new ArrayList();
    do
    {
      retry = false;
      try
      {
        for (Iterator iter = tables.values().iterator(); iter.hasNext();)
        {
          TransactionLogicalLock l = ((DatabaseTable) iter.next()).getLock();
          if (!l.isLocked())
          {
            l.acquire(request);
            acquiredLocks.add(l);
          }
        }
      }
      catch (ConcurrentModificationException e)
      {
        retry = true;
      }
    }
    while (retry);
    return acquiredLocks;
  }

  /**
   * Returns the lock for this table.
   * 
   * @return a <code>TransactionLogicalLock</code> instance
   */
  public TransactionLogicalLock getLock()
  {
    return lock;
  }

  /**
   * When the database schema is reloaded, the locks held by active transactions
   * must be retained.
   * 
   * @param oldSchema the previous version of the schema.
   */
  public void setLocks(DatabaseSchema oldSchema)
  {
    lock = oldSchema.lock;
    for (Iterator iter = tables.values().iterator(); iter.hasNext();)
    {
      DatabaseTable table = (DatabaseTable) iter.next();
      DatabaseTable oldTable = oldSchema.getTable(table.getName(), true);
      if (oldTable != null)
        table.setLock(oldTable);
    }
  }

  /**
   * Returns the <code>DatabaseProcedure</code> object matching the given
   * procedure name or <code>null</code> if not found.
   * 
   * @param procedureKey the procedure key to look for
   * @return a <code>DatabaseProcedure</code> value or null
   */
  public DatabaseProcedure getProcedure(String procedureKey)
  {
    DatabaseProcedure proc = null;
    if (procedureKey == null)
      return null;

    synchronized (procedures)
    {
      proc = (DatabaseProcedure) procedures.get(procedureKey);
      // If we don't find the stored procedure in the procedures hashmap using
      // case sensitive matching, we try to find it in lower case.
      if (proc == null)
        proc = (DatabaseProcedure) procedures.get(procedureKey.toLowerCase());
      return proc;
    }
  }

  /**
   * Returns the <code>DatabaseProcedure</code> object matching the given
   * procedure or <code>null</code> if not found.
   * 
   * @param procedure the procedure to look for
   * @return a <code>DatabaseProcedure</code> value or null
   */
  public DatabaseProcedure getProcedure(DatabaseProcedure procedure)
  {
    if (procedure == null)
      return null;

    // Optimistic approach where we use an iterator and if the tables are
    // modified concurrently, iter.next() will fail and we will restart the
    // search.
    boolean retry;
    do
    {
      retry = false;
      try
      {
        for (Iterator iter = procedures.values().iterator(); iter.hasNext();)
        {
          DatabaseProcedure p = (DatabaseProcedure) iter.next();
          if (procedure.equals(p))
            return p;
        }
      }
      catch (ConcurrentModificationException e)
      {
        retry = true;
      }
    }
    while (retry);
    return null;
  }

  /**
   * Returns an <code>HashMap</code> of <code>DatabaseProcedure</code>
   * objects describing the database. The key entry is given by
   * DatabaseProcedure.getKey().
   * 
   * @return an <code>HashMap</code> of <code>DatabaseProcedure</code>
   */
  public HashMap getProcedures()
  {
    return procedures;
  }

  /**
   * Returns the <code>DatabaseTable</code> object matching the given table
   * name or <code>null</code> if not found.
   * 
   * @param tableName the table name to look for
   * @return a <code>DatabaseTable</code> value or null
   */
  public DatabaseTable getTable(String tableName)
  {
    if ((tableName == null) || (tableName.length() == 0))
      return null;

    synchronized (tables)
    {
      DatabaseTable t = (DatabaseTable) tables.get(tableName);
      if (t == null)
      {
        // Check if we have a fully qualified table name prefixed by the
        // database name (e.g. mydb.table1).
        if (tableName.startsWith(vdbNameWithDot))
        { // Strip the (virtual) database prefix if any
          t = (DatabaseTable) tables.get(tableName.substring(vdbNameWithDot
              .length()));
        }
        // SEQUOIA-518: (Database specific) use of quoted tableNames:
        // PostreSQL accepts '"', MySQL accepts '`'
        // so we need to trim them
        else
        {
          char firstChar = tableName.charAt(0);
          if (firstChar == '\"' || firstChar == '`' || firstChar == '\'')
          {
            String trimedTableName = tableName.substring(1,
                tableName.length() - 1);
            t = (DatabaseTable) tables.get(trimedTableName);
          }
        }
      }
      return t;
    }
  }

  private DatabaseTable getTable(DatabaseTable other)
  {
    if (other.getSchema() != null)
      return getTable(other.getSchema() + "." + other.getName());
    else
      return getTable(other.getName());
  }

  /**
   * Returns the <code>DatabaseTable</code> object matching the given table
   * name or <code>null</code> if not found. An extra boolean indicates if
   * table name matching is case sensitive or not.
   * 
   * @param tableName the table name to look for
   * @param isCaseSensitive true if name matching must be case sensitive
   * @return a <code>DatabaseTable</code> value or null
   */
  public DatabaseTable getTable(String tableName, boolean isCaseSensitive)
  {
    if ((tableName == null) || (tableName.length() == 0))
      return null;

    DatabaseTable t = getTable(tableName);
    if (isCaseSensitive || (t != null))
      return t;

    // Not found with the case sensitive approach, let's try a case insensitive
    // way

    // Strip the (virtual) database prefix if any for fully qualified table
    // names
    if (tableName.startsWith(vdbNameWithDot))
      tableName = tableName.substring(vdbNameWithDot.length());

    // Optimistic approach where we use an iterator and if the tables are
    // modified concurrently, iter.next() will fail and we will restart the
    // search.
    boolean retry;
    do
    {
      retry = false;
      try
      {
        for (Iterator iter = tables.values().iterator(); iter.hasNext();)
        {
          t = (DatabaseTable) iter.next();
          if (tableName.equalsIgnoreCase(t.getName()))
            return t;
        }
      }
      catch (ConcurrentModificationException e)
      {
        retry = true;
      }
    }
    while (retry);
    return null;
  }

  /**
   * Returns an <code>HashMap</code> of <code>DatabaseTable</code> objects
   * describing the database. The key is the table name.
   * 
   * @return an <code>HashMap</code> of <code>DatabaseTable</code>
   */
  public synchronized HashMap getTables()
  {
    return new HashMap(tables);
  }

  /**
   * Returns the virtual database name value.
   * 
   * @return Returns the virtual database name.
   */
  public final String getVirtualDatabaseName()
  {
    return vdbNameWithDot;
  }

  /**
   * Returns <code>true</code> if the given <code>ProcedureName</code> is
   * found in this schema.
   * 
   * @param procedureName the name of the procedure you are looking for
   * @param nbOfParameters number of parameters of the stored procecdure
   * @return <code>true</code> if the procedure has been found
   */
  public boolean hasProcedure(String procedureName, int nbOfParameters)
  {
    return procedures.containsKey(DatabaseProcedure.buildKey(procedureName,
        nbOfParameters));
  }

  /**
   * Returns true if the given transaction locks (or wait for a lock) on any of
   * the table of this schema.
   * 
   * @param transactionId the transaction identifier
   * @return true if the transaction locks or wait for a lock on at least one
   *         table
   */
  public boolean hasATableLockedByTransaction(long transactionId)
  {
    // Optimistic approach where we use an iterator and if the tables are
    // modified concurrently, iter.next() will fail and we will restart the
    // search.
    boolean retry;
    do
    {
      retry = false;
      try
      {
        for (Iterator iter = tables.values().iterator(); iter.hasNext();)
        {
          TransactionLogicalLock l = ((DatabaseTable) iter.next()).getLock();
          if (l.isLocked()
              && ((l.getLocker() == transactionId) || (l
                  .isWaiting(transactionId))))
            return true;
        }
      }
      catch (ConcurrentModificationException e)
      {
        retry = true;
      }
    }
    while (retry);
    return false;
  }

  /**
   * Returns <code>true</code> if the given <code>TableName</code> is found
   * in this schema.
   * 
   * @param tableName the name of the table you are looking for
   * @return <code>true</code> if the table has been found
   */
  public boolean hasTable(String tableName)
  {
    // Optimistic approach, if the tables are modified concurrently we will fail
    // and we will restart the search.
    do
    {
      try
      {
        return tables.containsKey(tableName);
      }
      catch (ConcurrentModificationException e)
      {
      }
    }
    while (true);
  }

  /**
   * Checks if this <code>DatabaseSchema</code> is a compatible subset of a
   * given schema. It means that all tables in this schema must be present with
   * the same definition in the other schema.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the two schemas are compatible
   */
  public boolean isCompatibleSubset(DatabaseSchema other)
  {
    if (other == null)
      return false;

    DatabaseTable table, otherTable;
    for (Iterator iter = tables.values().iterator(); iter.hasNext();)
    { // Parse all tables
      table = (DatabaseTable) iter.next();
      otherTable = other.getTable(table);
      if (otherTable == null)
        return false; // Not present
      else if (!table.equalsIgnoreType(otherTable))
        return false; // Not compatible
    }
    DatabaseProcedure procedure, otherProcedure;
    for (Iterator iter = procedures.values().iterator(); iter.hasNext();)
    { // Parse all procedures
      procedure = (DatabaseProcedure) iter.next();
      otherProcedure = other.getProcedure(procedure.getName());
      if (otherProcedure == null)
        return false; // Not present
      else if (!procedure.equals(otherProcedure))
        return false; // Not compatible
    }
    return true; // Ok, all tables passed the test
  }

  /**
   * Checks if this <code>DatabaseSchema</code> is compatible with the given
   * schema. It means that all tables in this schema that are common with the
   * other schema must be identical.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the two schemas are compatible
   */
  public boolean isCompatibleWith(DatabaseSchema other)
  {
    DatabaseTable table, otherTable;
    for (Iterator iter = tables.values().iterator(); iter.hasNext();)
    { // Parse all tables
      table = (DatabaseTable) iter.next();
      otherTable = other.getTable(table);
      if (otherTable == null)
        continue; // Not present in other schema
      else if (!table.equalsIgnoreType(otherTable))
        return false; // Not compatible
    }
    DatabaseProcedure procedure, otherProcedure;
    for (Iterator iter = procedures.values().iterator(); iter.hasNext();)
    { // Parse all procedures
      procedure = (DatabaseProcedure) iter.next();
      otherProcedure = other.getProcedure(procedure.getName());
      if (otherProcedure == null)
        continue; // Not present
      else if (!procedure.equals(otherProcedure))
        return false; // Not compatible
    }
    return true; // Ok, all tables passed the test
  }

  /**
   * Merges the given schema with the current one. All missing tables or columns
   * are added if no conflict is detected. An exception is thrown if the given
   * schema definition conflicts with the current one.
   * 
   * @param databaseSchema the schema to merge
   * @throws SQLException if the schemas conflict
   */
  public void mergeSchema(DatabaseSchema databaseSchema) throws SQLException
  {
    if (databaseSchema == null)
      throw new IllegalArgumentException(
          "Illegal null database schema in mergeSchema(DatabaseSchema) method");

    HashMap otherTables = databaseSchema.getTables();
    if ((otherTables == null) || (otherTables.size() == 0))
      return;

    DatabaseTable table, originalTable;
    for (Iterator iter = otherTables.values().iterator(); iter.hasNext();)
    { // Parse all tables
      table = (DatabaseTable) iter.next();
      originalTable = getTable(table);
      if (originalTable == null)
        addTable(table);
      else
        originalTable.merge(table);
    }

    HashMap otherProcedures = databaseSchema.getProcedures();
    if ((otherProcedures == null) || (otherProcedures.size() == 0))
      return;

    DatabaseProcedure procedure, originalProcedure;
    for (Iterator iter = otherProcedures.values().iterator(); iter.hasNext();)
    { // Parse all procedures
      procedure = (DatabaseProcedure) iter.next();
      originalProcedure = getProcedure(procedure);
      if (originalProcedure == null)
        addProcedure(procedure);
      else
      {
        DatabaseProcedureSemantic originalSemantic = originalProcedure
            .getSemantic();
        if ((originalSemantic == null || originalSemantic
            .isUseDefaultSemantic())
            && procedure.getSemantic() != null)
        {
          addProcedure(procedure);
        }
      }
    }
  }

  /**
   * Release locks held by the given transaction on all tables.
   * 
   * @param transactionId the transaction identifier
   */
  public void releaseLocksOnAllTables(long transactionId)
  {
    // Release global lock
    lock.release(transactionId);

    // Optimistic approach where we use an iterator and if the tables are
    // modified concurrently, iter.next() will fail and we will restart the
    // search.
    boolean retry;
    do
    {
      retry = false;
      try
      {
        for (Iterator iter = tables.values().iterator(); iter.hasNext();)
        {
          TransactionLogicalLock l = ((DatabaseTable) iter.next()).getLock();
          l.release(transactionId);
        }
      }
      catch (ConcurrentModificationException e)
      {
        retry = true;
      }
    }
    while (retry);
  }

  /**
   * removes a <code>DatabaseProcedure</code> describing a procedure of the
   * database.
   * 
   * @param procedure to remove
   * @return true if the procedure was successfully removed
   */
  public synchronized boolean removeProcedure(DatabaseProcedure procedure)
  {
    if (procedure == null)
      throw new IllegalArgumentException(
          "Illegal null database procedure in removeProcedure(DatabaseProcedure) method");
    return procedures.remove(procedure.getKey()) != null;
  }

  /**
   * Removes a <code>DatabaseTable</code> describing a table of the database.
   * 
   * @param table the table to remove
   * @return true if the table was successfully removed
   */
  public synchronized boolean removeTable(DatabaseTable table)
  {
    if (table == null)
      throw new IllegalArgumentException(
          "Illegal null database table in removeTable(DatabaseTable) method");
    if (table.getSchema() != null)
      return ((tables.remove(table.getName()) != null) && (tables.remove(table
          .getSchema()
          + "." + table.getName()) != null));
    else
      return (tables.remove(table.getName()) != null);
  }

  /**
   * Removes a <code>DatabaseTable</code> from the depending tables list of
   * all tables in the schema.
   * 
   * @param table the table to remove
   */
  public synchronized void removeTableFromDependingTables(DatabaseTable table)
  {
    Iterator keys = getTables().keySet().iterator();
    while (keys.hasNext())
    {
      String dependingTableName = (String) keys.next();
      DatabaseTable dependingTable = getTable(dependingTableName);
      if (dependingTable.getDependingTables() != null)
      {
        dependingTable.getDependingTables().remove(table.getName());
      }
    }
  }

  /**
   * Updates the given schema with the current one. All missing tables or
   * columns are added and if the given schema definition conflicts with the
   * current one, the current schema definition is overriden with the one that
   * is provided.
   * 
   * @param databaseSchema the schema to merge
   */
  public void updateSchema(DatabaseSchema databaseSchema)
  {
    if (databaseSchema == null)
      throw new IllegalArgumentException(
          "Illegal null database schema in mergeSchema(DatabaseSchema) method");

    HashMap otherTables = databaseSchema.getTables();

    // Remove tables that do not exist anymore in new schema
    for (Iterator iter = tables.values().iterator(); iter.hasNext();)
    {
      DatabaseTable t = (DatabaseTable) iter.next();
      if (!databaseSchema.hasTable(t.getSchema() + "." + t.getName()))
        iter.remove();
    }

    // Add missing tables
    DatabaseTable table, originalTable;
    for (Iterator iter = otherTables.values().iterator(); iter.hasNext();)
    {
      table = (DatabaseTable) iter.next();
      originalTable = getTable(table);
      if (originalTable == null)
        addTable(table);
      else
        originalTable.updateColumns(table);
    }

    // Just replace the stored procedures
    this.procedures = databaseSchema.getProcedures();
  }

  /**
   * Two <code>DatabaseSchema</code> are considered equal if they have the
   * same tables and the same procedures.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the schemas are equals
   */
  public boolean equals(Object other)
  {
    boolean equal = true;
    if ((other == null) || !(other instanceof DatabaseSchema))
      return false;
    if (tables == null)
      equal &= ((DatabaseSchema) other).getTables() == null;
    else
      equal &= tables.equals(((DatabaseSchema) other).getTables());
    if (procedures == null)
      equal &= ((DatabaseSchema) other).getProcedures() == null;
    else
      equal &= procedures.equals(((DatabaseSchema) other).getProcedures());
    return equal;
  }

  /**
   * Get xml information about this schema.
   * 
   * @return xml formatted information on this database schema.
   */
  public String getXml()
  {
    StringBuffer info = new StringBuffer();
    info.append("<" + DatabasesXmlTags.ELT_DatabaseStaticSchema + ">");
    for (Iterator iter = tables.values().iterator(); iter.hasNext();)
      info.append(((DatabaseTable) iter.next()).getXml());
    for (Iterator iter = procedures.values().iterator(); iter.hasNext();)
      info.append(((DatabaseProcedure) iter.next()).getXml());
    info.append("</" + DatabasesXmlTags.ELT_DatabaseStaticSchema + ">");
    return info.toString();
  }

}