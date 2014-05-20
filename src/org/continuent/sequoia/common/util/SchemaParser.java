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
 * Initial developer(s): Dylan Hansen.
 */

package org.continuent.sequoia.common.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;

/**
 * The <code>SchemaParser</code> is an executable Java application that can be
 * used to analyze a databse backend, gather it's schema information and create
 * an XML document that can be used in the virtual database config file of a
 * Sequoia controller. Note that only PostgreSQL backends are currently
 * supported.
 * 
 * @author <a href="mailto:dhansen@h2st.com">Dylan Hansen</a>
 * @version 1.0
 */
public class SchemaParser
{

  /**
   * Parameters are as follows: - DatabaseType - URL - User - Password - Gather
   * System Tables (true/false) - File To Write (optional)
   * 
   * @param args List of arguments
   */
  public static void main(String[] args)
  {
    SchemaParser tools = new SchemaParser();

    // Check for valid number of parameters
    if (args.length < 5 || args.length > 6)
    {
      System.out.println("Usage: SchemaParser <Database Type> <URL>"
          + " <User> <Password> <Gather System Tables?>");
      return;
    }

    // Does not check if parameters exist, assumes they are there in propert
    // order
    String dbType = args[0];
    String url = args[1];
    String user = args[2];
    String password = args[3];
    boolean gatherSystemTables = args[4].equalsIgnoreCase("true")
        ? true
        : false;
    String fileLocation = null;
    if (args.length == 6)
      fileLocation = args[5];

    ArrayList<DatabaseTable> schema = tools.getSchemaAsXML(dbType, url, user, password,
        gatherSystemTables);

    if (schema != null)
    {
      if (fileLocation != null)
        tools.writeXMLToFile(schema, fileLocation);
      else
        tools.displayXML(schema);
    }
  }

  /**
   * Get a connection to the database and build an ArrayList of DatabaseTable
   * objects. This ArrayList will be used to print out the schema in an XML
   * format that Sequoia can understand.
   * 
   * @param dbType Type of backend (currently only PostgreSQL is supported)
   * @param url URL of backend to connect to
   * @param user Database user
   * @param password User password
   * @param gatherSystemTables Boolean flag to gather system tables
   * @return ArrayList of DatabaseTable objects.
   */
  private ArrayList<DatabaseTable> getSchemaAsXML(String dbType, String url, String user,
      String password, boolean gatherSystemTables)
  {
    ResultSet tableRs = null;
    ResultSet colRs = null;
    ResultSet idxRs = null;
    ArrayList<DatabaseTable> tables = new ArrayList<DatabaseTable>();

    Connection conn = getConnection(dbType, url, user, password);

    if (conn == null)
    {
      System.out.println("Unable to get database connection!");
      return null;
    }

    try
    {
      DatabaseMetaData dbMetaData = conn.getMetaData();

      // Get a list of tables (default schema and catalog)
      if (gatherSystemTables)
        tableRs = dbMetaData.getTables(null, null, "%", new String[]{"TABLE",
            "SYSTEM TABLE"});
      else
        tableRs = dbMetaData.getTables(null, null, "%", new String[]{"TABLE"});

      while (tableRs.next())
      {
        String tableName = tableRs.getString(3);
        DatabaseTable dbTable = new DatabaseTable(tableName);
        colRs = dbMetaData.getColumns(null, null, tableName, "%");
        while (colRs.next())
        {
          String column = colRs.getString(4);
          boolean isUnique = false;

          idxRs = dbMetaData.getIndexInfo(null, null, tableName, false, false);
          while (idxRs.next())
          {
            if (idxRs.getString(9).equals(column))
            {
              isUnique = idxRs.getBoolean(4) == true ? false : true;
              break;
            }
          }
          DatabaseColumn dbCol = new DatabaseColumn(column, isUnique);
          dbTable.addColumn(dbCol);
        }
        tables.add(dbTable);
      }
    }
    catch (SQLException sqle)
    {
      System.out.println("Exception in gathering database info!");
      System.out.println(sqle);
    }
    finally
    {
      try
      {
        if (tableRs != null)
          tableRs.close();
        if (colRs != null)
          colRs.close();
        if (idxRs != null)
          idxRs.close();
        if (conn != null)
          conn.close();
      }
      catch (Exception e)
      {
      }
    }

    return tables;
  }

  /**
   * Takes an ArrayList of DatabaseTable objects and prints them out.
   * 
   * @param schema ArrayList of DatabaseTable objects
   */
  private void displayXML(ArrayList<DatabaseTable> schema)
  {
    Iterator<DatabaseTable> it = schema.iterator();
    while (it.hasNext())
      System.out.println(((DatabaseTable) it.next()).getXml());
  }

  /**
   * Takes an ArrayList of DatabaseTable objects and writes them to a file at
   * the location provided. Have not implemented any checking to see if the file
   * exists, is writable, etc.
   * 
   * @param schema ArrayList of DatabaseTable objects
   * @param fileLocation Location to write file to
   */
  private void writeXMLToFile(ArrayList<DatabaseTable> schema, String fileLocation)
  {
    BufferedWriter out = null;
    try
    {
      out = new BufferedWriter(new FileWriter(fileLocation));
      Iterator<DatabaseTable> it = schema.iterator();
      while (it.hasNext())
      {
        out.write(((DatabaseTable) it.next()).getXml());
        out.write("\n");
      }

    }
    catch (IOException ioe)
    {
      System.out.println("Unable to write to file!");
      System.out.println(ioe);
    }
    finally
    {
      try
      {
        if (out != null)
          out.close();
      }
      catch (Exception e)
      {
      }
    }
  }

  /**
   * Get a connection from the database
   * 
   * @param dbType Database type (currently only PostgreSQL supported)
   * @param url Full JDBC url to database
   * @param user Database user
   * @param password User password
   * @return Valid connection to database, null otherwise.
   */
  private Connection getConnection(String dbType, String url, String user,
      String password)
  {
    Connection conn = null;
    try
    {
      if (dbType.equalsIgnoreCase("postgresql"))
        Class.forName("org.postgresql.Driver");
      conn = DriverManager.getConnection(url, user, password);
    }
    catch (Exception e)
    {
    }
    return conn;
  }

}
