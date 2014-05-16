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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Nicolas Modrzyk, Marc Herbert.
 */

package org.continuent.sequoia.common.protocol;

import java.io.IOException;
import java.io.Serializable;

import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;
import org.continuent.sequoia.driver.ResultSetMetaData;

/**
 * Field is our private implementation of <code>ResultSetMetaData</code>,
 * holding the information for one column.
 * <p>
 * The first version was inspired from the MM MySQL driver by Mark Matthews.
 * 
 * @see org.continuent.sequoia.driver.DriverResultSet
 * @see org.continuent.sequoia.controller.backend.result.ControllerResultSet
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @version 1.0
 */
public class Field implements Serializable
{
  //
  // This object is manually (de-)serialized below for compatibility with C.
  // It also implements Serializable for the convenience of Java-Java
  // communication (typically between controllers).
  //
  // Ideally:
  // (1) unneeded fields for Java-Java communication are all tagged as
  // "transient"
  // (2) C-Java and Java-Java need to send the exact same fields.
  // And so:
  // (3) keeping up-to-date manual serialization below is easy: just check
  // "transient" tags.

  private static final long serialVersionUID = 1050843622547803111L;

  private String            tableName;
  private String            fieldName;
  private String            fieldLabel;
  private int               columnDisplaySize;
  private int               sqlType;
  private String            typeName;
  private String            columnClassName;
  private boolean           isAutoIncrement;
  private boolean           isCaseSensitive;
  private boolean           isCurrency;
  private int               isNullable;
  private boolean           isReadOnly;
  private boolean           isWritable;
  private boolean           isDefinitelyWritable;
  private boolean           isSearchable;
  private boolean           isSigned;
  private int               precision;
  private int               scale;
  private String            encoding;

  /**
   * Create a new field with some default common values.
   * 
   * @param table the table name
   * @param columnName the field name
   * @param columnDisplaySize the column display size
   * @param sqlType the SQL type
   * @param typeName the type name
   * @param columnClassName the column class name
   */
  public Field(String table, String columnName, int columnDisplaySize,
      int sqlType, String typeName, String columnClassName)
  {
    this(table, columnName, columnName, columnDisplaySize, sqlType, typeName,
        columnClassName, false, true, false, ResultSetMetaData.columnNullable,
        true, false, false, false, false, 0, 0, null);
  }

  /**
   * Creates a new <code>Field</code> instance using the reference arguments
   * (arguments are NOT cloned).
   * 
   * @param table the table name
   * @param columnName the field name
   * @param columnLabel the field label
   * @param columnDisplaySize the column display size
   * @param sqlType the SQL type
   * @param typeName the type name
   * @param columnClassName the column class name
   * @param isAutoIncrement true if field is auto incremented
   * @param isCaseSensitive true if field is case sensitive
   * @param isCurrency true if field is currency
   * @param isNullable indicates the nullability of the field
   * @param isReadOnly true if field is read only
   * @param isWritable true if field is writable
   * @param isDefinitelyWritable true if field is definetly writable
   * @param isSearchable true if field is searchable
   * @param isSigned true if field is signed
   * @param precision decimal precision
   * @param scale number of digits to right of decimal point
   * @param encoding the encoding of this field, if any
   */
  public Field(String table, String columnName, String columnLabel,
      int columnDisplaySize, int sqlType, String typeName,
      String columnClassName, boolean isAutoIncrement, boolean isCaseSensitive,
      boolean isCurrency, int isNullable, boolean isReadOnly,
      boolean isWritable, boolean isDefinitelyWritable, boolean isSearchable,
      boolean isSigned, int precision, int scale, String encoding)
  {
    this.tableName = table;
    this.fieldName = columnName;
    this.fieldLabel = columnLabel;
    this.columnDisplaySize = columnDisplaySize;
    this.sqlType = sqlType;
    this.typeName = typeName;
    this.columnClassName = columnClassName;
    this.isAutoIncrement = isAutoIncrement;
    this.isCaseSensitive = isCaseSensitive;
    this.isCurrency = isCurrency;
    this.isNullable = isNullable;
    this.isReadOnly = isReadOnly;
    this.isWritable = isWritable;
    this.isDefinitelyWritable = isDefinitelyWritable;
    this.isSearchable = isSearchable;
    this.isSigned = isSigned;
    this.precision = precision;
    this.scale = scale;
    this.encoding = encoding;
  }

  /**
   * Creates a new <code>Field</code> object, deserializing it from an input
   * stream. Has to mirror the serialization method below.
   * 
   * @param in input stream
   * @throws IOException if a stream error occurs
   */
  public Field(DriverBufferedInputStream in) throws IOException
  {
    if (in.readBoolean())
      this.tableName = in.readLongUTF();
    else
      this.tableName = null;

    this.fieldName = in.readLongUTF();
    this.fieldLabel = in.readLongUTF();
    this.columnDisplaySize = in.readInt();
    this.sqlType = in.readInt();
    this.typeName = in.readLongUTF();
    this.columnClassName = in.readLongUTF();
    this.isAutoIncrement = in.readBoolean();
    this.isCaseSensitive = in.readBoolean();
    this.isCurrency = in.readBoolean();
    this.isNullable = in.readInt();
    this.isReadOnly = in.readBoolean();
    this.isWritable = in.readBoolean();
    this.isDefinitelyWritable = in.readBoolean();
    this.isSearchable = in.readBoolean();
    this.isSigned = in.readBoolean();
    this.precision = in.readInt();
    this.scale = in.readInt();
    this.encoding = in.readLongUTF();
  }

  /**
   * Serialize the <code>Field</code> on the output stream by sending only the
   * needed parameters to reconstruct it on the controller. Has to mirror the
   * deserialization method above.
   * 
   * @param out destination stream
   * @throws IOException if a stream error occurs
   */
  public void sendToStream(DriverBufferedOutputStream out) throws IOException
  {
    if (null == this.tableName)
      out.writeBoolean(false);
    else
    {
      out.writeBoolean(true);
      out.writeLongUTF(this.tableName);
    }

    out.writeLongUTF(this.fieldName);
    out.writeLongUTF(this.fieldLabel);
    out.writeInt(this.columnDisplaySize);
    out.writeInt(this.sqlType);
    out.writeLongUTF(this.typeName);
    out.writeLongUTF(this.columnClassName);
    out.writeBoolean(this.isAutoIncrement);
    out.writeBoolean(this.isCaseSensitive);
    out.writeBoolean(this.isCurrency);
    out.writeInt(this.isNullable);
    out.writeBoolean(this.isReadOnly);
    out.writeBoolean(this.isWritable);
    out.writeBoolean(this.isDefinitelyWritable);
    out.writeBoolean(this.isSearchable);
    out.writeBoolean(this.isSigned);
    out.writeInt(this.precision);
    out.writeInt(this.scale);
    out.writeLongUTF(this.encoding);

  }

  /**
   * Returns the fieldLabel value.
   * 
   * @return Returns the fieldLabel.
   */
  public final String getFieldLabel()
  {
    return fieldLabel;
  }

  /**
   * Gets the field name.
   * 
   * @return a <code>String</code> value
   */
  public String getFieldName()
  {
    return fieldName;
  }

  /**
   * Gets the full name: "tableName.fieldName"
   * 
   * @return a <code>String</code> value
   */
  public String getFullName()
  {
    return tableName + "." + fieldName;
  }

  /**
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */
  public int getPrecision()
  {
    return precision;
  }

  /**
   * @see java.sql.ResultSetMetaData#getScale(int)
   */
  public int getScale()
  {
    return scale;
  }

  /**
   * Returns the JDBC type code.
   * 
   * @return int Type according to {@link java.sql.Types}
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   */
  public int getSqlType()
  {
    return sqlType;
  }

  /**
   * Gets the table name.
   * 
   * @return a <code>String</code> value
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Returns the SQL type name used by the database.
   * 
   * @return the SQL type name
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */
  public String getTypeName()
  {
    return typeName;
  }

  /**
   * Returns the Java class used by the mapping.
   * 
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */
  public String getColumnClassName()
  {
    return columnClassName;
  }

  /**
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */
  public int getColumnDisplaySize()
  {
    return columnDisplaySize;
  }

  /**
   * @return the encoding used for this field
   */
  public String getEncoding()
  {
    return encoding;
  }

  /**
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */
  public boolean isAutoIncrement()
  {
    return isAutoIncrement;
  }

  /**
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */
  public boolean isCaseSensitive()
  {
    return isCaseSensitive;
  }

  /**
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */
  public boolean isCurrency()
  {
    return isCurrency;
  }

  /**
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */
  public boolean isDefinitelyWritable()
  {
    return isDefinitelyWritable;
  }

  /**
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */
  public int isNullable()
  {
    return isNullable;
  }

  /**
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */
  public boolean isReadOnly()
  {
    return isReadOnly;
  }

  /**
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */
  public boolean isSearchable()
  {
    return isSearchable;
  }

  /**
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */
  public boolean isSigned()
  {
    return isSigned;
  }

  /**
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */
  public boolean isWritable()
  {
    return isWritable;
  }

  /**
   * Returns the full name.
   * 
   * @return <code>String</code> value
   * @see #getFullName()
   */
  public String toString()
  {
    return getFullName();
  }

}