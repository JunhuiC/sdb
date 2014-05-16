/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2005 Emic Networks
 * Copyright (C) 2006-2007 Continuent, Inc.
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
 * Initial developer(s): Marc Herbert
 * Contributor(s): Gilles Rayrat.
 */

package org.continuent.sequoia.common.protocol;

import java.io.IOException;
import java.sql.Types;

import org.continuent.sequoia.common.stream.DriverBufferedInputStream;
import org.continuent.sequoia.common.stream.DriverBufferedOutputStream;

/**
 * This class implements protocol type tags with an internal String, but offers
 * an abstract interface on top of it in order to be transparently substituted
 * some day (with enums for instance).
 * <p>
 * Advantages of using string types is human-readability (debugging, trace
 * analysis, etc.) and earlier detection in case of protocol corruption.
 * Drawback maybe a small performance cost.
 * <p>
 * Check "the importance of being textual" - by Eric S. Raymond.
 * http://www.faqs.org/docs/artu/ch05s01.html
 * 
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @version 1.0
 */
public final class TypeTag
{
  private int                 typeNumber;
  private String              typeName;

  /** ** Actual Types *** */
  private static final String TPREFIX           = "T-";
  private static int          commandNumber     = -1;

  /* SQL "objects" */
  /*
   * The reference is table 47.9.3 "JDBC Types Mapped to Java Object Types" in
   * 2nd edition (JDBC 2.1) of the book "JDBC API Tutorial and reference", or
   * table 50.3 in the 3rd edition (JDBC 3.0). Also available online in Sun's
   * "JDBC Technology Guide: Getting Started", section "Mapping SQL and Java
   * Types".
   */
  /* unsupported are: DISTINCT, Array, Struct/SQLData, Ref, JAVA_OBJECT */
  /* JDBC 3.0 added the type: java.net.URL, also currently unsupported */

  // WARNING: ORDER MATTERS HERE since these statics are internally numbered
  // in initialization order.
  /** Constant for a SQL/Java type */
  public static final TypeTag TYPE_ERROR        = new TypeTag(
                                                    TPREFIX
                                                        + "Type not found or unsupported");
  /** Constant for a SQL/Java type */
  public static final TypeTag STRING            = new TypeTag(TPREFIX
                                                    + "String");
  /** Constant for a SQL/Java type */
  public static final TypeTag BIGDECIMAL        = new TypeTag(TPREFIX
                                                    + "BigDecimal");
  /** Constant for a SQL/Java type */
  public static final TypeTag BOOLEAN           = new TypeTag(TPREFIX
                                                    + "Boolean");
  /** Constant for a SQL/Java type */
  public static final TypeTag INTEGER           = new TypeTag(TPREFIX
                                                    + "Integer");
  /** Constant for a SQL/Java type */
  public static final TypeTag LONG              = new TypeTag(TPREFIX + "Long");
  /** Constant for a SQL/Java type */
  public static final TypeTag FLOAT             = new TypeTag(TPREFIX + "Float");
  /** Constant for a SQL/Java type */
  public static final TypeTag DOUBLE            = new TypeTag(TPREFIX
                                                    + "Double");
  /** Constant for a SQL/Java type */
  public static final TypeTag BYTE_ARRAY        = new TypeTag(TPREFIX
                                                    + "Byte[]");
  /** Constant for a SQL/Java type */
  public static final TypeTag SQL_DATE          = new TypeTag(TPREFIX
                                                    + "SqlDate");
  /** Constant for a SQL/Java type */
  public static final TypeTag SQL_TIME          = new TypeTag(TPREFIX
                                                    + "SqlTime");
  /** Constant for a SQL/Java type */
  public static final TypeTag SQL_TIMESTAMP     = new TypeTag(TPREFIX
                                                    + "SqlTimestamp");
  /** Constant for a SQL/Java type */
  public static final TypeTag CLOB              = new TypeTag(TPREFIX + "Clob");
  /** Constant for a SQL/Java type */
  public static final TypeTag BLOB              = new TypeTag(TPREFIX + "Blob");

  /** Constant for a SQL/Java type */
  public static final TypeTag SQL_ARRAY         = new TypeTag(TPREFIX + "Array");
  
  /** Constant for a Java Serializable object */
  public static final TypeTag JAVA_SERIALIZABLE = new TypeTag(TPREFIX
                                                    + "Serializable");

  /** Constant for a null "object" */
  public static final TypeTag JAVA_NULL         = new TypeTag(TPREFIX + "Null");

  /* Structs */

  /** Constant for a SQL structure */
  public static final TypeTag RESULTSET         = new TypeTag(TPREFIX
                                                    + "ResultSet");
  /** Null ResultSet used as a special flag */
  public static final TypeTag NULL_RESULTSET    = new TypeTag("null ResultSet");

  /** Constant for a SQL structure */
  public static final TypeTag FIELD             = new TypeTag(TPREFIX + "Field");
  /** Constant for a SQL structure */
  public static final TypeTag COL_TYPES         = new TypeTag(TPREFIX
                                                    + "Column types");
  /** Constant for a SQL structure */
  public static final TypeTag ROW               = new TypeTag(TPREFIX + "Row");

  /** used when there is no type ambiguity; no need to type */
  public static final TypeTag NOT_EXCEPTION     = new TypeTag("OK");

  /** Constant for an exception */
  public static final TypeTag EXCEPTION         = new TypeTag(TPREFIX
                                                    + "Exception");
  /** Constant for an exception */
  public static final TypeTag BACKEND_EXCEPTION = new TypeTag(TPREFIX
                                                    + "BackendException");
  /** Constant for an exception */
  public static final TypeTag CORE_EXCEPTION    = new TypeTag(TPREFIX
                                                    + "CoreException");
  /** Constant for a PreparedStatement Parameter Metadata */
  public static final TypeTag PP_PARAM_METADATA = new TypeTag(
                                                    TPREFIX
                                                        + "PreparedStatementParameterMetada");

  /** Constant for internal protocol data */
  public static final TypeTag CONTROLLER_READY  = new TypeTag("Ready");

  /** Used when the whole column is null AND we could not guess in any other way */
  public static final TypeTag UNDEFINED         = new TypeTag("Undef");

  private TypeTag(String typeName)
  {
    this.typeNumber = commandNumber++;
    this.typeName = typeName;
  }

  private TypeTag(int typeNumber)
  {
    this.typeNumber = typeNumber;
  }

  /**
   * Read/deserialize/construct a TypeTag from a stream.
   * 
   * @param in input stream
   * @throws IOException stream error
   */
  public TypeTag(DriverBufferedInputStream in) throws IOException
  {
    this(in.readInt());
  }

  /**
   * Serialize "this" tag on the stream.
   * 
   * @param out output stream
   * @throws IOException stream error
   */
  public void sendToStream(DriverBufferedOutputStream out) throws IOException
  {
    out.writeInt(this.typeNumber);
  }

  /**
   * Calling this method is a bug, check the type of your argument.
   * 
   * @param o compared object (which should be a TypeTag!)
   * @return a buggy result
   * @see java.lang.Object#equals(java.lang.Object)
   * @deprecated
   */
  public boolean equals(Object o)
  {
    System.err
        .println("internal bug: TypeTag was compared with something else at:");
    (new Throwable()).printStackTrace();
    return false;
  }

  /**
   * Compares two TypeTags.
   * 
   * @param b compared TypeTag
   * @return true if same value
   */
  public boolean equals(TypeTag b)
  {
    /**
     * Even if the constants above are static, we cannot use "==" since we also
     * have to consider objects built from the stream
     * {@link #TypeTag(DriverBufferedInputStream)}
     */
    /*
     * We could reimplement String.equals here without the instanceof in order
     * to get back a couple of CPU cycles.
     */
    return this.typeNumber == b.typeNumber;
  }

  /**
   * Returns a string representation, useful for logging and debugging.
   * 
   * @return string representation of the tag
   */
  public String toString()
  {
    if (typeName == null)
    {
      if (typeNumber == TYPE_ERROR.typeNumber)
        return TYPE_ERROR.typeName;
      if (typeNumber == STRING.typeNumber)
        return STRING.typeName;
      if (typeNumber == BIGDECIMAL.typeNumber)
        return BIGDECIMAL.typeName;
      if (typeNumber == BOOLEAN.typeNumber)
        return BOOLEAN.typeName;
      if (typeNumber == STRING.typeNumber)
        return INTEGER.typeName;
      if (typeNumber == INTEGER.typeNumber)
        return LONG.typeName;
      if (typeNumber == LONG.typeNumber)
        return FLOAT.typeName;
      if (typeNumber == FLOAT.typeNumber)
        return DOUBLE.typeName;
      if (typeNumber == DOUBLE.typeNumber)
        return BYTE_ARRAY.typeName;
      if (typeNumber == BYTE_ARRAY.typeNumber)
        return SQL_DATE.typeName;
      if (typeNumber == SQL_DATE.typeNumber)
        return SQL_TIME.typeName;
      if (typeNumber == SQL_TIME.typeNumber)
        return SQL_TIME.typeName;
      if (typeNumber == SQL_TIMESTAMP.typeNumber)
        return SQL_TIMESTAMP.typeName;
      if (typeNumber == CLOB.typeNumber)
        return CLOB.typeName;
      if (typeNumber == BLOB.typeNumber)
        return BLOB.typeName;
      if (typeNumber == JAVA_SERIALIZABLE.typeNumber)
        return JAVA_SERIALIZABLE.typeName;
      if (typeNumber == JAVA_NULL.typeNumber)
        return JAVA_NULL.typeName;
      if (typeNumber == RESULTSET.typeNumber)
        return RESULTSET.typeName;
      if (typeNumber == NULL_RESULTSET.typeNumber)
        return NULL_RESULTSET.typeName;
      if (typeNumber == FIELD.typeNumber)
        return FIELD.typeName;
      if (typeNumber == COL_TYPES.typeNumber)
        return COL_TYPES.typeName;
      if (typeNumber == ROW.typeNumber)
        return ROW.typeName;
      if (typeNumber == NOT_EXCEPTION.typeNumber)
        return NOT_EXCEPTION.typeName;
      if (typeNumber == EXCEPTION.typeNumber)
        return EXCEPTION.typeName;
      if (typeNumber == BACKEND_EXCEPTION.typeNumber)
        return BACKEND_EXCEPTION.typeName;
      if (typeNumber == CORE_EXCEPTION.typeNumber)
        return CORE_EXCEPTION.typeName;
      if (typeNumber == CONTROLLER_READY.typeNumber)
        return CONTROLLER_READY.typeName;
      return "Unknown type number: " + typeNumber;
    }
    return typeName;
  }

  /**
   * Gives the standard JDBC type to Java Object type conversion according to
   * table "JDBC type to Java Object Type" of the JDBC reference book. (Table
   * 47.9.3 in 2nd Edition, table 50.3 in 3rd edition). This is the conversion
   * that the getObject() method of every JDBC driver should perform by default.
   * 
   * @param jdbcType the JDBC type to convert
   * @return the Java Object type resulting from the standard type conversion.
   * @see java.sql.Types
   */

  public static TypeTag jdbcToJavaObjectType(int jdbcType)
  {
    switch (jdbcType)
    {
      case Types.ARRAY:
        return TypeTag.SQL_ARRAY;
        
      case Types.CHAR :
      case Types.VARCHAR :
      case Types.LONGVARCHAR :
        return TypeTag.STRING;

      case Types.NUMERIC :
      case Types.DECIMAL :
        return TypeTag.BIGDECIMAL;

      case Types.BIT :
        return TypeTag.BOOLEAN;

      case Types.TINYINT :
      case Types.SMALLINT :
      case Types.INTEGER :
        return TypeTag.INTEGER;

      case Types.BIGINT :
        return TypeTag.LONG;

      case Types.REAL :
        return TypeTag.FLOAT;

      case Types.FLOAT :
      case Types.DOUBLE :
        return TypeTag.DOUBLE;

      case Types.BINARY :
      case Types.VARBINARY :
      case Types.LONGVARBINARY :
        return TypeTag.BYTE_ARRAY;

      case Types.DATE :
        return TypeTag.SQL_DATE;

      case Types.TIME :
        return TypeTag.SQL_TIME;

      case Types.TIMESTAMP :
        return TypeTag.SQL_TIMESTAMP;

        // DISTINCT unsupported

      case Types.CLOB :
        return TypeTag.CLOB;

      case Types.BLOB :
        return TypeTag.BLOB;

      case Types.JAVA_OBJECT :
        return TypeTag.JAVA_SERIALIZABLE;

        // ARRAY, STRUCT/SQLData, REF, JAVA_OBJECT unsupported

        // JDBC 3.0 java.net.URL unsupported

      default :
        return TypeTag.TYPE_ERROR;
    }
  }
}
