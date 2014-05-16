/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.continuent.sequoia.common.exceptions.NotImplementedException;
import org.continuent.sequoia.common.stream.LongUTFDataInputStream;
import org.continuent.sequoia.common.stream.LongUTFDataOutputStream;

/**
 * This class defines Serializers for SQL Data: per type serialization +
 * deserialization methods and information wrapped in one object. Serializers
 * are implemented as singletons for efficiency.
 * 
 * @author <a href="mailto:Marc.Herbert@continuent.com">Marc Herbert </a>
 * @author <a href="mailto:Gilles.Rayrat@continuent.com">Gilles Rayrat </a>
 * @version 1.0
 */
public final class SQLDataSerialization
{

  /**
   * Unsupported types listed in {@link TypeTag}won't be added in the near
   * future, except maybe java.net.URL for JDBC 3.0
   */
  /**
   * CLOB support should be easy to base on BLOB implementation once we figure
   * out the encoding issues.
   */

  private static final Serializer JAVA_STRING       = new StringSerializer();
  private static final Serializer MATH_BIGDECIMAL   = new BigDecimalBytesSerializer();
  private static final Serializer JAVA_BOOLEAN      = new BooleanSerializer();
  private static final Serializer JAVA_INTEGER      = new IntegerSerializer();
  private static final Serializer JAVA_LONG         = new LongSerializer();
  private static final Serializer JAVA_FLOAT        = new FloatSerializer();
  private static final Serializer JAVA_DOUBLE       = new DoubleSerializer();
  private static final Serializer JAVA_BYTES        = new BytesSerializer();

  private static final Serializer SQL_DATE          = new DateSerializer();
  private static final Serializer SQL_TIME          = new TimeSerializer();
  private static final Serializer SQL_TIMESTAMP     = new TimestampSerializer();

  private static final Serializer SQL_CLOB          = new ClobSerializer();
  private static final Serializer SQL_BLOB          = new BlobSerializer();

  private static final Serializer SQL_ARRAY         = new ArraySerializer();

  private static final Serializer JAVA_SERIALIZABLE = new JavaSerializableSerializer();

  /** Serializer returned for nulls. Public static final so we can == */
  private static final Serializer UNKNOWN_TYPE      = new UndefinedSerializer();

  private static final int        STREAM_BUF_SIZE   = 65536;

  // java.net.URL: TODO

  /** Abstract class hiding type-specific serialization methods and information */
  public abstract static class Serializer
  {
    protected TypeTag typeTag;

    /**
     * Special value: non-existing serializer of null references.
     * 
     * @return true if undefined
     */
    public boolean isUndefined()
    {
      return this == UNKNOWN_TYPE;
    }

    /**
     * @return the corresponding TypeTag
     */
    public TypeTag getTypeTag()
    {
      return typeTag;
    }

    /**
     * Serialize the object to the stream. Warning: the caller must ensure that
     * Serializer subtype and Object subtype are compatible.
     * 
     * @param obj object to send
     * @param output Output stream
     * @throws IOException stream error
     * @throws ClassCastException wrong serializer for this object type
     */

    public abstract void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException, ClassCastException;

    /**
     * De-serialize an object from the stream. Warning: the caller must ensure
     * that Serializer subtype and the incoming object are compatible.
     * 
     * @param input Input stream
     * @return the object received from the stream
     * @throws IOException stream error
     */
    public abstract Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException;

  }

  /**
   * Returns the de/serializer appropriate for the given TypeTag, or for the
   * type of the given SQL object if argument is not a TypeTag (TypeTag already
   * knows how to serialize itself).
   * 
   * @param sqlObjOrTypeTag a typetag or a sample SQL object of the type of
   *          interest
   * @return appropriate serialization + deserialization methods. Returns the
   *         UNKNOWN_TYPE serializer on "null" arg.
   * @throws NotImplementedException if we don't know how to serialize objects
   *           such as the given one. Set with a default message but caller has
   *           generally more useful information and should catch and replace
   *           (not even chain) this NotImplementedException by a better one.
   * @throws IllegalArgumentException if we gave a wrong TypeTag
   */
  public static Serializer getSerializer(Object sqlObjOrTypeTag)
      throws NotImplementedException, IllegalArgumentException
  {
    return getSerializerImpl(sqlObjOrTypeTag);
  }

  /** @see #getSerializer(Object) */
  public static Serializer getSerializer(TypeTag t)
      throws IllegalArgumentException
  {
    try
    {
      return getSerializerImpl(t);
    }
    catch (NotImplementedException nie)
    {
      // since we passed a TypeTag it's really impossible to come here
      IllegalArgumentException ipe = new IllegalArgumentException(
          "Internal bug: there should be a serializer available for every"
              + " existing TypeTag, including:" + t);
      ipe.initCause(nie);
      throw ipe;
    }
  }

  private static Serializer getSerializerImpl(Object sqlObjOrTypeTag)
      throws NotImplementedException, IllegalArgumentException
  {
    /*
     * Default values that never match anything: we just need any reference that
     * is both non-SQL and non-null.
     */
    TypeTag tag = TypeTag.CONTROLLER_READY;
    Object obj = JAVA_STRING;

    /**
     * Now let's get rid of all these nasty type issues for good by casting once
     * for all.
     */
    if (sqlObjOrTypeTag instanceof TypeTag)
      tag = (TypeTag) sqlObjOrTypeTag;
    else
      obj = sqlObjOrTypeTag;

    if (obj == null || TypeTag.UNDEFINED.equals(tag))
      return UNKNOWN_TYPE;

    /**
     * THE big switch on (type). "instanceof" is used on the serialization side,
     * "TypeTag.equals()" is used on the de-serialization side. We could for
     * performance split this method into two different methods (with the added
     * burden of keeping them perfectly synchronized)
     * 
     * @see TypeTag
     */
    // STRING
    if (obj instanceof String || TypeTag.STRING.equals(tag))
      return JAVA_STRING;

    // BIGDECIMAL
    if (obj instanceof BigDecimal || TypeTag.BIGDECIMAL.equals(tag))
      return MATH_BIGDECIMAL;

    // BOOLEAN
    if (obj instanceof Boolean || TypeTag.BOOLEAN.equals(tag))
      return JAVA_BOOLEAN;

    // INTEGER
    if (obj instanceof Integer || TypeTag.INTEGER.equals(tag))
      return JAVA_INTEGER;

    // LONG
    if (obj instanceof Long || TypeTag.LONG.equals(tag))
      return JAVA_LONG;

    // FLOAT
    if (obj instanceof Float || TypeTag.FLOAT.equals(tag))
      return JAVA_FLOAT;

    // DOUBLE
    if (obj instanceof Double || TypeTag.DOUBLE.equals(tag))
      return JAVA_DOUBLE;

    // BYTE ARRAY
    if (obj instanceof byte[] || TypeTag.BYTE_ARRAY.equals(tag))
      return JAVA_BYTES;

    // DATE
    if (obj instanceof java.sql.Date || TypeTag.SQL_DATE.equals(tag))
      return SQL_DATE;

    // TIME
    if (obj instanceof java.sql.Time || TypeTag.SQL_TIME.equals(tag))
      return SQL_TIME;

    // TIMESTAMP
    if (obj instanceof Timestamp || TypeTag.SQL_TIMESTAMP.equals(tag))
      return SQL_TIMESTAMP;

    // CLOB: TODO
    if (obj instanceof Clob || TypeTag.CLOB.equals(tag))
      return SQL_CLOB;

    // BLOB
    if (obj instanceof java.sql.Blob || TypeTag.BLOB.equals(tag))
      return SQL_BLOB;

    // Sql Array
    if (obj instanceof java.sql.Array || TypeTag.SQL_ARRAY.equals(tag))
    {
      return SQL_ARRAY;
    }
    // java.net.URL: TODO

    // Serializable Java object, MUST be last!
    if (sqlObjOrTypeTag instanceof Serializable
        || TypeTag.JAVA_SERIALIZABLE.equals(tag))
      return JAVA_SERIALIZABLE;

    if (sqlObjOrTypeTag instanceof TypeTag)
      throw new IllegalArgumentException(
          "Internal error: getSerializer() misused with unknown TypeTag argument:"
              + tag);

    // An alternative could be to tag only the problematic column, return a
    // "do-nothing" serializer and send the rest of the result set anyway.

    // Should be replaced by caller, see javadoc above
    throw new NotImplementedException("Unable to serialize unknown type "
        + sqlObjOrTypeTag.getClass() + " of object " + sqlObjOrTypeTag);
  }

  /*
   * These classes define one serializer per type
   */

  // STRING
  private static final class StringSerializer extends Serializer
  {
    {
      typeTag = TypeTag.STRING;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      output.writeLongUTF((String) obj);
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return input.readLongUTF();

    }
  }

  // we serialize this by sending the value as 4-bytes packs inside ints,
  // then by sending the scale as an integer
  private static final class BigDecimalBytesSerializer extends Serializer
  {
    {
      typeTag = TypeTag.BIGDECIMAL;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      // A BigDecimal is a BigInteger called 'value' and a scale
      // To serialize it, we first convert the value to a byte array using
      // BigInteger.toByteArray(). Then we serialize it using integers (for
      // perfomance purpose)
      // NOTE: we cannot directly use the int array reprensentation of the
      // BigIntegers because there is no public accessor for it (!!!)

      // The array of bytes returned by .toByteArray() is the two's-complement
      // representation of the BigInteger in big-endian byte-order.
      // But to save everyone's time and sweat, we actually split the
      // BigInteger into its sign and absolute value, avoiding the useless cost
      // of computing the two's complement for both the sender and the receiver.

      // To send this array of bytes in integers, we have to align the bytes.
      // This is done by padding the first bytes
      // For example, let's take the byte array AA BB CC DD EE (hexa values)
      // We will need two integers for these five bytes:
      // i1, the first integer, will be padded until byte array tail is aligned,
      // so i1 will be equal to '00 00 00 AA' while i2 will be 'BB CC DD EE'

      // Number to serialize
      BigDecimal toBeSerialized = (BigDecimal) obj;

      // 1. Send unscaled, absolute value:
      // 1.1 Send byte array size
      byte[] byteArray = toBeSerialized.unscaledValue().abs().toByteArray();
      output.writeInt(byteArray.length);

      // 1.2 Compute head-padding. The padding is done on the beginning of the
      // array. Zeros are send before first "real" byte in order to align the
      // array on integers
      int idx = 0, word = 0;
      int padding = byteArray.length % 4;
      if (padding > 0)
      {
        // This should be hard to read so:
        // bytes are shifted so that last byte is the least-significant byte of
        // the integer 'word'. More materially:
        // if padding is 1, no shift -> byte[0] is put at the tail of the int
        // if padding is 2, shift first byte from 8 while 2nd has no shift
        // if padding is 3, shift 1st byte from 16, 2nd from 8, none for 3rd
        for (idx = 0; idx < padding; idx++)
        {
          // 0xFF is needed because of sign extension
          word |= (byteArray[idx] & 0xFF) << (8 * (padding - idx - 1));
        }
        // let's write this padded bytes-as-integer
        output.writeInt(word);
      }
      // 1.3 Send the rest of the byte array 4 bytes by 4 bytes in an int
      // we start from the first aligned byte
      for (; idx < byteArray.length; idx += 4)
      {
        word = (byteArray[idx] & 0xFF) << 24
            | (byteArray[idx + 1] & 0xFF) << 16
            | (byteArray[idx + 2] & 0xFF) << 8 | byteArray[idx + 3] & 0xFF;
        output.writeInt(word);
      }

      // 1.4 Send sign as an int
      output.writeInt(toBeSerialized.signum());

      // 2. Send scale
      output.writeInt(toBeSerialized.scale());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      // new method - Deserialize int value then scale
      // 1. Read intVal:
      // 1.1 Read byte array length
      int byteArrayLength = input.readInt();
      byte[] byteArray = new byte[byteArrayLength];
      // 1.2 Compute padding
      int idx = 0, wordRead = 0;
      int padding = byteArrayLength % 4;
      // If there is a padding, we must read the first 'padding' bytes
      // so we are aligned for the rest of the bytes
      if (padding > 0)
      {
        wordRead = input.readInt();
        for (idx = 0; idx < padding; idx++)
        {
          byteArray[idx] = (byte) ((wordRead >> (8 * (padding - idx - 1))) & 0xFF);
        }
      }
      // 1.3 Read the byte array from integers
      // we start from the first aligned byte
      for (; idx < byteArrayLength; idx += 4)
      {
        wordRead = input.readInt();
        byteArray[idx] = (byte) ((wordRead >> 24) & 0xFF);
        byteArray[idx + 1] = (byte) ((wordRead >> 16) & 0xFF);
        byteArray[idx + 2] = (byte) ((wordRead >> 8) & 0xFF);
        byteArray[idx + 3] = (byte) (wordRead & 0xFF);
      }
      BigInteger intVal = new BigInteger(byteArray);

      // 1.4 read sign as an int
      if (input.readInt() < 0)
        intVal = intVal.negate();

      // 2. Read scale
      int scale = input.readInt();

      return new BigDecimal(intVal, scale);
    }
  }

  // BOOLEAN
  private static final class BooleanSerializer extends Serializer
  {
    {
      typeTag = TypeTag.BOOLEAN;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      output.writeBoolean(((Boolean) obj).booleanValue());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return new Boolean(input.readBoolean());
    }
  }

  // INTEGER
  private static final class IntegerSerializer extends Serializer
  {
    {
      typeTag = TypeTag.INTEGER;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      /**
       * let's also accept Short, see PostgreSQL bug explained here
       * 
       * @see org.continuent.sequoia.driver.DriverResultSet#initSerializers()
       */
      output.writeInt(((Number) obj).intValue());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return new Integer(input.readInt());
    }
  }

  // LONG
  private static final class LongSerializer extends Serializer
  {
    {
      typeTag = TypeTag.LONG;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      output.writeLong(((Long) obj).longValue());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return new Long(input.readLong());
    }
  }

  // FLOAT
  private static final class FloatSerializer extends Serializer
  {
    {
      typeTag = TypeTag.FLOAT;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      output.writeFloat(((Float) obj).floatValue());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return new Float(input.readFloat());
    }
  }

  // DOUBLE
  private static final class DoubleSerializer extends Serializer
  {
    {
      typeTag = TypeTag.DOUBLE;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      output.writeDouble(((Double) obj).doubleValue());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return new Double(input.readDouble());
    }
  }

  // BYTE ARRAY
  private static final class BytesSerializer extends Serializer
  {
    {
      typeTag = TypeTag.BYTE_ARRAY;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      byte[] b = (byte[]) obj;
      output.writeInt(b.length);
      output.write(b);
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      int len = input.readInt();
      byte[] b = new byte[len];
      input.readFully(b);
      return b;
    }
  }

  // DATE
  private static final class DateSerializer extends Serializer
  {
    {
      typeTag = TypeTag.SQL_DATE;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      output.writeLong(((java.sql.Date) obj).getTime());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return new java.sql.Date(input.readLong());
    }
  }

  // TIME
  private static final class TimeSerializer extends Serializer
  {
    {
      typeTag = TypeTag.SQL_TIME;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      output.writeInt((int) ((java.sql.Time) obj).getTime());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      return new java.sql.Time(input.readInt());
    }
  }

  // TIMESTAMP
  private static final class TimestampSerializer extends Serializer
  {
    {
      typeTag = TypeTag.SQL_TIMESTAMP;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      Timestamp ts = (Timestamp) obj;
      // put the milliseconds trick/CPU load on the driver side
      output.writeLong(ts.getTime());
      output.writeInt(ts.getNanos());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      long tsWithMilli = input.readLong();
      // we don't want the milliseconds twice
      Timestamp ts = new Timestamp((tsWithMilli / 1000) * 1000);
      ts.setNanos(input.readInt());
      return ts;
    }
  }

  // CLOB
  private static final class ClobSerializer extends Serializer
  {
    {
      typeTag = TypeTag.CLOB;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      java.sql.Clob clob = (java.sql.Clob) obj;
      output.writeLongUTF(clob.toString());
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      String stringClob = input.readLongUTF();
      return new org.continuent.sequoia.common.protocol.StringClob(stringClob);
    }
  }

  // BLOB
  private static final class BlobSerializer extends Serializer
  {
    {
      typeTag = TypeTag.BLOB;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      java.sql.Blob blob = (java.sql.Blob) obj;
      try
      {
        // Be very careful to be compatible with JAVA_BYTES.sendToStream(),
        // since we use JAVA_BYTES.receiveFromStream on the other side.
        // We don't use it on this side to save memory.

        if (blob.length() > Integer.MAX_VALUE)
          // FIXME: this is currently corrupting protocol with driver
          throw new IOException("Blobs bigger than " + Integer.MAX_VALUE
              + " are not supported");

        // send the size of the byte array
        output.writeInt((int) blob.length());

        byte[] tempBuffer = new byte[STREAM_BUF_SIZE];
        java.io.InputStream input = blob.getBinaryStream();
        int nbRead;
        while (true)
        {
          nbRead = input.read(tempBuffer);
          if (-1 == nbRead)
            break;
          output.write(tempBuffer, 0, nbRead);
        }
      }
      catch (SQLException e)
      {
        // Exceptions for Blobs is unfortunately tricky because we can't know in
        // advance if a java array will be big enough (2^31) to hold them.
        throw (IOException) new IOException(e.getLocalizedMessage())
            .initCause(e);
      }
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      byte[] b = (byte[]) JAVA_BYTES.receiveFromStream(input);
      return new org.continuent.sequoia.common.protocol.ByteArrayBlob(b);
    }
  }

  // JAVA_SERIALIZABLE
  private static final class JavaSerializableSerializer extends Serializer
  {
    {
      typeTag = TypeTag.JAVA_SERIALIZABLE;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();

      // send first the size of the byte array
      output.writeInt(baos.size());
      baos.writeTo(output);
      baos.close();
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      byte[] b = (byte[]) JAVA_BYTES.receiveFromStream(input);
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(b));
      try
      {
        Object obj = ois.readObject();
        return obj;
      }
      catch (ClassNotFoundException e)
      {
        ClassCastException ioe = new ClassCastException(
            "Class of deserialized object not found");
        ioe.initCause(e);
        throw ioe;
      }
    }
  }

  // UNKNOWN TYPE
  private static final class UndefinedSerializer extends Serializer
  {
    {
      typeTag = TypeTag.UNDEFINED;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
    {
      throw new RuntimeException(
          "Internal bug: tried to send using the UNDEFINED serializer");
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws ClassCastException
    {
      throw new RuntimeException(
          "Internal bug: tried to receive using the UNDEFINED deserializer");
    }
  }
  private static final class ArraySerializer extends Serializer
  {
    {
      typeTag = TypeTag.SQL_ARRAY;
    }

    public void sendToStream(Object obj, LongUTFDataOutputStream output)
        throws IOException
    {
      int sql_type = 0;
      Object baseObj = null;
      String baseTypeName = null;

      try
      {
        // if any of these triggers an exception, we need to make sure we won't
        // try to serialize the object, otherwise it would cause problems in
        // receiving end.
        java.sql.Array sql_array = (java.sql.Array) obj;
        baseTypeName = sql_array.getBaseTypeName();
        sql_type = sql_array.getBaseType();
        baseObj = sql_array.getArray();
      }
      catch (SQLException sqle)
      {
        sql_type = java.sql.Types.NULL;
        baseObj = null;
        baseTypeName = "null";
      }

      if (baseObj == null)
      {
        // If we had any errors in serialization of the object,
        // only output null type
        // => the receiver end will know we had an error here and can cope with
        // it.
        output.writeInt(java.sql.Types.NULL);
        return;
      }

      // Write basic information of the array:
      // - sql type
      // - sql type name
      output.writeInt(sql_type);
      output.writeLongUTF(baseTypeName);

      // Since baseObj is always Object[], it can be serialized using standard
      // java serialization.
      JAVA_SERIALIZABLE.sendToStream(baseObj, output);
    }

    public Object receiveFromStream(LongUTFDataInputStream input)
        throws IOException
    {
      Object[] oa = null;
      String typeName = null;
      // Read basic information from the array
      // sql type
      // sql type name
      int type = input.readInt();

      if (type != java.sql.Types.NULL)
      {
        typeName = input.readLongUTF();
        try
        {
          oa = (Object[]) JAVA_SERIALIZABLE.receiveFromStream(input);
        }
        catch (ClassCastException cce)
        {
          throw new IOException(cce.getClass().getName() + " in "
              + this.getClass().getName() + " " + cce.getMessage());
        }
      }
      else
      {
        // if there was problems serializing theArray Object, let's just create
        // an empty object array.
        oa = new Object[0];
        typeName = new String("Array Error");
      }
      // create new Object implementing java.sql.Array
      return new Array(oa, type, typeName);
    }

  }

}
