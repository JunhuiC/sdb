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
 * Contributor(s): Emmanuel Cecchet. Marc Herbert.
 */

package org.continuent.sequoia.common.stream;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;

/**
 * Decorates DataInputStream with the {@link #readLongUTF()} method that allows
 * reading of UTF strings larger than <code>65535</code> bytes
 * 
 * @see java.io.DataInputStream
 * @see org.continuent.sequoia.common.stream.DriverBufferedOutputStream
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @author <a href="mailto:Gilles.Rayrat@emicnetworks.com">Gilles Rayrat</a>
 */
public class LongUTFDataInputStream extends DataInputStream
{
  private final CharsetDecoder utf8dec = DriverStream.UTF8Codec.newDecoder();

  /**
   * @see DataInputStream#DataInputStream(java.io.InputStream)
   */
  public LongUTFDataInputStream(InputStream in)
  {
    super(in);
  }

  /**
   * @see LongUTFDataOutputStream#writeLongUTF(String)
   * @return a String in UTF format
   * @throws IOException if an error occurs
   */
  public String readLongUTF() throws IOException
  {
    if (!super.readBoolean())
      return null;

    final int maxSize = DriverStream.STRING_CHUNK_SIZE;

    int strlen = super.readInt();
    StringBuffer sbuf = new StringBuffer(strlen);

    // idx semantic: chars at idx and after had not yet the opportunity
    // to be received.
    for (int idx = 0; idx < strlen; idx += maxSize)
      sbuf.append(readUTF8());

    return new String(sbuf);
  }

  /**
   * @return decoded string from stream
   * @throws IOException network error
   * @see LongUTFDataOutputStream#writeUTF8(String)
   * @see org.continuent.sequoia.common.protocol.SQLDataSerialization.BytesSerializer
   */
  String readUTF8() throws IOException
  {
    int len = super.readInt();
      byte[] b = new byte[len];
      super.readFully(b);
      ByteBuffer bb = ByteBuffer.wrap(b); // no copy, nice.
      CharBuffer cb = utf8dec.decode(bb);
      return cb.toString();
  }
}