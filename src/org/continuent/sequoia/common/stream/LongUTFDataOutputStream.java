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
 * Contributor(s): Emmanuel Cecchet, Marc Herbert
 */

package org.continuent.sequoia.common.stream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;

/**
 * Decorates a DataOutputStream with the {@link #writeLongUTF(String)} method
 * that allows writing of UTF strings larger than <code>65535</code> bytes
 * 
 * @see java.io.DataOutputStream
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @author <a href="mailto:Gilles.Rayrat@emicnetworks.com">Gilles Rayrat</a>
 */
public class LongUTFDataOutputStream extends DataOutputStream
{

  private final CharsetEncoder utf8enc = DriverStream.UTF8Codec.newEncoder();

  /**
   * @see DataOutputStream#DataOutputStream(java.io.OutputStream)
   */
  public LongUTFDataOutputStream(OutputStream out)
  {
    super(out);
  }

  /**
   * Sends UTF strings larger than <code>65535</code> bytes (encoded), chunk
   * by chunk. Historically the purpose of these functions was to work around
   * the limitation of {@link java.io.DataInputStream#readUTF()}, but now we
   * use real UTF8, and no more modified UTF8
   * http://en.wikipedia.org/wiki/UTF-8#Modified_UTF-8. Chunking is still useful
   * to avoid handling big strings all at once and being a memory hog.
   * 
   * @see java.io.DataOutputStream#writeUTF(java.lang.String)
   * @param string a String to write in UTF form to the stream
   * @throws IOException if an error occurs
   */
  public void writeLongUTF(String string) throws IOException
  {
    if (null == string)
    {
      super.writeBoolean(false);
      return;
    }

    super.writeBoolean(true);
    int idx;
    final int maxSize = DriverStream.STRING_CHUNK_SIZE;

    this.writeInt(string.length());

    // First send all full, maxSize long chunks
    for (idx = 0; idx + maxSize <= string.length(); idx += maxSize)
      // substring() does no copy, cool.
      writeUTF8(string.substring(idx, idx + maxSize));

    // Send the tail separately because
    // - string.substring(begin, TOO_LONG) is unfortunately not legal.
    // - we do not send any empty string, this is useless and would complexify
    // the receiver.
    // The tail is in most (short) cases just the string as is.

    if (string.length() > idx)
      writeUTF8(string.substring(idx));
  }

  /**
   * Sending real UTF-8, not the modified one.
   * 
   * @throws IOException
   * @see org.continuent.sequoia.common.protocol.SQLDataSerialization.BytesSerializer
   */
  void writeUTF8(String s) throws IOException
  {
    CharBuffer cb = CharBuffer.wrap(s); // no copy; good.
      ByteBuffer bb = utf8enc.encode(cb);
      super.writeInt(bb.remaining());
      super.write(bb.array(), 0, bb.remaining()); // no copy either
  }
}
