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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.stream.encoding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * This class defines ZipEncoding/Decoding methods
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class ZipEncoding
{
  /**
   * Encode data using ZIP compression
   * 
   * @param data byte array to compress
   * @return <code>byte[]</code> of zip encoded data
   * @throws IOException if fails reading/writing streams
   */
  public static final byte[] encode(byte[] data) throws IOException
  {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    //GZIPOutputStream zipOutputStream = new GZIPOutputStream(baos);
    DeflaterOutputStream zipOutputStream = new DeflaterOutputStream(baos,
        new Deflater(Deflater.BEST_COMPRESSION, true));

    //BufferedOutputStream bos = new BufferedOutputStream(zipOutputStream);
    byte[] bdata = new byte[1024];
    int byteCount;
    while ((byteCount = bais.read(bdata, 0, 1024)) > -1)
    {
      zipOutputStream.write(bdata, 0, byteCount);
    }
    zipOutputStream.flush();
    zipOutputStream.finish();
    zipOutputStream.close();
    return baos.toByteArray();
  }

  /**
   * Decode data using ZIP Decompression
   * 
   * @param data the encoded data
   * @return <code>byte[]</code> of decoded data
   * @throws IOException if fails
   */
  public static final byte[] decode(byte[] data) throws IOException
  {
    InflaterInputStream input = new InflaterInputStream(
        new ByteArrayInputStream(data), new Inflater(true));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] bdata = new byte[1024];
    int byteCount;
    while ((byteCount = input.read(bdata, 0, 1024)) > -1)
      baos.write(bdata, 0, byteCount);
    baos.flush();
    baos.close();

    return baos.toByteArray();
  }
}