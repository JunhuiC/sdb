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

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * This class defines a BlobOutputStream.
 * <p>
 * TODO: we should implement close() then error once we are closed.
 * 
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert</a>
 * @version 1.0
 */
public class ByteArrayBlobOutputStream
    extends OutputStream
{
  /** The actual Blob we are pointing to */
  ByteArrayBlob blob;
  /** The current offset in the stream, counting from zero (NOT SQL style) */
  int  currentPos;

  /**
   * Creates a new <code>BlobOutputStream</code> object pointing to the given
   * Blob (currently implemented as an array).
   * 
   * @param b the reference to the underlying blob
   * @param startPos the starting position in the array (counting from zero).
   */
  public ByteArrayBlobOutputStream(ByteArrayBlob b, int startPos)
  {
    super();
    this.blob = b;
    currentPos = startPos;
  }

  /**
   * @see java.io.OutputStream#write(int)
   */
  public void write(int b) throws IOException
  {
    blob.getInternalByteArray()[currentPos] = (byte) b;
    currentPos++;
  }

  /**
   * @see java.io.OutputStream#write(byte[], int, int)
   */
  public void write(byte[] b, int off, int len) throws IOException
  {
    try
    {
      // SQL indexes count from 1
      blob.setBytes(currentPos + 1, b, off, len);
      currentPos += len;
    }
    catch (SQLException sqle)
    {
      throw (IOException) new IOException(sqle.getLocalizedMessage())
          .initCause(sqle);
    }

  }

}
