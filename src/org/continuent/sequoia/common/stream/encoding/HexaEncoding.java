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

/**
 * This class implements Hexa encoding and decoding
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class HexaEncoding
{
  /**
   * Convert data into hexa
   * 
   * @param data to convert
   * @return the converted string
   */
  public static final String data2hex(byte[] data)
  {
    if (data == null)
      return null;

    int len = data.length;
    StringBuffer buf = new StringBuffer(len * 2);
    for (int pos = 0; pos < len; pos++)
      buf.append(toHexChar((data[pos] >>> 4) & 0x0F)).append(
          toHexChar(data[pos] & 0x0F));
    return buf.toString();
  }

  /**
   * convert hexa into data
   * 
   * @param str to convert
   * @return the converted byte array
   */
  public static final byte[] hex2data(String str)
  {
    if (str == null)
      return new byte[0];

    int len = str.length();
    char[] hex = str.toCharArray();
    byte[] buf = new byte[len / 2];

    for (int pos = 0; pos < len / 2; pos++)
      buf[pos] = (byte) (((toDataNibble(hex[2 * pos]) << 4) & 0xF0) | (toDataNibble(hex[2 * pos + 1]) & 0x0F));

    return buf;
  }

  /**
   * convert value to hexa value
   * 
   * @param i byte to convert
   * @return hexa char
   */
  public static char toHexChar(int i)
  {
    if ((0 <= i) && (i <= 9))
      return (char) ('0' + i);
    else
      return (char) ('a' + (i - 10));
  }

  /**
   * convert hexa char to byte value
   * 
   * @param c hexa character
   * @return corresponding byte value
   */
  public static byte toDataNibble(char c)
  {
    if (('0' <= c) && (c <= '9'))
      return (byte) ((byte) c - (byte) '0');
    else if (('a' <= c) && (c <= 'f'))
      return (byte) ((byte) c - (byte) 'a' + 10);
    else if (('A' <= c) && (c <= 'F'))
      return (byte) ((byte) c - (byte) 'A' + 10);
    else
      return -1;
  }
}
