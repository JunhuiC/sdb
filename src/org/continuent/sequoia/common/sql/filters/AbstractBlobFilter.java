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
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.common.sql.filters;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This class defines a BlobFilterInterface. All implementing interface should
 * satisfy the following: - Implementation is not dependant of the database -
 * decode(encode(data)) = data
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.fr">Emmanuel Cecchet
 *         </a>
 * @version 1.0
 */
public abstract class AbstractBlobFilter
{
  private static final Base64Filter      BASE64FILTER        = new Base64Filter();
  private static final Base64ZipFilter   BASE64_ZIP_FILTER   = new Base64ZipFilter();
  private static final HexaBlobFilter    HEXA_BLOB_FILTER    = new HexaBlobFilter();

  /**
   * Get an instance of an <code>AbstractBlobFilter</code> given the
   * blobEndodingMethod description. Currently supported are: <br>
   * <code>hexa</code><br>
   * <code>base64</code><br>
   * <code>base64zip</code><br>
   * <code>escaped</code><br>
   * If the parameter specified is not appropriate then a
   * <code>NoneBlobFilter</code> instance is returned.
   * 
   * @param blobEncodingMethod the string description
   * @return <code>AbstractBlobFilter</code> instance
   */
  public static AbstractBlobFilter getBlobFilterInstance(
      String blobEncodingMethod)
  {
    if (blobEncodingMethod.equals(DatabasesXmlTags.VAL_base64))
      return BASE64FILTER;
    if (blobEncodingMethod.equals(DatabasesXmlTags.VAL_base64zip))
      return BASE64_ZIP_FILTER;
    else if (blobEncodingMethod.equals(DatabasesXmlTags.VAL_hexa))
      return HEXA_BLOB_FILTER;
    else
      throw new RuntimeException("Unknown Blob encoder " + blobEncodingMethod);
  }

  /**
   * Returns the default blob filter. This is used for serializing
   * PreparedStatement parameters in driverProcessed=false mode.
   * 
   * @return the default blob filter.
   */
  public static AbstractBlobFilter getDefaultBlobFilter()
  {
    return BASE64FILTER;
  }
  
  /**
   * Encode the blob data in a form that is independant of the database.
   * 
   * @param data the byte array to convert
   * @return <code>String</code> object is returned for convenience as this is
   *         the way it is going to be handled afterwards.
   */
  public abstract String encode(byte[] data);

  /**
   * Decode the blob data from the database. This must done in a database
   * independant manner.
   * 
   * @param data the data to decode
   * @return <code>byte[]</code> decoded byte array of data
   */
  public abstract byte[] decode(String data);

  /**
   * Get the XML attribute value of the filter as defined in the DTD.
   * 
   * @return XML attribute value
   */
  public abstract String getXml();
}