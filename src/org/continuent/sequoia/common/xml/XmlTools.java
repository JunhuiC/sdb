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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.xml;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.log.Trace;

/**
 * This class defines a XmlTools
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public final class XmlTools
{

  /** Logger instance. */
  static Trace                      logger = Trace
                                               .getLogger("org.continuent.sequoia.common.xml");

  private static final String PRETTIFY_XSL = "org/continuent/sequoia/common/xml/prettify.xsl";
  
  /** XSL Transformation */
  private static TransformerFactory tFactory;
  private static Transformer        infoTransformer;

  /**
   * Indent xml with xslt
   * 
   * @param xml to indent
   * @return indented xml
   * @throws Exception if an error occurs
   */
  public static String prettyXml(String xml) throws Exception
  {
    return applyXsl(xml, PRETTIFY_XSL);
  }

  /**
   * Apply xslt to xml
   * 
   * @param xml to transform
   * @param xsl transformation to apply
   * @return xml formatted string or error message
   */
  private static String applyXsl(String xml, String xsl)
  {
    try
    {
      StringWriter result = new StringWriter();
      if (tFactory == null)
        tFactory = TransformerFactory.newInstance();
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("controller.xml.use.xsl", xsl));
      // if(infoTransformer==null)
      infoTransformer = tFactory.newTransformer(new StreamSource(ClassLoader
          .getSystemResourceAsStream(xsl)));
      infoTransformer.transform(new StreamSource(new StringReader(xml)),
          new StreamResult(result));
      return result.toString();
    }
    catch (Exception e)
    {
      String msg = Translate.get("controller.xml.transformation.failed", e);

      if (logger.isErrorEnabled())        
        logger.error(msg, e);
      return msg;
    }
  }

  /**
   * Insert a doctype in a XML file. Ugly hack: the DOCTYPE is inserted this way
   * since the DOCTYPE is stripped from the xml when applying the pretty xsl
   * stylesheet and I could not find a way to access it from within the xsl. Any
   * suggestion is welcome...
   * 
   * @param xml XML content
   * @param doctype the DTD Doctype to insert
   * @return the xml where the DTD doctype has been inserted so that the xml can
   *         be validated against this DTD
   */
  public static String insertDoctype(String xml, String doctype)
  {
    int index = xml.indexOf("?>");
    if (index < 0)
    {
      return xml;
    }
    String xmlWithDoctype = xml.substring(0, index + 2);
    xmlWithDoctype += "\n";
    xmlWithDoctype += doctype;
    xmlWithDoctype += "\n";
    xmlWithDoctype += xml.substring(index + 3, xml.length());
    return xmlWithDoctype;
  }
}