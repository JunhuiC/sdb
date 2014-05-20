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
 * Contributor(s): 
 */

package org.continuent.sequoia.common.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;

import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * Validate a document and its DTD.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 */
public class XmlValidator extends DefaultHandler
    implements
      ErrorHandler,
      LexicalHandler
{

  /** XML parser. */
  private XMLReader parser;
  private String    pathToDtd;
  private boolean   isXmlValid = false;
  private boolean   isDtdValid = false;
  private String    xmlContent;
  private ArrayList<Exception> errors;
  private ArrayList<SAXParseException> warnings;

  /**
   * Allow to use the xml validator as an external program
   * 
   * @param args the xmlfile and the dtd file
   * @throws Exception if fails
   */
  public static void main(String[] args) throws Exception
  {
    if (args.length < 1 || args.length > 2)
    {
      System.out.println("usage: XmlValidator [xmlFile] ([dtd]) ");
      System.exit(0);
    }

    String fileName = args[0];
    String dtdName = ControllerConstants.SEQUOIA_DTD_FILE;
    if (args.length == 2)
      dtdName = args[1];
    else
      System.out.println("Using default DTD:" + ControllerConstants.SEQUOIA_DTD_FILE);

    File dtd = null;
    dtd = new File(ClassLoader.getSystemResource(dtdName).getFile());
    File xmlFile = null;
    try
    {
      xmlFile = new File(ClassLoader.getSystemResource(fileName).getFile());
    }
    catch (RuntimeException e)
    {
      xmlFile = new File(fileName);
    }

    if (!dtd.exists())
    {
      System.out.println("Cannot find specified dtd");
      System.exit(1);
    }
    if (!xmlFile.exists())
    {
      System.out.println("Cannot find specified xml file");
      System.exit(1);
    }

    System.out.println("Validating:\tFile:" + xmlFile.getName() + " with dtd:"
        + dtd.getName());

    // Validate xml and dtd
    XmlValidator validator = new XmlValidator(dtd.getAbsolutePath(),
        new FileReader(xmlFile));

    // Display Results
    if (!validator.isDtdValid())
      System.out.println("[FAILED:Dtd is not valid]");
    else if (!validator.isXmlValid())
      System.out.println("[FAILED:xml is not valid]");
    else if (validator.isXmlValid())
      System.out.println("[OK]");

    if (validator.getLastException() != null)
    {
      ArrayList<Exception> errors = validator.getExceptions();
      for (int i = 0; i < errors.size(); i++)
        System.out.println("\t(parsing error):"
            + ((Exception) errors.get(i)).getMessage());
    }
  }

  /**
   * Check the given dtd, and the given xml are valid.
   * 
   * @param pathToDtd path to dtd
   * @param xml source to parse as a string
   */
  public XmlValidator(String pathToDtd, String xml)
  {
    validate(pathToDtd, xml);
  }

  /**
   * @see #XmlValidator(String pathToDtd,String xml)
   */
  public XmlValidator(String pathToDtd, FileReader file) throws IOException
  {
    // Read the file
    BufferedReader in = new BufferedReader(file);
    StringBuffer xml = new StringBuffer();
    String line;
    do
    {
      line = in.readLine();
      if (line != null)
        xml.append(line.trim());
    }
    while (line != null);
    xmlContent = xml.toString();
    validate(pathToDtd, xmlContent);
  }

  /**
   * get the xml that was formatted
   * 
   * @return xml
   */
  public String getXmlContent()
  {
    return xmlContent;
  }

  /**
   * Starts the verification of the xml document AND the dtd
   * 
   * @param pathToDtd path
   * @param xml content
   */
  public void validate(String pathToDtd, String xml)
  {
    System.setProperty("org.xml.sax.driver",
        "org.apache.crimson.parser.XMLReaderImpl");
    errors = new ArrayList<Exception>();
    warnings = new ArrayList<SAXParseException>();
    try
    {
      // Store dtd reference
      this.pathToDtd = pathToDtd;
      // Instantiate a new parser
      parser = XMLReaderFactory.createXMLReader();
      // Activate validation
      parser.setFeature("http://xml.org/sax/features/validation", true);
      // Install error handler
      parser.setErrorHandler(this);
      // Install document handler
      parser.setContentHandler(this);
      parser.setProperty("http://xml.org/sax/properties/lexical-handler", this);
      // Install local entity resolver
      parser.setEntityResolver(this);
      InputSource input = new InputSource(new StringReader(xml));
      parser.parse(input);
    }
    catch (Exception e)
    {
      //throw new Exception("Xml document can not be validated.");
      //e.printStackTrace();
      addError(e);
      isXmlValid = false;
    }
  }

  /**
   * Allows to parse the document with a local copy of the DTD whatever the
   * original <code>DOCTYPE</code> found. Warning, this method is called only
   * if the XML document contains a <code>DOCTYPE</code>.
   * 
   * @see org.xml.sax.EntityResolver#resolveEntity(java.lang.String,
   *      java.lang.String)
   */
  public InputSource resolveEntity(String publicId, String systemId)
      throws SAXException
  {

    File dtd = new File(pathToDtd);
    if (dtd.exists())
    {
      try
      {
        FileReader reader = new FileReader(dtd);
        return new InputSource(reader);
      }
      catch (Exception e)
      { //impossible
      }
    }

    InputStream stream = XmlValidator.class
        .getResourceAsStream("/" + pathToDtd);
    if (stream == null)
    {
      SAXException sax = new SAXException(Translate.get(
          "virtualdatabase.xml.dtd.not.found", ControllerConstants.PRODUCT_NAME, pathToDtd)); //$NON-NLS-1$
      addError(sax);
      throw sax;
    }

    return new InputSource(stream);
  }

  /**
   * @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException)
   */
  public void error(SAXParseException exception) throws SAXException
  {
    addError(exception);
  }

  /**
   * @see org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
   */
  public void fatalError(SAXParseException exception) throws SAXException
  {
    addError(exception);
  }

  /**
   * @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException)
   */
  public void warning(SAXParseException exception) throws SAXException
  {
    warnings.add(exception);
  }

  /**
   * @see org.xml.sax.ContentHandler#endDocument()
   */
  public void endDocument() throws SAXException
  {
    if (errors.size() == 0)
      this.isXmlValid = true;
  }

  /**
   * @return Returns the isXmlValid.
   */
  public boolean isValid()
  {
    return isXmlValid && isDtdValid;
  }

  /**
   * Return the last cause of parsing failure
   * 
   * @return exception, null if no exception
   */
  public Exception getLastException()
  {
    if (errors.size() == 0)
      return null;
    else
      return (Exception) errors.get(errors.size() - 1);
  }

  /**
   * Retrieve an <code>ArrayList</code> of all parsing exceptions
   * 
   * @return an <code>ArrayList</code> of <code>Exception</code>
   */
  public ArrayList<Exception> getExceptions()
  {
    return errors;
  }

  /**
   * @see org.xml.sax.ext.LexicalHandler#comment(char[], int, int)
   */
  public void comment(char[] ch, int start, int length) throws SAXException
  {
  }

  /**
   * @see org.xml.sax.ext.LexicalHandler#endCDATA()
   */
  public void endCDATA() throws SAXException
  {
  }

  /**
   * @see org.xml.sax.ext.LexicalHandler#endDTD()
   */
  public void endDTD() throws SAXException
  {
    if (errors.size() == 0)
    {
      isDtdValid = true;
    }
    else
    {
      isDtdValid = false;
    }
  }

  /**
   * @see org.xml.sax.ext.LexicalHandler#endEntity(java.lang.String)
   */
  public void endEntity(String name) throws SAXException
  {
  }

  /**
   * @see org.xml.sax.ext.LexicalHandler#startCDATA()
   */
  public void startCDATA() throws SAXException
  {
  }

  /**
   * @see org.xml.sax.ext.LexicalHandler#startDTD(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  public void startDTD(String name, String publicId, String systemId)
      throws SAXException
  {
  }

  /**
   * @see org.xml.sax.ext.LexicalHandler#startEntity(java.lang.String)
   */
  public void startEntity(String name) throws SAXException
  {
  }

  /**
   * @return Returns the isDtdValid.
   */
  public boolean isDtdValid()
  {
    return isDtdValid;
  }

  /**
   * @param isDtdValid The isDtdValid to set.
   */
  public void setDtdValid(boolean isDtdValid)
  {
    this.isDtdValid = isDtdValid;
  }

  /**
   * @return Returns the isXmlValid.
   */
  public boolean isXmlValid()
  {
    return isXmlValid;
  }

  /**
   * @param isXmlValid The isXmlValid to set.
   */
  public void setXmlValid(boolean isXmlValid)
  {
    this.isXmlValid = isXmlValid;
  }

  private void addError(Exception e)
  {
    errors.add(e);
  }

  /**
   * @return Returns the warnings.
   */
  public ArrayList<SAXParseException> getWarnings()
  {
    return warnings;
  }
}