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
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.common.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

/**
 * Defines the SplitXml Ant target used to prepare the Sequoia scripts
 * generation.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk</a>
 * @author <a href="mailto:mathieu.peltier@emicnetworks.com">Mathieu Peltier</a>
 * @version 1.0
 */
public class SplitXmlTask extends Task
{
  private String xmlFilePath;
  private String outputDir;
  private String attributeName;
  private String startTagName;
  private String endTagName;

  /**
   * @see org.apache.tools.ant.Task#execute()
   */
  public void execute() throws BuildException
  {
    try
    {
      File baseDir = getProject().getBaseDir();
      File sourceFile = new File(baseDir, xmlFilePath);
      BufferedReader reader = new BufferedReader(new FileReader(sourceFile));
      String lineBuffer;
      while ((lineBuffer = reader.readLine()) != null)
      {
        if (lineBuffer.indexOf(startTagName) != -1)
        {
          int index = lineBuffer.indexOf(attributeName)
              + attributeName.length() + 2;
          String fileName = lineBuffer.substring(index, lineBuffer.indexOf(
              '\"', index));
          File generatedFile = new File(outputDir + File.separator + fileName
              + ".xml");
          if (generatedFile.lastModified() < sourceFile.lastModified())
          {
            // Source file has been modified, so regenerate xml file
            BufferedWriter writer = new BufferedWriter(new FileWriter(
                generatedFile));
            writer.write(lineBuffer + System.getProperty("line.separator"));
            while ((lineBuffer = reader.readLine()) != null
                && lineBuffer.indexOf(endTagName) == -1)
            {
              writer.write(lineBuffer + System.getProperty("line.separator"));
            }
            if (lineBuffer != null) // append last line
              writer.write(lineBuffer + System.getProperty("line.separator"));
            writer.flush();
            writer.close();
          }
          else
          {
            // Go to next file because source file has not been modified
            do
            {
              lineBuffer = reader.readLine();
            }
            while ((lineBuffer != null) && lineBuffer.indexOf(endTagName) == -1);
          }
          continue;
        }
      }
    }
    catch (Exception e)
    {
      throw new BuildException(e.getMessage());
    }
  }

  /**
   * Set the path to the xml path containing the scripts definition.
   * 
   * @param xmlFilePath path to the xml file
   */
  public void setScriptXmlFile(String xmlFilePath)
  {
    this.xmlFilePath = xmlFilePath;
  }

  /**
   * Specify the output directory.
   * 
   * @param outputDirPath the path to the directory
   */
  public void setOutputDir(String outputDirPath)
  {
    this.outputDir = outputDirPath;
    File newDir = new File(outputDir);
    newDir.mkdirs();
  }

  /**
   * Set parsing tag name.
   * 
   * @param tagName the tag name
   */
  public void setParsingTagName(String tagName)
  {
    this.startTagName = "<" + tagName + " ";
    this.endTagName = "</" + tagName + ">";
  }

  /**
   * Set the attribute that contains the name of the file.
   * 
   * @param attributeName the name of the attribute to get the name of the file
   *          to write
   */
  public void setOuputFileAttribute(String attributeName)
  {
    this.attributeName = attributeName;
  }
}