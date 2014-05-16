
package org.continuent.sequoia.controller.core;

import java.io.IOException;
import java.io.PrintStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.continuent.sequoia.common.util.Constants;

/**
 * This class can be used to display information on the Controller and its
 * runtime environment
 */
public class ControllerInfo
{

  private static Map getManifestHeaders()
  {
    // akward way to retrieve the manifest file of the
    // jar containing the ControllerInfo class
    String resource = "/" + ControllerInfo.class.getName().replace('.', '/')
        + ".class";
    URL url = ControllerInfo.class.getResource(resource);
    JarURLConnection conn;
    try
    {
      conn = (JarURLConnection) url.openConnection();
      if (conn == null)
      {
        return new HashMap();
      }
      Manifest manifest = conn.getManifest();
      return manifest.getMainAttributes();
    }
    catch (IOException e)
    {
      return new HashMap();
    }
  }

  /**
   * Print information on the controller and its runtime environment
   * on the specified PrintStream.
   * 
   * @param out the PrintStream which is used to print the information
   */
  public static void printOn(PrintStream out)
  {
    out.println("Controller name:    " + ControllerConstants.PRODUCT_NAME);
    out.println("Controller version: " + Constants.VERSION);
    Map headers = getManifestHeaders();
    if (headers.containsKey(new Attributes.Name("Build-Number")))
    {
      out.println("Build Number:       "
          + headers.get(new Attributes.Name("Build-Number")));
    }
    out.println("OS Name:            " + System.getProperty("os.name"));
    out.println("OS Version:         " + System.getProperty("os.version"));
    out.println("Architecture:       " + System.getProperty("os.arch"));
    out.println("JVM Version:        " + System.getProperty("java.runtime.version"));
    out.println("JVM Vendor:         " + System.getProperty("java.vm.vendor"));
  }

  public static void main(String[] args)
  {
    printOn(System.out);
  }

}
