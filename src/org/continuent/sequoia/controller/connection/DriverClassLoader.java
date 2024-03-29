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
 * Initial developer(s): Marc Wick.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.connection;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * This class defines a DriverClassLoader used to load drivers with their own
 * classloder to be able to handle different implementations of drivers sharing
 * the same class name. For example if you want to connect to two backends of
 * the same vendor, but running with different releases and requiring a driver
 * compatible with the respective database release
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DriverClassLoader extends ClassLoader
{

  /** path on filesystem where the driver is located */
  private File path = null;

  /**
   * Creates a new <code>DriverClassLoader.java</code> object
   * 
   * @param parent classloader, null if no parent classloader should be used
   * @param pPath path where the driver classfiles of jar files are located
   */
  public DriverClassLoader(ClassLoader parent, File pPath)
  {
    super(parent);
    path = pPath;
    if (path == null)
      path = new File("");
  }

  /**
   * @see java.lang.ClassLoader#loadClass(java.lang.String, boolean)
   */
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException
  {
    try
    {
      return super.loadClass(name, resolve);
    }
    catch (ClassNotFoundException e)
    {
      Class<?> c = findClass(name);
      if (resolve)
        resolveClass(c);
      return c;
    }
  }

  /**
   * Finds the specified class including in jar files in the classpath
   * 
   * @see java.lang.ClassLoader#findClass(java.lang.String)
   */
  protected Class<?> findClass(String className) throws ClassNotFoundException
  {
    FileInputStream fis = null;

    try
    {
      byte[] classBytes = null;

      // first we try to locate a class file.
      String pathName = className.replace('.', File.separatorChar);
      File file = new File(path.getAbsolutePath(), pathName + ".class");
      if (file.exists())
      {
        // we have found a class file and read it
        fis = new FileInputStream(file);
        classBytes = new byte[fis.available()];
        fis.read(classBytes);
      }
      else
      {
        // no class file exists we have to check jar files
        classBytes = findClassInJarFile(path, className);
      }

      // we convert the bytes into the specified class
      Class<?> clazz = defineClass(className, classBytes, 0, classBytes.length);
      return clazz;
    }
    catch (Exception e)
    {
      // We could not find the class, so indicate the problem with an exception
      throw new ClassNotFoundException(className, e);
    }
    finally
    {
      if (null != fis)
      {
        try
        {
          fis.close();
        }
        catch (Exception e)
        {
        }
      }
    }
  }

  /**
   * we cache the contents of the jar files, as we don't want to have to read
   * the file for every single class we are going to need
   */
  private Hashtable<String, byte[]> htJarContents = new Hashtable<String, byte[]>();

  /**
   * Find the first jar file containing the className and load it
   * 
   * @param dir directory where we are looking for jar files
   * @param className name of the class we are looking for
   * @return the class as byte[]
   * @throws IOException if an error occurs
   */
  private byte[] findClassInJarFile(File dir, String className)
      throws IOException
  {
    // is the class already cached ?
    String resourceName = convertClassNameToResourceName(className);
    byte[] classBytes = (byte[]) htJarContents.get(resourceName);
    if (classBytes != null)
    {
      // it has been cached, we return
      return classBytes;
    }

    if (!dir.canRead())
      throw new IOException(dir + " is not readable.");

    if (dir.isFile())
    {
      // driverPath specified a jar file
      loadJarFile(dir.getAbsolutePath());
      // after loading the jar file the class bytes are in the cache
      return (byte[]) htJarContents.get(resourceName);
    }

    // the class is not yet cached we have to find the right jar file

    // find all jar files in the directory
    String[] jarFiles = dir.list(new FilenameFilter()
    {
      public boolean accept(File dir, String name)
      {
        return name.endsWith(".jar");
      }
    });

    if (jarFiles == null)
      throw new IOException("Invalid path " + dir);

    // loop over jar files
    for (int i = 0; i < jarFiles.length; i++)
    {
      File file = new File(dir, jarFiles[i]);
      JarFile jarFile = new JarFile(file);

      // we see whether the jar file contains the class we are looking for
      // no need in loading jar files as long as we don't really need the
      // content.
      if (jarFile.getEntry(resourceName) != null)
      {
        // we have found the right jar file and are loading it now
        loadJarFile(jarFile.getName());

        // after loading the jar file the class bytes are in the cache
        classBytes = (byte[]) htJarContents.get(resourceName);
      }
    }
    return classBytes;
  }

  /**
   * @see java.lang.ClassLoader#findResource(java.lang.String)
   */
  @SuppressWarnings("deprecation")
protected URL findResource(String name)
  {

    // we try to locate the resource unjarred
    if (path.isDirectory())
    {
      File searchResource = new File(path, name);
      if (searchResource.exists())
      {
        try
        {
          return searchResource.toURL();
        }
        catch (MalformedURLException mfe)
        {
        }
      }
    }
    else if (path.isFile())
    {
      // try getting the resource from the file
      try
      {
        new JarFile(path);
        // convert the jar entry into URL format
        return new URL("jar:" + path.toURL() + "!/" + name);
      }
      catch (Exception e)
      {
        // we couldn't find resource in file
        return null;
      }
    }

    // now we are checking the jar files
    try
    {
      // find all jar files in the directory
      String[] jarFiles = path.list(new FilenameFilter()
      {
        public boolean accept(File dir, String name)
        {
          return name.endsWith(".jar");
        }
      });
      // loop over jar files
      for (int i = 0; i < jarFiles.length; i++)
      {
        File file = new File(path, jarFiles[i]);
        JarFile jarFile = new JarFile(file);

        // we see whether the jar file contains the resource we are looking for
        if (jarFile.getJarEntry(name) != null)
        {
          // convert the jar entry into URL format
          return new URL("jar:" + file.toURL() + "!/" + name);
        }
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * convert the class name into the rescource name. This method is just
   * replacing the '.' in the name with a '/' and adding the suffix '.class'
   * 
   * @param className
   * @return resource name
   */
  private String convertClassNameToResourceName(String className)
  {
    String resourceName = className;
    resourceName = resourceName.replace('.', '/');
    resourceName = resourceName + ".class";
    return resourceName;
  }

  /**
   * Load the contents of jar file in the cache
   * 
   * @param jarFileName name of the jar file we want to load
   * @throws IOException
   */
  private void loadJarFile(String jarFileName) throws IOException
  {
    Hashtable<String, Integer> htSizes = new Hashtable<String, Integer>();
    // extracts just sizes only.
    // For a reason I dont' understand not all files return the size in the loop
    // below (using ZipInputStream). So we cache the sizes here in case the loop
    // below does not give us the file size
    ZipFile zf = new ZipFile(jarFileName);
    Enumeration<?> e = zf.entries();
    while (e.hasMoreElements())
    {
      ZipEntry ze = (ZipEntry) e.nextElement();

      htSizes.put(ze.getName(), new Integer((int) ze.getSize()));
    }
    zf.close();

    // extract resources and put them into the hashtable.
    FileInputStream fis = new FileInputStream(jarFileName);
    BufferedInputStream bis = new BufferedInputStream(fis);
    ZipInputStream zis = new ZipInputStream(bis);
    ZipEntry ze = null;
    while ((ze = zis.getNextEntry()) != null)
    {
      if (ze.isDirectory())
      {
        continue;
      }

      int size = (int) ze.getSize();
      // -1 means unknown size.
      if (size == -1)
      {
        // that is the reason we have cached the file size above.
        size = ((Integer) htSizes.get(ze.getName())).intValue();
      }

      byte[] b = new byte[size];
      int rb = 0;
      int chunk = 0;
      while ((size - rb) > 0)
      {
        chunk = zis.read(b, rb, size - rb);
        if (chunk == -1)
        {
          break;
        }
        rb += chunk;
      }

      // add to internal resource hashtable
      htJarContents.put(ze.getName(), b);
    }

  }
}