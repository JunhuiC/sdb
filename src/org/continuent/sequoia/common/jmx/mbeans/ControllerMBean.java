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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.common.jmx.mbeans;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

import org.continuent.sequoia.common.exceptions.ControllerException;

/**
 * JMX Interface of the Sequoia Controller.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @version 1.0
 */
public interface ControllerMBean
{

  //
  // Virtual databases management
  //

  /**
   * Registers one or several virtual databases in the controller. The
   * description of each Virtual Database must contain the definition of the
   * backends and components (cache, scheduler, load balancer) to use.
   * <p>
   * This function expects the content of an XML file conforming to the Sequoia
   * DTD to be given as a single <code>String</code> object.
   * 
   * @param xml XML code to parse
   * @exception ControllerException if an error occurs while interpreting XML
   */
  void addVirtualDatabases(String xml) throws ControllerException;

  /**
   * Same as addVirtualDatabases(String xml) above, except that a force option
   * can be specified to bypass the load-order (last-man-down) check.
   * 
   * @param xml vdb.xml config data
   * @param force set to true to bypass the load-order (last-man-down) check.
   * @exception ControllerException if an error occurs (vdb xml not valid, other...)
   */
  void addVirtualDatabases(String xml, boolean force) throws ControllerException;

  /**
   * Loads a vdb part and initialize it as the first vdb part. This clears the
   * recovery log. Similarly to addVirtualDatabases() above, several vdb parts
   * can be specified at once.
   * 
   * @param vdbXmlSpec an xml string containing the vdb part specification (s)
   * @throws ControllerException if there is a problem (vdb xml not valid, not
   *           first in group, ...)
   */
  void initializeVirtualDatabases(String vdbXmlSpec) throws ControllerException;

  /**
   * Returns the names of currently available virtual databases.
   * 
   * @return List of <code>String</code> objects.
   */
  List getVirtualDatabaseNames();

  //
  // Controller operations
  //

  /**
   * Adds a driver jar file sent in its binary form in the drivers directory of
   * the controller.
   * 
   * @param bytes the data in a byte array
   * @throws Exception if fails
   */
  void addDriver(byte[] bytes) throws Exception;

  /**
   * Save current configuration of the controller to a default file location.
   * 
   * @return status message
   * @throws Exception if fails
   * 
   * @see #getXml()
   */
  String saveConfiguration() throws Exception;

  /**
   * Shuts the controller down. The controller can not been shut down if all its
   * hosted virtual database have not been shut down before
   * 
   * @throws ControllerException if all the virtual database have not been shut
   *           down or if an error occurs
   */
  void shutdown() throws ControllerException;

  //
  // Controller information
  //

  /**
   * Get the controller socket backlog size.
   * 
   * @return the backlog size
   */
  int getBacklogSize();

  //TODO rename it to getName()
  /**
   * Gets the controller name.
   * 
   * @return a <code>String</code> value containing the controller name.
   */
  String getControllerName();

  /**
   * Gets the JMX name of the controller.
   * 
   * @return a <code>String</code> value containing the jmx name of the
   *         controller
   */
  String getJmxName();

  /**
   * Return this controller port number
   * 
   * @return a <code>int</code> containing the port code number
   */
  int getPortNumber();

  //FIXME rename to getVersion()    
  /**
   * Gets the controller version.
   * 
   * @return a <code>String</code> value containing the version number
   * @throws RemoteException if an error occurs
   */
  String getVersionNumber() throws RemoteException;

  /**
   * Return the xml version of the controller.xml file without doc type
   * declaration, just data. The content is formatted using the controller xsl
   * stylesheet.
   * 
   * @return controller xml data
   */
  String getXml();

  /**
   * Is the controller shutting down ?
   * 
   * @return <tt>true</tt> if the controller is no more accepting connection
   */
  boolean isShuttingDown();

  /**
   * Set the controller socket backlog size.
   * 
   * @param size backlog size
   */
  void setBacklogSize(int size);

  // 
  // Logging system
  //

  /**
   * Refreshs the logging system configuration by re-reading the
   * <code>log4j.properties</code> file.
   * 
   * @exception ControllerException if the <code>log4j.properties</code> file
   *              cannot be found in classpath
   */
  void refreshLogConfiguration() throws ControllerException;

  //TODO rename to setLoggingConfiguration(String) to make it
  // a JMX attribute
  /**
   * Update the log4j configuration file with the given content Also call
   * <code>refreshLogConfiguration</code> method
   * 
   * @param newConfiguration the content of the new log4j configuration
   * @throws IOException if cannot access the log4j file
   * @throws ControllerException if could not refresh the logs
   */
  void updateLogConfigurationFile(String newConfiguration) throws IOException,
      ControllerException;

  //TODO rename to getLoggingConfiguration() to make it
  // a JMX attribute
  /**
   * Retrieve the content of the log4j configuration file
   * 
   * @return <code>String</code>
   * @throws IOException if IO problems
   */
  String viewLogConfigurationFile() throws IOException;

}