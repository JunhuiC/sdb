/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2005 Emic Networks.
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
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.backup;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;

/**
 * This class defines a BackupManager that is responsible for registering
 * backupers and retrieving them as needed for backup/restore operations.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class BackupManager implements XmlComponent
{
  /** Dump server socket timeout in ms in case of failure */
  private static final int SERVER_SOCKET_SO_TIMEOUT = 60000;

  static Trace             logger                   = Trace
                                                        .getLogger(BackupManager.class
                                                            .getName());

  /**
   * This is a HashMap of backuperName -> Backuper HashMap<String,Backuper>
   */
  private HashMap          backupers;

  /**
   * Creates a new <code>BackupManager</code> object
   */
  public BackupManager()
  {
    backupers = new HashMap();
  }

  /**
   * Retrieve a backuper given its name. If the backuper has not been registered
   * null is returned.
   * 
   * @param name the backuper to look for
   * @return the backuper or null if not found
   */
  public synchronized Backuper getBackuperByName(String name)
  {
    return (Backuper) backupers.get(name);
  }

  /**
   * Get the names of the <code>Backupers</code> available from this
   * <code>BackupManager</code>.
   * 
   * @return an (possibly 0-sized) array of <code>String</code> representing
   *         the name of the <code>Backupers</code>
   */
  public synchronized String[] getBackuperNames()
  {
    Set backuperNames = backupers.keySet();
    return (String[]) backuperNames.toArray(new String[backuperNames.size()]);
  }

  /**
   * Get the first backuper that supports the given dump format. If no backuper
   * supporting that format can be found, null is returned.
   * 
   * @param format the dump format that the backuper must handle
   * @return a backuper or null if not found
   */
  public synchronized Backuper getBackuperByFormat(String format)
  {
    if (format == null)
      return null;
    for (Iterator iter = backupers.values().iterator(); iter.hasNext();)
    {
      Backuper b = (Backuper) iter.next();
      if (format.equals(b.getDumpFormat()))
        return b;
    }
    return null;
  }

  /**
   * Register a new backuper under a logical name.
   * 
   * @param name backuper logical name
   * @param backuper the backuper instance
   * @throws BackupException if a backuper is null or a backuper has already
   *           been registered with the given name.
   */
  public synchronized void registerBackuper(String name, Backuper backuper)
      throws BackupException
  {
    // Sanity checks
    if (backupers.containsKey(name))
      throw new BackupException(
          "A backuper has already been registered with name " + name);
    if (backuper == null)
      throw new BackupException(
          "Trying to register a null backuper under name " + name);
    String dumpFormat = backuper.getDumpFormat();
    if (dumpFormat == null)
      throw new BackupException("Invalid null dump format for backuper " + name);

    // Check that an already loaded backuper does no already handle that format
    for (Iterator iter = backupers.values().iterator(); iter.hasNext();)
    {
      Backuper b = (Backuper) iter.next();
      if (b.getDumpFormat().equals(dumpFormat))
        throw new BackupException("Backuper " + b.getClass()
            + " already handles " + dumpFormat + " dump format");
    }

    if (logger.isInfoEnabled())
      logger.info("Registering backuper " + name + " to handle format "
          + dumpFormat);

    backupers.put(name, backuper);
  }

  /**
   * Unregister a Backuper given its logical name.
   * 
   * @param name the name of the backuper to unregister
   * @return true if the backuper was removed successfully, false if it was not
   *         registered
   */
  public synchronized boolean unregisterBackuper(String name)
  {
    Object backuper = backupers.remove(name);

    if (logger.isInfoEnabled() && (backuper != null))
      logger.info("Unregistering backuper " + name + " that handled format "
          + ((Backuper) backuper).getDumpFormat());

    return backuper != null;
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public synchronized String getXml()
  {
    StringBuffer sb = new StringBuffer("<" + DatabasesXmlTags.ELT_Backup + "> ");
    for (Iterator iter = backupers.keySet().iterator(); iter.hasNext();)
    {
      String backuperName = (String) iter.next();
      Backuper b = (Backuper) backupers.get(backuperName);
      sb.append("<" + DatabasesXmlTags.ELT_Backuper + " "
          + DatabasesXmlTags.ATT_backuperName + "=\"" + backuperName + "\" "
          + DatabasesXmlTags.ATT_className + "=\"" + b.getClass().getName()
          + "\" " + DatabasesXmlTags.ATT_options + "=\"" + b.getOptions()
          + "\"  />");
    }
    sb.append("</" + DatabasesXmlTags.ELT_Backup + ">");
    return sb.toString();
  }

  /**
   * Fetches a dumpFile from a remote dumpFileServer. The remote dump file to
   * fetch is specified by its name and path. The connection to the remote
   * dumpFileServer is initiated and authenticated using the specified
   * DumpTransferInfo. The dump file is then fetched and stored locally at the
   * same path as it was on the remote site.
   * 
   * @param info the DumpTransferInfo specifying where to get the dump from.
   * @param path the path where the dump is stored (both remote and local).
   * @param dumpName the name of the remote dump to fetch.
   * @throws IOException if a networking error occurs during the fetch process.
   * @throws BackupException if an authentication error occurs, or if parameters
   *           are invalid.
   */
  public static void fetchDumpFile(DumpTransferInfo info, String path,
      String dumpName) throws IOException, BackupException
  {
    //
    // Phase 1: talk to dump server using it's very smart protocol
    //
    logger.info("Dump fetch starting from " + info.getBackuperServerAddress());

    Socket soc = new Socket();
    try
    {
      soc.connect(info.getBackuperServerAddress());

      ObjectOutputStream oos = new ObjectOutputStream(soc.getOutputStream());

      oos.writeLong(info.getSessionKey());
      oos.writeObject(path);
      oos.writeObject(dumpName);

      // end of very smart protocol: read server response.
      InputStream is = new BufferedInputStream(soc.getInputStream());
      int response = is.read();
      if (response != 0xEC) // server replies "EC" to say it's happy to carry
        // on.
        throw new BackupException("bad response from dump server");

      //
      // Phase 2: protocolar ablutions ok, go copy the stream into a local file
      //
      logger.info("Dump fetch authentication ok. Fetching " + path
          + File.separator + dumpName);

      File thePath = new File(path);
      if (!thePath.exists())
        thePath.mkdirs();

      File theFile = new File(path + File.separator + dumpName);
      theFile.createNewFile();

      OutputStream os = new BufferedOutputStream(new FileOutputStream(theFile));
      int c = is.read();
      while (c != -1)
      {
        os.write(c);
        c = is.read();
      }
      os.flush();
      os.close();
    }
    finally
    {
      if (soc != null)
        soc.close();
    }

    logger.info("Dump fetch done.");
  }

  /**
   * Sets up a DumpFileServer for a remote client to use with fetchDumpFile.
   * 
   * @param dumpServerIpAddress IP address of the dump server
   * @return a DumpTransferInfo to be used by the client to connect and
   *         authenticate to this dumpFileServer.
   * @throws IOException if the server socket can not be created.
   */
  public static DumpTransferInfo setupDumpFileServer(String dumpServerIpAddress)
      throws IOException
  {
    ServerSocket soc;

    InetSocketAddress dumpServerAddress;
    if (dumpServerIpAddress != null)
    {
      logger.info("Using provided dump-server address: " + dumpServerIpAddress);
      if (dumpServerIpAddress.indexOf(":") == -1)
      { // No port number
        soc = new ServerSocket();
        soc.bind(null);
      }
      else
      { // Extract IP and port number
        String ipPort[] = dumpServerIpAddress.split(":");
        dumpServerIpAddress = ipPort[0];
        try
        {
          int port = Integer.valueOf(ipPort[1]).intValue();
          soc = new ServerSocket(port);
        }
        catch (NumberFormatException e)
        {
          logger.error("Invalid port number " + ipPort[1]
              + " in dump-server address (" + dumpServerIpAddress + ")");
          soc = new ServerSocket();
          soc.bind(null);
        }
      }
      // If DumpServer fails, the socket is not closed
      soc.setSoTimeout(SERVER_SOCKET_SO_TIMEOUT);

      dumpServerAddress = new InetSocketAddress(dumpServerIpAddress, soc
          .getLocalPort());
    }
    else
    {
      logger.info("Using InetAddress.getLocalHost() as dump-server address: "
          + InetAddress.getLocalHost());
      soc = new ServerSocket();
      soc.bind(null);
      dumpServerAddress = new InetSocketAddress(InetAddress.getLocalHost(), soc
          .getLocalPort());
    }

    if (dumpServerAddress.getAddress() == null)
      throw new IOException(
          "Cannot resolve provided IP address for dump server ("
              + dumpServerAddress + ")");
    else if (dumpServerAddress.getAddress().isLoopbackAddress())
      throw new IOException(
          "NOT setting-up a dump server on a loopback address.\n"
              + "Please update your network configuration "
              + "or specify option dumpServer in vdb.xml <Backuper ... option=/>");

    long sessionKey = soc.hashCode();
    new DumpTransferServerThread(soc, sessionKey).start();

    // FIXME: sending the address we are listening to is dirty because:
    // - it prevents us from listening to any interface/address
    // - we may listen on some wrong interface/address!
    // The Right Way to do this would be to bootstrap this socket from the
    // existing inter-controller communication/addresses, this way we would be
    // 100% sure to succeed.
    return new DumpTransferInfo(dumpServerAddress, sessionKey);
  }

  /**
   * Sets up a DumpFileServer for a remote client to use with fetchDumpFile.
   * 
   * @return a DumpTransferInfo to be used by the client to connect and
   *         authenticate to this dumpFileServer.
   * @throws IOException if the server socket can not be created.
   */
  public static DumpTransferInfo setupDumpFileServer() throws IOException
  {
    return setupDumpFileServer(null);
  }

  static class DumpTransferServerThread extends Thread
  {
    private ServerSocket serverSocket;
    private long         sessionKey;

    public void run()
    {
      Socket soc = null;
      try
      {
        logger.info("Dump server started @ "
            + serverSocket.getLocalSocketAddress());
        //
        // Wait for client to connect
        //
        soc = serverSocket.accept();

        logger.info("Client connected to dump server from "
            + soc.getRemoteSocketAddress());

        ObjectInputStream ois = new ObjectInputStream(soc.getInputStream());

        //
        // Phase 1: server side very smart protocol to authenticate client
        //
        long key = ois.readLong();

        if (key != this.sessionKey)
        {
          logger.error("Bad session key from client: "
              + soc.getRemoteSocketAddress());
          soc.close();
          return; // read will fail on client side
        }

        String path = (String) ois.readObject();
        String dumpName = (String) ois.readObject();

        File theFile = new File(path + File.separator + dumpName);

        if (!theFile.exists())
        {
          logger.error("Requested dump does not exist: " + theFile.getPath());
          soc.close();
          return;
        }

        InputStream is = new BufferedInputStream(new FileInputStream(theFile));
        OutputStream os = new BufferedOutputStream(soc.getOutputStream());

        // end of very smart protocol: return "EC" to client to say it's ok to
        // fetch the dump
        os.write(0xEC);

        //
        // Phase 2: burst the dump file over the wire.
        //
        int c = is.read();
        while (c != -1)
        {
          os.write(c);
          c = is.read();
        }
        os.flush();
        os.close();

        logger.info("Dump server terminated.");
      }
      catch (Exception e)
      {
        logger.error(e);
      }
      finally
      {
        if (soc != null)
          try
          {
            soc.close();
          }
          catch (IOException ignore)
          {
          }
      }
    }

    DumpTransferServerThread(ServerSocket serverSocket, long sessionKey)
    {
      setName("DumpTransfer server thread");
      this.serverSocket = serverSocket;
      this.sessionKey = sessionKey;
    }
  }

}