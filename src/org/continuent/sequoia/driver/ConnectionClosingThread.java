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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________________________.
 */

package org.continuent.sequoia.driver;

import java.util.ArrayList;

/**
 * The <code>ConnectionClosingThread</code> wakes up every 5 seconds when
 * close() has been called on a connection and it frees the connection if it has
 * not been reused.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class ConnectionClosingThread extends Thread
{
  /* wait time before closing a connection in ms */
  private static final int WAIT_TIME = 5000;

  private Driver           driver;
  private ArrayList<Connection>        pendingConnectionClosing;

  /**
   * Builds a new ConnectionClosingThread
   * 
   * @param driver The driver that created us
   */
  public ConnectionClosingThread(Driver driver)
  {
    super("ConnectionClosingThread");
    this.driver = driver;
    this.pendingConnectionClosing = driver.pendingConnectionClosing;
    driver.connectionClosingThreadisAlive = true;
  }

  /**
   * The connection closing thread wakes up every WAIT_TIME seconds when close()
   * has been called on a connection and it frees the connection if it has not
   * been reused.
   */
  public void run()
  {
    try
    {
      Connection firstConnectionToClose = null;
      Connection lastConnectionToClose = null;
      int pendingConnectionSize;
      ArrayList<Connection> closingList = new ArrayList<Connection>();
      boolean killed = false;

      while (!killed)
      {
        synchronized (pendingConnectionClosing)
        {
          pendingConnectionSize = pendingConnectionClosing.size();
          if (pendingConnectionSize == 0)
            break;

          try
          {
            // Look at the connections in the queue before sleeping
            firstConnectionToClose = (Connection) pendingConnectionClosing
                .get(0);
            lastConnectionToClose = (Connection) pendingConnectionClosing
                .get(pendingConnectionSize - 1);

            // Sleep
            pendingConnectionClosing.wait(WAIT_TIME);
          }
          catch (InterruptedException ignore)
          {
          }

          pendingConnectionSize = pendingConnectionClosing.size();
          // Exit, no more connections
          if (pendingConnectionSize == 0)
            break;

          // Compare the queue now with its state when we got to sleep
          if (firstConnectionToClose == pendingConnectionClosing.get(0))
          { // Ok, the connection has not been reused, let's close it
            if (lastConnectionToClose == (Connection) pendingConnectionClosing
                .get(pendingConnectionSize - 1))
            { // No connection has been reused, remove them all
              closingList.addAll(pendingConnectionClosing);
              pendingConnectionClosing.clear();
              killed = true; // Let's die, there are no more connections
            }
            else
              // Close only the first connection
              closingList.add(pendingConnectionClosing.remove(0));
          }
        }

        // Effectively close the connections outside the synchronized block
        while (!closingList.isEmpty())
          closeConnection((Connection) closingList.remove(0));
      }
    }
    catch (RuntimeException e)
    {
      e.printStackTrace();
    }
    finally
    {
      synchronized (pendingConnectionClosing)
      {
        driver.connectionClosingThreadisAlive = false;
      }
    }
  }

  /**
   * Closes a connection. This cleanup should belong to the underlying class.
   * 
   * @param c the connection to close
   */
  private void closeConnection(Connection c)
  {
    try
    {
      // Free remote resources
      if (c.socketOutput != null)
      {
        c.reallyClose();
        // The following probably better belongs to Connection.reallyClose(),
        // so there would be no external class messing up with c.socketXXXput,,
        // and Connection can close itself.
        c.socketOutput.flush();
        if (c.socketInput != null)
        { // Wait for the controller to receive the connection and close the
          // stream. If we do not wait for the controller ack, the connection is
          // closed on the controller before the closing is handled which
          // results in an ugly warning message on the controller side. We are
          // not in a hurry when closing the connection so let do the things
          // nicely!
          c.socketInput.readBoolean();
          c.socketInput.close();
        }
        c.socketOutput.close();
      }

      if (c.socket != null)
        c.socket.close();
    }
    catch (Exception ignore)
    {
    }
  }

}