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

package org.continuent.sequoia.controller.requestmanager;

import java.sql.SQLException;

import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This thread is used to process request parsing in background.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ParserThread extends Thread
{
  private boolean         isCaseSensitive;
  private AbstractRequest request;
  private DatabaseSchema  dbs;
  private int             granularity;

  /**
   * Creates a new ParserThread
   * 
   * @param request the request to parse
   * @param dbs the database schema
   * @param granularity the parsing granularity to use
   * @param isCaseSensitive true if parsing is case sensitive
   */
  public ParserThread(AbstractRequest request, DatabaseSchema dbs,
      int granularity, boolean isCaseSensitive)
  {
    this.request = request;
    this.dbs = dbs;
    this.granularity = granularity;
    this.isCaseSensitive = isCaseSensitive;
    start();
  }

  /**
   * @see java.lang.Runnable#run()
   */
  public void run()
  {
    try
    {
      if (!request.isParsed())
        request.parse(dbs, granularity, isCaseSensitive);
    }
    catch (SQLException e)
    {
      System.err.println("Error while parsing request (" + e + ")");
    }
  }

}
