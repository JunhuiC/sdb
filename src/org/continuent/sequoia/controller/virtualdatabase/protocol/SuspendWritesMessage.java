/**
 * Sequoia: Database clustering technology
 * Copyright 2005-2006 Continuent, Inc.
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
 * Initial developer(s): Stephane Giron.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

/**
 * @author <a href="mailto:Stephane.Giron@continuent.com">Stephane Giron </a>
 */
public class SuspendWritesMessage
{

  String suspendingOperation = "";

  /**
   * Used to check in the total order queue if there is a suspend operation in
   * progress before another task can run (DistributedStatementExecuteUpdate for
   * example)
   * 
   * @param operation This is the operation that is suspending writes
   */
  public SuspendWritesMessage(String operation)
  {
    suspendingOperation = operation;
  }

}
