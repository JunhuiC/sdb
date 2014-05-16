/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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
 */

package org.continuent.sequoia.controller.virtualdatabase.activity;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * An ActivityService is responsible to centralize "activities" information related
 * to virtual databases
 */
public final class ActivityService
{
  private static final ActivityService instance = new ActivityService();
  
  /*
   * vdbs is a Map<String,Map> where:
   * - the key is a String representing the name of a virtual database
   * - the value is a Map<Object,Boolean> where
   *     - the key is an Object representing a vdb member (likely a Member)
   *     - the value is a Boolean used to flag if the vdb has had activity since the given
   *       member was flagged as unreachable:
   *         * TRUE means *some* activity since the member was flagged as unreachable
   *         * FALSE means *no* activity since the member was flagged as unreachable
   */
  private final Map/*<String, Map>*/ vdbs = new HashMap();

  private ActivityService()
  {
  }

  /**
   * Returns the instance of the ActivityService.
   * This instance is global to the Controller.
   * 
   * @return the instance of the ActivityService
   */
  public static final ActivityService getInstance()
  {
    return instance;
  }

  /**
   * Stops the activity service
   */
  public synchronized void stop()
  {
    vdbs.clear();
  }

  /**
   * Resets the activity information related to the given virtual database.
   *
   * @param vdb the name of a virtual database
   */
  public synchronized void reset(String vdb)
  {
    vdbs.remove(vdb);
  }

  /**
   * Notifies that there has been activity related to the given virtual database
   * 
   * @param vdb the name of the virtual database
   */
  public void notifyActivityFor(String vdb)
  {
    Map unreachableMembers = (Map) vdbs.get(vdb);
    if (unreachableMembers != null)
    {
      Iterator iter = unreachableMembers.entrySet().iterator();
      while (iter.hasNext())
      {
        Entry entry = (Entry) iter.next();
        // This is only writing an object reference, which is guaranteed to be
        // an atomic operation by the JLS (beginning of chapter 17. Threads and
        // locks). No need to synchronize here.
        entry.setValue(Boolean.TRUE);
      }
    }
  }

  /**
   * Flags the given member as unreachable for the given virtual database.
   * 
   * @param vdb the name of a virtual database
   * @param member an Object representing the member of a virtual database
   */
  public synchronized void addUnreachableMember(String vdb, Object member)
  {
    Map unreachableMembers = (Map) vdbs.get(vdb);
    if (unreachableMembers == null)
    {
      // 1st unreachable member for the vdb -> lazy creation of the unreachableMembers Map
      unreachableMembers = new HashMap();
      vdbs.put(vdb, unreachableMembers);
    }
    // set the boolean to false only if the member is not already flagged as unreachable
    if (!unreachableMembers.containsKey(member))
    {
      unreachableMembers.put(member, Boolean.FALSE);
    }
  }

  /**
   * Checks if there has been some activities for the given virtual database 
   * since the member was flagged as unreachable.
   * 
   * @param vdb  the name of a virtual database
   * @param member an Object representing the member of a virtual database
   * @return <code>true</code> if there has been some activities for the vdb 
   *   since the member was flagged as unreachable, <code>false</code> in any
   *   other cases
   */
  public boolean hasActivitySinceUnreachable(String vdb, Object member)
  {
    Map unreachableMembers = (Map) vdbs.get(vdb);
    if (unreachableMembers == null)
    {
      return false;
    }
    Boolean activity = (Boolean) unreachableMembers.get(member);
    if (activity == null)
    {
      return false;
    }
    return activity.booleanValue();
  }
}
