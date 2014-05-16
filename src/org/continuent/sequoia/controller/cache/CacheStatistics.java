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

package org.continuent.sequoia.controller.cache;

import org.continuent.sequoia.common.util.Stats;

/**
 * This class handles the statistics for request caches.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class CacheStatistics
{
  // Cache statistics
  private Stats select;
  private Stats hits;
  private Stats insert;
  private Stats update;
  private Stats uncacheable;
  private Stats delete;
  private Stats unknown;
  private Stats remove;
  private Stats create;
  private Stats drop;

  /**
   * Creates a new CacheStatistics object.
   */
  public CacheStatistics()
  {
    select = new Stats("select");
    hits = new Stats("hits");
    insert = new Stats("insert");
    update = new Stats("update");
    uncacheable = new Stats("uncacheable");
    delete = new Stats("delete");
    unknown = new Stats("unknown");
    remove = new Stats("remove");
    create = new Stats("create");
    drop = new Stats("drop");
  }

  /**
   * Resets all stats to zero.
   */
  public void reset()
  {
    select.reset();
    hits.reset();
    insert.reset();
    update.reset();
    uncacheable.reset();
    delete.reset();
    unknown.reset();
    remove.reset();
    create.reset();
    drop.reset();
  }

  /**
   * Returns the create.
   * 
   * @return an <code>int</code> value
   */
  public int getCreate()
  {
    return create.getCount();
  }

  /**
   * Returns the delete.
   * 
   * @return an <code>int</code> value
   */
  public int getDelete()
  {
    return delete.getCount();
  }

  /**
   * Returns the drop.
   * 
   * @return an <code>int</code> value
   */
  public int getDrop()
  {
    return drop.getCount();
  }

  /**
   * Returns the hits.
   * 
   * @return an <code>int</code> value
   */
  public int getHits()
  {
    return hits.getCount();
  }

  /**
   * Returns the insert.
   * 
   * @return an <code>int</code> value
   */
  public int getInsert()
  {
    return insert.getCount();
  }

  /**
   * Returns the remove.
   * 
   * @return an <code>int</code> value
   */
  public int getRemove()
  {
    return remove.getCount();
  }

  /**
   * Returns the select.
   * 
   * @return an <code>int</code> value
   */
  public int getSelect()
  {
    return select.getCount();
  }

  /**
   * Returns the unknown.
   * 
   * @return an <code>int</code> value
   */
  public int getUnknown()
  {
    return unknown.getCount();
  }

  /**
   * Returns the update.
   * 
   * @return an <code>int</code> value
   */
  public int getUpdate()
  {
    return update.getCount();
  }

  /**
   * Returns the uncacheable.
   * 
   * @return an <code>int</code> value
   */
  public int getUncacheable()
  {
    return uncacheable.getCount();
  }

  /**
   * Increments the create count.
   */
  public void addCreate()
  {
    create.incrementCount();
  }

  /**
   * Increments the delete count.
   */
  public void addDelete()
  {
    delete.incrementCount();
  }

  /**
   * Increments the drop count.
   */
  public void addDrop()
  {
    drop.incrementCount();
  }

  /**
   * Increments the hits count.
   */
  public void addHits()
  {
    hits.incrementCount();
  }

  /**
   * Increments the insert count.
   */
  public void addInsert()
  {
    insert.incrementCount();
  }

  /**
   * Increments the remove count.
   */
  public void addRemove()
  {
    remove.incrementCount();
  }

  /**
   * Increments the select count.
   */
  public void addSelect()
  {
    select.incrementCount();
  }

  /**
   * Increments the unkwnown count.
   */
  public void addUnknown()
  {
    unknown.incrementCount();
  }

  /**
   * Increments the update count.
   */
  public void addUpdate()
  {
    update.incrementCount();
  }

  /**
   * Increments the uncacheable count.
   */
  public void addUncacheable()
  {
    uncacheable.incrementCount();
  }

  /**
   * Retrieve cache statistics as a table
   * 
   * @return an array of String containing the different cache values, like
   *         number of select, number of hits ...
   */
  public String[] getCacheStatsData()
  {
    String[] stats = new String[11];
    stats[0] = "" + getSelect();
    stats[1] = "" + getHits();
    stats[2] = "" + getInsert();
    stats[3] = "" + getUpdate();
    stats[4] = "" + getUncacheable();
    stats[5] = "" + getDelete();
    stats[6] = "" + getUnknown();
    stats[7] = "" + getRemove();
    stats[8] = "" + getCreate();
    stats[9] = "" + getDrop();
    stats[10] = "" + getCacheHitRatio();
    return stats;
  }

  /**
   * Get percentage of hits
   * 
   * @return hits / select
   */
  public long getCacheHitRatio()
  {
    if (select.getCount() == 0)
      return 0;
    else
      return (long) ((float) hits.getCount() / (float) select.getCount() * 100.0);
  }
}
