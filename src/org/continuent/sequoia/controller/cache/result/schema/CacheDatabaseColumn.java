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
 * Contributor(s): Julie Marguerite, Sara Bouchenak.
 */

package org.continuent.sequoia.controller.cache.result.schema;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.continuent.sequoia.controller.cache.result.entries.AbstractResultCacheEntry;
import org.continuent.sequoia.controller.requests.RequestType;
import org.continuent.sequoia.controller.requests.SelectRequest;

/**
 * A <code>CacheDatabaseColumn</code> represents a column of a database table.
 * It is composed of a <code>DatabaseColumn</code> object and an
 * <code>ArrayList</code> of cache entries.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Julie.Marguerite@inria.fr">Julie Marguerite </a>
 * @author <a href="mailto:Sara.Bouchenak@epfl.ch">Sara Bouchenak </a>
 * @version 1.0
 */
public class CacheDatabaseColumn
{
  private String    name;
  private ArrayList cacheEntries;

  /**
   * Creates a new <code>CacheDatabaseColumn</code> instance.
   * 
   * @param name name of the column
   */
  public CacheDatabaseColumn(String name)
  {
    this.name = name;
    cacheEntries = new ArrayList();
  }

  /**
   * Gets the column name.
   * 
   * @return the column name
   */
  public String getName()
  {
    return name;
  }

  /**
   * Two <code>CacheDatabaseColumn</code> are equals if they have the same
   * <code>DatabaseColumn</code>.
   * 
   * @param other the object to compare with
   * @return <code>true</code> if the objects are the same
   */
  public boolean equals(Object other)
  {
    if (!(other instanceof CacheDatabaseColumn))
      return false;

    return name.equals(((CacheDatabaseColumn) other).getName());
  }

  /**
   * Adds an <code>AbstractResultCacheEntry</code> object whose consistency
   * depends on this column.
   * 
   * @param ce a <code>AbstractResultCacheEntry</code> value
   */
  public synchronized void addCacheEntry(AbstractResultCacheEntry ce)
  {
    cacheEntries.add(ce);
  }

  /**
   * Marks dirty all valid cache entries depending on this colum that are non
   * unique.
   */
  public synchronized void markDirtyAllNonUnique()
  {
    // Do not try to optimize by moving cacheEntries.size()
    // out of the for statement
    for (int i = 0; i < cacheEntries.size(); i++)
    {
      AbstractResultCacheEntry ce = (AbstractResultCacheEntry) cacheEntries
          .get(i);
      if ((ce.getRequest().getCacheAbility() != RequestType.UNIQUE_CACHEABLE)
          && ce.isValid())
        ce.markDirty();
    }
  }

  /**
   * Invalidates all cache entries depending on this column.
   */
  public synchronized void invalidateAll()
  {
    for (Iterator i = cacheEntries.iterator(); i.hasNext();)
    {
      AbstractResultCacheEntry entry = (AbstractResultCacheEntry) i.next();
      entry.invalidate();
    }
    cacheEntries.clear();
  }

  /**
   * Invalidates all cache entries depending on this column that are non
   * <code>UNIQUE</code>.
   */
  public synchronized void invalidateAllNonUnique()
  {
    // Do not try to optimize by moving cacheEntries.size()
    // out of the for statement
    for (int i = 0; i < cacheEntries.size();)
    {
      AbstractResultCacheEntry ce = (AbstractResultCacheEntry) cacheEntries
          .get(i);
      if (ce.getRequest().getCacheAbility() != RequestType.UNIQUE_CACHEABLE)
      {
        ce.invalidate();
        cacheEntries.remove(i);
      }
      else
      {
        i++;
      }
    }
  }

  /**
   * Invalidates all cache entries depending on this column that are either non-
   * unique or unique and associated with given values.
   * 
   * @param val a <code>String</code> representing the value of the current
   *          column.
   * @param columns an <code>ArrayList</code> of CacheDatabaseColumn objects
   * @param values an <code>ArrayList</code> of String objects representing
   *          values.
   */
  public synchronized void invalidateAllUniqueWithValuesAndAllNonUnique(
      String val, ArrayList columns, ArrayList values)
  {
    // Do not try to optimize by moving cacheEntries.size()
    // out of the for statement
    for (int i = 0; i < cacheEntries.size();)
    {
      AbstractResultCacheEntry ce = (AbstractResultCacheEntry) cacheEntries
          .get(i);
      if (ce.getRequest().getCacheAbility() == RequestType.UNIQUE_CACHEABLE)
      {
        Hashtable queryValues;
        String value, v;
        SelectRequest query;
        int size, j;

        query = ce.getRequest();
        queryValues = query.getWhereValues();
        // queryValues != null in a UNIQUE_CACHEABLE request
        value = (String) queryValues.get(this.name);
        if (value.compareToIgnoreCase(val) == 0)
        {
          // The value associated with this column in the WHERE clause
          // of the UNIQUE SELECT query equals val:
          // Check if the values associated with the other columns are equal.
          size = values.size();
          j = 0;
          for (Iterator it = columns.iterator(); it.hasNext() && (j < size); j++)
          {
            CacheDatabaseColumn cdc = (CacheDatabaseColumn) it.next();
            if (!this.equals(cdc))
            {
              v = (String) values.get(j);
              value = (String) queryValues.get(cdc.getName());
              if (value.compareToIgnoreCase(v) != 0)
              {
                // UNIQUE_CACHEABLE request with a different value
                // Do not invalidate it
                return;
              }
            }
          }
          // UNIQUE_CACHEABLE request with same values
          // Invalidate it
          ce.invalidate();
          cacheEntries.remove(i);
        }
        else
        {
          // UNIQUE_CACHEABLE request with a different value
          // Do not invalidate it
          i++;
        }
      }
      else
      {
        // NON UNIQUE_CACHEABLE request
        // Invalidate it
        ce.invalidate();
        cacheEntries.remove(i);
      }
    }
  }

  /**
   * Invalidates all cache entries depending on this column that are non
   * <code>UNIQUE</code> and mark dirty <code>UNIQUE</code> queries.
   */
  public synchronized void invalidateAllNonUniqueAndMarkDirtyUnique()
  {
    // Do not try to optimize by moving cacheEntries.size()
    // out of the for statement
    for (int i = 0; i < cacheEntries.size(); i++)
    {
      AbstractResultCacheEntry ce = (AbstractResultCacheEntry) cacheEntries
          .get(i);
      if ((ce.getRequest().getCacheAbility() != RequestType.UNIQUE_CACHEABLE)
          && ce.isValid())
        ce.markDirty();
      else
      {
        ce.invalidate();
        cacheEntries.remove(i);
      }
    }
  }

  /**
   * Returns the column name.
   * 
   * @return a <code>String</code> value
   */
  public String getInformation()
  {
    return name;
  }
}
