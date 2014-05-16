package org.continuent.sequoia.console.text.commands.dbadmin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

public class JMXUtils
{

  /**
   * Convert a Collection&lt;CompositeData&gt; to a matrix of String.
   * 
   * @param collection a Collection&lt;CompositeData&gt;
   * @param headers the headers corresponding to the items to retrieve for each
   *          CompositeDate
   * @return a matrix of String
   */
  static String[][] from(Collection/*<CompositeData>*/ collection, String[] headers)
  {
    String[][] entries = new String[collection.size()][];
    Iterator iter = collection.iterator();
    int i = 0;
    while (iter.hasNext())
    {
      CompositeData logEntry = (CompositeData) iter.next();
      Object[] logEntryItems = logEntry.getAll(headers);
      String[] entry = new String[logEntryItems.length];
      for (int j = 0; j < entry.length; j++)
      {
        Object logEntryItem = logEntryItems[j];
        entry[j] = "" + logEntryItem; //$NON-NLS-1$          
      }
      entries[i] = entry;
      i++;
    }
    return entries;
  }

  /**
   * Sort tabular data based on one given key. 
   * The entries will be sorted only if the value corresponding to the given 
   * key is Comparable.
   * 
   * @param tabularData a TabularData
   * @param key the key to use to sort the tabular data.
   * 
   * @return a List&lt;CompositeData&gt; where the composite data are sorted by
   *         the specified <code>key</code>
   */
  static List/*<CompositeData>*/ sortEntriesByKey(TabularData tabularData, final String key)
  {
    List entries = new ArrayList(tabularData.values());
    Collections.sort(entries, new Comparator()
    {
  
      public int compare(Object o1, Object o2)
      {
        CompositeData entry1 = (CompositeData) o1;
        CompositeData entry2 = (CompositeData) o2;
  
        Object value1 = entry1.get(key);
        Object value2 = entry2.get(key);
  
        if (value1 != null
            && value1 instanceof Comparable 
            && value2 instanceof Comparable) {
          Comparable comp1 = (Comparable) value1;
          Comparable comp2 = (Comparable) value2;
          return comp1.compareTo(comp2);
        }
        return 0;
      }
    });
    return entries;
  }
}
