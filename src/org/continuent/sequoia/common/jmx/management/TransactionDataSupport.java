/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent
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
 */

package org.continuent.sequoia.common.jmx.management;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.continuent.sequoia.common.log.Trace;

public class TransactionDataSupport
{

  protected static Trace             logger = Trace
                                                .getLogger(TransactionDataSupport.class
                                                    .getName());

  private final static CompositeType TRANSACTION_COMPOSITE_TYPE;
  private final static TabularType   TRANSACTION_TABULAR_TYPE;

  public static final String[]       NAMES  = new String[]{"tid", "time"};
  private static final OpenType<?>[]    TYPES  = new OpenType[]{SimpleType.LONG, SimpleType.LONG};

  static
  {
    try
    {
      TRANSACTION_COMPOSITE_TYPE = new CompositeType("transaction",
          "transaction information", NAMES, NAMES, TYPES);
      TRANSACTION_TABULAR_TYPE = new TabularType("transactions",
          "table of transaction informations", TRANSACTION_COMPOSITE_TYPE,
          new String[]{"tid"});
    }
    catch (OpenDataException e)
    {
      // should never happens but if it does, we're seriously screwed.
      throw new ExceptionInInitializerError(e);
    }
  }

  public static CompositeData newCompositeData(long tid, long time)
      throws OpenDataException
  {
    Object[] values = new Object[]{new Long(tid), new Long(time)};
    return new CompositeDataSupport(TRANSACTION_COMPOSITE_TYPE, NAMES, values);
  }

  public static TabularData newTabularData() throws Exception
  {
    TabularData data = new TabularDataSupport(TRANSACTION_TABULAR_TYPE);
    return data;
  }
}
