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

import java.util.Date;

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

public class DumpInfoSupport
{

  protected static Trace logger = Trace.getLogger(DumpInfoSupport.class.getName());

  private final static CompositeType DUMP_INFO_COMPOSITE_TYPE;
  private final static TabularType   DUMP_INFO_TABULAR_TYPE;

  public static final String[]       NAMES  = new String[] {
    "dumpName",
    "dumpDate",
    "dumpPath",
    "dumpFormat",
    "checkpointName",
    "backendName",
    "tables"
  };
  private static final OpenType[]    TYPES  = new OpenType[] {
    SimpleType.STRING,
    SimpleType.DATE,
    SimpleType.STRING,
    SimpleType.STRING,
    SimpleType.STRING,
    SimpleType.STRING,
    SimpleType.STRING
  };

  static
  {
    try
    {
      DUMP_INFO_COMPOSITE_TYPE = new CompositeType("dump_info",
          "dump information", NAMES, NAMES, TYPES);
      // a table of dumps is indexed by dumpName
      DUMP_INFO_TABULAR_TYPE = new TabularType("dump_info_list",
          "table of dump informations", DUMP_INFO_COMPOSITE_TYPE,
          new String[]{"dumpName"});
    }
    catch (OpenDataException e)
    {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static CompositeData newCompositeData(DumpInfo info)
      throws OpenDataException
  {
    Date dumpDate = new java.util.Date(info.getDumpDate().getTime());
    Object[] values = new Object[]{info.getDumpName(), dumpDate,
        info.getDumpPath(), info.getDumpFormat(), info.getCheckpointName(),
        info.getBackendName(), info.getTables()};
    return new CompositeDataSupport(DUMP_INFO_COMPOSITE_TYPE, NAMES, values);
  }

  public static TabularData newTabularData() throws Exception
  {
    TabularData data = new TabularDataSupport(DUMP_INFO_TABULAR_TYPE);
    return data;
  }
}
