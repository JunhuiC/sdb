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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Emmanuel Cecchet.
 */

package org.continuent.sequoia.controller.virtualdatabase;

import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.metadata.MetadataContainer;
import org.continuent.sequoia.controller.backend.DatabaseBackend;

/**
 * Class gathering the static metadata related to the database. We collect
 * information from the underlying driver and keep this object for further
 * usage.
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class VirtualDatabaseStaticMetaData
{
  private String            vdbName;
  private Trace             logger;
  private MetadataContainer metadataContainer = null;

  /**
   * Reference the database for this metadata. Do not fetch any data at this
   * time
   * 
   * @param database to link this metadata to
   */
  public VirtualDatabaseStaticMetaData(VirtualDatabase database)
  {
    this.vdbName = database.getVirtualDatabaseName();
    this.logger = Trace
        .getLogger("org.continuent.sequoia.controller.virtualdatabase.VirtualDatabaseWorkerThread."
            + vdbName + ".metadata");
  }

  /**
   * Save the driver metadata of the backend if this is the first one to be
   * collected. If not display a warning for each incompatible value.
   * 
   * @param backend the new backend to get metadata from
   */
  public void gatherStaticMetadata(DatabaseBackend backend)
  {
    MetadataContainer newContainer = backend.getDatabaseStaticMetadata();
    if (logger.isDebugEnabled())
      logger.debug("fetching static metadata for backend:" + backend.getName());
    if (metadataContainer == null)
      metadataContainer = newContainer;
    else
    {
      boolean isCompatible = metadataContainer.isCompatible(newContainer,
          logger);
      if (logger.isDebugEnabled())
        logger.debug("Backend static metadata is compatible with current ones:"
            + isCompatible);
    }
  }

  /**
   * Returns the ("getXXX(Y,Z,...)", value) hash table holding metadata queries.
   * 
   * @return the metadataContainer.
   */
  public MetadataContainer getMetadataContainer()
  {
    return metadataContainer;
  }

  /**
   * Sets the ("getXXX(Y,Z,...)", value) hash table holding metadata queries.
   * 
   * @param container the metadata container.
   */
  public void setMetadataContainer(MetadataContainer container)
  {
    this.metadataContainer = container;
  }
}