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
 *
 */

package org.continuent.sequoia.controller.virtualdatabase;

/**
 * Type-safe enum representing outcomes of a reconciliation between two virtual databases
 * parts after a network partition has been detected
 */
final class PartitionReconciliationStatus
{

  static final PartitionReconciliationStatus NO_ACTIVITY              = new PartitionReconciliationStatus(
                                                                        "No activity detected on both virtual database parts during network failure");
  static final PartitionReconciliationStatus ALONE_IN_THE_WORLD       = new PartitionReconciliationStatus(
                                                                        "This virtual database part was isolated during network failure");
  static final PartitionReconciliationStatus OTHER_ALONE_IN_THE_WORLD = new PartitionReconciliationStatus(
                                                                        "The other virtual database part was isolated during network failure");
  static final PartitionReconciliationStatus SPLIT_BRAIN              = new PartitionReconciliationStatus(
                                                                        "Activities on both virtual database parts during network failure: Split brain detected!!!");

  private String                           description;

  private PartitionReconciliationStatus(String description)
  {
    this.description = description;
  }

  /**
   * {@inheritDoc} 
   */
  public String toString()
  {
    return description;
  }
}
