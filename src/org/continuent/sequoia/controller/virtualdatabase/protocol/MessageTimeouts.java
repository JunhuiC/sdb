/**
 * C-JDBC: Clustered JDBC.
 * Copyright (C) 2005 Emic Networks
 * Science And Control (INRIA).
 * Contact: c-jdbc@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation; either version 2.1 of the License, or any later
 * version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.controller.virtualdatabase.protocol;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This class defines message timeouts for all messages exchanged between
 * controllers. There is a default timeout for all messages but each message can
 * be assigned a specific timeout.
 * 
 * @author <a href="mailto:emmanuel.cecchet@emicnetworks.com">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MessageTimeouts
{
  /** Default timeout value to be used if value has not been specified */
  private long             defaultTimeout;

  /** List of messages for which we can set a specific timeout */
  private static final int DEFAULT_TIMEOUT_VALUE               = -1;
  private long             backendStatusTimeout                = DEFAULT_TIMEOUT_VALUE;
  private long             backendTransferTimeout              = DEFAULT_TIMEOUT_VALUE;
  private long             cacheInvalidateTimeout              = DEFAULT_TIMEOUT_VALUE;
  private long             commitTimeout                       = DEFAULT_TIMEOUT_VALUE;
  private long             controllerNameTimeout               = DEFAULT_TIMEOUT_VALUE;
  private long             copyLogEntryTimeout                 = DEFAULT_TIMEOUT_VALUE;
  private long             disableBackendTimeout               = DEFAULT_TIMEOUT_VALUE;
  private long             enableBackendTimeout                = DEFAULT_TIMEOUT_VALUE;
  private long             execReadRequestTimeout              = DEFAULT_TIMEOUT_VALUE;
  private long             execReadStoredProcedureTimeout      = DEFAULT_TIMEOUT_VALUE;
  private long             execWriteRequestTimeout             = DEFAULT_TIMEOUT_VALUE;
  private long             execWriteRequestWithKeysTimeout     = DEFAULT_TIMEOUT_VALUE;
  private long             execWriteStoredProcedureTimeout     = DEFAULT_TIMEOUT_VALUE;
  private long             initiateDumpCopyTimeout             = DEFAULT_TIMEOUT_VALUE;
  private long             notifyCompletionTimeout             = DEFAULT_TIMEOUT_VALUE;
  private long             releaseSavepointTimeout             = DEFAULT_TIMEOUT_VALUE;
  private long             replicateLogEntriesTimeout          = DEFAULT_TIMEOUT_VALUE;
  private long             rollbackTimeout                     = DEFAULT_TIMEOUT_VALUE;
  private long             rollbackToSavepointTimeout          = DEFAULT_TIMEOUT_VALUE;
  private long             setCheckpointTimeout                = DEFAULT_TIMEOUT_VALUE;
  private long             setSavepointTimeout                 = DEFAULT_TIMEOUT_VALUE;
  private long             unlogCommitTimeout                  = DEFAULT_TIMEOUT_VALUE;
  private long             unlogRequestTimeout                 = DEFAULT_TIMEOUT_VALUE;
  private long             unlogRollbackTimeout                = DEFAULT_TIMEOUT_VALUE;
  private long             virtualDatabaseConfigurationTimeout = DEFAULT_TIMEOUT_VALUE;

  /**
   * Creates a new <code>MessageTimeouts</code> object
   * 
   * @param defaultTimeout the default timeout to set in ms
   */
  public MessageTimeouts(long defaultTimeout)
  {
    this.defaultTimeout = defaultTimeout;
  }

  /**
   * Return the shortest timeout between the 2 given timeouts. A value of 0
   * means an infinite timeout.
   * 
   * @param timeout1 the first timeout
   * @param timeout2 the second timeout
   * @return the shortest timeout in ms
   */
  public static long getMinTimeout(long timeout1, long timeout2)
  {
    if (timeout1 == timeout2)
      return timeout1;
    if (timeout1 == 0)
      return timeout2;
    if (timeout2 == 0)
      return timeout1;
    if (timeout1 < timeout2)
      return timeout1;
    else
      return timeout2;
  }

  /**
   * Returns the backendStatusTimeout value.
   * 
   * @return Returns the backendStatusTimeout.
   */
  public long getBackendStatusTimeout()
  {
    if (backendStatusTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return backendStatusTimeout;
  }

  /**
   * Sets the backendStatusTimeout value.
   * 
   * @param backendStatusTimeout The backendStatusTimeout to set.
   */
  public void setBackendStatusTimeout(long backendStatusTimeout)
  {
    this.backendStatusTimeout = backendStatusTimeout;
  }

  /**
   * Returns the backendTransferTimeout value.
   * 
   * @return Returns the backendTransferTimeout.
   */
  public long getBackendTransferTimeout()
  {
    if (backendTransferTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return backendTransferTimeout;
  }

  /**
   * Sets the backendTransferTimeout value.
   * 
   * @param backendTransferTimeout The backendTransferTimeout to set.
   */
  public void setBackendTransferTimeout(long backendTransferTimeout)
  {
    this.backendTransferTimeout = backendTransferTimeout;
  }

  /**
   * Returns the cacheInvalidateTimeout value.
   * 
   * @return Returns the cacheInvalidateTimeout.
   */
  public long getCacheInvalidateTimeout()
  {
    if (cacheInvalidateTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return cacheInvalidateTimeout;
  }

  /**
   * Sets the cacheInvalidateTimeout value.
   * 
   * @param cacheInvalidateTimeout The cacheInvalidateTimeout to set.
   */
  public void setCacheInvalidateTimeout(long cacheInvalidateTimeout)
  {
    this.cacheInvalidateTimeout = cacheInvalidateTimeout;
  }

  /**
   * Returns the commitTimeout value.
   * 
   * @return Returns the commitTimeout.
   */
  public long getCommitTimeout()
  {
    if (commitTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return commitTimeout;
  }

  /**
   * Sets the commitTimeout value.
   * 
   * @param commitTimeout The commitTimeout to set.
   */
  public void setCommitTimeout(long commitTimeout)
  {
    this.commitTimeout = commitTimeout;
  }

  /**
   * Returns the controllerNameTimeout value.
   * 
   * @return Returns the controllerNameTimeout.
   */
  public long getControllerNameTimeout()
  {
    if (controllerNameTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return controllerNameTimeout;
  }

  /**
   * Sets the controllerNameTimeout value.
   * 
   * @param controllerNameTimeout The controllerNameTimeout to set.
   */
  public void setControllerNameTimeout(long controllerNameTimeout)
  {
    this.controllerNameTimeout = controllerNameTimeout;
  }

  /**
   * Returns the copyLogEntryTimeout value.
   * 
   * @return Returns the copyLogEntryTimeout.
   */
  public long getCopyLogEntryTimeout()
  {
    if (copyLogEntryTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return copyLogEntryTimeout;
  }

  /**
   * Sets the copyLogEntryTimeout value.
   * 
   * @param copyLogEntryTimeout The copyLogEntryTimeout to set.
   */
  public void setCopyLogEntryTimeout(long copyLogEntryTimeout)
  {
    this.copyLogEntryTimeout = copyLogEntryTimeout;
  }

  /**
   * Returns the defaultTimeout value.
   * 
   * @return Returns the defaultTimeout.
   */
  public long getDefaultTimeout()
  {
    return defaultTimeout;
  }

  /**
   * Sets the defaultTimeout value.
   * 
   * @param defaultTimeout The defaultTimeout to set.
   */
  public void setDefaultTimeout(long defaultTimeout)
  {
    this.defaultTimeout = defaultTimeout;
  }

  /**
   * Returns the disableBackendTimeout value.
   * 
   * @return Returns the disableBackendTimeout.
   */
  public long getDisableBackendTimeout()
  {
    if (disableBackendTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return disableBackendTimeout;
  }

  /**
   * Sets the disableBackendTimeout value.
   * 
   * @param disableBackendTimeout The disableBackendTimeout to set.
   */
  public void setDisableBackendTimeout(long disableBackendTimeout)
  {
    this.disableBackendTimeout = disableBackendTimeout;
  }

  /**
   * Returns the enableBackendTimeout value.
   * 
   * @return Returns the enableBackendTimeout.
   */
  public long getEnableBackendTimeout()
  {
    if (enableBackendTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return enableBackendTimeout;
  }

  /**
   * Sets the enableBackendTimeout value.
   * 
   * @param enableBackendTimeout The enableBackendTimeout to set.
   */
  public void setEnableBackendTimeout(long enableBackendTimeout)
  {
    this.enableBackendTimeout = enableBackendTimeout;
  }

  /**
   * Returns the execReadRequestTimeout value.
   * 
   * @return Returns the execReadRequestTimeout.
   */
  public long getExecReadRequestTimeout()
  {
    if (execReadRequestTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return execReadRequestTimeout;
  }

  /**
   * Sets the execReadRequestTimeout value.
   * 
   * @param execReadRequestTimeout The execReadRequestTimeout to set.
   */
  public void setExecReadRequestTimeout(long execReadRequestTimeout)
  {
    this.execReadRequestTimeout = execReadRequestTimeout;
  }

  /**
   * Returns the execReadStoredProcedureTimeout value.
   * 
   * @return Returns the execReadStoredProcedureTimeout.
   */
  public long getExecReadStoredProcedureTimeout()
  {
    if (execReadStoredProcedureTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return execReadStoredProcedureTimeout;
  }

  /**
   * Sets the execReadStoredProcedureTimeout value.
   * 
   * @param execReadStoredProcedureTimeout The execReadStoredProcedureTimeout to
   *          set.
   */
  public void setExecReadStoredProcedureTimeout(
      long execReadStoredProcedureTimeout)
  {
    this.execReadStoredProcedureTimeout = execReadStoredProcedureTimeout;
  }

  /**
   * Returns the execWriteRequestTimeout value.
   * 
   * @return Returns the execWriteRequestTimeout.
   */
  public long getExecWriteRequestTimeout()
  {
    if (execWriteRequestTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return execWriteRequestTimeout;
  }

  /**
   * Returns the execWriteRequestWithKeysTimeout value.
   * 
   * @return Returns the execWriteRequestWithKeysTimeout.
   */
  public long getExecWriteRequestWithKeysTimeout()
  {
    if (execWriteRequestWithKeysTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return execWriteRequestWithKeysTimeout;
  }

  /**
   * Sets the execWriteRequestWithKeysTimeout value.
   * 
   * @param execWriteRequestWithKeysTimeout The execWriteRequestWithKeysTimeout
   *          to set.
   */
  public void setExecWriteRequestWithKeysTimeout(
      long execWriteRequestWithKeysTimeout)
  {
    this.execWriteRequestWithKeysTimeout = execWriteRequestWithKeysTimeout;
  }

  /**
   * Sets the execWriteRequestTimeout value.
   * 
   * @param execWriteRequestTimeout The execWriteRequestTimeout to set.
   */
  public void setExecWriteRequestTimeout(long execWriteRequestTimeout)
  {
    this.execWriteRequestTimeout = execWriteRequestTimeout;
  }

  /**
   * Returns the execWriteStoredProcedureTimeout value.
   * 
   * @return Returns the execWriteStoredProcedureTimeout.
   */
  public long getExecWriteStoredProcedureTimeout()
  {
    if (execWriteStoredProcedureTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return execWriteStoredProcedureTimeout;
  }

  /**
   * Sets the execWriteStoredProcedureTimeout value.
   * 
   * @param execWriteStoredProcedureTimeout The execWriteStoredProcedureTimeout
   *          to set.
   */
  public void setExecWriteStoredProcedureTimeout(
      long execWriteStoredProcedureTimeout)
  {
    this.execWriteStoredProcedureTimeout = execWriteStoredProcedureTimeout;
  }

  /**
   * Returns the initiateDumpCopyTimeout value.
   * 
   * @return Returns the initiateDumpCopyTimeout.
   */
  public long getInitiateDumpCopyTimeout()
  {
    if (initiateDumpCopyTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return initiateDumpCopyTimeout;
  }

  /**
   * Sets the initiateDumpCopyTimeout value.
   * 
   * @param initiateDumpCopyTimeout The initiateDumpCopyTimeout to set.
   */
  public void setInitiateDumpCopyTimeout(long initiateDumpCopyTimeout)
  {
    this.initiateDumpCopyTimeout = initiateDumpCopyTimeout;
  }

  /**
   * Returns the notifyCompletionTimeout value.
   * 
   * @return Returns the notifyCompletionTimeout.
   */
  public long getNotifyCompletionTimeout()
  {
    if (notifyCompletionTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return notifyCompletionTimeout;
  }

  /**
   * Sets the notifyCompletionTimeout value.
   * 
   * @param notifyCompletionTimeout The notifyCompletionTimeout to set.
   */
  public void setNotifyCompletionTimeout(long notifyCompletionTimeout)
  {
    this.notifyCompletionTimeout = notifyCompletionTimeout;
  }

  /**
   * Returns the releaseSavepointTimeout value.
   * 
   * @return Returns the releaseSavepointTimeout.
   */
  public long getReleaseSavepointTimeout()
  {
    if (releaseSavepointTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return releaseSavepointTimeout;
  }

  /**
   * Sets the releaseSavepointTimeout value.
   * 
   * @param releaseSavepointTimeout The releaseSavepointTimeout to set.
   */
  public void setReleaseSavepointTimeout(long releaseSavepointTimeout)
  {
    this.releaseSavepointTimeout = releaseSavepointTimeout;
  }

  /**
   * Returns the replicateLogEntriesTimeout value.
   * 
   * @return Returns the replicateLogEntriesTimeout.
   */
  public long getReplicateLogEntriesTimeout()
  {
    if (replicateLogEntriesTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return replicateLogEntriesTimeout;
  }

  /**
   * Sets the replicateLogEntriesTimeout value.
   * 
   * @param replicateLogEntriesTimeout The replicateLogEntriesTimeout to set.
   */
  public void setReplicateLogEntriesTimeout(long replicateLogEntriesTimeout)
  {
    this.replicateLogEntriesTimeout = replicateLogEntriesTimeout;
  }

  /**
   * Returns the rollbackTimeout value.
   * 
   * @return Returns the rollbackTimeout.
   */
  public long getRollbackTimeout()
  {
    if (rollbackTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return rollbackTimeout;
  }

  /**
   * Sets the rollbackTimeout value.
   * 
   * @param rollbackTimeout The rollbackTimeout to set.
   */
  public void setRollbackTimeout(long rollbackTimeout)
  {
    this.rollbackTimeout = rollbackTimeout;
  }

  /**
   * Returns the rollbackToSavepointTimeout value.
   * 
   * @return Returns the rollbackToSavepointTimeout.
   */
  public long getRollbackToSavepointTimeout()
  {
    if (rollbackToSavepointTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return rollbackToSavepointTimeout;
  }

  /**
   * Sets the rollbackToSavepointTimeout value.
   * 
   * @param rollbackToSavepointTimeout The rollbackToSavepointTimeout to set.
   */
  public void setRollbackToSavepointTimeout(long rollbackToSavepointTimeout)
  {
    this.rollbackToSavepointTimeout = rollbackToSavepointTimeout;
  }

  /**
   * Returns the setCheckpointTimeout value.
   * 
   * @return Returns the setCheckpointTimeout.
   */
  public long getSetCheckpointTimeout()
  {
    if (setCheckpointTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return setCheckpointTimeout;
  }

  /**
   * Sets the setCheckpointTimeout value.
   * 
   * @param setCheckpointTimeout The setCheckpointTimeout to set.
   */
  public void setSetCheckpointTimeout(long setCheckpointTimeout)
  {
    this.setCheckpointTimeout = setCheckpointTimeout;
  }

  /**
   * Returns the setSavepointTimeout value.
   * 
   * @return Returns the setSavepointTimeout.
   */
  public long getSetSavepointTimeout()
  {
    if (setSavepointTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return setSavepointTimeout;
  }

  /**
   * Sets the setSavepointTimeout value.
   * 
   * @param setSavepointTimeout The setSavepointTimeout to set.
   */
  public void setSetSavepointTimeout(long setSavepointTimeout)
  {
    this.setSavepointTimeout = setSavepointTimeout;
  }

  /**
   * Returns the unlogCommitTimeout value.
   * 
   * @return Returns the unlogCommitTimeout.
   */
  public long getUnlogCommitTimeout()
  {
    if (unlogCommitTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return unlogCommitTimeout;
  }

  /**
   * Sets the unlogCommitTimeout value.
   * 
   * @param unlogCommitTimeout The unlogCommitTimeout to set.
   */
  public void setUnlogCommitTimeout(long unlogCommitTimeout)
  {
    this.unlogCommitTimeout = unlogCommitTimeout;
  }

  /**
   * Returns the unlogRequestTimeout value.
   * 
   * @return Returns the unlogRequestTimeout.
   */
  public long getUnlogRequestTimeout()
  {
    if (unlogRequestTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return unlogRequestTimeout;
  }

  /**
   * Sets the unlogRequestTimeout value.
   * 
   * @param unlogRequestTimeout The unlogRequestTimeout to set.
   */
  public void setUnlogRequestTimeout(long unlogRequestTimeout)
  {
    this.unlogRequestTimeout = unlogRequestTimeout;
  }

  /**
   * Returns the unlogRollbackTimeout value.
   * 
   * @return Returns the unlogRollbackTimeout.
   */
  public long getUnlogRollbackTimeout()
  {
    if (unlogRollbackTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return unlogRollbackTimeout;
  }

  /**
   * Sets the unlogRollbackTimeout value.
   * 
   * @param unlogRollbackTimeout The unlogRollbackTimeout to set.
   */
  public void setUnlogRollbackTimeout(long unlogRollbackTimeout)
  {
    this.unlogRollbackTimeout = unlogRollbackTimeout;
  }

  /**
   * Returns the virtualDatabaseConfigurationTimeout value.
   * 
   * @return Returns the virtualDatabaseConfigurationTimeout.
   */
  public long getVirtualDatabaseConfigurationTimeout()
  {
    if (virtualDatabaseConfigurationTimeout == DEFAULT_TIMEOUT_VALUE)
      return defaultTimeout;
    return virtualDatabaseConfigurationTimeout;
  }

  /**
   * Sets the virtualDatabaseConfigurationTimeout value.
   * 
   * @param virtualDatabaseConfigurationTimeout The
   *          virtualDatabaseConfigurationTimeout to set.
   */
  public void setVirtualDatabaseConfigurationTimeout(
      long virtualDatabaseConfigurationTimeout)
  {
    this.virtualDatabaseConfigurationTimeout = virtualDatabaseConfigurationTimeout;
  }

  /**
   * Fills the passed StringBuffer with the xml representation of this
   * <code>MessageTimeouts</code> object. Default timeout values are not
   * exported.
   * 
   * @param sb StringBuffer to which the xml will be appended
   */
  public void generateXml(StringBuffer sb)
  {
    sb.append("<" + DatabasesXmlTags.ELT_MessageTimeouts + " ");

    if (backendStatusTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_backendStatusTimeout + " = \""
          + backendStatusTimeout + "\"");
    if (backendTransferTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_backendTransferTimeout + " = \""
          + backendTransferTimeout + "\"");
    if (cacheInvalidateTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_cacheInvalidateTimeout + " = \""
          + cacheInvalidateTimeout + "\"");
    if (commitTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_commitTimeout + " = \""
          + commitTimeout + "\"");
    if (controllerNameTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_controllerNameTimeout + " = \""
          + controllerNameTimeout + "\"");
    if (copyLogEntryTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_copyLogEntryTimeout + " = \""
          + copyLogEntryTimeout + "\"");
    if (disableBackendTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_disableBackendTimeout + " = \""
          + disableBackendTimeout + "\"");
    if (enableBackendTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_enableBackendTimeout + " = \""
          + enableBackendTimeout + "\"");
    if (execReadRequestTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_execReadRequestTimeout + " = \""
          + execReadRequestTimeout + "\"");
    if (execReadStoredProcedureTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_execReadStoredProcedureTimeout
          + " = \"" + execReadStoredProcedureTimeout + "\"");
    if (execWriteRequestTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_execWriteRequestTimeout + " = \""
          + execWriteRequestTimeout + "\"");
    if (execWriteRequestWithKeysTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_execWriteRequestWithKeysTimeout
          + " = \"" + execWriteRequestWithKeysTimeout + "\"");
    if (execWriteStoredProcedureTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_execWriteStoredProcedureTimeout
          + " = \"" + execWriteStoredProcedureTimeout + "\"");
    if (initiateDumpCopyTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_initiateDumpCopyTimeout + " = \""
          + initiateDumpCopyTimeout + "\"");
    if (notifyCompletionTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_notifyCompletionTimeout + " = \""
          + notifyCompletionTimeout + "\"");
    if (releaseSavepointTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_releaseSavepointTimeout + " = \""
          + releaseSavepointTimeout + "\"");
    if (replicateLogEntriesTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_replicateLogEntriesTimeout + " = \""
          + replicateLogEntriesTimeout + "\"");
    if (rollbackTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_rollbackTimeout + " = \""
          + rollbackTimeout + "\"");
    if (rollbackToSavepointTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_rollbackToSavepointTimeout + " = \""
          + rollbackToSavepointTimeout + "\"");
    if (setCheckpointTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_setCheckpointTimeout + " = \""
          + setCheckpointTimeout + "\"");
    if (setSavepointTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_setSavepointTimeout + " = \""
          + setSavepointTimeout + "\"");
    if (unlogCommitTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_unlogCommitTimeout + " = \""
          + unlogCommitTimeout + "\"");
    if (unlogRequestTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_unlogRequestTimeout + " = \""
          + unlogRequestTimeout + "\"");
    if (unlogRollbackTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_unlogRollbackTimeout + " = \""
          + unlogRollbackTimeout + "\"");
    if (virtualDatabaseConfigurationTimeout != defaultTimeout)
      sb.append(" " + DatabasesXmlTags.ATT_virtualDatabaseConfigurationTimeout
          + " = \"" + virtualDatabaseConfigurationTimeout + "\"");

    sb.append("/>");
  }

}
