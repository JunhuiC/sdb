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
 * Contributor(s): Jean-Bernard van Zuylen, Damian Arregui.
 */

package org.continuent.sequoia.controller.requestmanager.distributed;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.continuent.hedera.adapters.MulticastRequestAdapter;
import org.continuent.hedera.adapters.MulticastResponse;
import org.continuent.hedera.common.Member;
import org.continuent.sequoia.common.exceptions.NoMoreBackendException;
import org.continuent.sequoia.common.exceptions.NoResultAvailableException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.controller.backend.result.ControllerResultSet;
import org.continuent.sequoia.controller.backend.result.ExecuteResult;
import org.continuent.sequoia.controller.backend.result.ExecuteUpdateResult;
import org.continuent.sequoia.controller.backend.result.GeneratedKeysResult;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.AllBackendsFailedException;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.requests.AbstractRequest;
import org.continuent.sequoia.controller.requests.AbstractWriteRequest;
import org.continuent.sequoia.controller.requests.SelectRequest;
import org.continuent.sequoia.controller.requests.StoredProcedure;
import org.continuent.sequoia.controller.requests.StoredProcedureCallResult;
import org.continuent.sequoia.controller.requests.UnknownWriteRequest;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedAbort;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCallableStatementExecute;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCallableStatementExecuteQuery;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCallableStatementExecuteUpdate;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedCommit;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedReleaseSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRequest;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollback;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedRollbackToSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedSetSavepoint;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedStatementExecute;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedStatementExecuteQuery;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedStatementExecuteUpdate;
import org.continuent.sequoia.controller.virtualdatabase.protocol.DistributedStatementExecuteUpdateWithKeys;
import org.continuent.sequoia.controller.virtualdatabase.protocol.ExecRemoteStatementExecuteQuery;
import org.continuent.sequoia.controller.virtualdatabase.protocol.MessageTimeouts;

/**
 * This class defines a RAIDb2DistributedRequestManager
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:jbvanzuylen@transwide.com">Jean-Bernard van Zuylen
 *         </a>
 * @author <a href="mailto:Damian.Arregui@emicnetworks.com">Damian Arregui</a>
 * @version 1.0
 */
public class RAIDb2DistributedRequestManager extends DistributedRequestManager
{

  /**
   * Creates a new <code>RAIDb2DistributedRequestManager</code> instance
   * 
   * @param vdb the virtual database this request manager belongs to
   * @param scheduler the Request Scheduler to use
   * @param cache a Query Cache implementation
   * @param loadBalancer the Request Load Balancer to use
   * @param recoveryLog the Log Recovery to use
   * @param beginTimeout timeout in seconds for begin
   * @param commitTimeout timeout in seconds for commit
   * @param rollbackTimeout timeout in seconds for rollback
   * @throws SQLException if an error occurs
   * @throws NotCompliantMBeanException if the MBean is not JMX compliant
   */
  public RAIDb2DistributedRequestManager(DistributedVirtualDatabase vdb,
      AbstractScheduler scheduler, AbstractResultCache cache,
      AbstractLoadBalancer loadBalancer, RecoveryLog recoveryLog,
      long beginTimeout, long commitTimeout, long rollbackTimeout)
      throws SQLException, NotCompliantMBeanException
  {
    super(vdb, scheduler, cache, loadBalancer, recoveryLog, beginTimeout,
        commitTimeout, rollbackTimeout);
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedStatementExecuteQuery(org.continuent.sequoia.controller.requests.SelectRequest)
   */
  public ControllerResultSet distributedStatementExecuteQuery(
      SelectRequest request) throws SQLException
  {
    return executeRequestReturningResultSet(request,
        new DistributedStatementExecuteQuery(request), dvdb
            .getMessageTimeouts().getExecReadRequestTimeout());
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#execRemoteStatementExecuteQuery(org.continuent.sequoia.controller.requests.SelectRequest)
   */
  public ControllerResultSet execRemoteStatementExecuteQuery(
      SelectRequest request) throws SQLException
  {
    if (dvdb.isProcessMacroBeforeBroadcast())
      loadBalancer.handleMacros(request);
    try
    {
      // Iterate over controllers in members list (but us) until someone
      // successfully executes our request.
      // TODO: We could improve by jumping the controller we know in advance
      // that they don't have the necessary tables.
      Iterator<?> i = dvdb.getAllMemberButUs().iterator();
      SQLException error = null;
      while (i.hasNext())
      {
        Member controller = (Member) i.next();
        List<Member> groupMembers = new ArrayList<Member>();
        groupMembers.add(controller);

        if (logger.isDebugEnabled())
          logger.debug("Sending request "
              + request.getSqlShortForm(dvdb.getSqlShortFormLength())
              + (request.isAutoCommit() ? "" : " transaction "
                  + request.getTransactionId()) + " to " + controller);

        // Send query to remote controller
        MulticastResponse responses;
        responses = dvdb.getMulticastRequestAdapter().multicastMessage(
            groupMembers,
            new ExecRemoteStatementExecuteQuery(request),
            MulticastRequestAdapter.WAIT_ALL,
            MessageTimeouts.getMinTimeout(dvdb.getMessageTimeouts()
                .getExecReadRequestTimeout(), request.getTimeout() * 1000L));

        if (logger.isDebugEnabled())
          logger.debug("Request "
              + request.getSqlShortForm(dvdb.getSqlShortFormLength())
              + " completed.");

        Object ret = responses.getResult(controller);
        if (ret instanceof ControllerResultSet)
          return (ControllerResultSet) ret;
        else if (ret instanceof SQLException)
          error = (SQLException) ret;
      }

      if (error == null)
      {
        // No one answered, throw
        throw new NoMoreBackendException();
      }
      else
        throw error;
    }
    catch (Exception e)
    {
      String msg = "An error occured while executing remote select request "
          + request.getId();
      logger.warn(msg, e);
      if (e instanceof SQLException)
        throw (SQLException) e;
      else
        throw new SQLException(msg + " (" + e + ")");
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedStatementExecuteUpdate(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public ExecuteUpdateResult distributedStatementExecuteUpdate(
      AbstractWriteRequest request) throws SQLException
  {
    return executeRequestReturningInt(request,
        new DistributedStatementExecuteUpdate(request), dvdb
            .getMessageTimeouts().getExecWriteRequestTimeout());
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedStatementExecuteUpdateWithKeys(org.continuent.sequoia.controller.requests.AbstractWriteRequest)
   */
  public GeneratedKeysResult distributedStatementExecuteUpdateWithKeys(
      AbstractWriteRequest request) throws SQLException
  {
    return executeRequestReturningGeneratedKeys(request,
        new DistributedStatementExecuteUpdateWithKeys(request), dvdb
            .getMessageTimeouts().getExecWriteRequestWithKeysTimeout());
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedStatementExecute(org.continuent.sequoia.controller.requests.AbstractRequest)
   */
  public ExecuteResult distributedStatementExecute(AbstractRequest request)
      throws SQLException
  {
    return executeRequestReturningExecuteResult(request,
        new DistributedStatementExecute(request), dvdb.getMessageTimeouts()
            .getExecReadStoredProcedureTimeout());
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedCallableStatementExecuteQuery(StoredProcedure)
   */
  public ControllerResultSet distributedCallableStatementExecuteQuery(
      StoredProcedure proc) throws SQLException
  {
    StoredProcedureCallResult result = executeRequestReturningStoredProcedureCallResult(
        proc, new DistributedCallableStatementExecuteQuery(proc), dvdb
            .getMessageTimeouts().getExecReadStoredProcedureTimeout());
    return (ControllerResultSet) result.getResult();
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedCallableStatementExecuteUpdate(org.continuent.sequoia.controller.requests.StoredProcedure)
   */
  public ExecuteUpdateResult distributedCallableStatementExecuteUpdate(
      StoredProcedure proc) throws SQLException
  {
    StoredProcedureCallResult result = executeRequestReturningStoredProcedureCallResult(
        proc, new DistributedCallableStatementExecuteUpdate(proc), dvdb
            .getMessageTimeouts().getExecWriteStoredProcedureTimeout());
    return ((ExecuteUpdateResult) result.getResult());
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedCallableStatementExecute(StoredProcedure)
   */
  public ExecuteResult distributedCallableStatementExecute(StoredProcedure proc)
      throws SQLException
  {
    StoredProcedureCallResult result = executeRequestReturningStoredProcedureCallResult(
        proc, new DistributedCallableStatementExecute(proc), dvdb
            .getMessageTimeouts().getExecReadStoredProcedureTimeout());
    return (ExecuteResult) result.getResult();
  }

  /**
   * Execute a distributed request that returns a GeneratedKeysResult.
   * 
   * @param request the request to execute
   * @param requestMsg message to be sent through the group communication
   * @param messageTimeout group message timeout
   * @return the corresponding ExecuteResult
   * @throws SQLException if an error occurs
   */
  private ExecuteResult executeRequestReturningExecuteResult(
      AbstractRequest request, DistributedRequest requestMsg,
      long messageTimeout) throws SQLException
  {
    if (dvdb.isProcessMacroBeforeBroadcast())
      loadBalancer.handleMacros(request);
    try
    {
      ExecuteResult requestResult = null;

      List<Member> groupMembers = dvdb.getAllMembers();

      MulticastResponse responses = multicastRequest(request, requestMsg,
          messageTimeout, groupMembers);

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;
      int size = groupMembers.size();
      List<Member> successfulControllers = null;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof ExecuteResult)
        {
          if (requestResult == null)
            requestResult = (ExecuteResult) r;
        }
        else if (DistributedRequestManager.SUCCESSFUL_COMPLETION.equals(r))
        {
          // Remote controller success
          if (requestResult == null)
          {
            if (successfulControllers == null)
              successfulControllers = new ArrayList<Member>();
            successfulControllers.add(member);
          }
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to execute query (" + r + ")");
        }
        else if (r instanceof Exception)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "Request " + request.getId() + " failed on controller "
              + member + " (" + r + ")";
          logger.warn(msg);
          if (r instanceof SQLException)
            exception = (SQLException) r;
          else
          {
            exception = new SQLException("Internal exception " + r);
            exception.initCause((Throwable) r);
          }
        }
        else
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isWarnEnabled())
            logger.warn("Unexpected answer from controller " + member + " ("
                + r + ") for request "
                + request.getSqlShortForm(vdb.getSqlShortFormLength()));
        }
      }

      if ((requestResult == null) && (successfulControllers != null))
      { // Local controller could not get the result, ask it to a successful
        // remote controller
        try
        {
          requestResult = (ExecuteResult) getRequestResultFromFailoverCache(
              successfulControllers, request.getId());
        }
        catch (NoResultAvailableException e)
        {
          exception = new SQLException(
              "Request '"
                  + request
                  + "' was successfully executed on remote controllers, but all successful controllers failed before retrieving result");
        }
      }

      if ((controllersWithoutBackends != null) || (failedOnAllBackends != null))
      { // Notify all controllers of completion
        // Re-create fake stored procedure for proper scheduler notification
        // (see DistributedStatementExecute#scheduleRequest)
        StoredProcedure proc = new StoredProcedure(request.getSqlOrTemplate(),
            request.getEscapeProcessing(), request.getTimeout(), request
                .getLineSeparator());
        proc.setIsAutoCommit(request.isAutoCommit());
        proc.setTransactionId(request.getTransactionId());
        proc.setTransactionIsolation(request.getTransactionIsolation());
        proc.setId(request.getId());

        /*
         * Notify all controllers where all backend failed (if any) that
         * completion was 'success'. We determine success from the successful
         * controllers even though they may have failed afterwards (in which
         * case requestResult may be null).
         */
        boolean success = (requestResult != null || successfulControllers != null);
        notifyRequestCompletion(proc, success, true,
            failedOnAllBackends);

        // Notify controllers without active backends (if any)
        notifyRequestCompletion(proc, success, false,
            controllersWithoutBackends);
      }

      if (requestResult != null)
      {
        if (logger.isDebugEnabled())
          logger.debug("Request " + request.getId()
              + " completed successfully.");
        return requestResult;
      }

      // At this point, all controllers failed

      if (exception != null)
        throw exception;
      else
      {
        String msg = "Request '"
            + request.getSqlShortForm(vdb.getSqlShortFormLength())
            + "' failed on all controllers";
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    catch (SQLException e)
    {
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Execute a distributed request that returns a GeneratedKeysResult.
   * 
   * @param request the request to execute
   * @param requestMsg message to be sent through the group communication
   * @param messageTimeout group message timeout
   * @return the corresponding GeneratedKeysResult
   * @throws SQLException if an error occurs
   */
  private GeneratedKeysResult executeRequestReturningGeneratedKeys(
      AbstractRequest request, DistributedRequest requestMsg,
      long messageTimeout) throws SQLException
  {
    if (dvdb.isProcessMacroBeforeBroadcast())
      loadBalancer.handleMacros(request);
    try
    {
      GeneratedKeysResult requestResult = null;

      List<Member> groupMembers = dvdb.getAllMembers();

      MulticastResponse responses = multicastRequest(request, requestMsg,
          messageTimeout, groupMembers);

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that gave an inconsistent result
      ArrayList<Member> inconsistentControllers = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;
      List<Member> successfulControllers = null;
      int size = groupMembers.size();
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof GeneratedKeysResult)
        {
          if (requestResult == null)
          {
            requestResult = (GeneratedKeysResult) r;
          }
          else
          {
            if (requestResult.getUpdateCount() != ((GeneratedKeysResult) r)
                .getUpdateCount())
            {
              if (inconsistentControllers == null)
                inconsistentControllers = new ArrayList<Member>();
              inconsistentControllers.add(member);
              logger
                  .error("Controller " + member
                      + " returned an inconsistent results (" + requestResult
                      + " when expecting " + r + ") for request "
                      + request.getId());
            }
          }
        }
        else if (DistributedRequestManager.SUCCESSFUL_COMPLETION.equals(r))
        {
          // Remote controller success
          if (requestResult == null)
          {
            if (successfulControllers == null)
              successfulControllers = new ArrayList<Member>();
            successfulControllers.add(member);
          }
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to execute query (" + r + ")");
        }
        else if (r instanceof Exception)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "Request " + request.getId() + " failed on controller "
              + member + " (" + r + ")";
          logger.warn(msg);
          if (r instanceof SQLException)
            exception = (SQLException) r;
          else
          {
            exception = new SQLException("Internal exception " + r);
            exception.initCause((Throwable) r);
          }
        }
        else
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isWarnEnabled())
            logger.warn("Unexpected answer from controller " + member + " ("
                + r + ") for request "
                + request.getSqlShortForm(vdb.getSqlShortFormLength()));
        }
      }

      if ((requestResult == null) && (successfulControllers != null))
      { // Local controller could not get the result, ask it to a successful
        // remote controller
        try
        {
          requestResult = (GeneratedKeysResult) getRequestResultFromFailoverCache(
              successfulControllers, request.getId());
        }
        catch (NoResultAvailableException e)
        {
          exception = new SQLException(
              "Request '"
                  + request
                  + "' was successfully executed on remote controllers, but all successful controllers failed before retrieving result");
        }
      }

      /*
       * Notify all controllers where all backend failed (if any) that
       * completion was 'success'. We determine success from the successful
       * controllers even though they may have failed afterwards (in which case
       * requestResult may be null).
       */
      boolean success = (requestResult != null || successfulControllers != null);
      notifyRequestCompletion(request, success, true,
          failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(request, success, false,
          controllersWithoutBackends);

      if (inconsistentControllers != null)
      { // Notify all inconsistent controllers to disable their backends
        notifyControllerInconsistency(request, inconsistentControllers);
      }

      if (requestResult != null)
      {
        if (logger.isDebugEnabled())
          logger.debug("Request " + request.getId()
              + " completed successfully.");
        return requestResult;
      }

      // At this point, all controllers failed

      if (exception != null)
        throw exception;
      else
      {
        String msg = "Request '" + request + "' failed on all controllers";
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    catch (SQLException e)
    {
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Execute a distributed request that returns a ResultSet.
   * 
   * @param request the request to execute
   * @param requestMsg message to be sent through the group communication
   * @param messageTimeout group message timeout
   * @return the corresponding ResultSet
   * @throws SQLException if an error occurs
   */
  private ExecuteUpdateResult executeRequestReturningInt(
      AbstractRequest request, DistributedRequest requestMsg,
      long messageTimeout) throws SQLException
  {
    if (dvdb.isProcessMacroBeforeBroadcast())
      loadBalancer.handleMacros(request);
    try
    {
      ExecuteUpdateResult requestResult = null;

      List<Member> groupMembers = dvdb.getAllMembers();

      MulticastResponse responses = multicastRequest(request, requestMsg,
          messageTimeout, groupMembers);

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that gave an inconsistent result
      ArrayList<Member> inconsistentControllers = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;
      int size = groupMembers.size();
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof ExecuteUpdateResult)
        {
          if (requestResult == null)
            requestResult = (ExecuteUpdateResult) r;
          else
          {
            if (requestResult.getUpdateCount() != ((ExecuteUpdateResult) r)
                .getUpdateCount())
            {
              if (inconsistentControllers == null)
                inconsistentControllers = new ArrayList<Member>();
              inconsistentControllers.add(member);
              logger.error("Controller " + member
                  + " returned an inconsistent results ("
                  + requestResult.getUpdateCount() + " when expecting "
                  + ((ExecuteUpdateResult) r).getUpdateCount()
                  + ") for request " + request.getId());
            }
          }
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to execute query (" + r + ")");
        }
        else if (r instanceof Exception)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "Request " + request.getId() + " failed on controller "
              + member + " (" + r + ")";
          logger.warn(msg);
          if (r instanceof SQLException)
            exception = (SQLException) r;
          else
          {
            exception = new SQLException("Internal exception " + r);
            exception.initCause((Throwable) r);
          }
        }
        else
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isWarnEnabled())
            logger.warn("Unexpected answer from controller " + member + " ("
                + r + ") for request "
                + request.getSqlShortForm(vdb.getSqlShortFormLength()));
        }
      }

      // Notify all controllers where all backends failed (if any) that
      // completion was 'success'
      notifyRequestCompletion(request, requestResult != null, true,
          failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(request, requestResult != null, false,
          controllersWithoutBackends);

      if (inconsistentControllers != null)
      { // Notify all inconsistent controllers to disable their backends
        notifyControllerInconsistency(request, inconsistentControllers);
      }

      if (requestResult != null)
      {
        if (logger.isDebugEnabled())
          logger.debug("Request " + request.getId()
              + " completed successfully.");
        return requestResult;
      }

      // At this point, all controllers failed

      if (exception != null)
        throw exception;
      else
      {
        String msg = "Request '" + request + "' failed on all controllers";
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    catch (SQLException e)
    {
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Execute a distributed request that returns a ResultSet.
   * 
   * @param request the request to execute
   * @param requestMsg message to be sent through the group communication
   * @param messageTimeout group message timeout
   * @return the corresponding ResultSet
   * @throws SQLException if an error occurs
   */
  private ControllerResultSet executeRequestReturningResultSet(
      AbstractRequest request, DistributedRequest requestMsg,
      long messageTimeout) throws SQLException
  {
    if (dvdb.isProcessMacroBeforeBroadcast())
      loadBalancer.handleMacros(request);
    try
    {
      ControllerResultSet requestResult = null;

      List<Member> groupMembers = dvdb.getAllMembers();

      MulticastResponse responses = multicastRequest(request, requestMsg,
          messageTimeout, groupMembers);

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;
      int size = groupMembers.size();
      List<Member> successfulControllers = null;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof ControllerResultSet)
        {
          if (requestResult == null)
            requestResult = (ControllerResultSet) r;
        }
        else if (DistributedRequestManager.SUCCESSFUL_COMPLETION.equals(r))
        {
          // Remote controller success
          if (requestResult == null)
          {
            if (successfulControllers == null)
              successfulControllers = new ArrayList<Member>();
            successfulControllers.add(member);
          }
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to execute query (" + r + ")");
        }
        else if (r instanceof Exception)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "Request " + request.getId() + " failed on controller "
              + member + " (" + r + ")";
          logger.warn(msg);
          if (r instanceof SQLException)
            exception = (SQLException) r;
          else
          {
            exception = new SQLException("Internal exception " + r);
            exception.initCause((Throwable) r);
          }
        }
        else
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isWarnEnabled())
            logger.warn("Unexpected answer from controller " + member + " ("
                + r + ") for request "
                + request.getSqlShortForm(vdb.getSqlShortFormLength()));
        }
      }

      if ((requestResult == null) && (successfulControllers != null))
      { // Local controller could not get the result, ask it to a successful
        // remote controller
        try
        {
          requestResult = (ControllerResultSet) getRequestResultFromFailoverCache(
              successfulControllers, request.getId());
        }
        catch (NoResultAvailableException e)
        {
          exception = new SQLException(
              "Request '"
                  + request
                  + "' was successfully executed on remote controllers, but all successful controllers failed before retrieving result");
        }
      }

      /*
       * Notify all controllers where all backend failed (if any) that
       * completion was 'success'. We determine success from the successful
       * controllers even though they may have failed afterwards (in which case
       * requestResult may be null).
       */
      boolean success = (requestResult != null || successfulControllers != null);
      notifyRequestCompletion(request, success, true,
          failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(request, success, false,
          controllersWithoutBackends);

      if (requestResult != null)
      {
        if (logger.isDebugEnabled())
          logger.debug("Request " + request.getId()
              + " completed successfully.");
        return requestResult;
      }

      // At this point, all controllers failed

      if (exception != null)
        throw exception;
      else
      {
        String msg = "Request '" + request + "' failed on all controllers";
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    catch (SQLException e)
    {
      String msg = Translate
          .get("loadbalancer.request.failed", new String[]{
              request.getSqlShortForm(vdb.getSqlShortFormLength()),
              e.getMessage()});
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Execute a distributed request (usually a stored procedure) that returns a
   * StoredProcedureCallResult.
   * 
   * @param proc the stored procedure to execute
   * @param requestMsg message to be sent through the group communication
   * @param messageTimeout group message timeout
   * @return the corresponding StoredProcedureCallResult
   * @throws SQLException if an error occurs
   */
  private StoredProcedureCallResult executeRequestReturningStoredProcedureCallResult(
      StoredProcedure proc, DistributedRequest requestMsg, long messageTimeout)
      throws SQLException
  {
    if (dvdb.isProcessMacroBeforeBroadcast())
      loadBalancer.handleMacros(proc);
    try
    {
      StoredProcedureCallResult result = null;

      List<Member> groupMembers = dvdb.getAllMembers();

      MulticastResponse responses = multicastRequest(proc, requestMsg,
          messageTimeout, groupMembers);

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;
      int size = groupMembers.size();
      List<Member> successfulControllers = null;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof StoredProcedureCallResult)
        {
          if (result == null)
            result = (StoredProcedureCallResult) r;
        }
        else if (DistributedRequestManager.SUCCESSFUL_COMPLETION.equals(r))
        {
          // Remote controller success
          if (result == null)
          {
            if (successfulControllers == null)
              successfulControllers = new ArrayList<Member>();
            successfulControllers.add(member);
          }
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to execute query (" + r + ")");
        }
        else if (r instanceof Exception)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "Request " + proc.getId() + " failed on controller "
              + member + " (" + r + ")";
          logger.warn(msg);
          if (r instanceof SQLException)
            exception = (SQLException) r;
          else
          {
            exception = new SQLException("Internal exception " + r);
            exception.initCause((Throwable) r);
          }
        }
        else
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isWarnEnabled())
            logger.warn("Unexpected answer from controller " + member + " ("
                + r + ") for request "
                + proc.getSqlShortForm(vdb.getSqlShortFormLength()));
        }
      }

      if ((result == null) && (successfulControllers != null))
      { // Local controller could not get the result, ask it to a successful
        // remote controller
        try
        {
          result = (StoredProcedureCallResult) getRequestResultFromFailoverCache(
              successfulControllers, proc.getId());
        }
        catch (NoResultAvailableException e)
        {
          exception = new SQLException(
              "Request '"
                  + proc
                  + "' was successfully executed on remote controllers, but all successful controllers failed before retrieving result");
        }
      }

      /*
       * Notify all controllers where all backend failed (if any) that
       * completion was 'success'. We determine success from the successful
       * controllers even though they may have failed afterwards (in which case
       * requestResult may be null).
       */
      boolean success = (result != null || successfulControllers != null);
      notifyRequestCompletion(proc, success, true, failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(proc, success, false,
          controllersWithoutBackends);

      if (result != null)
      {
        proc.copyNamedAndOutParameters(result.getStoredProcedure());
        if (logger.isDebugEnabled())
          logger.debug("Request " + proc.getId() + " completed successfully.");
        return result;
      }

      // At this point, all controllers failed

      if (exception != null)
        throw exception;
      else
      {
        String msg = "Request '" + proc + "' failed on all controllers";
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    catch (SQLException e)
    {
      String msg = Translate.get("loadbalancer.request.failed", new String[]{
          proc.getSqlShortForm(vdb.getSqlShortFormLength()), e.getMessage()});
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * Multicast a request to a set of controllers.
   * 
   * @param request the request that is executed
   * @param requestMsg the group message to send
   * @param messageTimeout timeout on the group message
   * @param groupMembers list of controllers to send the message to
   * @return the responses to the multicast
   * @throws SQLException if an error occurs
   */
  private MulticastResponse multicastRequest(AbstractRequest request,
      Serializable requestMsg, long messageTimeout, List<Member> groupMembers)
      throws SQLException
  {
    if (logger.isDebugEnabled())
      logger.debug("Broadcasting request "
          + request.getSqlShortForm(dvdb.getSqlShortFormLength())
          + (request.isAutoCommit() ? "" : " transaction "
              + request.getTransactionId()) + " to all controllers ("
          + dvdb.getChannel().getLocalMembership() + "->"
          + groupMembers.toString() + ")");

    // Send the query to everybody including us
    MulticastResponse responses;
    try
    {
      // Warning! Message timeouts are in ms but request timeout is in seconds
      responses = dvdb.getMulticastRequestAdapter().multicastMessage(
          groupMembers,
          requestMsg,
          MulticastRequestAdapter.WAIT_ALL,
          MessageTimeouts.getMinTimeout(messageTimeout,
              request.getTimeout() * 1000L));
    }
    catch (Exception e)
    {
      String msg = "An error occured while executing distributed request "
          + request.getId();
      logger.warn(msg, e);
      throw new SQLException(msg + " (" + e + ")");
    }

    if (logger.isDebugEnabled())
      logger.debug("Request "
          + request.getSqlShortForm(dvdb.getSqlShortFormLength())
          + " completed.");

    if (responses.getFailedMembers() != null)
    { // Some controllers failed ... too bad !
      logger.warn(responses.getFailedMembers().size()
          + " controller(s) died during execution of request "
          + request.getId());
    }
    return responses;
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedAbort(String,
   *      long)
   */
  public void distributedAbort(String login, long transactionId)
      throws SQLException
  {
    try
    {
      List<Member> groupMembers = dvdb.getAllMembers();

      AbstractRequest abortRequest = new UnknownWriteRequest("abort", false, 0,
          "\n");
      abortRequest.setTransactionId(transactionId);

      MulticastResponse responses = multicastRequest(abortRequest,
          new DistributedAbort(login, transactionId), dvdb.getMessageTimeouts()
              .getRollbackTimeout(), groupMembers);

      if (logger.isDebugEnabled())
        logger.debug("Abort of transaction " + transactionId + " completed.");

      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      int size = groupMembers.size();
      boolean success = false;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof Boolean)
        {
          if (((Boolean) r).booleanValue())
            success = true;
          else
            logger.error("Unexpected result for controller  " + member);
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to abort transaction  "
                + transactionId + " (" + r + ")");
        }
      }

      // Notify controllers without active backends
      notifyRequestCompletion(abortRequest, success, false,
          controllersWithoutBackends);

      if (success)
        return; // This is a success if at least one controller has succeeded

      

      // At this point, all controllers failed

      String msg = "Transaction " + transactionId
          + " failed to abort on all controllers";
      logger.warn(msg);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = "Transaction " + transactionId + " abort failed (" + e + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedCommit(String,
   *      long)
   */
  public void distributedCommit(String login, long transactionId)
      throws SQLException
  {
    try
    {
      List<Member> groupMembers = dvdb.getAllMembers();
      AbstractRequest commitRequest = new UnknownWriteRequest("commit", false,
          0, "\n");
      commitRequest.setTransactionId(transactionId);
      commitRequest.setLogin(login);
      commitRequest.setIsAutoCommit(false);

      MulticastResponse responses = multicastRequest(commitRequest,
          new DistributedCommit(login, transactionId), dvdb
              .getMessageTimeouts().getCommitTimeout(), groupMembers);

      if (logger.isDebugEnabled())
        logger.debug("Commit of transaction " + transactionId + " completed.");

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;
      // get a list that won't change while we go through it
      groupMembers = dvdb.getAllMembers();
      int size = groupMembers.size();
      boolean success = false;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof Boolean)
        {
          if (((Boolean) r).booleanValue())
            success = true;
          else
            logger.error("Unexpected result for controller  " + member);
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to commit transaction  "
                + transactionId + " (" + r + ")");
        }
        else if (r instanceof AllBackendsFailedException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Commit failed on all backends of controller "
                + member + " (" + r + ")");
        }
        else if (r instanceof SQLException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "Commit of transaction " + transactionId
              + " failed on controller " + member + " (" + r + ")";
          logger.warn(msg);
          exception = (SQLException) r;
        }
      }

      // Notify all controllers where all backend failed (if any) that
      // completion was 'success'
      notifyRequestCompletion(commitRequest, success, true, failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(commitRequest, success, false,
          controllersWithoutBackends);

      if (success)
        return; // This is a success if at least one controller has succeeded

      // At this point, all controllers failed

      if (exception != null)
        throw exception;
      else
      {
        String msg = "Transaction " + transactionId
            + " failed to commit on all controllers";
        logger.warn(msg);
        throw new SQLException(msg);
      }
    }
    catch (SQLException e)
    {
      String msg = "Transaction " + transactionId + " commit failed (" + e
          + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedRollback(String,
   *      long)
   */
  public void distributedRollback(String login, long transactionId)
      throws SQLException
  {
    try
    {
      List<Member> groupMembers = dvdb.getAllMembers();

      AbstractRequest rollbackRequest = new UnknownWriteRequest("rollback",
          false, 0, "\n");
      rollbackRequest.setTransactionId(transactionId);
      rollbackRequest.setLogin(login);
      rollbackRequest.setIsAutoCommit(false);

      MulticastResponse responses = multicastRequest(rollbackRequest,
          new DistributedRollback(login, transactionId), dvdb
              .getMessageTimeouts().getRollbackTimeout(), groupMembers);

      if (logger.isDebugEnabled())
        logger
            .debug("Rollback of transaction " + transactionId + " completed.");

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;

      // get a list that won't change while we go through it
      groupMembers = dvdb.getAllMembers();
      int size = groupMembers.size();
      boolean success = false;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof Boolean)
        {
          if (((Boolean) r).booleanValue())
            success = true;
          else
            logger.error("Unexpected result for controller  " + member);
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member
                + " has no more backends to rollback transaction  "
                + transactionId + " (" + r + ")");
        }
        else if (r instanceof AllBackendsFailedException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Rollback failed on all backends of controller "
                + member + " (" + r + ")");
        }
        else if (r instanceof SQLException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "Rollback of transaction " + transactionId
              + " failed on controller " + member + " (" + r + ")";
          logger.warn(msg);
          exception = (SQLException) r;
        }
      }

      // Notify all controllers where all backend failed (if any) that
      // completion was 'success'
      notifyRequestCompletion(rollbackRequest, success, true,
          failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(rollbackRequest, success, false,
          controllersWithoutBackends);

      if (success)
        return; // This is a success if at least one controller has succeeded

      if (exception != null)
        throw exception;

      // At this point, all controllers failed

      String msg = "Transaction " + transactionId
          + " failed to rollback on all controllers";
      logger.warn(msg);
      throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = "Transaction " + transactionId + " rollback failed (" + e
          + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedRollback(String,
   *      long, String)
   */
  public void distributedRollback(String login, long transactionId,
      String savepointName) throws SQLException
  {
    try
    {
      List<Member> groupMembers = dvdb.getAllMembers();

      AbstractRequest rollbackRequest = new UnknownWriteRequest("rollback "
          + savepointName, false, 0, "\n");
      rollbackRequest.setTransactionId(transactionId);
      rollbackRequest.setLogin(login);
      rollbackRequest.setIsAutoCommit(false);

      MulticastResponse responses = multicastRequest(rollbackRequest,
          new DistributedRollbackToSavepoint(transactionId, savepointName),
          dvdb.getMessageTimeouts().getRollbackToSavepointTimeout(),
          groupMembers);

      if (logger.isDebugEnabled())
        logger.debug("Rollback to savepoint " + savepointName + " for "
            + "transaction " + transactionId + " completed.");

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;

      // get a list that won't change while we go through it
      groupMembers = dvdb.getAllMembers();
      int size = groupMembers.size();
      boolean success = false;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof Boolean)
        {
          if (((Boolean) r).booleanValue())
            success = true;
          else
            logger.error("Unexpected result for controller  " + member);
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member + " has no more backends to "
                + "rollback to savepoint " + savepointName + " for "
                + "transaction " + transactionId + " (" + r + ")");
        }
        else if (r instanceof AllBackendsFailedException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("rollback to savepoint failed on all backends of "
                + "controller " + member + " (" + r + ")");
        }
        else if (r instanceof SQLException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "rollback to savepoint " + savepointName + " for "
              + "transaction " + transactionId + " failed on controller "
              + member + " (" + r + ")";
          logger.warn(msg);
          exception = (SQLException) r;
        }
      }

      // Notify all controllers where all backend failed (if any) that
      // completion was 'success'
      notifyRequestCompletion(rollbackRequest, success, true,
          failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(rollbackRequest, success, false,
          controllersWithoutBackends);

      if (success)
        return; // This is a success if at least one controller has succeeded

      if (exception != null)
        throw exception;

      // At this point, all controllers failed

      String msg = "Rollback to savepoint " + savepointName + " for "
	    + "transaction " + transactionId + " failed on all controllers";
	logger.warn(msg);
	throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = "Rollback to savepoint " + savepointName + " for "
          + "transaction " + transactionId + " failed (" + e + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedSetSavepoint(String,
   *      long, String)
   */
  public void distributedSetSavepoint(String login, long transactionId,
      String name) throws SQLException
  {
    try
    {
      List<Member> groupMembers = dvdb.getAllMembers();

      AbstractRequest setSavepointRequest = new UnknownWriteRequest(
          "savepoint " + name, false, 0, "\n");
      setSavepointRequest.setTransactionId(transactionId);
      setSavepointRequest.setLogin(login);
      setSavepointRequest.setIsAutoCommit(false);

      MulticastResponse responses = multicastRequest(setSavepointRequest,
          new DistributedSetSavepoint(login, transactionId, name), dvdb
              .getMessageTimeouts().getSetSavepointTimeout(), groupMembers);

      if (logger.isDebugEnabled())
        logger.debug("Set savepoint " + name + " to transaction "
            + transactionId + " completed.");

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;

      // get a list that won't change while we go through it
      groupMembers = dvdb.getAllMembers();
      int size = groupMembers.size();
      boolean success = false;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof Boolean)
        {
          if (((Boolean) r).booleanValue())
            success = true;
          else
            logger.error("Unexpected result for controller  " + member);
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member + " has no more backends to "
                + "set savepoint " + name + " to transaction " + transactionId
                + " (" + r + ")");
        }
        else if (r instanceof AllBackendsFailedException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("set savepoint failed on all backends of controller "
                + member + " (" + r + ")");
        }
        else if (r instanceof SQLException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "set savepoint " + name + " to transaction "
              + transactionId + " failed on controller " + member + " (" + r
              + ")";
          logger.warn(msg);
          exception = (SQLException) r;
        }
      }

      // Notify all controllers where all backend failed (if any) that
      // completion was 'success'
      notifyRequestCompletion(setSavepointRequest, success, true,
          failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(setSavepointRequest, success, false,
          controllersWithoutBackends);

      if (success)
        return; // This is a success if at least one controller has succeeded

      if (exception != null)
        throw exception;

      // At this point, all controllers failed

      String msg = "Set savepoint " + name + " to transaction "
	    + transactionId + " failed on all controllers";
	logger.warn(msg);
	throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = "Set savepoint " + name + " to transaction " + transactionId
          + " failed (" + e + ")";
      logger.warn(msg);
      throw e;
    }
  }

  /**
   * @see org.continuent.sequoia.controller.requestmanager.distributed.DistributedRequestManager#distributedReleaseSavepoint(String,
   *      long, String)
   */
  public void distributedReleaseSavepoint(String login, long transactionId,
      String name) throws SQLException
  {
    try
    {
      List<Member> groupMembers = dvdb.getAllMembers();

      AbstractRequest releaseSavepointRequest = new UnknownWriteRequest(
          "release " + name, false, 0, "\n");
      releaseSavepointRequest.setTransactionId(transactionId);
      releaseSavepointRequest.setLogin(login);
      releaseSavepointRequest.setIsAutoCommit(false);

      MulticastResponse responses = multicastRequest(releaseSavepointRequest,
          new DistributedReleaseSavepoint(transactionId, name), dvdb
              .getMessageTimeouts().getReleaseSavepointTimeout(), groupMembers);

      if (logger.isDebugEnabled())
        logger.debug("Release savepoint " + name + " from transaction "
            + transactionId + " completed.");

      // List of controllers that gave a AllBackendsFailedException
      ArrayList<Member> failedOnAllBackends = null;
      // List of controllers that have no more backends to execute queries
      ArrayList<Member> controllersWithoutBackends = null;
      SQLException exception = null;

      // get a list that won't change while we go through it
      groupMembers = dvdb.getAllMembers();
      int size = groupMembers.size();
      boolean success = false;
      // Get the result of each controller
      for (int i = 0; i < size; i++)
      {
        Member member = (Member) groupMembers.get(i);
        if ((responses.getFailedMembers() != null)
            && responses.getFailedMembers().contains(member))
        {
          logger.warn("Controller " + member + " is suspected of failure.");
          continue;
        }
        Object r = responses.getResult(member);
        if (r instanceof Boolean)
        {
          if (((Boolean) r).booleanValue())
            success = true;
          else
            logger.error("Unexpected result for controller  " + member);
        }
        else if (r instanceof NoMoreBackendException)
        {
          if (controllersWithoutBackends == null)
            controllersWithoutBackends = new ArrayList<Member>();
          controllersWithoutBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("Controller " + member + " has no more backends to "
                + "release savepoint " + name + " from transaction "
                + transactionId + " (" + r + ")");
        }
        else if (r instanceof AllBackendsFailedException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          if (logger.isDebugEnabled())
            logger.debug("release savepoint failed on all backends of "
                + "controller " + member + " (" + r + ")");
        }
        else if (r instanceof SQLException)
        {
          if (failedOnAllBackends == null)
            failedOnAllBackends = new ArrayList<Member>();
          failedOnAllBackends.add(member);
          String msg = "release savepoint " + name + " from transaction "
              + transactionId + " failed on controller " + member + " (" + r
              + ")";
          logger.warn(msg);
          exception = (SQLException) r;
        }
      }

      // Notify all controllers where all backend failed (if any) that
      // completion was 'success'
      notifyRequestCompletion(releaseSavepointRequest, success, true,
          failedOnAllBackends);

      // Notify controllers without active backends (if any)
      notifyRequestCompletion(releaseSavepointRequest, success, false,
          controllersWithoutBackends);

      if (success)
        return; // This is a success if at least one controller has succeeded

      if (exception != null)
        throw exception;

      // At this point, all controllers failed

      String msg = "Release savepoint " + name + " from transaction "
	    + transactionId + " failed on all controllers";
	logger.warn(msg);
	throw new SQLException(msg);
    }
    catch (SQLException e)
    {
      String msg = "Release savepoint " + name + " from transaction "
          + transactionId + " failed (" + e + ")";
      logger.warn(msg);
      throw e;
    }
  }

}