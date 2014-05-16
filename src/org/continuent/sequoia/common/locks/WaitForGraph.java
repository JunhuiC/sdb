/**
 * Sequoia: Database clustering technology.
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
 * Initial developer(s): Damian Arregui.
 * Contributor(s): _________________________.
 */

package org.continuent.sequoia.common.locks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.continuent.sequoia.common.locks.TransactionLogicalLock.WaitingListElement;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.loadbalancer.BackendTaskQueueEntry;
import org.continuent.sequoia.controller.loadbalancer.tasks.AbstractTask;

/**
 * This class defines a WaitForGraph. Nodes in the graph represent active
 * transactions. Edges represent "waitin for" relationships: the source node is
 * waiting for the sink node to release a lock on a given resource. This class
 * allows to detect cycles in the graph, i.e. deadlocks, and to determine which
 * transaction to abort in order to break one or more cycles.
 *
 * @author <a href="mailto:Damian.Arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public class WaitForGraph
{
  private static final String INDENT = "    ";
  private DatabaseBackend     backend;
  private List                storedProcedureQueue;
  private Node                victim;

  private static Trace        logger = Trace
                                         .getLogger("org.continuent.sequoia.controller.loadbalancer");

  /**
   * Creates a new <code>WaitForGraph</code> object.
   *
   * @param backend database backend that will be used to build the graph.
   * @param storedProcedureQueue also used to build the graph.
   */
  public WaitForGraph(DatabaseBackend backend, List storedProcedureQueue)
  {
    this.backend = backend;
    this.storedProcedureQueue = storedProcedureQueue;
  }

  /**
   * Detects deadlocks in the wait-for graph. First build the graph from the
   * database schema, then walk the graph to detect cycles, and finally examines
   * the cycles (if found) in order to determine a suitable transaction to be
   * aborted.
   *
   * @return true is a deadlock has been detected, false otherwise.
   */
  public boolean detectDeadlocks()
  {
    Collection nodes = build();
    Collection cycles = walk(nodes);
    kill(cycles);
    return (victim != null);
  }

  /**
   * Returns the ID of the transaction that should be aborted first. This method
   * returns a value computed during the last run of {@link #detectDeadlocks()}.
   *
   * @return victiom transaction ID.
   */
  public long getVictimTransactionId()
  {
    return victim.getTransactionId();
  }

  private Collection build()
  {
    if (logger.isDebugEnabled())
      logger.debug("Building wait-for graph...");

    // Triggers schema update
    DatabaseSchema schema = backend.getDatabaseSchema();

    Map transactionToNode = new HashMap();
    TransactionLogicalLock globalLock = schema.getLock();
    // Make sure we don't process a DatabaseTable object twice,
    // which may happen if we reference a table
    // both by its fully qualified name (i.e. schemaName.tableName)
    // and by its short name (i.e. tableName).
    Set uniqueTables = new HashSet(schema.getTables().values());
    for (Iterator iter = uniqueTables.iterator(); iter.hasNext();)
    {
      DatabaseTable table = (DatabaseTable) iter.next();
      TransactionLogicalLock lock = table.getLock();
      if (lock.isLocked())
      {
        long lockerTransactionId = lock.getLocker();
        if (logger.isDebugEnabled())
          logger.debug(table.getName() + " locked by " + lockerTransactionId);
        Node lockerNode = (Node) transactionToNode.get(new Long(
            lockerTransactionId));
        if (lockerNode == null)
        {
          lockerNode = new Node(lockerTransactionId);
          transactionToNode.put(new Long(lockerTransactionId), lockerNode);
        }
        for (Iterator iter2 = lock.getWaitingList().iterator(); iter2.hasNext();)
        {
          long waitingTransactionId = ((WaitingListElement) iter2.next())
              .getTransactionId();
          Node waitingNode = (Node) transactionToNode.get(new Long(
              waitingTransactionId));
          if (waitingNode == null)
          {
            waitingNode = new Node(waitingTransactionId);
            transactionToNode.put(new Long(waitingTransactionId), waitingNode);
          }
          Edge edge = new Edge(waitingNode, lockerNode, table);
          waitingNode.addOutgoingEdge(edge);
          lockerNode.addIncomingEdge(edge);
        }

        // Check for blocked stored procedures
        if ((storedProcedureQueue != null) && (globalLock.isLocked()))
        {
          for (Iterator iter2 = storedProcedureQueue.iterator(); iter2
              .hasNext();)
          {
            AbstractTask task = ((BackendTaskQueueEntry) iter2.next())
                .getTask();
            long waitingTransactionId = task.getTransactionId();
            // TODO check this test
            if ((waitingTransactionId != lockerTransactionId)
                && ((task.getLocks(backend) == null) || (task.getLocks(backend)
                    .contains(lock))))
            {
              Node node = (Node) transactionToNode.get(new Long(
                  waitingTransactionId));
              if (node == null)
              {
                node = new Node(waitingTransactionId);
                transactionToNode.put(new Long(waitingTransactionId), node);
              }
              Edge edge = new Edge(node, lockerNode, table);
              node.addOutgoingEdge(edge);
              lockerNode.addIncomingEdge(edge);
            }
          }
        }
      }
    }
    return transactionToNode.values();
  }

  private Collection walk(Collection nodes)
  {
    if (logger.isDebugEnabled())
      logger.debug("Walking wait-for graph...");
    List startNodes = new ArrayList();
    List cycles = new ArrayList();
    String indent = new String();

    // Need to start from different nodes since graph may not be fully connected
    for (Iterator iter = nodes.iterator(); iter.hasNext();)
    {
      Node node = (Node) iter.next();
      if (!startNodes.contains(node))
      {
        List visitedNodes = new ArrayList();
        doWalk(node, visitedNodes, new Path(), cycles, indent);
        startNodes.addAll(visitedNodes);
      }
    }
    return cycles;
  }

  private void doWalk(Node node, List visitedNodes, Path path, List cycles,
      String indent)
  {
    // Check that we haven't met a cycle
    if (path.containsSource(node))
    {
      if (logger.isDebugEnabled())
        logger.debug(indent + "Cycle!");
      path.trimBeforeSource(node);

      // Check if it is a new cycle
      if (!cycles.contains(path))
      {
        cycles.add(path);
      }

      return;
    }

    // No cycle, proceed with exploration
    visitedNodes.add(node);
    for (Iterator iter = node.getOutgoingEdges().iterator(); iter.hasNext();)
    {
      Edge edge = (Edge) iter.next();
      if (!path.containsEdge(edge))
      {
        Path newPath = new Path(path);
        newPath.addEdge(edge);
        if (logger.isDebugEnabled())
          logger.debug(indent + node.getTransactionId() + " waits for "
              + ((DatabaseTable) edge.getResource()).getName() + " locked by "
              + edge.getSink().getTransactionId());
        doWalk(edge.getSink(), visitedNodes, newPath, cycles, indent + INDENT);
      }
    }
  }

  private Node kill(Collection cycles)
  {
    if (logger.isDebugEnabled())
      logger.debug("Choosing victim node...");
    if (logger.isDebugEnabled())
      logger.debug(cycles.size() + " cycles detected");
    Map appearances = new HashMap();
    int maxCount = 0;
    victim = null;

    // Count in how many cycles each node appears
    // FIXME : some cycles may have been detected multiple times
    for (Iterator iter = cycles.iterator(); iter.hasNext();)
    {
      Path cycle = (Path) iter.next();
      for (Iterator iter2 = cycle.getSources().iterator(); iter2.hasNext();)
      {
        Node node = (Node) iter2.next();
        if (appearances.containsKey(node))
        {
          appearances.put(node, new Integer(((Integer) appearances.get(node))
              .intValue() + 1));
        }
        else
        {
          appearances.put(node, new Integer(1));
        }
        int value = ((Integer) appearances.get(node)).intValue();
        if (value > maxCount)
        {
          maxCount = value;
          victim = node;
        }
      }
    }

    if (logger.isDebugEnabled())
    {
      for (Iterator iter = cycles.iterator(); iter.hasNext();)
      {
        StringBuffer printableCycle = new StringBuffer(INDENT);
        Path cycle = (Path) iter.next();
        Edge edge = null;
        for (Iterator iter2 = cycle.getEdges().iterator(); iter2.hasNext();)
        {
          edge = (Edge) iter2.next();
          printableCycle.append(edge.getSource().getTransactionId() + " --"
              + ((DatabaseTable) edge.getResource()).getName() + "--> ");
        }
        printableCycle.append(edge.getSink().getTransactionId());
        logger.debug(printableCycle);
      }
      if (victim == null)
      {
        logger.debug("No victim");
      }
      else
      {
        logger.debug("Victim: " + victim.getTransactionId());
      }
    }
    return victim;
  }

  private class Node
  {
    private long transactionId;
    private Set  outgoingEdges = new HashSet();
    private Set  incomingEdges = new HashSet();

    /**
     * Creates a new <code>Node</code> object
     *
     * @param transactionId the transaction id
     */
    public Node(long transactionId)
    {
      this.transactionId = transactionId;
    }

    /**
     * Add an incoming edge
     *
     * @param edge the edge to add
     */
    public void addIncomingEdge(Edge edge)
    {
      incomingEdges.add(edge);
    }

    /**
     * Add an outgoing edge
     *
     * @param edge the edge to add
     */
    public void addOutgoingEdge(Edge edge)
    {
      outgoingEdges.add(edge);
    }

    /**
     * Returns the outgoingEdges value.
     *
     * @return Returns the outgoingEdges.
     */
    public Set getOutgoingEdges()
    {
      return outgoingEdges;
    }

    /**
     * Returns the transactionId value.
     *
     * @return Returns the transactionId.
     */
    public long getTransactionId()
    {
      return transactionId;
    }
  }

  private class Edge
  {
    private Object resource;
    private Node   source;
    private Node   sink;

    /**
     * Creates a new <code>Edge</code> object
     *
     * @param source the source node
     * @param sink the sink node
     * @param resource the resource
     */
    public Edge(Node source, Node sink, Object resource)
    {
      this.source = source;
      this.sink = sink;
      this.resource = resource;
    }

    /**
     * Returns the resource value.
     *
     * @return Returns the resource.
     */
    public Object getResource()
    {
      return resource;
    }

    /**
     * Returns the sink value.
     *
     * @return Returns the sink.
     */
    public Node getSink()
    {
      return sink;
    }

    /**
     * Returns the source value.
     *
     * @return Returns the source.
     */
    public Node getSource()
    {
      return source;
    }

  }

  private class Path
  {
    private List edges;
    private List sources;
    private Map  sourceToEdge;

    /**
     * Creates a new <code>Path</code> object
     */
    public Path()
    {
      edges = new ArrayList();
      sources = new ArrayList();
      sourceToEdge = new HashMap();
    }

    /**
     * Creates a new <code>Path</code> object
     *
     * @param path the path to clone
     */
    public Path(Path path)
    {
      edges = new ArrayList(path.edges);
      sources = new ArrayList(path.sources);
      sourceToEdge = new HashMap(path.sourceToEdge);
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj)
    {
      if (obj == null)
      {
        return false;
      }
      if (obj instanceof Path)
      {
        Set thisEdgesSet = new HashSet(edges);
        Set objEdgesSet = new HashSet(((Path) obj).edges);
        return thisEdgesSet.equals(objEdgesSet);
      }
      return false;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode()
    {
      return (new HashSet(edges)).hashCode();
    }

    /**
     * Add an edge
     *
     * @param edge the edge to add
     */
    public void addEdge(Edge edge)
    {
      edges.add(edge);
      sources.add(edge.getSource());
      sourceToEdge.put(edge.getSource(), edge);
    }

    /**
     * Returns true if the given edge is contained in the path
     *
     * @param edge the edge to look for
     * @return true if the edge has been found
     */
    public boolean containsEdge(Edge edge)
    {
      return edges.contains(edge);
    }

    /**
     * Returns true if the sources contain the given node
     *
     * @param node the node to look for
     * @return true if the node has been found
     */
    public boolean containsSource(Node node)
    {
      return sources.contains(node);
    }

    /**
     * Returns the edges value.
     *
     * @return Returns the edges.
     */
    public List getEdges()
    {
      return edges;
    }

    /**
     * Returns the sources value.
     *
     * @return Returns the sources.
     */
    public List getSources()
    {
      return sources;
    }

    /**
     * Remove elements before the given source node
     *
     * @param node the source node
     */
    public void trimBeforeSource(Node node)
    {
      edges.subList(0, edges.indexOf(sourceToEdge.get(node))).clear();
      sources.subList(0, sources.indexOf(node)).clear();
      // Starting here this instance of Path should be garbage-collected
      // (internal consistency lost)
    }
  }

}
