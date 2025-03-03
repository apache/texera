package edu.uci.ics.amber.core.workflow

import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.util.VirtualIdentityUtils
import scalax.collection.OneOrMore
import scalax.collection.generic.{AbstractDiEdge, MultiEdge}
import scalax.collection.mutable.Graph

import scala.collection.mutable
import scala.language.implicitConversions

case class PhysicalEdge(physicalLink: PhysicalLink)
    extends AbstractDiEdge(physicalLink.fromOpId, physicalLink.toOpId)
    with MultiEdge {

  override def extendKeyBy: OneOrMore[Any] =
    OneOrMore.one((physicalLink.fromPortId, physicalLink.toPortId))
}

case class PhysicalPlan(
    operators: Set[PhysicalOp],
    links: Set[PhysicalLink]
) extends LazyLogging {

  implicit def physicalLinkToEdge(physicalLink: PhysicalLink): PhysicalEdge = {
    PhysicalEdge(physicalLink)
  }

  @transient private lazy val operatorMap: Map[PhysicalOpIdentity, PhysicalOp] =
    operators.map(o => (o.id, o)).toMap

  // the dag will be re-computed again once it reaches the coordinator.
  @transient lazy val dag: Graph[PhysicalOpIdentity, PhysicalEdge] = {
    val scalaDAG = Graph.empty[PhysicalOpIdentity, PhysicalEdge]()
    operatorMap.foreach(op => scalaDAG.add(op._1))
    links.foreach(l => scalaDAG.add(l))
    scalaDAG
  }

  @transient lazy val maxChains: Set[Set[PhysicalLink]] = this.getMaxChains

  @JsonIgnore
  def getSourceOperatorIds: Set[PhysicalOpIdentity] =
    operatorMap.keys.filter(op => dag.get(op).inDegree == 0).toSet

  def getPhysicalOpsOfLogicalOp(logicalOpId: OperatorIdentity): List[PhysicalOp] = {
    topologicalIterator()
      .filter(physicalOpId => physicalOpId.logicalOpId == logicalOpId)
      .map(physicalOpId => getOperator(physicalOpId))
      .toList
  }

  def getOperator(physicalOpId: PhysicalOpIdentity): PhysicalOp = operatorMap(physicalOpId)

  /**
    * returns a sub-plan that contains the specified operators and the links connected within these operators
    */
  def getSubPlan(subOperators: Set[PhysicalOpIdentity]): PhysicalPlan = {
    val newOps = operators.filter(op => subOperators.contains(op.id))
    val newLinks =
      links.filter(link =>
        subOperators.contains(link.fromOpId) && subOperators.contains(link.toOpId)
      )
    PhysicalPlan(newOps, newLinks)
  }

  def getUpstreamPhysicalOpIds(physicalOpId: PhysicalOpIdentity): Set[PhysicalOpIdentity] = {
    dag.get(physicalOpId).incoming.map(e => e.physicalLink.fromOpId)
  }

  def getUpstreamPhysicalLinks(physicalOpId: PhysicalOpIdentity): Set[PhysicalLink] = {
    links.filter(l => l.toOpId == physicalOpId)
  }

  def getDownstreamPhysicalLinks(physicalOpId: PhysicalOpIdentity): Set[PhysicalLink] = {
    links.filter(l => l.fromOpId == physicalOpId)
  }

  def topologicalIterator(): Iterator[PhysicalOpIdentity] = {
    dag.topologicalSort match {
      case Left(value)  => throw new RuntimeException("topological sort failed")
      case Right(value) => value.iterator.map(_.outer)
    }
  }
  def addOperator(physicalOp: PhysicalOp): PhysicalPlan = {
    this.copy(operators = Set(physicalOp) ++ operators)
  }

  def addLink(link: PhysicalLink): PhysicalPlan = {
    val formOp = operatorMap(link.fromOpId)
    val (_, _, outputSchema) = formOp.outputPorts(link.fromPortId)
    val newFromOp = formOp.addOutputLink(link)
    val newToOp = getOperator(link.toOpId)
      .addInputLink(link)
      .propagateSchema(outputSchema.toOption.map(schema => (link.toPortId, schema)))

    val newOperators = operatorMap +
      (link.fromOpId -> newFromOp) +
      (link.toOpId -> newToOp)
    this.copy(newOperators.values.toSet, links ++ Set(link))
  }

  def removeLink(
      link: PhysicalLink
  ): PhysicalPlan = {
    val fromOpId = link.fromOpId
    val toOpId = link.toOpId
    val newOperators = operatorMap +
      (fromOpId -> getOperator(fromOpId).removeOutputLink(link)) +
      (toOpId -> getOperator(toOpId).removeInputLink(link))
    this.copy(operators = newOperators.values.toSet, links.filter(l => l != link))
  }

  def setOperator(physicalOp: PhysicalOp): PhysicalPlan = {
    this.copy(operators = (operatorMap + (physicalOp.id -> physicalOp)).values.toSet)
  }

  @JsonIgnore
  def getPhysicalOpByWorkerId(workerId: ActorVirtualIdentity): PhysicalOp =
    getOperator(VirtualIdentityUtils.getPhysicalOpId(workerId))

  @JsonIgnore
  def getLinksBetween(
      from: PhysicalOpIdentity,
      to: PhysicalOpIdentity
  ): Set[PhysicalLink] = {
    links.filter(link => link.fromOpId == from && link.toOpId == to)

  }

  @JsonIgnore
  def getOutputPartitionInfo(
      link: PhysicalLink,
      upstreamPartitionInfo: PartitionInfo,
      opToWorkerNumberMapping: Map[PhysicalOpIdentity, Int]
  ): PartitionInfo = {
    val fromPhysicalOp = getOperator(link.fromOpId)
    val toPhysicalOp = getOperator(link.toOpId)

    // make sure this input is connected to this port
    assert(
      toPhysicalOp
        .getInputLinks(Some(link.toPortId))
        .map(link => link.fromOpId)
        .contains(fromPhysicalOp.id)
    )

    // partition requirement of this PhysicalOp on this input port
    val requiredPartitionInfo =
      toPhysicalOp.partitionRequirement
        .lift(link.toPortId.id)
        .flatten
        .getOrElse(UnknownPartition())

    // the upstream partition info satisfies the requirement, and number of worker match
    if (
      upstreamPartitionInfo.satisfies(requiredPartitionInfo) && opToWorkerNumberMapping.getOrElse(
        fromPhysicalOp.id,
        0
      ) == opToWorkerNumberMapping.getOrElse(toPhysicalOp.id, 0)
    ) {
      upstreamPartitionInfo
    } else {
      // we must re-distribute the input partitions
      requiredPartitionInfo

    }
  }

  private def isMaterializedLink(link: PhysicalLink): Boolean = {
    getOperator(link.toOpId).isSinkOperator
  }

  @JsonIgnore
  def getNonMaterializedBlockingAndDependeeLinks: Set[PhysicalLink] = {
    operators
      .flatMap { physicalOp =>
        {
          getUpstreamPhysicalOpIds(physicalOp.id)
            .flatMap { upstreamPhysicalOpId =>
              links
                .filter(link =>
                  link.fromOpId == upstreamPhysicalOpId && link.toOpId == physicalOp.id
                )
                .filter(link =>
                  !isMaterializedLink(link) && (getOperator(physicalOp.id).isInputLinkDependee(
                    link
                  ) || getOperator(upstreamPhysicalOpId).isOutputLinkBlocking(link))
                )
            }
        }
      }
  }

  @JsonIgnore
  def getDependeeLinks: Set[PhysicalLink] = {
    operators
      .flatMap { physicalOp =>
        {
          getUpstreamPhysicalOpIds(physicalOp.id)
            .flatMap { upstreamPhysicalOpId =>
              links
                .filter(link =>
                  link.fromOpId == upstreamPhysicalOpId && link.toOpId == physicalOp.id
                )
                .filter(link => getOperator(physicalOp.id).isInputLinkDependee(link))
            }
        }
      }
  }

  /**
    * create a DAG similar to the physical DAG but with all dependee links removed.
    */
  @JsonIgnore // this is needed to prevent the serialization issue
  def getDependeeLinksRemovedDAG: PhysicalPlan = {
    this.copy(operators, links.diff(getDependeeLinks))
  }

  /**
    * Computes the bridges (cut-edges) in the given directed graph using Tarjan's Algorithm.
    * A bridge is an edge whose removal increases the number of connected components.
    *
    * @return A set of PhysicalLinks representing the bridges in the graph.
    */
  private def findBridges: Set[PhysicalLink] = {
    val weakBridges = mutable.Set[PhysicalLink]()
    val componentsBefore = this.dag.componentTraverser().size

    for (edge <- this.dag.edges) {
      val tempGraph = this.dag.clone()
      tempGraph -= edge

      val componentsAfter = tempGraph.componentTraverser().size

      if (componentsAfter > componentsBefore) {
        weakBridges.add(edge.physicalLink)
      }
    }

    weakBridges.toSet
  }

  /**
    * A link is a bridge if removal of that link would increase the number of (weakly) connected components in the DAG.
    * Assuming pipelining a link is more desirable than materializing it, and optimal physical plan always pipelines
    * a bridge. We can thus use bridges to optimize the process of searching for an optimal physical plan.
    *
    * @return All non-blocking links that are not bridges.
    */
  @JsonIgnore
  def getNonBridgeNonBlockingLinks: Set[PhysicalLink] = {
    this.links.diff(getNonMaterializedBlockingAndDependeeLinks).diff(findBridges)
  }

  /**
    * A chain in a physical plan is a path such that each of its operators (except the first and the last operators)
    * is connected only to operators on the path. Assuming pipelining a link is more desirable than materializations,
    * and optimal physical plan has at most one link on each chain. We can thus use chains to optimize the process of
    * searching for an optimal physical plan. A maximal chain is a chain that is not a sub-path of any other chain.
    * A maximal chain can cover the optimizations of all its sub-chains, so finding only maximal chains is adequate for
    * optimization purposes. Note the definition of a chain has nothing to do with that of a connected component.
    *
    * @return All the maximal chains of this physical plan, where each chain is represented as a set of links.
    */
  private def getMaxChains: Set[Set[PhysicalLink]] = {
    val allChains = mutable.Set[Set[PhysicalLink]]()

    /**
      * Recursively expands chains from the current node, enumerating all possible paths.
      * @param current The current operator identity from which to expand.
      * @param path    The accumulated list of operator IDs we have visited so far.
      */
    def expandChain(current: PhysicalOpIdentity, path: List[PhysicalOpIdentity]): Unit = {

      if (path.length > 2 && validIntermediateNodes(path)) {
        allChains += pathToLinks(path)
      }

      val successors = this.dag.get(current).outNeighbors.map(_.outer)

      successors.foreach { next =>
        // Avoid cycles by checking if we've already visited 'next'
        if (!path.contains(next)) {
          expandChain(next, path :+ next)
        }
      }
    }

    /**
      * Checks that all intermediate nodes in the path
      * (except the first and last) have inDegree == 1 and outDegree == 1.
      */
    def validIntermediateNodes(path: List[PhysicalOpIdentity]): Boolean = {
      // drop first and last, then check degree
      path.drop(1).dropRight(1).forall { mid =>
        val midNode = this.dag.get(mid)
        midNode.inDegree == 1 && midNode.outDegree == 1
      }
    }

    /**
      * Converts a path of operator IDs into a set of PhysicalLink
      * by looking up each consecutive pair in `links`.
      */
    def pathToLinks(path: List[PhysicalOpIdentity]): Set[PhysicalLink] = {
      path
        .sliding(2)
        .flatMap {
          case Seq(from, to) =>
            links.find(l => l.fromOpId == from && l.toOpId == to)
          case _ => None
        }
        .toSet
    }

    // Enumerate all possible starting nodes (potential ancestors)
    for (node <- this.dag.nodes.map(_.outer)) {
      expandChain(node, List(node))
    }

    // Now we have a bunch of chain candidates in `allChains`.
    // We remove those that are strictly sub-chains of others.
    val chainCandidates = allChains.toSet
    chainCandidates.filter(s1 => chainCandidates.forall(s2 => s1 == s2 || !s1.subsetOf(s2)))
  }

  def propagateSchema(inputSchemas: Map[PortIdentity, Schema]): PhysicalPlan = {
    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    this
      .topologicalIterator()
      .map(this.getOperator)
      .foreach({ physicalOp =>
        {
          val propagatedPhysicalOp = physicalOp.inputPorts.keys.foldLeft(physicalOp) {
            (op, inputPortId) =>
              op.propagateSchema(inputSchemas.get(inputPortId).map(schema => (inputPortId, schema)))
          }

          // Add the operator to the physical plan
          physicalPlan = physicalPlan.addOperator(propagatedPhysicalOp.propagateSchema())

          // Add internal links to the physical plan
          physicalPlan = getUpstreamPhysicalLinks(physicalOp.id).foldLeft(physicalPlan) {
            (plan, link) =>
              plan.addLink(link)
          }
        }
      })
    physicalPlan
  }
}
