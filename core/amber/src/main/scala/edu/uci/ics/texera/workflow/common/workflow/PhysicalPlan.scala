package edu.uci.ics.texera.workflow.common.workflow

import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.workflow.common.WorkflowContext
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.alg.shortestpath.AllDirectedPaths
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator
import org.jgrapht.util.SupplierUtil

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{IteratorHasAsScala, ListHasAsScala, SetHasAsScala}
import scala.util.{Failure, Success, Try}

object PhysicalPlan {
  def apply(
      context: WorkflowContext,
      logicalPlan: LogicalPlan,
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): PhysicalPlan = {

    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)

    logicalPlan.getTopologicalOpIds.asScala.foreach(logicalOpId =>
      Try {
        val logicalOp = logicalPlan.getOperator(logicalOpId)
        logicalOp.setContext(context)

        val subPlan = logicalOp.getPhysicalPlan(context.workflowId, context.executionId)
        subPlan
          .topologicalIterator()
          .map(subPlan.getOperator)
          .foreach({ physicalOp =>
            {
              val externalLinks = logicalPlan
                .getUpstreamLinks(logicalOp.operatorIdentifier)
                .filter(link => physicalOp.inputPorts.contains(link.toPortId))
                .flatMap { link =>
                  physicalPlan
                    .getPhysicalOpsOfLogicalOp(link.fromOpId)
                    .find(_.outputPorts.contains(link.fromPortId))
                    .map(fromOp =>
                      PhysicalLink(fromOp.id, link.fromPortId, physicalOp.id, link.toPortId)
                    )
                }

              val internalLinks = subPlan.getUpstreamPhysicalLinks(physicalOp.id)

              // Add the operator to the physical plan
              physicalPlan = physicalPlan.addOperator(physicalOp.propagateSchema())

              // Add all the links to the physical plan
              physicalPlan = (externalLinks ++ internalLinks)
                .foldLeft(physicalPlan) { (plan, link) => plan.addLink(link) }
            }
          })
      } match {
        case Success(_) =>
        case Failure(err) =>
          errorList match {
            case Some(list) => list.append((logicalOpId, err))
            case None       => throw err
          }
      }
    )
    physicalPlan
  }

}

case class PhysicalPlan(
    operators: Set[PhysicalOp],
    links: Set[PhysicalLink]
) extends LazyLogging {

  @transient private lazy val operatorMap: Map[PhysicalOpIdentity, PhysicalOp] =
    operators.map(o => (o.id, o)).toMap

  // the dag will be re-computed again once it reaches the coordinator.
  @transient lazy val dag: DirectedAcyclicGraph[PhysicalOpIdentity, PhysicalLink] = {
    val jgraphtDag = new DirectedAcyclicGraph[PhysicalOpIdentity, PhysicalLink](
      null, // vertexSupplier
      SupplierUtil.createSupplier(classOf[PhysicalLink]), // edgeSupplier
      false, // weighted
      true // allowMultipleEdges
    )
    operatorMap.foreach(op => jgraphtDag.addVertex(op._1))
    links.foreach(l => jgraphtDag.addEdge(l.fromOpId, l.toOpId, l))
    jgraphtDag
  }

  @transient lazy val maxChains: Set[Set[PhysicalLink]] = this.getMaxChains

  def getSourceOperatorIds: Set[PhysicalOpIdentity] =
    operatorMap.keys.filter(op => dag.inDegreeOf(op) == 0).toSet

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
    dag.incomingEdgesOf(physicalOpId).asScala.map(e => dag.getEdgeSource(e)).toSet
  }

  def getUpstreamPhysicalLinks(physicalOpId: PhysicalOpIdentity): Set[PhysicalLink] = {
    links.filter(l => l.toOpId == physicalOpId)
  }

  def getDownstreamPhysicalLinks(physicalOpId: PhysicalOpIdentity): Set[PhysicalLink] = {
    links.filter(l => l.fromOpId == physicalOpId)
  }

  def topologicalIterator(): Iterator[PhysicalOpIdentity] = {
    new TopologicalOrderIterator(dag).asScala
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

  def getPhysicalOpByWorkerId(workerId: ActorVirtualIdentity): PhysicalOp =
    getOperator(VirtualIdentityUtils.getPhysicalOpId(workerId))

  def getLinksBetween(
      from: PhysicalOpIdentity,
      to: PhysicalOpIdentity
  ): Set[PhysicalLink] = {
    links.filter(link => link.fromOpId == from && link.toOpId == to)

  }

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
    * A link is a bridge if removal of that link would increase the number of (weakly) connected components in the DAG.
    * Assuming pipelining a link is more desirable than materializing it, and optimal physical plan always pipelines
    * a bridge. We can thus use bridges to optimize the process of searching for an optimal physical plan.
    *
    * @return All non-blocking links that are not bridges.
    */
  def getNonBridgeNonBlockingLinks: Set[PhysicalLink] = {
    val bridges =
      new BiconnectivityInspector[PhysicalOpIdentity, PhysicalLink](this.dag).getBridges.asScala
        .map { edge =>
          {
            val fromOpId = this.dag.getEdgeSource(edge)
            val toOpId = this.dag.getEdgeTarget(edge)
            links.find(l => l.fromOpId == fromOpId && l.toOpId == toOpId)
          }
        }
        .flatMap(_.toList)
    this.links.diff(getNonMaterializedBlockingAndDependeeLinks).diff(bridges)
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
    val dijkstra = new AllDirectedPaths[PhysicalOpIdentity, PhysicalLink](this.dag)
    val chains = this.dag
      .vertexSet()
      .asScala
      .flatMap { ancestor =>
        {
          this.dag.getDescendants(ancestor).asScala.flatMap { descendant =>
            {
              dijkstra
                .getAllPaths(ancestor, descendant, true, Integer.MAX_VALUE)
                .asScala
                .filter(path =>
                  path.getLength > 1 &&
                    path.getVertexList.asScala
                      .filter(v => v != path.getStartVertex && v != path.getEndVertex)
                      .forall(v => this.dag.inDegreeOf(v) == 1 && this.dag.outDegreeOf(v) == 1)
                )
                .map(path =>
                  path.getEdgeList.asScala
                    .map { edge =>
                      {
                        val fromOpId = this.dag.getEdgeSource(edge)
                        val toOpId = this.dag.getEdgeTarget(edge)
                        links.find(l => l.fromOpId == fromOpId && l.toOpId == toOpId)
                      }
                    }
                    .flatMap(_.toList)
                    .toSet
                )
                .toSet
            }
          }
        }
      }
    chains.filter(s1 => chains.forall(s2 => s1 == s2 || !s1.subsetOf(s2))).toSet
  }

}
