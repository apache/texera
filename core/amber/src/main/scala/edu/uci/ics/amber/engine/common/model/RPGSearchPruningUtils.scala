package edu.uci.ics.amber.engine.common.model

import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.alg.shortestpath.AllDirectedPaths
import org.jgrapht.graph.DirectedAcyclicGraph

import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

object RPGSearchPruningUtils {

  /**
    * A non-blocking link is a bridge if removal of that link would increase the number of (weakly) connected components
    * in the DAG. Assuming pipelining a link is more desirable than materializing it, and optimal physical plan always
    * pipelines a bridge. We can thus use bridges to optimize the process of searching for an optimal physical plan.
    *
    * @return All the bridges in a DAG.
    */
  def getBridges(
      dag: DirectedAcyclicGraph[PhysicalOpIdentity, PhysicalLink],
      nonBlockingLinks: Set[PhysicalLink]
  ): Set[PhysicalLink] =
    nonBlockingLinks.intersect(
      new BiconnectivityInspector[PhysicalOpIdentity, PhysicalLink](
        dag
      ).getBridges.asScala.toSet
    )

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
  def getMaxChains(
      dag: DirectedAcyclicGraph[PhysicalOpIdentity, PhysicalLink]
  ): Set[Set[PhysicalLink]] = {
    val dijkstra = new AllDirectedPaths[PhysicalOpIdentity, PhysicalLink](dag)
    val chains = dag
      .vertexSet()
      .asScala
      .flatMap { ancestor =>
        {
          dag.getDescendants(ancestor).asScala.flatMap { descendant =>
            {
              dijkstra
                .getAllPaths(ancestor, descendant, true, Integer.MAX_VALUE)
                .asScala
                .filter(path =>
                  path.getLength > 1 &&
                    path.getVertexList.asScala
                      .filter(v => v != path.getStartVertex && v != path.getEndVertex)
                      .forall(v => dag.inDegreeOf(v) == 1 && dag.outDegreeOf(v) == 1)
                )
                .map(path =>
                  path.getEdgeList.asScala
                    .map { edge =>
                      {
                        val fromOpId = dag.getEdgeSource(edge)
                        val toOpId = dag.getEdgeTarget(edge)
                        dag
                          .edgeSet()
                          .asScala
                          .find(l => l.fromOpId == fromOpId && l.toOpId == toOpId)
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
