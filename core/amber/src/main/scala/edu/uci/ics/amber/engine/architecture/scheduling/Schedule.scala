package edu.uci.ics.amber.engine.architecture.scheduling

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

case class Schedule(private val regionPlan: RegionPlan) extends Iterator[Set[Region]] {
  private val levels = mutable.Map.empty[RegionIdentity, Int]
  private val levelSets = mutable.Map.empty[Int, mutable.Set[RegionIdentity]]
  private var currentLevel = 0

  regionPlan.topologicalIterator().foreach { currentVertex =>
    val level = regionPlan.dag.incomingEdgesOf(currentVertex).asScala.foldLeft(0) {
      (maxLevel, incomingEdge) =>
        val sourceVertex = regionPlan.dag.getEdgeSource(incomingEdge)
        math.max(maxLevel, levels.getOrElse(sourceVertex, 0) + 1)
    }

    levels(currentVertex) = level
    levelSets.getOrElseUpdate(level, mutable.Set.empty).add(currentVertex)
  }

  currentLevel = levelSets.keys.minOption.getOrElse(0)

  override def hasNext: Boolean = levelSets.isDefinedAt(currentLevel)

  override def next(): Set[Region] = {
    val regions = levelSets(currentLevel).map(regionId => regionPlan.getRegion(regionId)).toSet
    currentLevel += 1
    regions
  }
}
