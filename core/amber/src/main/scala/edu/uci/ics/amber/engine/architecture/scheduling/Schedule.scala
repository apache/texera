package edu.uci.ics.amber.engine.architecture.scheduling

case class Schedule(private val levelSets: Map[Int, Set[Region]]) extends Iterator[Set[Region]] {
  private var currentLevel = levelSets.keys.minOption.getOrElse(0)

  override def hasNext: Boolean = levelSets.isDefinedAt(currentLevel)

  override def next(): Set[Region] = {
    val regions = levelSets(currentLevel)
    currentLevel += 1
    regions
  }
}
