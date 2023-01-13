package edu.uci.ics.texera.workflow.common.workflow

sealed trait PartitionInfo {

  // whether this partition satisfies the other partition
  def satisfies(other: PartitionInfo): Boolean = {
    this == other || other == UnknownPartition()
  }

  // after a stream with this partition merges with another stream with the other partition
  // the result partition after the merge
  def merge(other: PartitionInfo): PartitionInfo = {
    if (this == other) this else UnknownPartition()
  }

}

object HashPartition {
  def apply(hashColumnIndices: Seq[Int]): PartitionInfo = {
    if (hashColumnIndices.nonEmpty)
      new HashPartition(hashColumnIndices)
    else
      UnknownPartition()
  }
}

case class HashPartition(hashColumnIndices: Seq[Int]) extends PartitionInfo

object RangePartition {

  def apply(rangeColumnIndices: Seq[Int], rangeMin: Long, rangeMax: Long): PartitionInfo = {
    if (rangeColumnIndices.nonEmpty)
      new RangePartition(rangeColumnIndices, rangeMin, rangeMax)
    else
      UnknownPartition()
  }

}

case class RangePartition(rangeColumnIndices: Seq[Int], rangeMin: Long, rangeMax: Long)
    extends PartitionInfo {

  override def merge(other: PartitionInfo): PartitionInfo = {
    UnknownPartition()
  }
}

case class SinglePartition() extends PartitionInfo {}

case class UnknownPartition() extends PartitionInfo {}
