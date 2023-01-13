package edu.uci.ics.texera.workflow.common.workflow

/**
  * The base interface of partition information in the compiler layer.
  */
sealed trait PartitionInfo {

  // whether this partition satisfies the other partition
  // in the default implementation, a partition only satisfies itself,
  // a partition also always satisfy unknown partition (indicating no partition requirement)
  def satisfies(other: PartitionInfo): Boolean = {
    this == other || other == UnknownPartition()
  }

  // after a stream with this partition merges with another stream with the other partition
  // returns the the result partition after the merge
  def merge(other: PartitionInfo): PartitionInfo = {
    // if merge with the same partition, the result is the same
    // if merge with a different partition, the result is unknown
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

/**
  * Represents an input stream is partitioned on multiple nodes
  * according to a hash function on the specified column indices.
  */
case class HashPartition(hashColumnIndices: Seq[Int]) extends PartitionInfo

object RangePartition {

  def apply(rangeColumnIndices: Seq[Int], rangeMin: Long, rangeMax: Long): PartitionInfo = {
    if (rangeColumnIndices.nonEmpty)
      new RangePartition(rangeColumnIndices, rangeMin, rangeMax)
    else
      UnknownPartition()
  }

}

/**
  * Represents an input stream is partitioned on multiple nodes
  *  and each node contains data fit in a specific range.
  * The data within each node is also sorted.
  */
case class RangePartition(rangeColumnIndices: Seq[Int], rangeMin: Long, rangeMax: Long)
    extends PartitionInfo {

  // if two streams of input with the same range partition are merged (without another sort),
  // we cannot ensure that the output stream follow the same sorting order.
  override def merge(other: PartitionInfo): PartitionInfo = {
    UnknownPartition()
  }
}

/**
  * Represent the input stream is not partitioned and all data are on a single node.
  */
case class SinglePartition() extends PartitionInfo {}

/**
  * Represents there is no specific partitioning scheme of the input stream.
  */
case class UnknownPartition() extends PartitionInfo {}
