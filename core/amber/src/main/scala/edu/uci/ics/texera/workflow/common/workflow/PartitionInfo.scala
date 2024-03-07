package edu.uci.ics.texera.workflow.common.workflow

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

/**
  * The base interface of partition information in the compiler layer.
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[HashPartition], name = "hash"),
    new Type(value = classOf[RangePartition], name = "range"),
    new Type(value = classOf[SinglePartition], name = "single"),
    new Type(value = classOf[BroadcastPartition], name = "broadcast"),
    new Type(value = classOf[UnknownPartition], name = "none")
  )
)
sealed abstract class PartitionInfo {

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

/**
  * Defines a partitioning strategy where an input stream is distributed across
  * multiple nodes based on a hash function applied to specified attribute names.
  * If the list of attribute names is empty, hashing is applied to all attributes.
  */
final case class HashPartition(hashAttributeNames: List[String] = List.empty) extends PartitionInfo
object RangePartition {

  def apply(rangeAttributeNames: List[String], rangeMin: Long, rangeMax: Long): PartitionInfo = {
    if (rangeAttributeNames.nonEmpty)
      new RangePartition(rangeAttributeNames, rangeMin, rangeMax)
    else
      UnknownPartition()
  }

}

/**
  * Represents an input stream is partitioned on multiple nodes
  * and each node contains data fit in a specific range.
  * The data within each node is also sorted.
  */
final case class RangePartition(rangeAttributeNames: List[String], rangeMin: Long, rangeMax: Long)
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
final case class SinglePartition() extends PartitionInfo {}

/**
  * Represents the input stream needs to send to every node
  */
final case class BroadcastPartition() extends PartitionInfo {}

/**
  * Represents there is no specific partitioning scheme of the input stream.
  */
final case class UnknownPartition() extends PartitionInfo {}
