package edu.uci.ics.texera.workflow.common.workflow

sealed trait PartitionInfo {}

case class HashPartition(hashColumnIndices: Seq[Int]) extends PartitionInfo

case class RangePartition(rangeColumnIndices: Seq[Int], rangeMin: Long, rangeMax: Long)
    extends PartitionInfo

case class Singleton() extends PartitionInfo

case class RoundRobin() extends PartitionInfo

case class Any() extends PartitionInfo
