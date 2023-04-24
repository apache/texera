package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners.RangeBasedShufflePartitioner
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RangeBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class RangeBasedShuffleSpec extends AnyFlatSpec with MockFactory {

  val fakeID1 = ActorVirtualIdentity("rec1")
  val fakeID2 = ActorVirtualIdentity("rec2")
  val fakeID3 = ActorVirtualIdentity("rec3")
  val fakeID4 = ActorVirtualIdentity("rec4")
  val fakeID5 = ActorVirtualIdentity("rec5")

  val partitioning: RangeBasedShufflePartitioning =
    RangeBasedShufflePartitioning(
      400,
      List(fakeID1, fakeID2, fakeID3, fakeID4, fakeID5),
      Array(0),
      -400,
      600
    )

  val partitioner = RangeBasedShufflePartitioner(partitioning)

  "RangeBasedShuffleSpec" should "return 0 when value is less than rangeMin" in {
    val tuple = Tuple.newBuilder().add("Attr1", AttributeType.INTEGER, -600).build()
    val idx = partitioner.getBucketIndex(tuple)
    assert(idx == 0)
  }

  "RangeBasedShuffleSpec" should "return last receiver when value is more than rangeMax" in {
    val tuple = Tuple.newBuilder().add("Attr1", AttributeType.INTEGER, 800).build()
    val idx = partitioner.getBucketIndex(tuple)
    assert(idx == 4)
  }

  "RangeBasedShuffleSpec" should "find index correctly" in {
    var tuple = Tuple.newBuilder().add("Attr1", AttributeType.INTEGER, -400).build()
    var idx = partitioner.getBucketIndex(tuple)
    assert(idx == 0)

    tuple = Tuple.newBuilder().add("Attr1", AttributeType.INTEGER, -200).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx == 0)

    tuple = Tuple.newBuilder().add("Attr1", AttributeType.INTEGER, -199).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx == 1)
  }

  "RangeBasedShuffleSpec" should "handle different data types correctly" in {
    var tuple = Tuple.newBuilder().add("Attr1", AttributeType.INTEGER, -90).build()
    var idx = partitioner.getBucketIndex(tuple)
    assert(idx == 1)

    tuple = Tuple.newBuilder().add("Attr1", AttributeType.DOUBLE, -90.5).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx == 1)

    tuple = Tuple.newBuilder().add("Attr1", AttributeType.LONG, -90L).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx == 1)
  }

}
