package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners.RangeBasedShufflePartitioner
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RangeBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class RangeBasedShuffleSpec extends AnyFlatSpec with MockFactory {

  val fakeID1: ActorVirtualIdentity = ActorVirtualIdentity("rec1")
  val fakeID2: ActorVirtualIdentity = ActorVirtualIdentity("rec2")
  val fakeID3: ActorVirtualIdentity = ActorVirtualIdentity("rec3")
  val fakeID4: ActorVirtualIdentity = ActorVirtualIdentity("rec4")
  val fakeID5: ActorVirtualIdentity = ActorVirtualIdentity("rec5")

  val partitioning: RangeBasedShufflePartitioning =
    RangeBasedShufflePartitioning(
      400,
      List(fakeID1, fakeID2, fakeID3, fakeID4, fakeID5),
      Seq(0),
      -400,
      600
    )
  val attr: Attribute = new Attribute("Attr1", AttributeType.INTEGER)
  val schema: Schema = Schema.newBuilder().add(attr).build()

  val partitioner: RangeBasedShufflePartitioner = RangeBasedShufflePartitioner(partitioning)

  "RangeBasedShuffleSpec" should "return 0 when value is less than rangeMin" in {
    val tuple = Tuple.newBuilder(schema).add(attr, -600).build()
    val idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 0)
  }

  "RangeBasedShuffleSpec" should "return last receiver when value is more than rangeMax" in {
    val tuple = Tuple.newBuilder(schema).add(attr, 800).build()
    val idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 4)
  }

  "RangeBasedShuffleSpec" should "find index correctly" in {
    var tuple = Tuple.newBuilder(schema).add(attr, -400).build()
    var idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 0)

    tuple = Tuple.newBuilder(schema).add(attr, -200).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 0)

    tuple = Tuple.newBuilder(schema).add(attr, -199).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 1)
  }

  "RangeBasedShuffleSpec" should "handle different data types correctly" in {
    var tuple = Tuple.newBuilder(schema).add(attr, -90).build()
    var idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 1)

    val doubleAttr: Attribute = new Attribute("Attr2", AttributeType.DOUBLE)
    val doubleSchema: Schema = Schema.newBuilder().add(doubleAttr).build()
    tuple = Tuple.newBuilder(doubleSchema).add(doubleAttr, -90.5).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 1)

    val longAttr: Attribute = new Attribute("Attr3", AttributeType.LONG)
    val longSchema: Schema = Schema.newBuilder().add(longAttr).build()
    tuple = Tuple.newBuilder(longSchema).add(longAttr, -90L).build()
    idx = partitioner.getBucketIndex(tuple)
    assert(idx.next() == 1)
  }

}
