package edu.uci.ics.texera.workflow.common.storage

import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.engine.common.storage.BufferedItemWriter
import edu.uci.ics.amber.engine.common.storage.mongodb.MemoryDocument
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ListBuffer

class MemoryDocumentSpec extends AnyFlatSpec with BeforeAndAfter {
  var memoryDocumentForTuple: MemoryDocument[Tuple] = _

  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)

  val inputSchema: Schema =
    Schema.builder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()

  before {
    memoryDocumentForTuple = new MemoryDocument[Tuple]
  }

  after {
    memoryDocumentForTuple.remove()
  }

  it should "write the tuples successfully through writer" in {
    val inputTuple1 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr1")
      .add(boolAttribute, true)
      .build()
    val inputTuple2 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 2)
      .add(stringAttribute, "string-attr2")
      .add(boolAttribute, false)
      .build()

    val writer: BufferedItemWriter[Tuple] = memoryDocumentForTuple.write()
    writer.open()
    writer.putOne(inputTuple1)
    writer.putOne(inputTuple2)
    writer.close()

    // now verify by reading them out
    val tuples = memoryDocumentForTuple.get()

    // check if tuples have inputTuple1 and inputTuple2
    assert(tuples.toList == List(inputTuple1, inputTuple2))
  }

  it should "fetch the correct tuples with order" in {
    val tupleBuffer: ListBuffer[Tuple] = ListBuffer()
    val numOfTuples = 1000
    val tuplesStartIdx = 200
    val tuplesEndIdx = 500
    val tupleIndexOffset = 25

    for (i <- 0 until numOfTuples) {
      tupleBuffer += Tuple
        .builder(inputSchema)
        .add(integerAttribute, i)
        .add(stringAttribute, f"string-attr$i")
        .add(boolAttribute, i % 2 == 0)
        .build()
    }

    val writer: BufferedItemWriter[Tuple] = memoryDocumentForTuple.write()
    writer.open()
    for (i <- 0 until numOfTuples) {
      writer.putOne(tupleBuffer.apply(i))
    }

    for (i <- 0 until tuplesStartIdx) {
      writer.removeOne(tupleBuffer.apply(i))
    }

    for (i <- tuplesEndIdx until numOfTuples) {
      writer.removeOne(tupleBuffer.apply(i))
    }

    writer.close()

    assert(memoryDocumentForTuple.getCount == tuplesEndIdx - tuplesStartIdx)
    assert(
      memoryDocumentForTuple.get().toList == tupleBuffer.toList.slice(tuplesStartIdx, tuplesEndIdx)
    )
    assert(
      memoryDocumentForTuple.getAfter(tupleIndexOffset).toList == tupleBuffer.toList
        .slice(tuplesStartIdx + tupleIndexOffset, tuplesEndIdx)
    )
    assert(
      memoryDocumentForTuple
        .getRange(tupleIndexOffset, tuplesEndIdx - tuplesStartIdx)
        .toList == tupleBuffer.toList.slice(tuplesStartIdx + tupleIndexOffset, tuplesEndIdx)
    )
  }
}
