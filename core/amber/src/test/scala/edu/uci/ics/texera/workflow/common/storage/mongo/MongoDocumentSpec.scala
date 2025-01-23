package edu.uci.ics.texera.workflow.common.storage.mongo

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

//TODO: All MongoDB testcases are commented,
// because currently we do not have MongoDB in our CICD env.
// We'll come back to this unit test in the future.
class MongoDocumentSpec extends AnyFlatSpec with BeforeAndAfter {
  /*
  var mongoDocumentForTuple: MongoDocument[Tuple] = _

  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)

  val inputSchema: Schema =
    Schema.builder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()

  val id: String = "testID"

  before {
    val tupleFromDocument: Document => Tuple = fromDocument(inputSchema)
    mongoDocumentForTuple = new MongoDocument[Tuple](id, Tuple.toDocument, tupleFromDocument)
  }

  after {
    mongoDocumentForTuple.remove()
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

    val writer: BufferedItemWriter[Tuple] = mongoDocumentForTuple.write()
    writer.open()
    writer.putOne(inputTuple1)
    writer.putOne(inputTuple2)
    writer.close()

    // now verify by reading them out
    val tuples = mongoDocumentForTuple.get().to(Iterable)

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

    val writer: BufferedItemWriter[Tuple] = mongoDocumentForTuple.write()
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

    assert(mongoDocumentForTuple.getCount == tuplesEndIdx - tuplesStartIdx)
    assert(
      mongoDocumentForTuple.get().toList == tupleBuffer.toList.slice(tuplesStartIdx, tuplesEndIdx)
    )
    assert(
      mongoDocumentForTuple.getAfter(tupleIndexOffset).toList == tupleBuffer.toList
        .slice(tuplesStartIdx + tupleIndexOffset, tuplesEndIdx)
    )
    assert(
      mongoDocumentForTuple
        .getRange(tupleIndexOffset, tuplesEndIdx - tuplesStartIdx)
        .toList == tupleBuffer.toList.slice(tuplesStartIdx + tupleIndexOffset, tuplesEndIdx)
    )
  }
   */
}
