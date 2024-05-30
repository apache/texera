package edu.uci.ics.texera.workflow.common.storage.mongo

import edu.uci.ics.amber.engine.common.storage.BufferedItemWriter
import edu.uci.ics.amber.engine.common.storage.mongodb.MongoDocument
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple.fromDocument
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import org.bson.Document
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class MongoDocumentSpec extends AnyFlatSpec with BeforeAndAfter {

  var mongoDocumentForTuple: MongoDocument[Tuple] = _

  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)

  val inputSchema: Schema =
    Schema.builder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()

  val id: String = "testID"

  before {
    val tupleFromDocument: Document => Tuple = fromDocument(Seq(inputSchema))
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
    val tuples = mongoDocumentForTuple.getAll()

    // check if tuples have inputTuple1 and inputTuple2
    assert(tuples.toList.toSet == Set(inputTuple1, inputTuple2))
  }
}
