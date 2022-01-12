package edu.uci.ics.texera.workflow.common.storage.mongo

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

//TODO: All MongoDB testcases are commented,
// because currently we do not have MongoDB in our CICD env.
// We'll come back to this unit test in the future.
class MongoOpResultStorageSpec extends AnyFlatSpec with BeforeAndAfter {

  /*
  var opResultStorage: OpResultStorage = _

  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)

  before {
    opResultStorage = new MongoOpResultStorage()
  }

  it should "read nothing from mongoDB" in {
    val res = opResultStorage.get("should return nothing")
    assert(0 == res.length)
  }

  it should "put and remove tuples without exceptions" in {

    val inputSchema =
      Schema.newBuilder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()
    val inputTuple1 = Tuple
      .newBuilder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr1")
      .add(boolAttribute, true)
      .build()
    val inputTuple2 = Tuple
      .newBuilder(inputSchema)
      .add(integerAttribute, 2)
      .add(stringAttribute, "string-attr2")
      .add(boolAttribute, false)
      .build()
    opResultStorage.put("put and remove", List[Tuple](inputTuple1, inputTuple2))
    opResultStorage.remove("put and remove")
  }

  it should "put, get and remove tuples without exceptions" in {
    val keyStr = "put, retrieve and remove"
    val inputSchema =
      Schema.newBuilder().add(stringAttribute).add(integerAttribute).add(boolAttribute).build()
    val inputTuple1 = Tuple
      .newBuilder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr1")
      .add(boolAttribute, true)
      .build()
    val inputTuple2 = Tuple
      .newBuilder(inputSchema)
      .add(integerAttribute, 2)
      .add(stringAttribute, "string-attr2")
      .add(boolAttribute, false)
      .build()
    opResultStorage.put(keyStr, List[Tuple](inputTuple1, inputTuple2))
    val tuples = opResultStorage.get(keyStr)
    assert(1 == tuples.head.getField[Int]("col-int"))
    assert("string-attr1".equals(tuples.head.getField[String]("col-string")))
    assert(tuples.head.getField[Boolean]("col-bool"))
    assert(2 == tuples(1).getField[Int]("col-int"))
    assert("string-attr2".equals(tuples(1).getField[String]("col-string")))
    assert(!tuples(1).getField[Boolean]("col-bool"))
    opResultStorage.remove(keyStr)
  }
   */

}
