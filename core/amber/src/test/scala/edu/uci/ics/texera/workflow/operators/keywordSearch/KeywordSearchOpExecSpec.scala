package edu.uci.ics.texera.workflow.operators.keywordSearch

import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class KeywordSearchOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val inputPort: Int = 0

  val schema: Schema = Schema
    .builder()
    .add(new Attribute("text", AttributeType.STRING))
    .build()

  def createTuple(text: String): Tuple = {
    Tuple
      .builder(schema)
      .add(new Attribute("text", AttributeType.STRING), text)
      .build()
  }

  val testData: List[Tuple] = List(
    createTuple("Trump Biden"),
    createTuple("Trump"),
    createTuple("3 stars"),
    createTuple("4 stars")
  )

  it should "find exact match with single number" in {
    val opExec = new KeywordSearchOpExec("text", "3")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "3 stars")
    opExec.close()
  }

  it should "find exact phrase match" in {
    val opExec = new KeywordSearchOpExec("text", "\"3 stars\"")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "3 stars")
    opExec.close()
  }

  it should "find all occurrences of Trump" in {
    val opExec = new KeywordSearchOpExec("text", "Trump")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 2)
    assert(results.map(_.getField[String]("text")).toSet == Set("Trump Biden", "Trump"))
    opExec.close()
  }

  it should "find all occurrences of Biden" in {
    val opExec = new KeywordSearchOpExec("text", "Biden")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "Trump Biden")
    opExec.close()
  }

  it should "find records containing both Trump AND Biden" in {
    val opExec = new KeywordSearchOpExec("text", "Trump AND Biden")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "Trump Biden")
    opExec.close()
  }

  it should "find no matches for exact phrase 'Trump AND Biden'" in {
    val opExec = new KeywordSearchOpExec("text", "\"Trump AND Biden\"")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }
}
