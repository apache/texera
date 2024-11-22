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
    createTuple("3 stars"),
    createTuple("4 stars"),
    createTuple("Trump"),
    createTuple("Trump Biden"),
    createTuple("hello"),
    createTuple("the name"),
    createTuple("an eye"),
    createTuple("to you"),
    createTuple("Twitter"),
    createTuple("안녕하세요"),
    createTuple("你好"),
    createTuple("_!@,-")
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

  it should "find no matches for partial word 'ell'" in {
    val opExec = new KeywordSearchOpExec("text", "ell")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }

  it should "find exact match for word 'the'" in {
    val opExec = new KeywordSearchOpExec("text", "the")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "the name")
    opExec.close()
  }

  it should "find exact match for word 'an'" in {
    val opExec = new KeywordSearchOpExec("text", "an")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "an eye")
    opExec.close()
  }

  it should "find exact match for word 'to'" in {
    val opExec = new KeywordSearchOpExec("text", "to")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "to you")
    opExec.close()
  }

  it should "find case-insensitive match for 'twitter'" in {
    val opExec = new KeywordSearchOpExec("text", "twitter")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "Twitter")
    opExec.close()
  }

  it should "find exact match for Korean text '안녕하세요'" in {
    val opExec = new KeywordSearchOpExec("text", "안녕하세요")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "안녕하세요")
    opExec.close()
  }

  it should "find exact match for Chinese text '你好'" in {
    val opExec = new KeywordSearchOpExec("text", "你好")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "你好")
    opExec.close()
  }

  it should "find no matches for special character '@'" in {
    val opExec = new KeywordSearchOpExec("text", "@")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }

  it should "find exact match for special characters '_!@,-'" in {
    val opExec = new KeywordSearchOpExec("text", "_!@,-")
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }
}
