package edu.uci.ics.amber.operator.keywordSearch

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class KeywordSearchOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val inputPort: Int = 0
  val opDesc: KeywordSearchOpDesc = new KeywordSearchOpDesc()

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
    opDesc.attribute = "text"
    opDesc.keyword = "3"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "3 stars")
    opExec.close()
  }

  it should "find exact phrase match" in {
    opDesc.attribute = "text"
    opDesc.keyword = "\"3 stars\""
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "3 stars")
    opExec.close()
  }

  it should "find all occurrences of Trump" in {
    opDesc.attribute = "text"
    opDesc.keyword = "Trump"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 2)
    assert(results.map(_.getField[String]("text")).toSet == Set("Trump Biden", "Trump"))
    opExec.close()
  }

  it should "find all occurrences of Biden" in {
    opDesc.attribute = "text"
    opDesc.keyword = "Biden"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "Trump Biden")
    opExec.close()
  }

  it should "find records containing both Trump AND Biden" in {
    opDesc.attribute = "text"
    opDesc.keyword = "Trump AND Biden"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).nonEmpty)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "Trump Biden")
    opExec.close()
  }

  it should "find no matches for exact phrase 'Trump AND Biden'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "\"Trump AND Biden\""
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }

  it should "find no matches for partial word 'ell'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "ell"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }

  it should "find exact match for word 'the'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "the"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "the name")
    opExec.close()
  }

  it should "find exact match for word 'an'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "an"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "an eye")
    opExec.close()
  }

  it should "find exact match for word 'to'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "to"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "to you")
    opExec.close()
  }

  it should "find case-insensitive match for 'twitter'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "twitter"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "Twitter")
    opExec.close()
  }

  it should "find exact match for Korean text '안녕하세요'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "안녕하세요"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "안녕하세요")
    opExec.close()
  }

  it should "find exact match for Chinese text '你好'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "你好"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.length == 1)
    assert(results.head.getField[String]("text") == "你好")
    opExec.close()
  }

  it should "find no matches for special character '@'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "@"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }

  it should "find exact match for special characters '_!@,-'" in {
    opDesc.attribute = "text"
    opDesc.keyword = "_!@,-"
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val results = testData.filter(t => opExec.processTuple(t, inputPort).hasNext)
    assert(results.isEmpty)
    opExec.close()
  }
}
