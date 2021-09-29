package edu.uci.ics.texera.workflow.operators.dictionary

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class DictionaryMatcherOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .newBuilder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))
    .build()

  val tuple: Tuple = Tuple
    .newBuilder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "nice a a person")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()

  var opExec: DictionaryMatcherOpExec = _
  var opDesc: DictionaryMatcherOpDesc = _
  val dictionaryEntry = "nice a a person"
  val dictionaryEntryStopWord = "nice and beautiful person"
  val dictionaryEntryOrder = "beautiful person and nice"

  before {
    opDesc = new DictionaryMatcherOpDesc()
    opDesc.attribute = "field1"
    opDesc.dictionary = dictionaryEntry
    opDesc.resultAttribute = "matched"
    val outputSchema: Schema = opDesc.getOutputSchema(Array(tupleSchema))
    val operatorSchemaInfo: OperatorSchemaInfo =
      OperatorSchemaInfo(Array(tupleSchema), outputSchema)
    opExec = new DictionaryMatcherOpExec(opDesc, operatorSchemaInfo)
  }

  it should "open" in {
    opExec.open()
    assert(opExec.dictionaryEntries != null)
    assert(opExec.tokenizedDictionaryEntries != null)
    assert(opExec.luceneAnalyzer != null)
  }

  /**
    * Test cases that all Matching Type should match the query
    */
  it should "match a tuple if present in the given dictionary entry when matching type is SCANBASED" in {
    opExec.open()
    opDesc.matchingType = MatchingType.SCANBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED" in {
    opExec.open()
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is PHRASE_INDEXBASED" in {
    opExec.open()
    opDesc.matchingType = MatchingType.PHRASE_INDEXBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  /**
    * Test cases that SCANBASED Matching Type should fail to match a query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED" in {
    opDesc.dictionary = dictionaryEntryStopWord
    opExec.open()
    opDesc.matchingType = MatchingType.SCANBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(!processedTuple.getField("matched").asInstanceOf[Boolean])
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED even if there are stop words" in {
    opDesc.dictionary = dictionaryEntryStopWord
    opExec.open()
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is PHRASE_INDEXBASED even if there are stop words" in {
    opDesc.dictionary = dictionaryEntryStopWord
    opExec.open()
    opDesc.matchingType = MatchingType.PHRASE_INDEXBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  /**
    * Test cases that only CONJUNCTION_INDEXBASED Matching Type should match the query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED when the order is different" in {
    opDesc.dictionary = dictionaryEntryOrder
    opExec.open()
    opDesc.matchingType = MatchingType.SCANBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(!processedTuple.getField("matched").asInstanceOf[Boolean])
    opExec.close()
  }

  it should "not match a tuple if not present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED when the order is different" in {
    opDesc.dictionary = dictionaryEntryOrder
    opExec.open()
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is PHRASE_INDEXBASED even when the order is different" in {
    opDesc.dictionary = dictionaryEntryOrder
    opExec.open()
    opDesc.matchingType = MatchingType.PHRASE_INDEXBASED
    val processedTuple = opExec.processTexeraTuple(Left(tuple), null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  it should "close properly" in {
    opExec.close()
    assert(opExec.dictionaryEntries == null)
    assert(opExec.tokenizedDictionaryEntries == null)
    assert(opExec.luceneAnalyzer == null)
  }
}
