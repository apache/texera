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
  val dictinaryScan = "nice a a person"
  val dictinarySubstring = "nice a a person and good"
  val dictionaryConjunction = "a person is nice"

  before {
    opDesc = new DictionaryMatcherOpDesc()
    opDesc.attribute = "field1"
    opDesc.dictionary = dictinaryScan
    opDesc.resultAttribute = "matched"
    opDesc.matchingType = MatchingType.SCANBASED
    val outputSchema: Schema = opDesc.getOutputSchema(Array(tupleSchema))
    val operatorSchemaInfo: OperatorSchemaInfo =
      OperatorSchemaInfo(Array(tupleSchema), Array(outputSchema))
    opExec = new DictionaryMatcherOpExec(opDesc, operatorSchemaInfo)
  }

  it should "open" in {
    opExec.open()
    assert(opExec.dictionaryEntries != null)
  }

  /**
    * Test cases that all Matching Types should match the query
    */
  it should "match a tuple if present in the given dictionary entry when matching type is SCANBASED" in {
    opDesc.matchingType = MatchingType.SCANBASED
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is SUBSTRING" in {
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED" in {
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  /**
    * Test cases that SCANBASED and SUBSTRING Matching Types should fail to match a query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED and not exact match" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.SCANBASED
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(!processedTuple.getField("matched").asInstanceOf[Boolean])
    opExec.close()
  }

  it should "not match a tuple if the given dictionary entry doesn't contain all the tuple when the matching type is SUBSTRING" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(!processedTuple.getField("matched").asInstanceOf[Boolean])
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED even with different order" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(processedTuple.getField("matched"))
    opExec.close()
  }

  /**
    * Test cases that only SUBSTRING Matching Type should match the query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED when the entry contains more text" in {
    opDesc.dictionary = dictinarySubstring
    opDesc.matchingType = MatchingType.SCANBASED
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(!processedTuple.getField("matched").asInstanceOf[Boolean])
    opExec.close()
  }

  it should "not match a tuple if not present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED when the entry contains more text" in {
    opDesc.dictionary = dictinarySubstring
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
    assert(!processedTuple.getField("matched").asInstanceOf[Boolean])
    opExec.close()
  }

  it should "match a tuple if not present in the given dictionary entry when matching type is SUBSTRING when the entry contains more text" in {
    opDesc.dictionary = dictinarySubstring
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec.open()
    val processedTuple = opExec.processTexeraTuple(Left(tuple), 0, null, null).next()
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
