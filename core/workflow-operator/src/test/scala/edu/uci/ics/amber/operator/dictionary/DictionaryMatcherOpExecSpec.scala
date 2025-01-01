package edu.uci.ics.amber.operator.dictionary

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, SchemaEnforceable, Tuple}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class DictionaryMatcherOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "nice a a person")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()

  var opExec: DictionaryMatcherOpExec = _
  val opDesc: DictionaryMatcherOpDesc = new DictionaryMatcherOpDesc()
  var outputSchema: Schema = _
  val dictionaryScan = "nice a a person"
  val dictionarySubstring = "nice a a person and good"
  val dictionaryConjunction = "a person is nice"

  before {
    opDesc.attribute = "field1"
    opDesc.dictionary = dictionaryScan
    opDesc.resultAttribute = "matched"
    opDesc.matchingType = MatchingType.SCANBASED
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
  }

  it should "open" in {
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    assert(opExec.dictionaryEntries != null)
  }

  /**
    * Test cases that all Matching Types should match the query
    */
  it should "match a tuple if present in the given dictionary entry when matching type is SCANBASED" in {
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema).getField("matched")
    )
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is SUBSTRING" in {
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema).getField("matched")
    )
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED" in {
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema).getField("matched")
    )
    opExec.close()
  }

  /**
    * Test cases that SCANBASED and SUBSTRING Matching Types should fail to match a query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED and not exact match" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "not match a tuple if the given dictionary entry doesn't contain all the tuple when the matching type is SUBSTRING" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "match a tuple if present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED even with different order" in {
    opDesc.dictionary = dictionaryConjunction
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  /**
    * Test cases that only SUBSTRING Matching Type should match the query
    */
  it should "not match a tuple if not present in the given dictionary entry when matching type is SCANBASED when the entry contains more text" in {
    opDesc.dictionary = dictionarySubstring
    opDesc.matchingType = MatchingType.SCANBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "not match a tuple if not present in the given dictionary entry when matching type is CONJUNCTION_INDEXBASED when the entry contains more text" in {
    opDesc.dictionary = dictionarySubstring
    opDesc.matchingType = MatchingType.CONJUNCTION_INDEXBASED
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      !processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "match a tuple if not present in the given dictionary entry when matching type is SUBSTRING when the entry contains more text" in {
    opDesc.dictionary = dictionarySubstring
    opDesc.matchingType = MatchingType.SUBSTRING
    opExec = new DictionaryMatcherOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    val processedTuple = opExec.processTuple(tuple, 0).next()
    assert(
      processedTuple
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
        .getField[Boolean]("matched")
    )
    opExec.close()
  }

  it should "close properly" in {
    opExec.close()
    assert(opExec.dictionaryEntries == null)
    assert(opExec.tokenizedDictionaryEntries == null)
    assert(opExec.luceneAnalyzer == null)
  }
}
