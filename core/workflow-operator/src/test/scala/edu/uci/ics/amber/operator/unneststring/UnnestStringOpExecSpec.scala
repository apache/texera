package edu.uci.ics.amber.operator.unneststring

import edu.uci.ics.amber.core.tuple._
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class UnnestStringOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .builder()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.STRING))
    .build()

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "a-b-c")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(new Attribute("field3", AttributeType.STRING), "a")
    .build()

  var opExec: UnnestStringOpExec = _
  var opDesc: UnnestStringOpDesc = _
  var outputSchema: Schema = _
  before {
    opDesc = new UnnestStringOpDesc()
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opDesc.resultAttribute = "split"
  }

  it should "open" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    opExec.open()
    assert(opExec.flatMapFunc != null)
  }

  it should "split value in the given attribute and output the split result in the result attribute, one for each tuple" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assert(processedTuple.next().getField("split").equals("b"))
    assert(processedTuple.next().getField("split").equals("c"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }

  it should "generate the correct tuple when there is no delimiter in the value" in {
    opDesc.attribute = "field3"
    opDesc.delimiter = "-"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }

  it should "only contain split results that are not null" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "/"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    val tuple: Tuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "//a//b/")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()

    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assert(processedTuple.next().getField("split").equals("b"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }

  it should "split by regex delimiter" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "<\\d*>"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    val tuple: Tuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "<>a<1>b<12>")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()

    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assert(processedTuple.next().getField("split").equals("b"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }
}
