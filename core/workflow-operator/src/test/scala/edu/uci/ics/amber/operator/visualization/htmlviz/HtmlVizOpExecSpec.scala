package edu.uci.ics.amber.operator.visualization.htmlviz

import edu.uci.ics.amber.core.tuple._
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class HtmlVizOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("field1", AttributeType.STRING),
    new Attribute("field2", AttributeType.STRING)
  )
  val opDesc: HtmlVizOpDesc = new HtmlVizOpDesc()

  val outputSchema: Schema =
    opDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head

  def tuple(): Tuple =
    Tuple
      .builder(schema)
      .addSequentially(Array("hello", "<html></html>"))
      .build()

  it should "process a target field" in {
    opDesc.htmlContentAttrName = "field1"
    val htmlVizOpExec = new HtmlVizOpExec(objectMapper.writeValueAsString(opDesc))
    htmlVizOpExec.open()
    val processedTuple: Tuple =
      htmlVizOpExec
        .processTuple(tuple(), 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)

    assert(processedTuple.getField("html-content").asInstanceOf[String] == "hello")

  }

  it should "process another target field" in {
    opDesc.htmlContentAttrName = "field2"
    val htmlVizOpExec = new HtmlVizOpExec(objectMapper.writeValueAsString(opDesc))
    htmlVizOpExec.open()
    val processedTuple: Tuple =
      htmlVizOpExec
        .processTuple(tuple(), 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)

    assert(processedTuple.getField("html-content").asInstanceOf[String] == "<html></html>")

  }
}
