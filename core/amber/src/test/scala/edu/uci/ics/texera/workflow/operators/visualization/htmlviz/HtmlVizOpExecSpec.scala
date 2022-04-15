package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class HtmlVizOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("field1", AttributeType.STRING),
    new Attribute("field2", AttributeType.STRING)
  )
  val desc: HtmlVizOpDesc = new HtmlVizOpDesc()

  val outputSchema: Schema = desc.getOutputSchema(Array(schema))
  val operatorSchemaInfo: OperatorSchemaInfo =
    OperatorSchemaInfo(Array(schema), Array(outputSchema))

  def tuple(): Tuple =
    Tuple
      .newBuilder(schema)
      .addSequentially(Array("hello", "<html></html>"))
      .build()

  it should "process a target field" in {
    val htmlVizOpExec = new HtmlVizOpExec("field1", operatorSchemaInfo)
    htmlVizOpExec.open()
    val processedTuple: Tuple = htmlVizOpExec.processTexeraTuple(Left(tuple()), null).next()

    assert(processedTuple.getField("html-content").asInstanceOf[String] == "hello")

  }

  it should "process another target field" in {

    val htmlVizOpExec = new HtmlVizOpExec("field2", operatorSchemaInfo)
    htmlVizOpExec.open()
    val processedTuple: Tuple = htmlVizOpExec.processTexeraTuple(Left(tuple()), null).next()

    assert(processedTuple.getField("html-content").asInstanceOf[String] == "<html></html>")

  }
}
