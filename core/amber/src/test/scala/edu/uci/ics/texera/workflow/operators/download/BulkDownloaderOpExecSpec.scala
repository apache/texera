package edu.uci.ics.texera.workflow.operators.download

import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import org.jooq.types.UInteger
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class BulkDownloaderOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .newBuilder()
    .add(new Attribute("url", AttributeType.STRING))
    .build()

  val resultSchema: Schema = Schema
    .newBuilder()
    .add(new Attribute("url", AttributeType.STRING))
    .add(new Attribute("url result", AttributeType.STRING))
    .build()

  val tuple: () => Tuple = () =>
    Tuple
      .newBuilder(tupleSchema)
      .add(new Attribute("url", AttributeType.STRING), "http://www.google.com")
      .build()

  val tuple2: () => Tuple = () =>
    Tuple
      .newBuilder(tupleSchema)
      .add(new Attribute("url", AttributeType.STRING), "https://www.google.com")
      .build()

  var opExec: BulkDownloaderOpExec = _
  before {
    opExec = new BulkDownloaderOpExec(
      new WorkflowContext(Some(UInteger.valueOf(1)), UInteger.valueOf(1), 1),
      "url",
      "url result",
      OperatorSchemaInfo(Array(tupleSchema), Array(resultSchema))
    )
  }

  it should "open" in {
    opExec.open()
  }

}
