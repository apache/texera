package edu.uci.ics.texera.workflow.operators.download

import edu.uci.ics.amber.engine.common.model.WorkflowContext
import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, AttributeType, Schema, Tuple}
import WorkflowContext.{DEFAULT_EXECUTION_ID, DEFAULT_WORKFLOW_ID}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class BulkDownloaderOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema
    .builder()
    .add(new Attribute("url", AttributeType.STRING))
    .build()

  val resultSchema: Schema = Schema
    .builder()
    .add(new Attribute("url", AttributeType.STRING))
    .add(new Attribute("url result", AttributeType.STRING))
    .build()

  val tuple: () => Tuple = () =>
    Tuple
      .builder(tupleSchema)
      .add(new Attribute("url", AttributeType.STRING), "http://www.google.com")
      .build()

  val tuple2: () => Tuple = () =>
    Tuple
      .builder(tupleSchema)
      .add(new Attribute("url", AttributeType.STRING), "https://www.google.com")
      .build()

  var opExec: BulkDownloaderOpExec = _
  before {
    opExec = new BulkDownloaderOpExec(
      new WorkflowContext(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID),
      urlAttribute = "url"
    )
  }

  it should "open" in {
    opExec.open()
  }

}
