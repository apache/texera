package edu.uci.ics.texera.workflow.operators.dummy.source

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

class DummySourceOpExec(val i: Int, val delay: Int) extends SourceOperatorExecutor {
  private val schema: Schema = new Schema(new Attribute("line", AttributeType.INTEGER), new Attribute("data", AttributeType.INTEGER))
  override def produceTexeraTuple(): Iterator[Tuple] = {
    (1 to i).iterator.map(i => {
      Thread.sleep(delay)
      val tuple = Tuple.newBuilder(schema)
      tuple.add(schema.getAttribute("line"), i)
      tuple.add(schema.getAttribute("data"), i)
      tuple.build()
    })
  }

  override def open(): Unit = {}
  override def close(): Unit = {}
}
