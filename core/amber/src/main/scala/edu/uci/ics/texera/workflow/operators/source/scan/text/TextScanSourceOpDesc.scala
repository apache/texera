package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{BufferedReader, FileReader}
import scala.collection.convert.ImplicitConversions.`iterator asScala`

class TextScanSourceOpDesc extends ScanSourceOpDesc{

  @Override
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    filePath match {
      case Some(path) =>
        // count lines and partition the task to each worker
        val reader = new BufferedReader(new FileReader(path))
        val offsetValue = offset.getOrElse(0)
        var lines = reader.lines().iterator().drop(offsetValue)
        if (limit.isDefined) lines = lines.take(limit.get)
        val count: Int = lines.map(_ => 1).sum
        reader.close()

        val numWorkers = Constants.currentWorkerNum

        OpExecConfig.localLayer(operatorIdentifier, p => {
          val i = p._1
          val startOffset: Int = offsetValue + count / numWorkers * i
          val endOffset: Int =
            offsetValue + (if (i != numWorkers - 1) count / numWorkers * (i + 1) else count)
          new TextScanSourceOpExec(this, startOffset, endOffset)
        })
      case None =>
        throw new RuntimeException("File path is not provided.")
    }
  }

  @Override
  override def inferSchema(): Schema = {
    Schema.newBuilder().add(new Attribute("line", AttributeType.STRING)).build()
  }
}
