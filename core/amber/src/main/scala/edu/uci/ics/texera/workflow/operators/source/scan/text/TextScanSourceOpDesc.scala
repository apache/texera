package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{BufferedReader, FileReader}
import scala.collection.convert.ImplicitConversions.`iterator asScala`

class TextScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(required = true)
  @JsonPropertyDescription("scan text file into single output tuple")
  var outputAsSingleTuple: Boolean = false

  fileTypeName = Option("Text")

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

        // using only 1 worker for text scan to maintain proper ordering
        OpExecConfig.localLayer(
          operatorIdentifier,
          _ => {
            val startOffset: Int = offsetValue
            val endOffset: Int = offsetValue + count
            new TextScanSourceOpExec(this, startOffset, endOffset, outputAsSingleTuple)
          }
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }
  }

  @Override
  override def inferSchema(): Schema = {
    Schema.newBuilder().add(new Attribute("line", AttributeType.STRING)).build()
  }
}
