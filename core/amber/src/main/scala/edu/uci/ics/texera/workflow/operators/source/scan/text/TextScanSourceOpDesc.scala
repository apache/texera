package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{BufferedReader, FileReader}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/* ignoring inherited limit and offset properties because they are not hideable
 *   new, identical limit and offset fields with additinoal annotations to make hideable
 *   are created and used from the TextSourceOpDesc trait
 *   TODO: to be considered in potential future refactor*/
@JsonIgnoreProperties(value = Array("limit", "offset"))
class TextScanSourceOpDesc extends ScanSourceOpDesc with TextSourceOpDesc {

  fileTypeName = Option("Text")

  @Override
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    filePath match {
      case Some(path) =>
        // get offset and max line values
        val reader = new BufferedReader(new FileReader(path))
        val offsetValue = offsetHideable.getOrElse(0)
        val count: Int = countNumLines(reader.lines().iterator().asScala, offsetValue)
        reader.close()

        // using only 1 worker for text scan to maintain proper ordering
        OpExecConfig.localLayer(
          operatorIdentifier,
          _ => {
            val startOffset: Int = offsetValue
            val endOffset: Int = offsetValue + count
            new TextScanSourceOpExec(this, startOffset, endOffset)
          }
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }
  }

  @Override
  override def inferSchema(): Schema = {
    Schema
      .newBuilder()
      .add(new Attribute(if (outputAsSingleTuple) "file" else "line", AttributeType.STRING))
      .build()
  }
}
