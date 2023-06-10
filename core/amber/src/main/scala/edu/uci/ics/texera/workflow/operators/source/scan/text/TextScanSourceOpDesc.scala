package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/* ignoring inherited limit and offset properties because they are not hideable
 *   new, identical limit and offset fields with additional annotations to make hideable
 *   are created and used from the TextSourceOpDesc trait
 *   TODO: to be considered in potential future refactor*/
@JsonIgnoreProperties(value = Array("limit", "offset"))
class TextScanSourceOpDesc extends ScanSourceOpDesc with TextSourceOpDesc {

  fileTypeName = Option("Text")

  @Override
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    filePath match {
      case Some(path) =>
        // get offset and max line values, unused if in output single tuple mode (i.e. binary or string as single tuple)
        val offsetValue = offsetHideable.getOrElse(0)
        var count: Int = 1

        if (!attributeType.isOutputSingleTuple) {
          // count number of rows in input text file
          val reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(path), fileEncoding.getCharset)
          )
          count = countNumLines(reader.lines().iterator().asScala, offsetValue)
          reader.close()
        }

        // default attribute name
        val defaultAttributeName: String = if (attributeType.isOutputSingleTuple) "file" else "line"

        // using only 1 worker for text scan to maintain proper ordering
        OpExecConfig.localLayer(
          operatorIdentifier,
          _ => {
            val startOffset: Int = offsetValue
            val endOffset: Int = offsetValue + count
            new TextScanSourceOpExec(
              this,
              startOffset,
              endOffset,
              if (attributeName.isEmpty || attributeName.get.isEmpty) defaultAttributeName
              else attributeName.get
            )
          }
        )
      case None =>
        throw new RuntimeException("File path is not provided.")
    }
  }

  @Override
  override def inferSchema(): Schema = {
    val defaultAttributeName: String = if (attributeType.isOutputSingleTuple) "file" else "line"
    Schema
      .newBuilder()
      .add(
        new Attribute(
          if (attributeName.isEmpty || attributeName.get.isEmpty) defaultAttributeName
          else attributeName.get,
          attributeType.getType
        )
      )
      .build()
  }
}
