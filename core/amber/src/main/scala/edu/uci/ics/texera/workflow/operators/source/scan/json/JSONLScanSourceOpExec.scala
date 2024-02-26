package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.source.scan.FileDecodingMethod
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import scala.jdk.CollectionConverters.{IterableHasAsScala, IteratorHasAsScala}

class JSONLScanSourceOpExec private[json] (
    filePath: String,
    fileEncoding: FileDecodingMethod,
    startOffset: Int,
    endOffset: Int,
    flatten: Boolean,
    schemaFunc: () => Schema
) extends ISourceOperatorExecutor {
  private var schema: Schema = _
  private var rows: Iterator[String] = _
  private var reader: BufferedReader = _

  override def produceTuple(): Iterator[TupleLike] = {
    rows
      .map(line => {
        try {
          val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
          val data = JSONToMap(objectMapper.readTree(line), flatten)

          for (fieldName <- schema.getAttributeNames.asScala) {
            if (data.contains(fieldName))
              fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
            else {
              fields += null
            }
          }
          TupleLike(fields.toSeq: _*)
        } catch {
          case _: Throwable => null
        }
      })
      .filter(tuple => tuple != null)

  }
  override def open(): Unit = {
    schema = schemaFunc()
    reader = new BufferedReader(
      new InputStreamReader(new FileInputStream(filePath), fileEncoding.getCharset)
    )
    rows = reader.lines().iterator().asScala.slice(startOffset, endOffset)
  }

  override def close(): Unit = reader.close()

}
