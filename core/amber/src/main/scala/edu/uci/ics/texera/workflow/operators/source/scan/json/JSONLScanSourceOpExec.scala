package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.source.scan.FileDecodingMethod
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

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
    rows.flatMap { line =>
      Try {
        val data = JSONToMap(objectMapper.readTree(line), flatten).withDefaultValue(null)
        val fields = schema.getAttributeNames.map { fieldName =>
          parseField(data(fieldName), schema.getAttribute(fieldName).getType)
        }
        TupleLike(fields: _*)
      } match {
        case Success(tuple) => Some(tuple)
        case Failure(_)     => None
      }
    }
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
