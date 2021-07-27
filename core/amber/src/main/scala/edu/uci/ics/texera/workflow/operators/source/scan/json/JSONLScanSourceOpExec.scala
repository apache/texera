package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap

import java.io.{BufferedReader, FileReader}
import scala.collection.Iterator
import scala.collection.JavaConverters._

class JSONLScanSourceOpExec private[json] (
    val desc: JSONLScanSourceOpDesc,
    val startOffset: Int,
    val endOffset: Int
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var rows: Iterator[String] = _
  private var reader: BufferedReader = _

  override def produceTexeraTuple(): Iterator[Tuple] = {
    rows
      .map(line => {
        try {
          val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
          val data = JSONToMap(objectMapper.readTree(line), flatten = desc.flatten)

          for (fieldName <- schema.getAttributeNames.asScala) {
            if (data.contains(fieldName))
              fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
            else {
              fields += null
            }
          }

          Tuple.newBuilder(schema).addSequentially(fields.toArray).build
        } catch {
          case _: Throwable => null
        }
      })
      .filter(tuple => tuple != null)

  }
  override def open(): Unit = {
    schema = desc.inferSchema()
    reader = new BufferedReader(new FileReader(desc.filePath.get))
    rows = reader.lines().iterator().asScala.slice(startOffset, endOffset)
  }

  override def close(): Unit = reader.close()

}
