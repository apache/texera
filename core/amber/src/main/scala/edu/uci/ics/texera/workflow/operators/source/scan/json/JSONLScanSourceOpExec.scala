package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap

import java.io.{BufferedReader, FileReader}
import scala.collection.Iterator
import scala.collection.JavaConverters._
class JSONLScanSourceOpExec private[json] (
    val localPath: String,
    val schema: Schema,
    val flatten: Boolean,
    val startOffset: Long,
    val endOffset: Long
) extends SourceOperatorExecutor {
  private var reader: BufferedReader = _
  private var curLineCount: Long = 0

  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = curLineCount < endOffset && reader.ready

      override def next: Tuple = {
        while (curLineCount < startOffset) {
          curLineCount += 1
          reader.readLine
        }

        val line = reader.readLine
        curLineCount += 1
        val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
        val data = JSONToMap(objectMapper.readTree(line), flatten = flatten)

        for (fieldName <- schema.getAttributeNames.asScala) {
          if (data.contains(fieldName))
            fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
          else {
            fields += null
          }
        }

        Tuple.newBuilder(schema).addSequentially(fields.toArray).build

      }
    }
  override def open(): Unit = reader = new BufferedReader(new FileReader(localPath))

  override def close(): Unit = reader.close()

}
