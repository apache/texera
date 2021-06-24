package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap

import java.io.{BufferedReader, FileReader}
import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class JSONLScanSourceOpExec private[json] (
    val desc: JSONLScanSourceOpDesc,
    val startOffset: Long,
    val endOffset: Long
) extends SourceOperatorExecutor {
  private val schema: Schema = desc.inferSchema()
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
        Try({
          val line = reader.readLine
          curLineCount += 1
          val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
          val data = JSONToMap(objectMapper.readTree(line), flatten = desc.flatten)

          for (fieldName <- schema.getAttributeNames.asScala) {
            if (data.contains(fieldName))
              fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
            else {
              fields += null
            }
          }

          Tuple.newBuilder.add(schema, fields.toArray).build
        }) match {
          case Success(tuple) => tuple
          case Failure(_)     => null
        }

      }
    }

  override def open(): Unit = reader = new BufferedReader(new FileReader(desc.filePath.get))

  override def close(): Unit = reader.close()

}
