package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.flattenJSON

import java.io.{BufferedReader, FileReader, IOException}
import scala.collection.Iterator
import scala.collection.JavaConverters._
class JSONLScanSourceOpExec private[json] (
    val localPath: String,
    val schema: Schema,
    val startOffset: Long,
    val endOffset: Long
) extends SourceOperatorExecutor {
  private var reader: BufferedReader = _
  private var curLineCount: Long = 0;

  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean =
        try curLineCount <= endOffset && reader.ready
        catch {
          case e: IOException =>
            e.printStackTrace()
            throw new RuntimeException(e)
        }

      override def next: Tuple =
        try {
          while (curLineCount < startOffset) {
            curLineCount += 1
            reader.readLine
          }
          val line = reader.readLine
          curLineCount += 1
          val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
          val data = flattenJSON(objectMapper.readTree(line))

          for (fieldName <- schema.getAttributeNames.asScala) {
            if (data.contains(fieldName))
              fields += data(fieldName)
            else {
              fields += null
            }
          }

          Tuple.newBuilder.add(schema, fields.toArray).build
        } catch {
          case e: IOException =>
            e.printStackTrace()
            throw new RuntimeException(e)
        }
    }

  override def open(): Unit =
    try this.reader = new BufferedReader(new FileReader(this.localPath))
    catch {
      case e: IOException =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }

  override def close(): Unit =
    try reader.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }
}
