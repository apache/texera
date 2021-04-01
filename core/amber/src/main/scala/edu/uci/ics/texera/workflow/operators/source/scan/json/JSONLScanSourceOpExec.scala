package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.flattenJSON

import java.io.{BufferedReader, FileReader, IOException}
import scala.collection.Iterator
import scala.collection.JavaConverters._
class JSONLScanSourceOpExec private[json] (val localPath: String, val schema: Schema)
    extends SourceOperatorExecutor {
  private var reader: BufferedReader = _

  override def produceTexeraTuple =
    new Iterator[Tuple]() {
      override def hasNext: Boolean =
        try reader.ready
        catch {
          case e: IOException =>
            e.printStackTrace()
            throw new RuntimeException(e)
        }

      override def next: Tuple =
        try { // obtain String representation of each field
          // a null value will present if omit in between fields, e.g., ['hello', null, 'world']
          val line = reader.readLine

          val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
          val data = flattenJSON(objectMapper.readTree(line))

          for (fieldName <- schema.getAttributeNames.asScala) {

            fields += data.get(fieldName)
          }
          Tuple.newBuilder.add(schema, fields.toArray).build
        } catch {
          case e: IOException =>
            e.printStackTrace()
            throw new RuntimeException(e)
        }
    }

  override def open() =
    try this.reader = new BufferedReader(new FileReader(this.localPath))
    catch {
      case e: IOException =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }

  override def close() =
    try reader.close()
    catch {
      case e: IOException =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }
}
