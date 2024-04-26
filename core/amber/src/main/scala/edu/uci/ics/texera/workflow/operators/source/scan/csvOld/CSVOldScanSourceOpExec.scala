package edu.uci.ics.texera.workflow.operators.source.scan.csvOld

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import edu.uci.ics.amber.engine.common.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.FileDecodingMethod

import scala.collection.compat.immutable.ArraySeq

class CSVOldScanSourceOpExec private[csvOld] (
    filePath: String,
    fileEncoding: FileDecodingMethod,
    limit: Option[Int],
    offset: Option[Int],
    customDelimiter: Option[String],
    hasHeader: Boolean,
    schemaFunc: () => Schema
) extends SourceOperatorExecutor {
  var schema: Schema = _
  var reader: CSVReader = _
  var rows: Iterator[Seq[String]] = _
  override def produceTuple(): Iterator[TupleLike] = {

    var tuples = rows
      .map(fields =>
        try {
          val parsedFields: Array[Any] = AttributeTypeUtils.parseFields(
            fields.toArray,
            schema.getAttributes
              .map((attr: Attribute) => attr.getType)
              .toArray
          )
          TupleLike(ArraySeq.unsafeWrapArray(parsedFields): _*)
        } catch {
          case _: Throwable => null
        }
      )
      .filter(tuple => tuple != null)

    if (limit.isDefined) tuples = tuples.take(limit.get)
    tuples
  }

  override def open(): Unit = {
    schema = schemaFunc()
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = customDelimiter.get.charAt(0)
    }
    reader = CSVReader.open(filePath, fileEncoding.getCharset.name())(CustomFormat)
    // skip line if this worker reads the start of a file, and the file has a header line
    val startOffset = offset.getOrElse(0) + (if (hasHeader) 1 else 0)

    rows = reader.iterator.drop(startOffset)
  }

  override def close(): Unit = {
    reader.close()
  }
}
