package edu.uci.ics.texera.workflow.operators.source.scan.csvOld

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import edu.uci.ics.amber.engine.common.executor.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.{
  Attribute,
  AttributeTypeUtils,
  Schema,
  TupleLike
}
import edu.uci.ics.amber.engine.common.storage.DocumentFactory
import edu.uci.ics.texera.workflow.operators.source.scan.FileDecodingMethod

import java.net.URI
import scala.collection.compat.immutable.ArraySeq

class CSVOldScanSourceOpExec private[csvOld] (
    fileUri: String,
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
    val filePath = DocumentFactory.newReadonlyDocument(new URI(fileUri)).asFile().toPath
    reader = CSVReader.open(filePath.toString, fileEncoding.getCharset.name())(CustomFormat)
    // skip line if this worker reads the start of a file, and the file has a header line
    val startOffset = offset.getOrElse(0) + (if (hasHeader) 1 else 0)

    rows = reader.iterator.drop(startOffset)
  }

  override def close(): Unit = {
    reader.close()
  }
}
