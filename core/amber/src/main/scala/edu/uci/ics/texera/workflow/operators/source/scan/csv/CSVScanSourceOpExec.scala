package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class CSVScanSourceOpExec private[csv] (
    val localPath: String,
    val schema: Schema,
    val customDelimiter: Char,
    val hasHeader: Boolean
) extends SourceOperatorExecutor {
  var rows: Iterator[Seq[String]] = _
  var reader: CSVReader = _
  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = rows.hasNext

      override def next: Tuple = {
        // obtain String representation of each field
        val fields: Seq[String] = rows.next
        // parse Strings into inferred AttributeTypes
        val parsedFields: Array[Object] = AttributeTypeUtils.parseFields(
          fields.toArray,
          schema.getAttributes
            .map((attr: Attribute) => attr.getType)
            .toArray
        )
        Tuple.newBuilder(schema).add(schema, parsedFields).build
      }

    }

  override def open(): Unit = {
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = customDelimiter
    }
    rows = CSVReader.open(localPath)(CustomFormat).iterator
    // skip line if this worker reads the start of a file, and the file has a header line
    if (hasHeader) rows.next()
  }

  override def close(): Unit = reader.close()

}
