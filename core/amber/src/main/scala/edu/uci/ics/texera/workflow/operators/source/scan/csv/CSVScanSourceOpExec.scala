package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class CSVScanSourceOpExec private[csv] (val desc: CSVScanSourceOpDesc)
    extends SourceOperatorExecutor {
  val schema: Schema = desc.inferSchema()
  var reader: CSVReader = _
  var rows: Iterator[Seq[String]] = _
  override def produceTexeraTuple(): Iterator[Tuple] = {

    var tuples = rows
      .map(fields =>
        try {
          val parsedFields: Array[Object] = AttributeTypeUtils.parseFields(
            fields.toArray,
            schema.getAttributes
              .map((attr: Attribute) => attr.getType)
              .toArray
          )
          Tuple.newBuilder(schema).addSequentially(parsedFields).build
        } catch {
          case _: Throwable => null
        }
      )
      .filter(tuple => tuple != null)

    if (desc.limit.isDefined) tuples = tuples.take(desc.limit.get)
    tuples
  }

  override def open(): Unit = {
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = desc.customDelimiter.get.charAt(0)
    }
    reader = CSVReader.open(desc.filePath.get)(CustomFormat)
    // skip line if this worker reads the start of a file, and the file has a header line
    val startOffset = desc.offset.getOrElse(0) + (if (desc.hasHeader) 1 else 0)

    rows = reader.iterator.drop(startOffset)
  }

  override def close(): Unit = {
    reader.close()
  }
}
