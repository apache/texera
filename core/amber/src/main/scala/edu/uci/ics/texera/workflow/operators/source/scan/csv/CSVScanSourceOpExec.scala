package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.{Failure, Success, Try}

class CSVScanSourceOpExec private[csv] (val desc: CSVScanSourceOpDesc)
    extends SourceOperatorExecutor {
  val schema: Schema = desc.inferSchema()
  var rows: Iterator[Seq[String]] = _
  var curLimit: Option[Long] = desc.limit.asInstanceOf[Option[Long]]

  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = rows.hasNext && (curLimit.isEmpty || curLimit.get > 0L)

      override def next: Tuple = {
        // obtain String representation of each field
        val fields: Seq[String] = rows.next
        // parse Strings into inferred AttributeTypes
        Try({
          val parsedFields: Array[Object] = AttributeTypeUtils.parseFields(
            fields.toArray,
            schema.getAttributes
              .map((attr: Attribute) => attr.getType)
              .toArray
          )
          Tuple.newBuilder(schema).addSequentially(parsedFields).build
        }) match {
          case Success(tuple) =>
            curLimit match {
              case Some(limit) => curLimit = Some(limit - 1)
              case None        =>
            }
            tuple
          case Failure(_) => null
        }

      }

    }

  override def open(): Unit = {
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = desc.customDelimiter.get.charAt(0)
    }
    rows = CSVReader.open(desc.filePath.get)(CustomFormat).iterator
    // skip line if this worker reads the start of a file, and the file has a header line
    if (desc.hasHeader) rows.next()
  }

  override def close(): Unit = {}
}
