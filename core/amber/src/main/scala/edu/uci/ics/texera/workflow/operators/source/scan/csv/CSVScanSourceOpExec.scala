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
  var reader: CSVReader = _

  override def produceTexeraTuple(): Iterator[Tuple] = {

    // skip line if this worker reads the start of a file, and the file has a header line
    val startOffset = desc.offset.getOrElse(0).asInstanceOf[Int] + (if (desc.hasHeader) 1 else 0)
    var tuples = reader.iterator.map(fields =>
      Try({
        val parsedFields: Array[Object] = AttributeTypeUtils.parseFields(
          fields.toArray,
          schema.getAttributes
              .map((attr: Attribute) => attr.getType)
              .toArray
        )
        Tuple.newBuilder(schema).addSequentially(parsedFields).build
      }) match {
        case Success(tuple) => tuple
        case Failure(_) => null
      }

    ).drop(startOffset)

    desc.limit match {
      case Some(lim) => tuples = tuples.take(lim)
      case None =>
    }
    tuples
  }

  override def open(): Unit = {
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = desc.customDelimiter.get.charAt(0)
    }
    reader = CSVReader.open(desc.filePath.get)(CustomFormat)
  }

  override def close(): Unit = {
    reader.close()
  }
}
