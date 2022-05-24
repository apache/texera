package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeTypeUtils, Schema}

import java.io.{File, FileInputStream, InputStreamReader}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class CSVScanSourceOpExec private[csv] (val desc: CSVScanSourceOpDesc)
    extends SourceOperatorExecutor {
  val schema: Schema = desc.inferSchema()
  var inputReader: InputStreamReader = _
  var parser: CsvParser = _
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
    inputReader = new InputStreamReader(new FileInputStream(new File(desc.filePath.get)))

    val csvFormat = new CsvFormat()
    csvFormat.setDelimiter(desc.customDelimiter.get.charAt(0))
    val csvSetting = new CsvParserSettings()
    csvSetting.setMaxCharsPerColumn(-1)
    csvSetting.setFormat(csvFormat)
    csvSetting.setHeaderExtractionEnabled(desc.hasHeader)

    parser = new CsvParser(csvSetting)
    parser.beginParsing(inputReader)

    // drop start offset
    (0 until desc.offset.getOrElse(0)).foreach(parser.parseNext())
  }

  override def close(): Unit = {
    if (parser != null) {
      parser.stopParsing()
    }
    if (inputReader != null) {
      inputReader.close()
    }
  }
}
