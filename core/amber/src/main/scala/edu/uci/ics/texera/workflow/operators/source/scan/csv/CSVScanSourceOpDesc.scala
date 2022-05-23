package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.ManyToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import scala.jdk.CollectionConverters.asJavaIterableConverter

class CSVScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var customDelimiter: Option[String] = None

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  fileTypeName = Option("CSV")

  @throws[IOException]
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    // fill in default values
    if (customDelimiter.get.isEmpty)
      customDelimiter = Option(",")

    filePath match {
      case Some(_) =>
        new ManyToOneOpExecConfig(operatorIdentifier, _ => new CSVScanSourceOpExec(this))
      case None =>
        throw new RuntimeException("File path is not provided.")
    }

  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    *
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    if (customDelimiter.isEmpty) {
      return null
    }
    if (filePath.isEmpty) {
      return null
    }
    val inputReader = new InputStreamReader(new FileInputStream(
      new File(filePath.get)))

    val csvFormat = new CsvFormat()
    csvFormat.setDelimiter(customDelimiter.get.charAt(0))
    val csvSetting = new CsvParserSettings()
    csvSetting.setMaxCharsPerColumn(-1)
    csvSetting.setFormat(csvFormat)
    val csvParser = new CsvParser(csvSetting)
    csvParser.beginParsing(inputReader)

    csvSetting.setNumberOfRowsToSkip()

    val header =
      if (hasHeader) Some(csvParser.parseNext()) else None

    // skip offset
//    csvParser.


//    val startOffset = offset.getOrElse(0) + (if (hasHeader) 1 else 0)
//    val endOffset =
//      startOffset + limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
//    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
//      reader.iterator
//        .slice(startOffset, endOffset)
//        .map(seq => seq.toArray)
//    )
//
//    reader.close()
//
//    // build schema based on inferred AttributeTypes
//    Schema.newBuilder
//      .add(
//        firstRow.indices
//          .map((i: Int) =>
//            new Attribute(
//              if (hasHeader) firstRow.apply(i) else "column-" + (i + 1),
//              attributeTypeList.apply(i)
//            )
//          )
//          .asJava
//      )
//      .build
  }

}
