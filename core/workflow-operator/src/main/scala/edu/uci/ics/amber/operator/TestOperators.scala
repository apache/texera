package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.storage.FileResolver
import edu.uci.ics.amber.operator.aggregate.{
  AggregateOpDesc,
  AggregationFunction,
  AggregationOperation
}
import edu.uci.ics.amber.operator.hashJoin.HashJoinOpDesc
import edu.uci.ics.amber.operator.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.amber.operator.source.scan.json.JSONLScanSourceOpDesc
import edu.uci.ics.amber.operator.source.sql.asterixdb.AsterixDBSourceOpDesc
import edu.uci.ics.amber.operator.source.sql.mysql.MySQLSourceOpDesc
import edu.uci.ics.amber.operator.udf.python.PythonUDFOpDescV2
import edu.uci.ics.amber.util.PathUtils

object TestOperators {

  val parentDir = PathUtils.corePath.resolve("workflow-operator").toRealPath().toString
  val CountrySalesSmallCsvPath = s"$parentDir/src/test/resources/country_sales_small.csv"
  val CountrySalesMediumCsvPath = s"$parentDir/src/test/resources/country_sales_medium.csv"
  val CountrySalesHeaderlessSmallCsvPath =
    s"$parentDir/src/test/resources/country_sales_headerless_small.csv"
  val CountrySalesSmallMultiLineCsvPath =
    s"$parentDir/src/test/resources/country_sales_small_multi_line.csv"
  val CountrySalesSmallMultiLineCustomDelimiterCsvPath =
    s"$parentDir/src/test/resources/country_sales_headerless_small_multi_line_custom_delimiter.csv"
  val smallJsonLPath =
    s"$parentDir/src/test/resources/100.jsonl"
  val mediumJsonLPath =
    s"$parentDir/src/test/resources/1000.jsonl"
  val TestTextFilePath: String = s"$parentDir/src/test/resources/line_numbers.txt"
  val TestCRLFTextFilePath: String = s"$parentDir/src/test/resources/line_numbers_crlf.txt"
  val TestNumbersFilePath: String = s"$parentDir/src/test/resources/numbers.txt"

  def headerlessSmallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc(CountrySalesHeaderlessSmallCsvPath, header = false)
  }

  def headerlessSmallMultiLineDataCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc(
      CountrySalesHeaderlessSmallCsvPath,
      header = false,
      multiLine = true
    )
  }

  def smallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc(CountrySalesSmallCsvPath, header = true)
  }

  def smallJSONLScanOpDesc(): JSONLScanSourceOpDesc = {
    getJSONLScanOpDesc(smallJsonLPath)
  }

  def mediumFlattenJSONLScanOpDesc(): JSONLScanSourceOpDesc = {
    getJSONLScanOpDesc(mediumJsonLPath, flatten = true)
  }

  def getCsvScanOpDesc(
      fileName: String,
      header: Boolean,
      multiLine: Boolean = false
  ): CSVScanSourceOpDesc = {
    val csvHeaderlessOp = new CSVScanSourceOpDesc()
    csvHeaderlessOp.fileName = Some(fileName)
    csvHeaderlessOp.customDelimiter = Some(",")
    csvHeaderlessOp.hasHeader = header
    csvHeaderlessOp.setFileUri(FileResolver.resolve(fileName))
    csvHeaderlessOp

  }

  def getJSONLScanOpDesc(fileName: String, flatten: Boolean = false): JSONLScanSourceOpDesc = {
    val jsonlOp = new JSONLScanSourceOpDesc
    jsonlOp.fileName = Some(fileName)
    jsonlOp.flatten = flatten
    jsonlOp.setFileUri(FileResolver.resolve(fileName))
    jsonlOp
  }

  def joinOpDesc(buildAttrName: String, probeAttrName: String): HashJoinOpDesc[String] = {
    val joinOp = new HashJoinOpDesc[String]()
    joinOp.buildAttributeName = buildAttrName
    joinOp.probeAttributeName = probeAttrName
    joinOp
  }

  def mediumCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc(CountrySalesMediumCsvPath, header = true)
  }

  def keywordSearchOpDesc(attribute: String, keywordToSearch: String): KeywordSearchOpDesc = {
    val keywordSearchOp = new KeywordSearchOpDesc()
    keywordSearchOp.attribute = attribute
    keywordSearchOp.keyword = keywordToSearch
    keywordSearchOp
  }

  def aggregateAndGroupByDesc(
      attributeToAggregate: String,
      aggFunction: AggregationFunction,
      groupByAttributes: List[String]
  ): AggregateOpDesc = {
    val aggOp = new AggregateOpDesc()
    val aggFunc = new AggregationOperation()
    aggFunc.aggFunction = aggFunction
    aggFunc.attribute = attributeToAggregate
    aggFunc.resultAttribute = "aggregate-result"
    aggOp.aggregations = List(aggFunc)
    aggOp.groupByKeys = groupByAttributes
    aggOp
  }

  def inMemoryMySQLSourceOpDesc(
      host: String,
      port: String,
      database: String,
      table: String,
      username: String,
      password: String
  ): MySQLSourceOpDesc = {
    val inMemoryMySQLSourceOpDesc = new MySQLSourceOpDesc()
    inMemoryMySQLSourceOpDesc.host = host
    inMemoryMySQLSourceOpDesc.port = port
    inMemoryMySQLSourceOpDesc.database = database
    inMemoryMySQLSourceOpDesc.table = table
    inMemoryMySQLSourceOpDesc.username = username
    inMemoryMySQLSourceOpDesc.password = password
    inMemoryMySQLSourceOpDesc.limit = Some(1000)
    inMemoryMySQLSourceOpDesc
  }

  // TODO: use mock data to perform the test, remove dependency on the real AsterixDB
  def asterixDBSourceOpDesc(): AsterixDBSourceOpDesc = {
    val asterixDBOp = new AsterixDBSourceOpDesc()
    asterixDBOp.host = "ipubmed4.ics.uci.edu" // AsterixDB at version 0.9.5
    asterixDBOp.port = "default"
    asterixDBOp.database = "twitter"
    asterixDBOp.table = "ds_tweet"
    asterixDBOp.limit = Some(1000)
    asterixDBOp
  }

  def pythonOpDesc(): PythonUDFOpDescV2 = {
    val udf = new PythonUDFOpDescV2()
    udf.workers = 1
    udf.code = """
        |from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    @overrides
        |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        |        yield tuple_
        |""".stripMargin
    udf
  }
}
