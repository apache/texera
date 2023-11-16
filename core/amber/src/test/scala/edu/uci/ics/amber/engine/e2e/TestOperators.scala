package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.texera.workflow.operators.aggregate.{
  AggregationFunction,
  AggregationOperation,
  SpecializedAggregateOpDesc
}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONLScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.asterixdb.AsterixDBSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.mysql.MySQLSourceOpDesc
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpDescV2
import edu.uci.ics.texera.workflow.operators.visualization.wordCloud.WordCloudOpDesc

object TestOperators {

  def headerlessSmallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/country_sales_headerless_small.csv", header = false)
  }

  def headerlessSmallMultiLineDataCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc(
      "src/test/resources/country_sales_headerless_small_multi_line.csv",
      header = false,
      multiLine = true
    )
  }

  def smallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/country_sales_small.csv", header = true)
  }

  def smallJSONLScanOpDesc(): JSONLScanSourceOpDesc = {
    getJSONLScanOpDesc("src/test/resources/100.jsonl")
  }

  def mediumFlattenJSONLScanOpDesc(): JSONLScanSourceOpDesc = {
    getJSONLScanOpDesc("src/test/resources/1000.jsonl", flatten = true)
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
    csvHeaderlessOp
  }

  def getJSONLScanOpDesc(fileName: String, flatten: Boolean = false): JSONLScanSourceOpDesc = {
    val jsonlOp = new JSONLScanSourceOpDesc
    jsonlOp.fileName = Some(fileName)
    jsonlOp.flatten = flatten
    jsonlOp
  }

  def joinOpDesc(buildAttrName: String, probeAttrName: String): HashJoinOpDesc[String] = {
    val joinOp = new HashJoinOpDesc[String]()
    joinOp.buildAttributeName = buildAttrName
    joinOp.probeAttributeName = probeAttrName
    joinOp
  }

  def mediumCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/country_sales_medium.csv", header = true)
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
  ): SpecializedAggregateOpDesc = {
    val aggOp = new SpecializedAggregateOpDesc()
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

  def sinkOpDesc(): ProgressiveSinkOpDesc = {
    new ProgressiveSinkOpDesc()
  }

  def wordCloudOpDesc(textColumn: String, topN: Integer = null): WordCloudOpDesc = {
    val wordCountOpDesc = new WordCloudOpDesc()
    wordCountOpDesc.textColumn = textColumn
    wordCountOpDesc.topN = topN
    wordCountOpDesc
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
