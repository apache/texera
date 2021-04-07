package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.texera.workflow.operators.aggregate.{
  AggregationFunction,
  SpecializedAverageOpDesc
}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONLScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.asterixdb.AsterixDBSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.mysql.MySQLSourceOpDesc

object TestOperators {

  def headerlessSmallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataHeaderlessSmall.csv", header = false)
  }

  def smallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataSmall.csv", header = true)
  }

  def smallJSONLScanOpDesc(): JSONLScanSourceOpDesc = {
    getJSONLScanOpDesc("src/test/resources/100.jsonl")
  }

  def mediumFlattenJSONLScanOpDesc(): JSONLScanSourceOpDesc = {
    getJSONLScanOpDesc("src/test/resources/1000.jsonl", flatten = true)
  }
  def getCsvScanOpDesc(fileName: String, header: Boolean): CSVScanSourceOpDesc = {
    val csvHeaderlessOp = new CSVScanSourceOpDesc()
    csvHeaderlessOp.fileName = Option(fileName)
    csvHeaderlessOp.delimiter = Option(",")
    csvHeaderlessOp.hasHeader = header
    csvHeaderlessOp
  }

  def getJSONLScanOpDesc(fileName: String, flatten: Boolean = false): JSONLScanSourceOpDesc = {
    val jsonlOp = new JSONLScanSourceOpDesc
    jsonlOp.fileName = Option(fileName)
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
    getCsvScanOpDesc("src/test/resources/CountrySalesDataMedium.csv", header = true)
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
  ): SpecializedAverageOpDesc = {
    val aggOp = new SpecializedAverageOpDesc()
    aggOp.aggFunction = aggFunction
    aggOp.attribute = attributeToAggregate
    aggOp.resultAttribute = "aggregate-result"
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
    inMemoryMySQLSourceOpDesc.limit = Option(1000)
    inMemoryMySQLSourceOpDesc
  }

  def asterixDBSourceOpDesc(): AsterixDBSourceOpDesc = {
    val asterixDBOp = new AsterixDBSourceOpDesc()
    asterixDBOp.host = "ipubmed4.ics.uci.edu" // AsterixDB at version 0.9.5
    asterixDBOp.port = "default"
    asterixDBOp.database = "twitter"
    asterixDBOp.table = "ds_tweet"
    asterixDBOp.limit = Option(1000)
    asterixDBOp
  }

  def sinkOpDesc(): SimpleSinkOpDesc = {
    new SimpleSinkOpDesc()
  }
}
