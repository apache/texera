package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.texera.workflow.operators.aggregate.{
  AggregationFunction,
  SpecializedAverageOpDesc
}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.scan.CSVScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc

object TestOperators {

  def headerlessSmallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataHeaderlessSmall.csv", false)
  }

  def smallCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataSmall.csv", true)
  }

  def getCsvScanOpDesc(fileName: String, header: Boolean): CSVScanSourceOpDesc = {
    val csvHeaderlessOp = new CSVScanSourceOpDesc()
    csvHeaderlessOp.fileName = Option(fileName)
    csvHeaderlessOp.delimiter = Option(",")
    csvHeaderlessOp.hasHeader = header
    csvHeaderlessOp
  }

  def joinOpDesc(buildAttrName: String, probeAttrName: String): HashJoinOpDesc[String] = {
    val joinOp = new HashJoinOpDesc[String]()
    joinOp.buildAttributeName = buildAttrName
    joinOp.probeAttributeName = probeAttrName
    joinOp
  }

  def mediumCsvScanOpDesc(): CSVScanSourceOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataMedium.csv", true)
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

  def sinkOpDesc(): SimpleSinkOpDesc = {
    new SimpleSinkOpDesc()
  }
}
