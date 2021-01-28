package edu.uci.ics.amber.engine.e2e

import akka.stream.Attributes.Attribute
import edu.uci.ics.texera.workflow.operators.aggregate.{
  AggregationFunction,
  SpecializedAverageOpDesc
}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.localscan.LocalCsvFileScanOpDesc
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc

object TestOperators {

  def getCsvScanOpDesc(fileName: String, header: Boolean): LocalCsvFileScanOpDesc = {
    val csvHeaderlessOp = new LocalCsvFileScanOpDesc()
    csvHeaderlessOp.filePath = fileName
    csvHeaderlessOp.delimiter = ","
    csvHeaderlessOp.header = header
    csvHeaderlessOp
  }

  def headerlessSmallCsvScanOpDesc(): LocalCsvFileScanOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataHeaderlessSmall.csv", false)
  }

  def smallCsvScanOpDesc(): LocalCsvFileScanOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataSmall.csv", true)
  }

  def mediumCsvScanOpDesc(): LocalCsvFileScanOpDesc = {
    getCsvScanOpDesc("src/test/resources/CountrySalesDataMedium.csv", true)
  }

  def joinOpDesc(buildAttrName: String, probeAttrName: String): HashJoinOpDesc[String] = {
    val joinOp = new HashJoinOpDesc[String]()
    joinOp.buildAttributeName = buildAttrName
    joinOp.probeAttributeName = probeAttrName
    joinOp
  }

  def keywordSearchOpDesc(attribute: String, keywordToSearch: String): KeywordSearchOpDesc = {
    val keywordSearchOp = new KeywordSearchOpDesc()
    keywordSearchOp.attribute = attribute
    keywordSearchOp.keyword = keywordToSearch
    keywordSearchOp
  }

  def aggregateAndGroupbyDesc(
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
