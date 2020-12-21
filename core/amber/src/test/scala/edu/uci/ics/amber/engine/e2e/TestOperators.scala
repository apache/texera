package edu.uci.ics.amber.engine.e2e

import akka.stream.Attributes.Attribute
import edu.uci.ics.texera.workflow.operators.aggregate.{
  AggregationFunction,
  SpecializedAverageOpDesc
}
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.localscan.LocalCsvFileScanOpDesc
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc

object TestOperators {

  def headerlessCsvScanOpDesc(): LocalCsvFileScanOpDesc = {
    val csvHeaderlessOp = new LocalCsvFileScanOpDesc()
    csvHeaderlessOp.filePath = "src/test/resources/CountrySalesDataHeaderless.csv"
    csvHeaderlessOp.delimiter = ","
    csvHeaderlessOp.header = false
    csvHeaderlessOp
  }

  def csvScanOpDesc(): LocalCsvFileScanOpDesc = {
    val csvHeaderlessOp = new LocalCsvFileScanOpDesc()
    csvHeaderlessOp.filePath = "src/test/resources/CountrySalesData.csv"
    csvHeaderlessOp.delimiter = ","
    csvHeaderlessOp.header = true
    csvHeaderlessOp
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
