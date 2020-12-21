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

  def countOpDesc(attribute: String): SpecializedAverageOpDesc = {
    val countOp = new SpecializedAverageOpDesc()
    countOp.aggFunction = AggregationFunction.COUNT
    countOp.attribute = attribute
    countOp.resultAttribute = "count-result"
    countOp.groupByKeys = List[String]()
    countOp
  }

  def sinkOpDesc(): SimpleSinkOpDesc = {
    new SimpleSinkOpDesc()
  }
}
