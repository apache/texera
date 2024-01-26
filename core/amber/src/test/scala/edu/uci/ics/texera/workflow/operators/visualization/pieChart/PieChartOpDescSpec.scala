package edu.uci.ics.texera.workflow.operators.visualization.pieChart
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class PieChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: PieChartOpDesc = _
  before {
    opDesc = new PieChartOpDesc()
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }
}
