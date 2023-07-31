package edu.uci.ics.texera.workflow.operators.visualization.ganttChart
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class GanttChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: GanttChartOpDesc = _
  before {
    opDesc = new GanttChartOpDesc()
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }
}
