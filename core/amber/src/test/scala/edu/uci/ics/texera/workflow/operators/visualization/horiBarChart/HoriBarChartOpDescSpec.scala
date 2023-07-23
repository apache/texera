package edu.uci.ics.texera.workflow.operators.visualization.horiBarChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class HoriBarChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: HoriBarChartOpDesc = _

  before {
    opDesc = new HoriBarChartOpDesc()
  }

  it should "list titles of axes in the python code" in {
    opDesc.fields = "geo.state_name"
    opDesc.value = "person.count"
    val temp = opDesc.createPlotlyFigure()
    assert(temp.contains("geo.state_name"))
    assert(temp.contains("person.count"))
  }

  it should "throw assertion error if chart is empty" in {
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }
}