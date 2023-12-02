package edu.uci.ics.texera.workflow.operators.visualization.linechart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class TimeSeriesVisualizerOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: LineChartOpDesc = _

  before {
    opDesc = new LineChartOpDesc()
  }

  it should "throw assertion error if date is empty" in {
    opDesc.value = "column1"
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw assertion error if value is empty" in {
    opDesc.data = "column2"
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw assertion error if tick is not in the format 'M<n>'" in {
    opDesc.value = "column1"
    opDesc.data = "column2"
    opDesc.tick = "M0"
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
    opDesc.tick = "every month"
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
    opDesc.tick = "aM3b"
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
    opDesc.tick = "m3"
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }
}
