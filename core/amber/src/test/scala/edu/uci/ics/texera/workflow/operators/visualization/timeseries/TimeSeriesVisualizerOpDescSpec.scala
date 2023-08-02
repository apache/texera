package edu.uci.ics.texera.workflow.operators.visualization.timeseries

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class TimeSeriesVisualizerOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: TimeSeriesOpDesc = _

  before {
    opDesc = new TimeSeriesOpDesc()
  }

  it should "throw assertion error if date is empty" in {
    opDesc.value = "column1"
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw assertion error if value is empty" in {
    opDesc.date = "column2"
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw assertion error if tick is not in the format 'M<n>'" in {
    opDesc.value = "column1"
    opDesc.date = "column2"
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
