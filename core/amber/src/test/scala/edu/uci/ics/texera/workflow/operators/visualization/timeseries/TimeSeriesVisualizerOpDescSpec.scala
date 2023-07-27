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
}
