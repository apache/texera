package edu.uci.ics.amber.operator.visualization.scatterplot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class ScatterPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: ScatterplotOpDesc = _

  before {
    opDesc = new ScatterplotOpDesc()
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "throw assertion error if chart is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

}
