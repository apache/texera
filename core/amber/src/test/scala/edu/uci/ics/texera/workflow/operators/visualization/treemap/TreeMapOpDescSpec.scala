package edu.uci.ics.texera.workflow.operators.visualization.treemap

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class TreeMapOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: TreeMapOpDesc = _

  before {
    opDesc = new TreeMapOpDesc()
  }

  it should "generate a list of hierarchy sections in the python code" in {
    val attributes = Array.fill(3)(new HierarchySection())
    attributes(0).attributeName = "column_a"
    attributes(1).attributeName = "column_b"
    attributes(2).attributeName = "column_c"
    opDesc.hierarchy = attributes.toList
    assert(opDesc.createPlotlyFigure().contains("['column_a','column_b','column_c']"))
  }

  it should "throw assertion error if hierarchy is empty" in {
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
