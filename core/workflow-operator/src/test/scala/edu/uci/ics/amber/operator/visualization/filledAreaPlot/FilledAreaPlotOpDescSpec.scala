package edu.uci.ics.amber.operator.visualization.filledAreaPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class FilledAreaPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: FilledAreaPlotOpDesc = _

  before {
    opDesc = new FilledAreaPlotOpDesc()
  }

  it should "throw error if X is empty" in {
    val y = "test1"
    val group = "test2"
    opDesc.y = y
    opDesc.lineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if Y is empty" in {
    val x = "test1"
    val group = "test2"
    opDesc.x = x
    opDesc.lineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if LineGroup is not indicated facet column is checked" in {
    val x = "test1"
    val y = "test2"
    opDesc.x = x
    opDesc.y = y
    opDesc.facetColumn = true
    opDesc.color = "color"

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

}
