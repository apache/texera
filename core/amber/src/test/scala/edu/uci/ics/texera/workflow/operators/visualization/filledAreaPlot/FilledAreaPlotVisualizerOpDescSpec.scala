package edu.uci.ics.texera.workflow.operators.visualization.filledAreaPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class FilledAreaPlotVisualizerOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: FilledAreaPlotVisualizerOpDesc = _

  before {
    opDesc = new FilledAreaPlotVisualizerOpDesc()
  }

  it should "throw error if X is empty" in {
    val y = "test1"
    val group = "test2"
    opDesc.Y = y
    opDesc.LineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if Y is empty" in {
    val x = "test1"
    val group = "test2"
    opDesc.X = x
    opDesc.LineGroup = group

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

  it should "throw error if LineGroup is not indicated but color and facet column are checked" in {
    val x = "test1"
    val y = "test2"
    opDesc.X = x
    opDesc.Y = y
    opDesc.FacetColumn = true
    opDesc.Color = true

    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

}
