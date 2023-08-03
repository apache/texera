package edu.uci.ics.texera.workflow.operators.visualization.bubbleChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class BubbleChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: BubbleChartOpDesc = _

  before {
    opDesc = new BubbleChartOpDesc()
  }

  it should "generate a plotly python figure with 3 columns and a title" in {
    opDesc.xValue = "column1"
    opDesc.yValue = "column2"
    opDesc.zValue = "column3"
    opDesc.title = "myTitle"
    opDesc.enableColor = false

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = go.Figure(px.scatter(table, x='column1', y='column2', size='column3', size_max=100, title='myTitle'))"
        )
    )
  }

  it should "throw assertion error if variable xValue is empty" in {
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }
}
