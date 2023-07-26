package edu.uci.ics.texera.workflow.operators.visualization.bubbleChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class BubbleChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: BubbleChartOpDesc = _

  before {
    opDesc = new BubbleChartOpDesc()
  }

  it should "generate a plotly python figure with 3 columns and a title" in {
    opDesc.x_value = "column1"
    opDesc.y_value = "column2"
    opDesc.z_value = "column3"
    opDesc.title = "myTitle"

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = go.Figure(px.scatter(table, x='column1', " +
            "y='column2', size='column3', size_max=100, title = 'myTitle'))"
        )
    )
  }

  it should "throw assertion error if x_value is empty" in {
    assertThrows[AssertionError] {
      opDesc.createPlotlyFigure()
    }
  }

}
