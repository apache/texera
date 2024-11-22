package edu.uci.ics.amber.operator.visualization.DotPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class DotPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: DotPlotOpDesc = _

  before {
    opDesc = new DotPlotOpDesc()
  }

  it should "generate a plotly python figure with count aggregation" in {
    opDesc.countAttribute = "column1"

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "table = table.groupby(['column1'])['column1'].count().reset_index(name='counts')"
        )
    )
  }
}
