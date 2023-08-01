package edu.uci.ics.texera.workflow.operators.visualization.ganttChart
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class GanttChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: GanttChartOpDesc = _
  before {
    opDesc = new GanttChartOpDesc()
  }
  it should "generate a plotly python figure with 3 columns and no color" in {
    opDesc.start = "start"
    opDesc.finish = "finish"
    opDesc.task = "task"
    opDesc.color = ""

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = px.timeline(table, x_start='start', x_end='finish', y='task' )"
        )
    )
  }
  it should "generate a plotly python figure with 3 columns and color" in {
    opDesc.start = "start"
    opDesc.finish = "finish"
    opDesc.task = "task"
    opDesc.color = "color"

    assert(
      opDesc
        .createPlotlyFigure()
        .contains(
          "fig = px.timeline(table, x_start='start', x_end='finish', y='task' , color='color')"
        )
    )
  }
}
