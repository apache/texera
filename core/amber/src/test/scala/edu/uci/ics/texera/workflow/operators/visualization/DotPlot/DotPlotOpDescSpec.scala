package edu.uci.ics.texera.workflow.operators.visualization.DotPlot

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class DotPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var opDesc: DotPlotOpDesc = _

  before {
    opDesc = new DotPlotOpDesc()
  }

  it should "generate Plotly figure code in Python" in {
    opDesc.countAttribute = "count_attribute"
    opDesc.title = "Styled Categorical Dot Plot"
    val expectedFigureCode =
      s"""
         |        table = table.groupby(['count_attribute'])['count_attribute'].count().reset_index(name='counts')
         |        fig = px.strip(table, x='counts', y='count_attribute', orientation='h', color='count_attribute',
         |               color_discrete_sequence=px.colors.qualitative.Dark2)
         |
         |        fig.update_traces(marker=dict(size=12, line=dict(width=2, color='DarkSlateGrey')))
         |
         |        fig.update_layout(title='Styled Categorical Dot Plot',
         |                          xaxis_title='Counts',
         |                          yaxis_title='count_attribute',
         |                          yaxis=dict(showline=True, showgrid=False, showticklabels=True),
         |                          xaxis=dict(showline=True, showgrid=True, showticklabels=True),
         |                          height=800)
         |""".stripMargin

    assert(opDesc.createPlotlyFigure() === expectedFigureCode)
  }

  it should "generate Python code with the provided CountAttribute" in {
    opDesc.countAttribute = "count_attribute"
    val expectedPythonCode =
      s"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    def render_error(self, error_msg):
         |        return '''<h1>DotPlot is not available.</h1>
         |                  <p>Reasons are: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |        ${opDesc.createPlotlyFigure()}
         |        if table.empty:
         |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
         |            return
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin

    val generatedPythonCode =
      opDesc.generatePythonCode(null) // null value for OperatorSchemaInfo for simplicity

    assert(generatedPythonCode === expectedPythonCode)
  }

  it should "return 1 worker" in {
    assert(opDesc.numWorkers() === 1)
  }
}
