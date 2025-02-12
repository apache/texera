package edu.uci.ics.amber.operator.visualization.networkGraph

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.PythonOperatorDescriptor

class NetworkGraphOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Source Column")
  @JsonPropertyDescription("Source node for edge in graph")
  @AutofillAttributeName
  var source: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Destination Column")
  @JsonPropertyDescription("Destination node for edge in graph")
  @AutofillAttributeName
  var destination: String = ""

  @JsonProperty(defaultValue = "Network Graph")
  @JsonSchemaTitle("Title")
  var title: String = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Network Graph",
      "Visualize data in a network graph",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )
  def manipulateTable(): String = {
    assert(source.nonEmpty)
    assert(destination.nonEmpty)
    s"""
       |        table = table.dropna(subset = ['$source']) #remove missing values
       |        table = table.dropna(subset = ['$destination']) #remove missing values
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |import pandas as pd
         |import plotly.graph_objects as go
         |import plotly.io
         |import json
         |import pickle
         |import plotly
         |import networkx as nx
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>Network graph is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if not table.empty:
         |            sources = table['$source']
         |            destinations = table['$destination']
         |            nodes = set(sources + destinations)
         |            G = nx.Graph()
         |            for node in nodes:
         |                G.add_node(node)
         |            for i, j in table.iterrows():
         |                G.add_edges_from([(j['$source'], j['$destination'])])
         |            pos = nx.spring_layout(G, k=0.5, iterations=50)
         |            for n, p in pos.items():
         |                G.nodes[n]['pos'] = p
         |
         |            edge_trace = go.Scatter(
         |                x=[],
         |                y=[],
         |                name='Edges',
         |                line=dict(width=0.5, color='#888'),
         |                hoverinfo='none',
         |                mode='lines',
         |                visible=True
         |            )
         |
         |            for edge in G.edges():
         |                x0, y0 = G.nodes[edge[0]]['pos']
         |                x1, y1 = G.nodes[edge[1]]['pos']
         |                edge_trace['x'] += tuple([x0, x1, None])
         |                edge_trace['y'] += tuple([y0, y1, None])
         |
         |            node_trace = go.Scatter(
         |                x=[],
         |                y=[],
         |                name='Nodes',
         |                text=[],
         |                mode='markers',
         |                hoverinfo='text',
         |                visible=True,
         |                marker=dict(
         |                    showscale=True,
         |                    colorscale='plasma',
         |                    reversescale=True,
         |                    color=[],
         |                    size=15,
         |                    colorbar=dict(
         |                        thickness=10,
         |                        title='Node Connections',
         |                        xanchor='left',
         |                        titleside='right'
         |                    ),
         |                    line=dict(width=0)
         |                )
         |            )
         |
         |            for node in G.nodes():
         |                x, y = G.nodes[node]['pos']
         |                node_trace['x'] += tuple([x])
         |                node_trace['y'] += tuple([y])
         |
         |            for node, adjacencies in enumerate(G.adjacency()):
         |                node_trace['marker']['color'] += tuple([len(adjacencies[1])])
         |                node_info = str(adjacencies[0]) + ': ' + str(len(adjacencies[1])) + ' connections.'
         |                node_trace['text'] += tuple([node_info])
         |
         |            fig = go.Figure(
         |                data=[edge_trace, node_trace],
         |                layout=go.Layout(
         |                    title='<br>$title',
         |                    hovermode='closest',
         |                    showlegend=False,
         |                    margin=dict(b=20, l=5, r=5, t=40),
         |                    annotations=[
         |                        dict(
         |                            text='',
         |                            showarrow=False,
         |                            xref="paper",
         |                            yref="paper"
         |                        )
         |                    ],
         |                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
         |                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
         |                )
         |            )
         |            fig.update_layout(
         |                margin=dict(l=0, r=0, t=0, b=0),
         |                legend=dict(
         |                    itemclick=False,
         |                    itemdoubleclick=False
         |                )
         |            )
         |
         |            html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        else:
         |            html = self.render_error('Table should not have any empty/null values or fields.')
         |
         |        yield {'html-content': html}
         |
         |""".stripMargin
    finalCode
  }

}
