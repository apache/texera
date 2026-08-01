/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.visualization.treeplot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, SampleColumn}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

import javax.validation.constraints.NotNull

/**
  * Visualization Operator for Tree Plots.
  *
  * This operator uses a single column containing parent-child pairs
  * to construct and visualize an interactive, top-down tree that automatically
  * sizes itself and supports intuitive scroll/pinch zooming.
  */
class TreePlotOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  @JsonProperty(value = "Edge List Column", required = true)
  @JsonSchemaTitle("Edge List Column")
  @JsonPropertyDescription("Column with [parent, child] pairs")
  @AutofillAttributeName
  @SampleColumn("edge_pair")
  @NotNull(message = "Edge List Column cannot be empty")
  var edgeListColumn: EncodableString = ""

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      userFriendlyName = "Tree Plot",
      operatorDescription =
        "Visualize hierarchical data as a top-down, interactive, auto-sizing tree",
      operatorGroupName = OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    Map(
      operatorInfo.outputPorts.head.id -> Schema()
        .add("html-content", AttributeType.STRING)
    )
  }

  override def generatePythonCode(): String = {
    assert(edgeListColumn.nonEmpty, "Edge List Column cannot be empty")

    pyb"""
       |from pytexera import *
       |
       |import plotly.graph_objects as go
       |import plotly.io
       |import igraph
       |from igraph import Graph, EdgeSeq
       |import pandas as pd
       |import ast
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    def render_error(self, error_msg):
       |        return f'''<h1>Tree Plot is not available.</h1>
       |                   <p>Reason: {error_msg} </p>'''
       |
       |    def make_annotations(self, pos, text):
       |        font_color = 'rgb(250,250,250)'
       |        node_color = '#6175c1'
       |        font_size = 10
       |
       |        annotations = []
       |        for k, (node_name, coords) in enumerate(pos.items()):
       |            annotations.append(
       |                dict(
       |                    text=text[k],
       |                    x=coords[0],
       |                    y=coords[1],
       |                    xref='x1', yref='y1',
       |                    font=dict(color=font_color, size=font_size),
       |                    showarrow=False,
       |                    align='center',
       |                    bordercolor='rgb(50,50,50)',
       |                    borderwidth=1,
       |                    borderpad=5,
       |                    bgcolor=node_color,
       |                    opacity=0.8
       |                )
       |            )
       |        return annotations
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        if table.empty:
       |            yield {'html-content': self.render_error("Input table is empty.")}
       |            return
       |
       |        edges = []
       |        for item in table[$edgeListColumn].dropna():
       |            try:
       |                edge = ast.literal_eval(str(item))
       |                if isinstance(edge, (list, tuple)) and len(edge) == 2:
       |                    edges.append(list(edge))
       |            except (ValueError, SyntaxError):
       |                pass
       |
       |        if not edges:
       |            yield {'html-content': self.render_error("No valid [parent, child] pairs found in column " + $edgeListColumn + ".")}
       |            return
       |
       |        G = Graph.TupleList(edges, directed=True)
       |        labels = G.vs['name']
       |
       |        layout_algorithm = 'rt'
       |        try:
       |            lay = G.layout(layout_algorithm)
       |        except Exception as e:
       |             yield {'html-content': self.render_error(f"Layout algorithm '{layout_algorithm}' failed: {e}")}
       |             return
       |
       |        HORIZONTAL_DENSITY = 120
       |        VERTICAL_DENSITY = 120
       |        PADDING = 200
       |        MIN_WIDTH = 800
       |        MIN_HEIGHT = 600
       |
       |        if len(lay.coords) > 1:
       |            x_coords, y_coords = zip(*lay.coords)
       |            x_range = max(x_coords) - min(x_coords)
       |            y_range = max(y_coords) - min(y_coords)
       |            plot_width = max(MIN_WIDTH, x_range * HORIZONTAL_DENSITY + PADDING)
       |            plot_height = max(MIN_HEIGHT, y_range * VERTICAL_DENSITY + PADDING)
       |        else:
       |            plot_width = MIN_WIDTH
       |            plot_height = MIN_HEIGHT
       |
       |        # Invert the y-axis to make the tree grow top-down.
       |        position = {k: (lay[k][0], -lay[k][1]) for k in range(len(labels))}
       |
       |        Xe = []
       |        Ye = []
       |        for edge in G.get_edgelist():
       |            Xe += [position[edge[0]][0], position[edge[1]][0], None]
       |            Ye += [position[edge[0]][1], position[edge[1]][1], None]
       |
       |        fig = go.Figure()
       |
       |        fig.add_trace(go.Scatter(x=Xe, y=Ye, mode='lines',
       |                                 line=dict(color='rgb(210,210,210)', width=1),
       |                                 hoverinfo='none'))
       |
       |        axis = dict(showline=False, zeroline=False, showgrid=False, showticklabels=False)
       |
       |        fig.update_layout(title='Tree Plot',
       |                          width=int(plot_width),
       |                          height=int(plot_height),
       |                          annotations=self.make_annotations(position, labels),
       |                          font_size=12,
       |                          showlegend=False,
       |                          xaxis=axis,
       |                          yaxis=axis,
       |                          margin=dict(l=40, r=40, b=85, t=100),
       |                          dragmode='pan',
       |                          hovermode='closest',
       |                          plot_bgcolor='rgb(248,248,248)')
       |
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
       |        yield {'html-content': html}
       |
       |""".encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    s"""import ast
       |
       |try:
       |    from igraph import Graph
       |except ImportError:
       |    Graph = None
       |
       |def render_error(error_msg):
       |    return f'''<h1>Tree Plot is not available.</h1>
       |                   <p>Reason: {error_msg} </p>'''
       |
       |def make_annotations(pos):
       |    font_color = 'rgb(250,250,250)'
       |    node_color = '#6175c1'
       |    font_size = 10
       |    annotations = []
       |    for node_name, coords in pos.items():
       |        annotations.append(
       |            dict(
       |                text=str(node_name),
       |                x=coords[0],
       |                y=coords[1],
       |                xref='x1', yref='y1',
       |                font=dict(color=font_color, size=font_size),
       |                showarrow=False,
       |                align='center',
       |                bordercolor='rgb(50,50,50)',
       |                borderwidth=1,
       |                borderpad=5,
       |                bgcolor=node_color,
       |                opacity=0.8
       |            )
       |        )
       |    return annotations
       |
       |def compute_tree_layout(edges):
       |    if Graph is not None:
       |        graph = Graph.TupleList(edges, directed=True)
       |        labels = graph.vs['name']
       |        layout = graph.layout('rt')
       |        return {
       |            labels[index]: (layout[index][0], -layout[index][1])
       |            for index in range(len(labels))
       |        }
       |
       |    nodes = []
       |    children = {}
       |    parents = {}
       |    for parent, child in edges:
       |        if parent not in nodes:
       |            nodes.append(parent)
       |        if child not in nodes:
       |            nodes.append(child)
       |        children.setdefault(parent, []).append(child)
       |        parents[child] = parent
       |
       |    roots = [node for node in nodes if node not in parents] or nodes[:1]
       |    position = {}
       |    next_x = 0
       |
       |    def place(node, depth):
       |        nonlocal next_x
       |        child_nodes = children.get(node, [])
       |        if not child_nodes:
       |            x = next_x
       |            next_x += 1
       |        else:
       |            child_x = [place(child, depth + 1) for child in child_nodes]
       |            x = sum(child_x) / len(child_x)
       |        position[node] = (x, -depth)
       |        return x
       |
       |    for root in roots:
       |        place(root, 0)
       |    return position
       |
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("Input table is empty."))
       |else:
       |    edges = []
       |    for item in in1df[${pyStringLiteral(edgeListColumn)}].dropna():
       |        try:
       |            edge = ast.literal_eval(str(item))
       |            if isinstance(edge, (list, tuple)) and len(edge) == 2:
       |                edges.append(list(edge))
       |        except (ValueError, SyntaxError):
       |            pass
       |
       |    if not edges:
       |        with open("output.html", "w", encoding="utf-8") as output:
       |            output.write(render_error("No valid [parent, child] pairs found in column " + ${pyStringLiteral(
      edgeListColumn
    )} + "."))
       |    else:
       |        try:
       |            position = compute_tree_layout(edges)
       |
       |            HORIZONTAL_DENSITY = 120
       |            VERTICAL_DENSITY = 120
       |            PADDING = 200
       |            MIN_WIDTH = 800
       |            MIN_HEIGHT = 600
       |
       |            if len(position) > 1:
       |                x_coords, y_coords = zip(*position.values())
       |                x_range = max(x_coords) - min(x_coords)
       |                y_range = max(y_coords) - min(y_coords)
       |                plot_width = max(MIN_WIDTH, x_range * HORIZONTAL_DENSITY + PADDING)
       |                plot_height = max(MIN_HEIGHT, y_range * VERTICAL_DENSITY + PADDING)
       |            else:
       |                plot_width = MIN_WIDTH
       |                plot_height = MIN_HEIGHT
       |
       |            Xe = []
       |            Ye = []
       |            for parent, child in edges:
       |                Xe += [position[parent][0], position[child][0], None]
       |                Ye += [position[parent][1], position[child][1], None]
       |
       |            fig = go.Figure()
       |            fig.add_trace(go.Scatter(x=Xe, y=Ye, mode='lines',
       |                                     line=dict(color='rgb(210,210,210)', width=1),
       |                                     hoverinfo='none'))
       |            axis = dict(showline=False, zeroline=False, showgrid=False, showticklabels=False)
       |            fig.update_layout(title='Tree Plot',
       |                              width=int(plot_width),
       |                              height=int(plot_height),
       |                              annotations=make_annotations(position),
       |                              font_size=12,
       |                              showlegend=False,
       |                              xaxis=axis,
       |                              yaxis=axis,
       |                              margin=dict(l=40, r=40, b=85, t=100),
       |                              dragmode='pan',
       |                              hovermode='closest',
       |                              plot_bgcolor='rgb(248,248,248)')
       |            fig.write_json("output.json")
       |            fig.write_html("output.html")
       |            print("Tree plot saved to output.html")
       |        except Exception as e:
       |            with open("output.html", "w", encoding="utf-8") as output:
       |                output.write(render_error(str(e)))""".stripMargin
  }

}
