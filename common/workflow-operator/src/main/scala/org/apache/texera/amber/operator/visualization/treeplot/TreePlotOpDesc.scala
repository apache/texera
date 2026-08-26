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
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

import javax.validation.constraints.NotNull

/**
  * Visualization Operator for Tree Plots.
  *
  * This operator uses a single column containing parent-child pairs
  * to construct and visualize an interactive, top-down tree that automatically
  * sizes itself and supports intuitive scroll/pinch zooming.
  */
class TreePlotOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "Edge List Column", required = true)
  @JsonSchemaTitle("Edge List Column")
  @JsonPropertyDescription("Column with [parent, child] pairs")
  @AutofillAttributeName
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
       |    def build_tree_layout(self, edges):
       |        # Tidy top-down tree: depth picks the row, a leaf takes the next
       |        # free column and a parent sits centred over its own children.
       |        labels = []
       |        known = set()
       |        for parent, child in edges:
       |            for node in (parent, child):
       |                if node not in known:
       |                    known.add(node)
       |                    labels.append(node)
       |
       |        children = {label: [] for label in labels}
       |        has_parent = set()
       |        seen = set()
       |        for parent, child in edges:
       |            if (parent, child) not in seen:
       |                seen.add((parent, child))
       |                children[parent].append(child)
       |            has_parent.add(child)
       |
       |        depth = {}
       |        column = {}
       |        claimed = {}
       |        placed = set()
       |        next_column = 0
       |
       |        def grow(root):
       |            nonlocal next_column
       |            placed.add(root)
       |            stack = [(root, 0, False)]
       |            while stack:
       |                node, level, folded = stack.pop()
       |                if folded:
       |                    kids = claimed[node]
       |                    if kids:
       |                        column[node] = sum(column[kid] for kid in kids) / len(kids)
       |                    else:
       |                        column[node] = next_column
       |                        next_column += 1
       |                    continue
       |                depth[node] = level
       |                # A node belongs to whichever parent reaches it first, so a
       |                # cycle or a shared child is never laid out twice.
       |                kids = [kid for kid in children[node] if kid not in placed]
       |                placed.update(kids)
       |                claimed[node] = kids
       |                stack.append((node, level, True))
       |                for kid in reversed(kids):
       |                    stack.append((kid, level + 1, False))
       |
       |        for label in labels:
       |            if label not in placed and label not in has_parent:
       |                grow(label)
       |        # Whatever is left sits in a cycle that no root can reach.
       |        for label in labels:
       |            if label not in placed:
       |                grow(label)
       |        # The y-axis is inverted here so the tree grows top-down.
       |        return labels, [(column[label], -depth[label]) for label in labels]
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
       |        try:
       |            labels, coords = self.build_tree_layout(edges)
       |        except Exception as e:
       |             yield {'html-content': self.render_error(f"Tree layout failed: {e}")}
       |             return
       |
       |        HORIZONTAL_DENSITY = 120
       |        VERTICAL_DENSITY = 120
       |        PADDING = 200
       |        MIN_WIDTH = 800
       |        MIN_HEIGHT = 600
       |
       |        if len(coords) > 1:
       |            x_coords, y_coords = zip(*coords)
       |            x_range = max(x_coords) - min(x_coords)
       |            y_range = max(y_coords) - min(y_coords)
       |            plot_width = max(MIN_WIDTH, x_range * HORIZONTAL_DENSITY + PADDING)
       |            plot_height = max(MIN_HEIGHT, y_range * VERTICAL_DENSITY + PADDING)
       |        else:
       |            plot_width = MIN_WIDTH
       |            plot_height = MIN_HEIGHT
       |
       |        position = {k: coords[k] for k in range(len(labels))}
       |        index_of = {label: k for k, label in enumerate(labels)}
       |
       |        Xe = []
       |        Ye = []
       |        for parent, child in edges:
       |            edge = (index_of[parent], index_of[child])
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
}
