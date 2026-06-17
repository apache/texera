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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.visualization.DotPlot.DotPlotOpDesc
import org.apache.texera.amber.operator.visualization.ImageViz.ImageVisualizerOpDesc
import org.apache.texera.amber.operator.visualization.IcicleChart.IcicleChartOpDesc
import org.apache.texera.amber.operator.visualization.boxViolinPlot.BoxViolinPlotOpDesc
import org.apache.texera.amber.operator.visualization.bubbleChart.BubbleChartOpDesc
import org.apache.texera.amber.operator.visualization.bulletChart.BulletChartOpDesc
import org.apache.texera.amber.operator.visualization.candlestickChart.CandlestickChartOpDesc
import org.apache.texera.amber.operator.visualization.carpetPlot.CarpetPlotOpDesc
import org.apache.texera.amber.operator.visualization.choroplethMap.ChoroplethMapOpDesc
import org.apache.texera.amber.operator.visualization.continuousErrorBands.ContinuousErrorBandsOpDesc
import org.apache.texera.amber.operator.visualization.contourPlot.ContourPlotOpDesc
import org.apache.texera.amber.operator.visualization.ScatterMatrixChart.ScatterMatrixChartOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformVerificationRunnerSpec extends AnyFlatSpec with Matchers {
  import TransformVerificationRunner._

  "disposition" should "flag knownIssues operators with the triage reason" in {
    disposition(classOf[UnionOpDesc]) match {
      case Flagged(reason) => reason should include("known issue")
      case other           => fail(s"expected Flagged, got $other")
    }
  }

  it should "route visualization operators with JSON validation support to the visualization tier" in {
    disposition(classOf[BarChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[BulletChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[CandlestickChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[CarpetPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ChoroplethMapOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ContinuousErrorBandsOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ContourPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[DotPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[IcicleChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[BubbleChartOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[BoxViolinPlotOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ImageVisualizerOpDesc]) shouldBe Runnable("visualization")
    disposition(classOf[ScatterMatrixChartOpDesc]) shouldBe Runnable("visualization")
  }

  it should "route operators with a curated handler to the curated tier" in {
    disposition(classOf[IntersectOpDesc]) shouldBe Runnable("curated")
  }

  it should "route auto-configurable operators to the auto tier" in {
    disposition(classOf[LimitOpDesc]) shouldBe Runnable("auto")
  }

  // End-to-end smoke of the curated path: Intersect is fast, JVM-native, and
  // exercises two input ports + the order-insensitive comparator branch.
  "run" should "verify IntersectOpDesc end-to-end via the curated tier" in {
    TransformVerificationRunner.run(classOf[IntersectOpDesc])
  }

  // End-to-end smoke of the auto path: Limit is single-input single-output
  // and its config is fully derivable.
  it should "verify LimitOpDesc end-to-end via the auto tier" in {
    TransformVerificationRunner.run(classOf[LimitOpDesc])
  }

  it should "verify DotPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[DotPlotOpDesc])
  }

  it should "verify BarChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BarChartOpDesc])
  }

  it should "verify BulletChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BulletChartOpDesc])
  }

  it should "verify CandlestickChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[CandlestickChartOpDesc])
  }

  it should "verify CarpetPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[CarpetPlotOpDesc])
  }

  it should "verify ChoroplethMapOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ChoroplethMapOpDesc])
  }

  it should "verify ContinuousErrorBandsOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ContinuousErrorBandsOpDesc])
  }

  it should "verify ContourPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ContourPlotOpDesc])
  }

  it should "verify IcicleChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[IcicleChartOpDesc])
  }

  it should "verify BubbleChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BubbleChartOpDesc])
  }

  it should "verify BoxViolinPlotOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[BoxViolinPlotOpDesc])
  }

  it should "verify ImageVisualizerOpDesc end-to-end via HTML comparison" in {
    TransformVerificationRunner.run(classOf[ImageVisualizerOpDesc])
  }

  it should "verify ScatterMatrixChartOpDesc end-to-end via Plotly JSON comparison" in {
    TransformVerificationRunner.run(classOf[ScatterMatrixChartOpDesc])
  }
}
