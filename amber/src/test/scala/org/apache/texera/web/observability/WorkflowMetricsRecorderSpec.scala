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

package org.apache.texera.web.observability

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.{InstrumentType, SdkMeterProvider}
import io.opentelemetry.sdk.metrics.`export`.{MetricExporter, PeriodicMetricReader}
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import org.apache.texera.amber.core.virtualidentity.ExecutionIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.observability.WorkflowMetrics
import org.apache.texera.observability.WorkflowMetrics.WorkflowKind
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._

/**
  * Exercises the execution-state to metric mapping in WorkflowMetricsRecorder
  * against a real in-memory meter. WorkflowMetrics.bindForTest is package
  * private to common/observability, so we drive the binding the production way:
  * install an SDK into GlobalOpenTelemetry and let ensureBound pick it up.
  */
class WorkflowMetricsRecorderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val exporter = new CapturingMetricExporter
  private var provider: SdkMeterProvider = _
  private val nextId = new AtomicLong(1000)
  private def newId(): ExecutionIdentity = ExecutionIdentity(nextId.incrementAndGet())

  override def beforeAll(): Unit = {
    GlobalOpenTelemetry.resetForTest()
    val reader = PeriodicMetricReader.builder(exporter).setInterval(Duration.ofHours(1)).build()
    provider = SdkMeterProvider.builder().registerMetricReader(reader).build()
    GlobalOpenTelemetry.set(OpenTelemetrySdk.builder().setMeterProvider(provider).build())
    // Rebind the instruments to the SDK set above; a prior suite's run may have bound
    // them to the no-op global before this suite installed its own SDK.
    WorkflowMetrics.resetForTest()
  }

  override def afterAll(): Unit = {
    // Unbind before closing the provider so a later suite rebinds instead of recording
    // into a closed one.
    WorkflowMetrics.resetForTest()
    if (provider != null) provider.close()
    GlobalOpenTelemetry.resetForTest()
  }

  // Cumulative total of a counter, summed over all attribute points.
  private def counter(name: String): Long = {
    provider.forceFlush().join(5, TimeUnit.SECONDS)
    exporter.latest.asScala
      .find(_.getName == name)
      .map(_.getLongSumData.getPoints.asScala.map(_.getValue.longValue()).sum)
      .getOrElse(0L)
  }

  // Cumulative sample count of a histogram, summed over all attribute points.
  private def histogramCount(name: String): Long = {
    provider.forceFlush().join(5, TimeUnit.SECONDS)
    exporter.latest.asScala
      .find(_.getName == name)
      .map(_.getHistogramData.getPoints.asScala.map(_.getCount).sum)
      .getOrElse(0L)
  }

  "onStart" should "increment texera.workflow.starts" in {
    val before = counter("texera.workflow.starts")
    WorkflowMetricsRecorder.onStart(newId(), WorkflowKind.Interactive)
    counter("texera.workflow.starts") shouldBe before + 1
  }

  "onStateChange into COMPLETED" should "record a completion and a duration sample" in {
    val id = newId()
    WorkflowMetricsRecorder.onStart(id, WorkflowKind.Interactive)
    val beforeCompletions = counter("texera.workflow.completions")
    val beforeDurations = histogramCount("texera.workflow.duration")
    WorkflowMetricsRecorder.onStateChange(
      id,
      WorkflowAggregatedState.RUNNING,
      WorkflowAggregatedState.COMPLETED
    )
    counter("texera.workflow.completions") shouldBe beforeCompletions + 1
    histogramCount("texera.workflow.duration") shouldBe beforeDurations + 1
  }

  "onStateChange into FAILED" should "record a completion (outcome=failure) and a duration sample" in {
    val id = newId()
    WorkflowMetricsRecorder.onStart(id, WorkflowKind.Scheduled)
    val beforeCompletions = counter("texera.workflow.completions")
    val beforeDurations = histogramCount("texera.workflow.duration")
    WorkflowMetricsRecorder.onStateChange(
      id,
      WorkflowAggregatedState.RUNNING,
      WorkflowAggregatedState.FAILED
    )
    counter("texera.workflow.completions") shouldBe beforeCompletions + 1
    histogramCount("texera.workflow.duration") shouldBe beforeDurations + 1
  }

  "onStateChange into KILLED" should "record a cancellation and no duration sample" in {
    val id = newId()
    WorkflowMetricsRecorder.onStart(id, WorkflowKind.Interactive)
    val beforeCancellations = counter("texera.workflow.cancellations")
    val beforeDurations = histogramCount("texera.workflow.duration")
    WorkflowMetricsRecorder.onStateChange(
      id,
      WorkflowAggregatedState.RUNNING,
      WorkflowAggregatedState.KILLED
    )
    counter("texera.workflow.cancellations") shouldBe beforeCancellations + 1
    histogramCount("texera.workflow.duration") shouldBe beforeDurations
  }

  "onStateChange to a non-terminal state" should "record nothing" in {
    val id = newId()
    WorkflowMetricsRecorder.onStart(id, WorkflowKind.Interactive)
    val before = counter("texera.workflow.completions")
    WorkflowMetricsRecorder.onStateChange(
      id,
      WorkflowAggregatedState.RUNNING,
      WorkflowAggregatedState.PAUSED
    )
    counter("texera.workflow.completions") shouldBe before
  }

  "onStateChange whose old state is already terminal" should "record nothing (dedup)" in {
    val id = newId()
    WorkflowMetricsRecorder.onStart(id, WorkflowKind.Interactive)
    val before = counter("texera.workflow.completions")
    WorkflowMetricsRecorder.onStateChange(
      id,
      WorkflowAggregatedState.COMPLETED,
      WorkflowAggregatedState.FAILED
    )
    counter("texera.workflow.completions") shouldBe before
  }
}

/** Captures the most recent metric export so a test can read cumulative values. */
private class CapturingMetricExporter extends MetricExporter {
  @volatile private var captured: java.util.Collection[MetricData] =
    java.util.Collections.emptyList()
  def latest: java.util.Collection[MetricData] = captured
  override def `export`(metrics: java.util.Collection[MetricData]): CompletableResultCode = {
    captured = new java.util.ArrayList[MetricData](metrics)
    CompletableResultCode.ofSuccess()
  }
  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE
}
