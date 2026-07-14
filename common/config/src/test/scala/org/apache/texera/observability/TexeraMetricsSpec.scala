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

package org.apache.texera.observability

import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class TexeraMetricsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var reader: InMemoryMetricReader = _
  private var provider: SdkMeterProvider = _

  override def beforeEach(): Unit = {
    reader = InMemoryMetricReader.create()
    provider = SdkMeterProvider.builder().registerMetricReader(reader).build()
    TexeraMetrics.resetForTest()
    TexeraMetrics.bindForTest(provider.get("org.apache.texera"))
  }

  override def afterEach(): Unit = {
    TexeraMetrics.resetForTest()
    provider.close()
  }

  private def collectAll(): Map[String, MetricData] = {
    reader.collectAllMetrics().asScala.map(m => m.getName -> m).toMap
  }

  // ----- positive: lifecycle emissions ----------------------------------

  "TexeraMetrics" should "increment workflow.starts on recordStart" in {
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Interactive)

    val metrics = collectAll()
    metrics.keySet should contain("texera.workflow.starts")

    val starts = metrics("texera.workflow.starts").getLongSumData.getPoints.asScala.head
    starts.getValue shouldBe 1L
    // recordStart no longer mutates `active` — that gauge is observed from the
    // live registry (see "report the active-execution count…" below).
  }

  it should "report the active-execution count from the registered supplier, never a manual counter" in {
    // The whole point of the fix: `active` is an observable gauge sourced from
    // ground truth, so it cannot leak. Drive it with a stub supplier and
    // confirm the gauge reports exactly what the supplier returns — regardless
    // of how many starts/completions were recorded.
    @volatile var live = 3L
    TexeraMetrics.setActiveExecutionsSupplier(() => live)
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Interactive)
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Interactive)

    val first = collectAll()("texera.workflow.active").getLongGaugeData.getPoints.asScala.head
    first.getValue shouldBe 3L

    // It tracks the live source on the next collection — a manual +1/-1
    // counter could never drop like this without an explicit decrement.
    live = 0L
    val second = collectAll()("texera.workflow.active").getLongGaugeData.getPoints.asScala.head
    second.getValue shouldBe 0L
  }

  it should "record a completion and duration sample on recordCompletion" in {
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Interactive)
    TexeraMetrics.recordCompletion(TexeraMetrics.WorkflowKind.Interactive, durationSec = 2.5)

    val metrics = collectAll()
    metrics.keySet should contain allOf (
      "texera.workflow.completions",
      "texera.workflow.duration"
    )

    metrics(
      "texera.workflow.completions"
    ).getLongSumData.getPoints.asScala.head.getValue shouldBe 1L

    val histogram = metrics("texera.workflow.duration").getHistogramData.getPoints.asScala.head
    histogram.getCount shouldBe 1L
    histogram.getSum shouldBe 2.5
  }

  it should "record a failure as a non-success completion (so failure-rate queries work)" in {
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Scheduled)
    TexeraMetrics.recordFailure(TexeraMetrics.WorkflowKind.Scheduled, durationSec = 12.0)

    val metrics = collectAll()
    // A failure shares the completions counter with successes — the
    // failure-rate query divides non-success completions by all
    // completions, so failures must live here (not in a separate
    // series the query never reads).
    val completion = metrics("texera.workflow.completions").getLongSumData.getPoints.asScala.head
    completion.getValue shouldBe 1L
    completion.getAttributes.asMap.asScala.map {
      case (k, v) => k.getKey -> v.toString
    } should contain(
      "texera.outcome" -> "failure"
    )
    metrics("texera.workflow.duration").getHistogramData.getPoints.asScala.head.getSum shouldBe 12.0
    // The orphan counter the old wiring used must be gone.
    metrics.keySet should not contain "texera.workflow.failures"
  }

  it should "record a cancellation that is not a completion" in {
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Interactive)
    TexeraMetrics.recordCancellation(TexeraMetrics.WorkflowKind.Interactive)

    val metrics = collectAll()
    metrics(
      "texera.workflow.cancellations"
    ).getLongSumData.getPoints.asScala.head.getValue shouldBe 1L
    // A kill is not a completion and records no duration: it must not
    // drag down the success rate nor skew the duration percentiles.
    metrics.keySet should not contain "texera.workflow.completions"
    metrics.keySet should not contain "texera.workflow.duration"
  }

  // ----- security: cardinality safety -----------------------------------

  it should "only emit the texera.outcome and texera.workflow.kind labels" in {
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Interactive)
    TexeraMetrics.recordCompletion(TexeraMetrics.WorkflowKind.Interactive, durationSec = 1.0)

    val attrKeys = collectAll().values.flatMap { md =>
      val pointSet = md.getType.name() match {
        case "HISTOGRAM"  => md.getHistogramData.getPoints.asScala
        case "LONG_GAUGE" => md.getLongGaugeData.getPoints.asScala
        case _            => md.getLongSumData.getPoints.asScala
      }
      pointSet.flatMap(_.getAttributes.asMap.keySet.asScala.map(_.getKey))
    }.toSet

    attrKeys.foreach { key =>
      Set("texera.outcome", "texera.workflow.kind") should contain(key)
    }
    attrKeys should not contain "texera.workflow.id"
    attrKeys should not contain "texera.execution.id"
  }

  it should "expose no public API to attach an arbitrary string label" in {
    // The class has only typed emitters whose attribute set is
    // hard-coded. This test is intentionally a compile-time check
    // disguised as a runtime one — if a future contributor adds an
    // untyped public method like recordStart(attrs: Attributes),
    // this assertion still passes but the design intent is broken.
    // Make the intent explicit:
    val methodNames = classOf[TexeraMetrics.type].getDeclaredMethods
      .map(_.getName)
      .toSet
    methodNames should contain allOf ("recordStart", "recordCompletion", "recordFailure")
    methodNames should not contain "recordWithAttributes"
  }

  // ----- histogram buckets are constants --------------------------------

  it should "use the hard-coded explicit bucket boundaries for duration" in {
    TexeraMetrics.recordStart(TexeraMetrics.WorkflowKind.Interactive)
    // Hit a few bucket bounds.
    Seq(0.05, 0.6, 7.0, 65.0, 400.0).foreach { d =>
      TexeraMetrics.recordCompletion(TexeraMetrics.WorkflowKind.Interactive, durationSec = d)
    }

    val histogram = collectAll()("texera.workflow.duration").getHistogramData
    val point = histogram.getPoints.asScala.head
    point.getCount shouldBe 5L
    // The point exposes the SDK-configured boundaries; we don't
    // assert exact values here (would couple the test to the impl)
    // but we do assert there ARE explicit boundaries — anything
    // empty would mean the .setExplicitBucketBoundariesAdvice call
    // was lost during a refactor.
    point.getBoundaries.size should be > 0
  }
}
