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

import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.metrics.Meter

/**
 * Strongly-typed façade for Texera-emitted metrics.
 *
 * Cardinality safety is enforced by the API surface, not by
 * documentation: there is no public method that accepts an arbitrary
 * string as a label key or value. The only labels that ever land on
 * an instrument are the two enums [[Outcome]] and [[WorkflowKind]],
 * each restricted to a fixed set. ``workflow.id`` / ``execution.id``
 * are deliberately NOT metric labels — per-execution detail belongs
 * in traces and logs, joined on ``trace_id`` at query time.
 *
 * Histogram bucket bounds are hard-coded constants so they can't be
 * coerced by request input. The OTel SDK applies its own default
 * attribute-value-length cap to anything that does slip through.
 */
object TexeraMetrics extends LazyLogging {

  /** Outcome enum, the only mutable label on lifecycle counters. */
  sealed abstract class Outcome(val name: String)
  object Outcome {
    case object Success extends Outcome("success")
    case object Failure extends Outcome("failure")
    case object Cancelled extends Outcome("cancelled")
  }

  /** Workflow kind enum. Distinguishes interactive vs. scheduled
   *  workflows for the dashboard's basic split — extend deliberately. */
  sealed abstract class WorkflowKind(val name: String)
  object WorkflowKind {
    case object Interactive extends WorkflowKind("interactive")
    case object Scheduled extends WorkflowKind("scheduled")
    case object Unknown extends WorkflowKind("unknown")
  }

  private val OutcomeKey: AttributeKey[String] = AttributeKey.stringKey("texera.outcome")
  private val WorkflowKindKey: AttributeKey[String] = AttributeKey.stringKey("texera.workflow.kind")

  /** Histogram bucket bounds in seconds. Hard-coded — constants so
   *  request input can't reshape the histogram. Range covers
   *  fast (<1s) to long (>1h) workflows. */
  private val DurationBuckets: java.util.List[java.lang.Double] = {
    val builder = new java.util.ArrayList[java.lang.Double]()
    Seq(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0)
      .foreach(b => builder.add(java.lang.Double.valueOf(b)))
    builder
  }

  private val InstrumentationScope = "org.apache.texera"

  // Instruments are lazy + memoised. The first call after SDK init
  // builds them against the active GlobalOpenTelemetry meter; once
  // built they hold the meter instance, so a later GlobalOpenTelemetry
  // reset (in tests) wouldn't be visible here — see [[resetForTest]].
  @volatile private var _starts: io.opentelemetry.api.metrics.LongCounter = _
  @volatile private var _completions: io.opentelemetry.api.metrics.LongCounter = _
  @volatile private var _cancellations: io.opentelemetry.api.metrics.LongCounter = _
  // `texera.workflow.active` is an OBSERVABLE gauge, not a manual up/down
  // counter. A manual +1/-1 counter leaks whenever a run starts but its
  // terminal event never fires (the controller is killed, the process
  // restarts mid-run, or the +1 and -1 are split across the engine and web
  // tiers and one side is missed) — the gauge then drifts upward forever and
  // the dashboard shows phantom "active" executions. An observable gauge
  // instead reports the TRUE in-progress count from the live execution
  // registry on every collection, so it cannot leak. The count is supplied
  // by the host process via [[setActiveExecutionsSupplier]].
  @volatile private var _active: io.opentelemetry.api.metrics.ObservableLongGauge = _
  @volatile private var _duration: io.opentelemetry.api.metrics.DoubleHistogram = _

  // Supplier of the current in-progress execution count. Defaults to 0 until
  // the host process registers the real source (so a process that never
  // registers reports a flat 0 rather than a wrong number). Read by the gauge
  // callback on every metric collection.
  @volatile private var activeExecutionsSupplier: () => Long = () => 0L

  /** Register the authoritative source of "currently active executions".
   *  The supplier is polled on every metric collection, so the gauge always
   *  reflects ground truth and can never leak. Called once at process
   *  startup (e.g. by ComputingUnitMaster). */
  def setActiveExecutionsSupplier(supplier: () => Long): Unit = synchronized {
    activeExecutionsSupplier = supplier
    ensureBound()
  }

  /** Bind instruments to the current global meter. Idempotent — the
   *  first call wins; later calls are no-ops. Tests can call
   *  [[bindForTest]] with an explicit Meter, then [[resetForTest]] to
   *  rebind. */
  def ensureBound(): Unit = synchronized {
    if (_starts == null) bind(GlobalOpenTelemetry.getMeter(InstrumentationScope))
  }

  private[observability] def bindForTest(meter: Meter): Unit = synchronized {
    bind(meter)
  }

  private[observability] def resetForTest(): Unit = synchronized {
    _starts = null
    _completions = null
    _cancellations = null
    // The observable gauge registered a collection callback — close it so the
    // previous test's meter provider stops being polled after it's discarded.
    if (_active != null) _active.close()
    _active = null
    _duration = null
  }

  private def bind(meter: Meter): Unit = {
    _starts = meter
      .counterBuilder("texera.workflow.starts")
      .setDescription("Number of workflow executions started.")
      .build()
    // Completions carry texera.outcome={success|failure}. Both success
    // and self-terminating failures land here so the success/failure-rate
    // queries (non-success ÷ all completions) have a denominator that
    // means "runs that finished on their own". User-initiated kills are
    // NOT completions — see _cancellations.
    _completions = meter
      .counterBuilder("texera.workflow.completions")
      .setDescription("Number of workflow executions that ran to completion (success or failure).")
      .build()
    // Cancellations (user kills) are tracked separately so they decrement
    // the active gauge without polluting the success/failure-rate
    // denominator. Deliberately label-free apart from workflow.kind.
    _cancellations = meter
      .counterBuilder("texera.workflow.cancellations")
      .setDescription("Number of workflow executions cancelled/killed before finishing.")
      .build()
    // Observable gauge: the callback runs on each collection and reports the
    // live in-progress count, so the value cannot leak. No per-execution
    // labels (cardinality-safe); the dashboard queries sum(texera_workflow_active).
    _active = meter
      .gaugeBuilder("texera.workflow.active")
      .ofLongs()
      .setDescription("Number of workflow executions currently in progress (observed from the live registry).")
      .buildWithCallback(obs => obs.record(activeExecutionsSupplier()))
    _duration = meter
      .histogramBuilder("texera.workflow.duration")
      .setDescription("End-to-end duration of a workflow execution.")
      .setUnit("s")
      .setExplicitBucketBoundariesAdvice(DurationBuckets)
      .build()
    logger.info(s"Texera metric instruments bound to meter scope '$InstrumentationScope'")
  }

  // ---- Public emitters — typed, no untyped escape hatch --------------

  def recordStart(kind: WorkflowKind): Unit = {
    ensureBound()
    logger.debug(s"metric: workflow started (kind=${kind.name}) — starts +1")
    _starts.add(1L, Attributes.of(WorkflowKindKey, kind.name))
    // `active` is no longer mutated here — it is an observable gauge sourced
    // from the live execution registry (see setActiveExecutionsSupplier).
  }

  def recordCompletion(kind: WorkflowKind, durationSec: Double): Unit = {
    ensureBound()
    logger.debug(
      f"metric: workflow completed successfully (kind=${kind.name}, ${durationSec}%.1fs) — completions +1"
    )
    val attrs = Attributes.of(OutcomeKey, Outcome.Success.name, WorkflowKindKey, kind.name)
    _completions.add(1L, attrs)
    _duration.record(durationSec, attrs)
  }

  def recordFailure(kind: WorkflowKind, durationSec: Double): Unit = {
    ensureBound()
    logger.debug(
      f"metric: workflow failed (kind=${kind.name}, ${durationSec}%.1fs) — completions +1 (outcome=failure)"
    )
    // A failure is a completion with outcome=failure — it shares the
    // completions counter (and duration histogram) with successes so the
    // failure-rate query has both numerator and denominator.
    val attrs = Attributes.of(OutcomeKey, Outcome.Failure.name, WorkflowKindKey, kind.name)
    _completions.add(1L, attrs)
    _duration.record(durationSec, attrs)
  }

  /** A user-initiated kill/cancel. Bumps a dedicated counter. Deliberately
   *  NOT recorded as a completion: a cancelled run never finished on its own,
   *  so it must not drag down the success rate. No duration is recorded for
   *  the same reason — a killed run's wall-clock time is not a real runtime
   *  and would skew the duration percentiles. (The active gauge is observed
   *  from the live registry and needs no decrement here.) */
  def recordCancellation(kind: WorkflowKind): Unit = {
    ensureBound()
    logger.debug(s"metric: workflow cancelled (kind=${kind.name}) — cancellations +1")
    val attrs = Attributes.of(WorkflowKindKey, kind.name)
    _cancellations.add(1L, attrs)
  }
}
