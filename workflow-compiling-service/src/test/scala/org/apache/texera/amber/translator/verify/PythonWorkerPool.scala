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

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
  * Pools of persistent Python "worker" processes that eliminate the per-call
  * interpreter-boot + heavy-import cost the verify harness otherwise pays on
  * every subprocess spawn (profiled at ~260-310 ms, ~96% of a per-operator run;
  * the actual work on the canonical fixtures is ~ms). A worker imports its
  * heavy libraries ONCE at startup, then serves many jobs over its lifetime.
  *
  * Generic over the worker script: [[StandaloneRunner]] runs rendered standalone
  * scripts (`standalone_worker.py`), [[Comparator]] runs DataFrame comparisons
  * (`compare.py --serve`), and [[PyOpExecHarness]] runs platform operators
  * (`py_op_driver.py --serve`). Each distinct (resource, launchArgs, python, env)
  * combination gets its own sub-pool.
  *
  * Protocol (line-delimited JSON, shared by all worker scripts):
  *   startup   worker -> pool:  {"ready": true}
  *   request   pool -> worker:  <caller-supplied JSON object>\n
  *   response  worker -> pool:  {"exit": <int>, "stdout": "...", "stderr": "..."}\n
  *
  * Concurrency: the verify spec runs with ScalaTest `-P4`, so up to 4 calls can
  * be in flight per path. Each sub-pool holds up to [[maxWorkers]] workers; each
  * serves one job at a time (borrow -> run -> return). A worker script may chdir
  * per job, so a worker must never run two jobs at once — the borrow/return
  * discipline guarantees that.
  *
  * Robustness: an ordinary *job* failure comes back as an [[Outcome]] with
  * `exit != 0` (worker keeps running). Only a hard interpreter crash ends a
  * worker; the pool detects the EOF / broken pipe, discards it, and throws
  * [[WorkerDiedException]] so the caller can fall back to a one-shot subprocess
  * — behavior is then never worse than the pre-pool path.
  */
object PythonWorkerPool extends LazyLogging {

  /** Worker response: process-like exit code plus captured streams. */
  final case class Outcome(exit: Int, stdout: String, stderr: String)

  /** Thrown when a worker dies mid-job (hard crash / broken pipe). Callers
    * catch this and fall back to a one-shot subprocess.
    */
  final class WorkerDiedException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)

  /** Feature toggle. `VERIFY_PYTHON_WORKER=0` (or `false`/`off`) forces the old
    * one-subprocess-per-call paths everywhere — an escape hatch for debugging a
    * suspected isolation leak. Default on.
    */
  val enabled: Boolean =
    !sys.env.get("VERIFY_PYTHON_WORKER").map(_.trim.toLowerCase).exists(Set("0", "false", "off"))

  /** Max live workers per sub-pool. Defaults to 4 to match ScalaTest's `-P4`;
    * override via `VERIFY_PYTHON_WORKERS`.
    */
  private val maxWorkers: Int =
    sys.env
      .get("VERIFY_PYTHON_WORKERS")
      .flatMap(s => scala.util.Try(s.trim.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(4)

  /**
    * Run one job through a pooled worker for `resourcePath` launched with
    * `launchArgs` and extra environment `env` (e.g. PYTHONPATH for the
    * py_op_driver worker). `request` is the worker-specific JSON payload (the
    * pool does not interpret it). Throws [[WorkerDiedException]] on a hard
    * worker crash.
    */
  def run(
      resourcePath: String,
      launchArgs: Seq[String],
      pythonExe: String,
      request: ObjectNode,
      env: Map[String, String] = Map.empty
  ): Outcome = {
    // env is part of a worker's identity: one started with a different
    // PYTHONPATH is not interchangeable, so it gets its own sub-pool.
    val envKey = env.toSeq.sorted.map { case (k, v) => s"$k=$v" }
    val key = (resourcePath +: pythonExe +: (launchArgs ++ envKey)).mkString(" ")
    val pool =
      pools.computeIfAbsent(key, _ => new Pool(resourcePath, launchArgs, pythonExe, env))
    pool.run(request)
  }

  private val pools = new ConcurrentHashMap[String, Pool]()

  Runtime.getRuntime.addShutdownHook(new Thread(() => shutdownAll()))

  private def shutdownAll(): Unit =
    pools.values().forEach(_.shutdown())

  // A single sub-pool: up to `maxWorkers` live workers for one worker script.
  private final class Pool(
      resourcePath: String,
      launchArgs: Seq[String],
      pythonExe: String,
      env: Map[String, String]
  ) {
    private val idle = new LinkedBlockingQueue[Worker]()
    private val liveCount = new AtomicInteger(0)
    private val all = mutable.Set.empty[Worker] // guarded by `all`
    @volatile private var script: Path = _

    def run(request: ObjectNode): Outcome = {
      val w = borrow()
      try {
        val outcome = w.run(request)
        idle.offer(w) // healthy — return to pool
        outcome
      } catch {
        case e: WorkerDiedException =>
          discard(w)
          throw e
      }
    }

    private def borrow(): Worker = {
      val existing = idle.poll()
      if (existing != null) return existing
      if (liveCount.getAndIncrement() < maxWorkers) {
        try create()
        catch {
          case e: Throwable =>
            liveCount.decrementAndGet()
            throw e
        }
      } else {
        liveCount.decrementAndGet()
        idle.take() // at the cap — block until a worker is returned
      }
    }

    private def create(): Worker = {
      val cmd = (pythonExe +: ensureScript().toString +: launchArgs).asJava
      val pb = new ProcessBuilder(cmd).redirectErrorStream(false)
      env.foreach { case (k, v) => pb.environment().put(k, v) }
      val w = new Worker(pb.start(), s"$resourcePath ${launchArgs.mkString(" ")}".trim)
      w.awaitReady()
      all.synchronized(all.add(w))
      logger.debug(s"Started python worker for $resourcePath (live=${liveCount.get}/$maxWorkers)")
      w
    }

    private def discard(w: Worker): Unit = {
      all.synchronized(all.remove(w))
      liveCount.decrementAndGet()
      w.destroy()
    }

    private def ensureScript(): Path = {
      if (script == null) synchronized {
        if (script == null) {
          val stream = getClass.getResourceAsStream(resourcePath)
          require(stream != null, s"worker script not found on classpath at $resourcePath")
          try {
            val tmp = Files.createTempFile("py-worker-", ".py")
            Files.copy(stream, tmp, StandardCopyOption.REPLACE_EXISTING)
            tmp.toFile.deleteOnExit()
            script = tmp
          } finally stream.close()
        }
      }
      script
    }

    def shutdown(): Unit =
      all.synchronized {
        all.foreach(_.destroy())
        all.clear()
      }
  }

  // One live worker process plus its framed-JSON stdin/stdout and a background
  // drain of its own stderr (only non-empty on a hard crash).
  private final class Worker(process: Process, label: String) {
    private val stdin: BufferedWriter =
      new BufferedWriter(new OutputStreamWriter(process.getOutputStream, StandardCharsets.UTF_8))
    private val stdout: BufferedReader =
      new BufferedReader(new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
    private val errBuf = new StringBuilder

    private val errThread: Thread = {
      val t = new Thread(() => {
        val r =
          new BufferedReader(new InputStreamReader(process.getErrorStream, StandardCharsets.UTF_8))
        try {
          var line = r.readLine()
          while (line != null) {
            errBuf.synchronized(errBuf.append(line).append('\n'))
            line = r.readLine()
          }
        } catch { case NonFatal(_) => () }
      })
      t.setDaemon(true)
      t.setName("python-worker-stderr")
      t.start()
      t
    }

    /** Block until the worker's startup `{"ready": true}` arrives; if it dies
      * first (e.g. an import failed), surface its stderr.
      */
    def awaitReady(): Unit = {
      val line = stdout.readLine()
      if (line == null || !objectMapper.readTree(line).path("ready").asBoolean(false)) {
        throw new WorkerDiedException(
          s"python worker [$label] did not signal ready. stderr:\n${drainErr()}"
        )
      }
    }

    def run(request: ObjectNode): Outcome =
      try {
        stdin.write(objectMapper.writeValueAsString(request))
        stdin.write("\n")
        stdin.flush()
        val line = stdout.readLine()
        if (line == null) {
          throw new WorkerDiedException(s"python worker [$label] crashed. stderr:\n${drainErr()}")
        }
        val node = objectMapper.readTree(line)
        Outcome(
          node.path("exit").asInt(1),
          node.path("stdout").asText(""),
          node.path("stderr").asText("")
        )
      } catch {
        case e: WorkerDiedException => throw e
        case NonFatal(e) =>
          throw new WorkerDiedException(
            s"I/O error talking to python worker [$label]: ${e.getMessage}",
            e
          )
      }

    private def drainErr(): String = errBuf.synchronized(errBuf.toString)

    def destroy(): Unit = {
      try stdin.close()
      catch { case NonFatal(_) => () }
      process.destroy()
    }
  }
}
