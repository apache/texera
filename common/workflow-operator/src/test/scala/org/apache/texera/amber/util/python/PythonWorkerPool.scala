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

package org.apache.texera.amber.util.python

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.concurrent.{
  Callable,
  ConcurrentHashMap,
  ExecutionException,
  ExecutorService,
  Executors,
  LinkedBlockingQueue,
  TimeUnit,
  TimeoutException
}
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
  * Pools of persistent Python "worker" processes that eliminate the per-call
  * interpreter-boot + import cost a test otherwise pays on every subprocess
  * spawn. Testing operators one at a time does not scale when each one costs a
  * spawn: a bare `-I -S` interpreter boots in ~25 ms, and once pandas and plotly
  * are imported a spawn costs ~260-310 ms — ~96% of a job whose real work is
  * ~4 ms. A worker pays that once at startup, then serves many jobs over its
  * lifetime, so N spawns become one.
  *
  * Lives in test scope here, rather than beside a single caller, because tests in
  * several modules run generated operator code and would otherwise each
  * hand-roll a driver, a stdout protocol and a timeout. Other modules reach it
  * through a `test->test` dependency on this one.
  *
  * Generic over the worker script — the pool never interprets the payload — so
  * one implementation serves a syntax check, template execution and DataFrame
  * comparison alike. Each distinct (resource, interpreterArgs, launchArgs,
  * python, env) combination gets its own sub-pool.
  *
  * Protocol (line-delimited JSON, shared by all worker scripts):
  *   startup   worker -> pool:  {"ready": true}
  *   request   pool -> worker:  <caller-supplied JSON object>\n
  *   response  worker -> pool:  {"exit": <int>, "stdout": "...", "stderr": "..."}\n
  *
  * Concurrency: callers submit from several threads at once — a spec run with
  * ScalaTest's `-P4`, or one test fanning its cases out — so each sub-pool holds
  * up to [[maxWorkers]] workers, each serving one job at a time (borrow -> run
  * -> return). A worker script may chdir per job, so a worker must never run two
  * jobs at once — the borrow/return discipline guarantees that.
  *
  * Robustness: an ordinary *job* failure comes back as an [[Outcome]] with
  * `exit != 0` (worker keeps running). A hard interpreter crash ends a worker;
  * the pool detects the EOF / broken pipe, discards it, and throws
  * [[WorkerDiedException]] so the caller can fall back to a one-shot subprocess
  * — behavior is then never worse than the pre-pool path. A worker that stays
  * alive but stops answering ends the same way, on the [[Timeouts]] below.
  */
object PythonWorkerPool extends LazyLogging {

  /** Worker response: process-like exit code plus captured streams. */
  final case class Outcome(exit: Int, stdout: String, stderr: String)

  /** Thrown when a worker dies mid-job (hard crash / broken pipe). Callers
    * catch this and fall back to a one-shot subprocess.
    */
  final class WorkerDiedException(message: String, cause: Throwable = null)
      extends RuntimeException(message, cause)

  /** Feature toggle. `TEXERA_TEST_PYTHON_WORKER=0` (or `false`/`off`) forces the
    * one-subprocess-per-call paths everywhere — an escape hatch for debugging a
    * suspected isolation leak. Default on.
    */
  val enabled: Boolean =
    !sys.env
      .get("TEXERA_TEST_PYTHON_WORKER")
      .map(_.trim.toLowerCase)
      .exists(Set("0", "false", "off"))

  /** Max live workers per sub-pool. Defaults to 4 to match ScalaTest's `-P4`, so
    * the two concurrency bounds agree on how many interpreters may be live;
    * override via `TEXERA_TEST_PYTHON_WORKERS`. Public so a caller fanning out
    * jobs within one test can size that fan-out to the workers it will get.
    */
  val maxWorkers: Int =
    sys.env
      .get("TEXERA_TEST_PYTHON_WORKERS")
      .flatMap(s => scala.util.Try(s.trim.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(4)

  /** How long a caller waits on a worker before the pool kills and discards it.
    * A read on a process pipe cannot be interrupted — a suite or executor timeout
    * leaves the reading thread stuck on it — so a worker that stays alive without
    * answering has to be bounded here. `response` keeps the 30 seconds the
    * one-shot spawn this pool replaced allowed a job; `startup` is longer because
    * a worker imports its libraries before it reports ready, and a loaded CI
    * machine makes that slow. Override in seconds via
    * `TEXERA_TEST_PYTHON_WORKER_TIMEOUT` / `TEXERA_TEST_PYTHON_WORKER_STARTUP_TIMEOUT`.
    */
  final case class Timeouts(responseMillis: Long, startupMillis: Long)

  object Timeouts {
    private def envSeconds(name: String, default: Long): Long =
      sys.env
        .get(name)
        .flatMap(s => scala.util.Try(s.trim.toLong).toOption)
        .filter(_ > 0)
        .getOrElse(default) * 1000

    val Default: Timeouts = Timeouts(
      responseMillis = envSeconds("TEXERA_TEST_PYTHON_WORKER_TIMEOUT", 30),
      startupMillis = envSeconds("TEXERA_TEST_PYTHON_WORKER_STARTUP_TIMEOUT", 60)
    )
  }

  /**
    * Run one job through a pooled worker for `resourcePath`, launched as
    * `pythonExe <interpreterArgs> <script> <launchArgs>` with extra environment
    * `env`. `request` is the worker-specific JSON payload (the pool does not
    * interpret it). Throws [[WorkerDiedException]] on a hard worker crash.
    *
    * `interpreterArgs` are the flags that must precede the script — a syntax
    * checker wants `-I -S` so it validates under the same isolation a one-shot
    * `python -I -S -m py_compile` gave it. `launchArgs` are the script's own
    * (e.g. `--serve`), and `env` carries what a flag cannot (e.g. PYTHONPATH).
    * All three are part of a worker's identity: one started differently is not
    * interchangeable, so it gets its own sub-pool. `timeouts` is not — it bounds
    * this call, so a caller whose jobs are slower than most can raise it without
    * splitting the pool.
    */
  def run(
      resourcePath: String,
      launchArgs: Seq[String],
      pythonExe: String,
      request: ObjectNode,
      env: Map[String, String] = Map.empty,
      interpreterArgs: Seq[String] = Seq.empty,
      timeouts: Timeouts = Timeouts.Default
  ): Outcome = {
    val envKey = env.toSeq.sorted.map { case (k, v) => s"$k=$v" }
    val key =
      (resourcePath +: pythonExe +: (interpreterArgs ++ launchArgs ++ envKey)).mkString(" ")
    val pool = pools.computeIfAbsent(
      key,
      _ => new Pool(resourcePath, launchArgs, pythonExe, env, interpreterArgs)
    )
    pool.run(request, timeouts)
  }

  /** How long a caller at the worker cap waits before re-examining it. Not a
    * deadline — see [[Pool.borrow]].
    */
  private val CapRecheckMillis: Long = 250

  private val pools = new ConcurrentHashMap[String, Pool]()

  Runtime.getRuntime.addShutdownHook(new Thread(() => shutdownAll()))

  private def shutdownAll(): Unit =
    pools.values().forEach(_.shutdown())

  // A single sub-pool: up to `maxWorkers` live workers for one worker script.
  private final class Pool(
      resourcePath: String,
      launchArgs: Seq[String],
      pythonExe: String,
      env: Map[String, String],
      interpreterArgs: Seq[String]
  ) {
    private val idle = new LinkedBlockingQueue[Worker]()
    private val liveCount = new AtomicInteger(0)
    private val all = mutable.Set.empty[Worker] // guarded by `all`
    @volatile private var script: Path = _

    def run(request: ObjectNode, timeouts: Timeouts): Outcome = {
      val w = borrow(timeouts)
      try {
        val outcome = w.run(request, timeouts.responseMillis)
        idle.offer(w) // healthy — return to pool
        outcome
      } catch {
        case e: WorkerDiedException =>
          discard(w)
          throw e
      }
    }

    @tailrec
    private def borrow(timeouts: Timeouts): Worker = {
      val existing = idle.poll()
      if (existing != null) existing
      else if (liveCount.getAndIncrement() < maxWorkers) {
        try create(timeouts)
        catch {
          case e: Throwable =>
            liveCount.decrementAndGet()
            throw e
        }
      } else {
        liveCount.decrementAndGet()
        // At the cap. Waiting outright for a returned worker would strand this
        // caller when the ones ahead are discarded instead: a discard frees a
        // slot without putting anything back. So wait only briefly, then look at
        // the cap again — the next pass starts a replacement. A long queue still
        // waits as long as it takes; that is the caller's own backlog, not a hang.
        val returned = idle.poll(CapRecheckMillis, TimeUnit.MILLISECONDS)
        if (returned != null) returned else borrow(timeouts)
      }
    }

    private def create(timeouts: Timeouts): Worker = {
      val cmd =
        (((pythonExe +: interpreterArgs) :+ ensureScript().toString) ++ launchArgs).asJava
      val pb = new ProcessBuilder(cmd).redirectErrorStream(false)
      env.foreach { case (k, v) => pb.environment().put(k, v) }
      val w = new Worker(pb.start(), s"$resourcePath ${launchArgs.mkString(" ")}".trim)
      w.awaitReady(timeouts.startupMillis)
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

  // One live worker process plus its framed-JSON stdin and background drains of
  // its stdout (the protocol) and stderr (only non-empty on a hard crash).
  private final class Worker(process: Process, label: String) {
    private val stdin: BufferedWriter =
      new BufferedWriter(new OutputStreamWriter(process.getOutputStream, StandardCharsets.UTF_8))
    private val errBuf = new StringBuilder

    // Protocol lines the worker has written, `None` marking end of stream. A
    // dedicated thread owns the blocking read so a caller can wait with a
    // timeout: `readLine` on a process pipe answers neither an interrupt nor a
    // deadline, and only closing the pipe — killing the process — releases it.
    private val lines = new LinkedBlockingQueue[Option[String]]()

    // Owns the writing end for the same reason.
    private val writer: ExecutorService = Executors.newSingleThreadExecutor { r =>
      val t = new Thread(r, "python-worker-stdin")
      t.setDaemon(true)
      t
    }

    private val outThread: Thread = {
      val t = new Thread(() => {
        val r =
          new BufferedReader(new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
        try {
          var line = r.readLine()
          while (line != null) {
            lines.put(Some(line))
            line = r.readLine()
          }
        } catch { case NonFatal(_) => () }
        finally lines.put(None)
      })
      t.setDaemon(true)
      t.setName("python-worker-stdout")
      t.start()
      t
    }

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

    /** Wait for the worker's startup `{"ready": true}`; if it dies first (e.g. an
      * import failed) or never gets there, surface its stderr. A worker that
      * fails here is killed: it is not in the pool's set yet, so nothing else
      * will reap it.
      */
    def awaitReady(timeoutMillis: Long): Unit = {
      val line = nextLine(timeoutMillis, "signal ready")
      if (!objectMapper.readTree(line).path("ready").asBoolean(false)) {
        destroy()
        throw new WorkerDiedException(
          s"python worker [$label] did not signal ready. stderr:\n${drainErr()}"
        )
      }
    }

    def run(request: ObjectNode, timeoutMillis: Long): Outcome =
      try {
        send(request, timeoutMillis)
        val node = objectMapper.readTree(nextLine(timeoutMillis, "answer"))
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

    /** Hand the request over, on the same deadline as the answer. A worker that
      * has stopped reading its stdin blocks the write as soon as the payload
      * outgrows the pipe buffer, and that write is no more interruptible than the
      * read, so it too runs on a thread of its own — a daemon, so a lost one
      * cannot keep the JVM alive. Killing the worker closes the pipe, which is
      * what releases that thread.
      */
    private def send(request: ObjectNode, timeoutMillis: Long): Unit = {
      val write = writer.submit(new Callable[Unit] {
        override def call(): Unit = {
          stdin.write(objectMapper.writeValueAsString(request))
          stdin.write("\n")
          stdin.flush()
        }
      })
      try write.get(timeoutMillis, TimeUnit.MILLISECONDS)
      catch {
        case _: TimeoutException =>
          destroy()
          throw new WorkerDiedException(
            s"python worker [$label] did not read its request within ${timeoutMillis}ms;" +
              s" killed it. stderr:\n${drainErr()}"
          )
        case e: ExecutionException =>
          throw new WorkerDiedException(
            s"I/O error sending to python worker [$label]: ${e.getCause.getMessage}",
            e.getCause
          )
      }
    }

    /** One protocol line, or a [[WorkerDiedException]]: the worker ended the
      * stream, or it went `timeoutMillis` without writing. `what` names what was
      * being waited for. A worker that timed out is killed here — that both frees
      * the machine of a hung interpreter and unblocks [[outThread]] — and the
      * pool discards it, so it never serves another job.
      */
    private def nextLine(timeoutMillis: Long, what: String): String =
      lines.poll(timeoutMillis, TimeUnit.MILLISECONDS) match {
        case null => // poll's own signal that the deadline passed with nothing written
          destroy()
          throw new WorkerDiedException(
            s"python worker [$label] did not $what within ${timeoutMillis}ms; killed it." +
              s" stderr:\n${drainErr()}"
          )
        case None => // end of stream: the interpreter is gone
          throw new WorkerDiedException(s"python worker [$label] crashed. stderr:\n${drainErr()}")
        case Some(line) => line
      }

    private def drainErr(): String = errBuf.synchronized(errBuf.toString)

    /** Idempotent: called on a crash, a timeout, and again on pool shutdown.
      *
      * The process goes first and the streams after: a write blocked on a full
      * pipe only comes back once the pipe has no reader, and closing the buffered
      * writer would try to flush into that same pipe — so closing first is itself
      * a way to hang. Nothing is flushed on the way out; a worker being destroyed
      * has no use for the rest of a request.
      */
    def destroy(): Unit = {
      writer.shutdownNow()
      process.destroy()
      try if (!process.waitFor(2, TimeUnit.SECONDS)) process.destroyForcibly()
      catch { case NonFatal(_) => process.destroyForcibly() }
      try process.getOutputStream.close()
      catch { case NonFatal(_) => () }
    }
  }
}
