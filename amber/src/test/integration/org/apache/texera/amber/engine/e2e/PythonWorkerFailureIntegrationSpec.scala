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

package org.apache.texera.amber.engine.e2e

import com.twitter.util.Duration
import org.apache.commons.lang3.SystemUtils
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadResults,
  setUpWorkflowExecutionData
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
  * End-to-end: when the operating system kills a running Python worker's process (the
  * out-of-memory killer, for example), the execution must fail promptly and say which worker
  * died and how, instead of staying RUNNING forever with nothing reported.
  *
  * The workflow is CSV (20,000 rows) -> Python UDF (1 ms per row) -> Keyword Search, which runs
  * for tens of seconds, so the kill lands mid-run. Only this run's UDF process is killed: it is
  * picked out by the operator id in its startup configuration.
  *
  * Tagged @IntegrationTest because it spawns a real Python worker; routed to the
  * `amber-integration` CI job.
  */
@IntegrationTest
class PythonWorkerFailureIntegrationSpec
    extends TestKit(ActorSystem("PythonWorkerFailureIntegrationSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val specId = 7
  private val rows = 20000
  private var csvPath: Path = _

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData(specId)

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData(specId)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    // Load the JDBC driver explicitly, as the other e2e specs do, to avoid flaky CI lookups.
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
    csvPath = Files.createTempFile("python-worker-failure-", ".csv")
    val lines = "id,tag" +: (0 until rows).map(i => s"$i,${if (i % 2 == 0) "alpha" else "beta"}")
    Files.write(csvPath, lines.asJava, StandardCharsets.UTF_8)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    if (csvPath != null) Files.deleteIfExists(csvPath)
  }

  /** Python worker processes of this JVM whose startup configuration mentions `operatorId`. */
  private def pythonWorkersOf(operatorId: String): Seq[ProcessHandle] =
    ProcessHandle
      .current()
      .descendants()
      .iterator()
      .asScala
      .filter { p =>
        val info = p.info()
        info.commandLine().orElse("").contains("texera_run_python_worker") &&
        info.arguments().orElse(Array.empty[String]).lastOption.exists { arg =>
          Try(new String(Base64.getDecoder.decode(arg), StandardCharsets.UTF_8)).toOption
            .exists(_.contains(operatorId))
        }
      }
      .toSeq

  "Engine" should "fail the run, naming the worker, when its Python worker process is killed mid-run" in {
    val scan = TestOperators.getCsvScanOpDesc(csvPath.toString, header = true)
    val udf = TestOperators.pythonOpDesc()
    udf.code = """from pytexera import *
        |import time
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    @overrides
        |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        |        time.sleep(0.001)
        |        yield tuple_
        |""".stripMargin
    val keyword = TestOperators.keywordSearchOpDesc("tag", "alpha")
    val workflow = buildWorkflow(
      List(scan, udf, keyword),
      List(
        LogicalLink(
          scan.operatorIdentifier,
          PortIdentity(),
          udf.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          udf.operatorIdentifier,
          PortIdentity(),
          keyword.operatorIdentifier,
          PortIdentity()
        )
      ),
      TestUtils.workflowContext(specId)
    )

    // Once the UDF's Python process exists, let it run a few seconds, then SIGKILL it.
    val killedAtNanos = new AtomicLong(0L)
    val killer = new Thread(
      () => {
        val deadline = System.nanoTime() + 120L * 1000000000L
        var workers = pythonWorkersOf(udf.operatorIdentifier.id)
        while (workers.isEmpty && System.nanoTime() < deadline) {
          Thread.sleep(200)
          workers = pythonWorkersOf(udf.operatorIdentifier.id)
        }
        Thread.sleep(5000)
        workers.foreach(_.destroyForcibly())
        if (workers.nonEmpty) killedAtNanos.set(System.nanoTime())
      },
      "python-worker-killer"
    )
    killer.setDaemon(true)
    killer.start()

    val outcome = Try(
      runWorkflowAndReadResults(
        system,
        workflow,
        List(keyword.operatorIdentifier),
        _.getCount,
        Duration.fromSeconds(90)
      )
    )
    val endedAtNanos = System.nanoTime()
    killer.join(5000)

    assert(killedAtNanos.get() != 0L, "the UDF's Python worker process was never found to kill")
    outcome match {
      case Success(counts) =>
        fail(s"the run completed although its Python worker was killed: $counts")
      case Failure(e) =>
        val messages = Iterator
          .iterate[Throwable](e)(_.getCause)
          .takeWhile(_ != null)
          .map(x => String.valueOf(x.getMessage))
          .mkString(" | ")
        assert(
          messages.contains("exited") && messages.contains(udf.operatorIdentifier.id),
          s"expected a failure naming the UDF's worker, got ${e.getClass.getSimpleName}: $messages"
        )
        // A forcible kill is SIGKILL on POSIX, which the JVM reports as exit code 137.
        if (!SystemUtils.IS_OS_WINDOWS) assert(messages.contains("137"), messages)
        val secondsAfterKill = (endedAtNanos - killedAtNanos.get()) / 1e9
        assert(secondsAfterKill < 20, s"the failure took $secondsAfterKill s after the kill")
    }
  }
}
