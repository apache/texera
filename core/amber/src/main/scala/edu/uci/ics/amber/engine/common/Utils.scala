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

package edu.uci.ics.amber.engine.common

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.module.noctordeser.NoCtorDeserModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.operator.LogicalOp

import java.nio.file.{Files, Path, Paths}
import java.text.SimpleDateFormat
import java.util.concurrent.locks.Lock
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import edu.uci.ics.texera.web.model.websocket.request.{LogicalPlanPojo, WorkflowExecuteRequest}
import edu.uci.ics.texera.workflow.LogicalLink
import edu.uci.ics.amber.core.workflow.WorkflowContext

object Utils extends LazyLogging {

  final val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new NoCtorDeserModule())
    .setSerializationInclusion(Include.NON_NULL)
    .setSerializationInclusion(Include.NON_ABSENT)
    .setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

  /**
    * Gets the real path of the amber home directory by:
    * 1): check if the current directory is texera/core/amber
    * if it's not then:
    * 2): search the siblings and children to find the texera home path
    *
    * @return the real absolute path to amber home directory
    */
  lazy val amberHomePath: Path = {
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    // check if the current directory is the amber home path
    if (isAmberHomePath(currentWorkingDirectory)) {
      currentWorkingDirectory
    } else {
      // from current path's parent directory, search its children to find amber home path
      // current max depth is set to 2 (current path's siblings and direct children)
      val searchChildren = Files
        .walk(currentWorkingDirectory.getParent, 2)
        .filter((path: Path) => isAmberHomePath(path))
        .findAny
      if (searchChildren.isPresent) {
        searchChildren.get
      } else {
        throw new RuntimeException(
          "Finding texera home path failed. Current working directory is " + currentWorkingDirectory
        )
      }
    }
  }
  val AMBER_HOME_FOLDER_NAME = "amber";

  /**
    * Retry the given logic with a backoff time interval. The attempts are executed sequentially, thus blocking the thread.
    * Backoff time is doubled after each attempt.
    *
    * @param attempts            total number of attempts. if n <= 1 then it will not retry at all, decreased by 1 for each recursion.
    * @param baseBackoffTimeInMS time to wait before next attempt, started with the base time, and doubled after each attempt.
    * @param fn                  the target function to execute.
    * @tparam T any return type from the provided function fn.
    * @return the provided function fn's return, or any exception that still being raised after n attempts.
    */
  @tailrec
  def retry[T](attempts: Int, baseBackoffTimeInMS: Long)(fn: => T): T = {
    try {
      fn
    } catch {
      case e: Throwable =>
        if (attempts > 1) {
          logger.warn(
            "retrying after " + baseBackoffTimeInMS + "ms, number of attempts left: " + (attempts - 1),
            e
          )
          Thread.sleep(baseBackoffTimeInMS)
          retry(attempts - 1, baseBackoffTimeInMS * 2)(fn)
        } else throw e
    }
  }

  private def isAmberHomePath(path: Path): Boolean = {
    path.toRealPath().endsWith(AMBER_HOME_FOLDER_NAME)
  }

  def aggregatedStateToString(state: WorkflowAggregatedState): String = {
    state match {
      case WorkflowAggregatedState.UNINITIALIZED => "Uninitialized"
      case WorkflowAggregatedState.READY         => "Initializing"
      case WorkflowAggregatedState.RUNNING       => "Running"
      case WorkflowAggregatedState.PAUSING       => "Pausing"
      case WorkflowAggregatedState.PAUSED        => "Paused"
      case WorkflowAggregatedState.RESUMING      => "Resuming"
      case WorkflowAggregatedState.COMPLETED     => "Completed"
      case WorkflowAggregatedState.FAILED        => "Failed"
      case WorkflowAggregatedState.KILLED        => "Killed"
      case WorkflowAggregatedState.UNKNOWN       => "Unknown"
      case WorkflowAggregatedState.Unrecognized(unrecognizedValue) =>
        s"Unrecognized($unrecognizedValue)"
    }
  }

  def stringToAggregatedState(str: String): WorkflowAggregatedState = {
    str.trim.toLowerCase match {
      case "uninitialized" => WorkflowAggregatedState.UNINITIALIZED
      case "ready"         => WorkflowAggregatedState.READY
      case "initializing"  => WorkflowAggregatedState.READY // accept alias
      case "running"       => WorkflowAggregatedState.RUNNING
      case "pausing"       => WorkflowAggregatedState.PAUSING
      case "paused"        => WorkflowAggregatedState.PAUSED
      case "resuming"      => WorkflowAggregatedState.RESUMING
      case "completed"     => WorkflowAggregatedState.COMPLETED
      case "failed"        => WorkflowAggregatedState.FAILED
      case "killed"        => WorkflowAggregatedState.KILLED
      case "unknown"       => WorkflowAggregatedState.UNKNOWN
      case other           => throw new IllegalArgumentException(s"Unrecognized state: $other")
    }
  }

  /**
    * @param state indicates the workflow state
    * @return code indicates the status of the execution in the DB it is 0 by default for any unused states.
    *         This code is stored in the DB and read in the frontend.
    *         If these codes are changed, they also have to be changed in the frontend `ngbd-modal-workflow-executions.component.ts`
    */
  def maptoStatusCode(state: WorkflowAggregatedState): Byte = {
    state match {
      case WorkflowAggregatedState.UNINITIALIZED => 0
      case WorkflowAggregatedState.READY         => 0
      case WorkflowAggregatedState.RUNNING       => 1
      case WorkflowAggregatedState.PAUSED        => 2
      case WorkflowAggregatedState.COMPLETED     => 3
      case WorkflowAggregatedState.FAILED        => 4
      case WorkflowAggregatedState.KILLED        => 5
      case other                                 => -1
    }
  }

  def withLock[X](instructions: => X)(implicit lock: Lock): X = {
    lock.lock()
    try {
      instructions
    } catch {
      case e: Throwable =>
        throw e
    } finally {
      lock.unlock()
    }
  }

  /**
    * Construct a dummy WorkflowExecuteRequest containing one instance of every
    * concrete LogicalOp that has a public no-arg constructor, serialise it to
    * JSON, then immediately parse it back.  This forces Jackson to walk the
    * entire object graph of realistic size, exercising polymorphic resolution
    * of LogicalOp subclasses as well as everything used inside the request.
    *
    * It should be called once at bootstrap *after* warmUpObjectMapper().
    */
  def warmUpObjectMapperWithDummyPlan(): Unit = {
    val mapper = objectMapper

    // collect all subclasses of LogicalOp (similar to OperatorMetadataGenerator)
    val cfg = mapper.getDeserializationConfig
    val subtypes = mapper.getSubtypeResolver.collectAndResolveSubtypesByClass(
      cfg,
      AnnotatedClassResolver.resolveWithoutSuperTypes(cfg, classOf[LogicalOp])
    )

    import scala.jdk.CollectionConverters._
    val operatorInstances: List[LogicalOp] = new java.util.ArrayList[NamedType](subtypes).asScala
      .flatMap(nt => Option(nt.getType))
      .collect {
        case c: Class[_] if classOf[LogicalOp].isAssignableFrom(c) =>
          c.asInstanceOf[Class[_ <: LogicalOp]]
      }
      .flatMap { cls =>
        try {
          val ctor = cls.getDeclaredConstructor()
          ctor.setAccessible(true)
          Some(ctor.newInstance())
        } catch {
          case _: Throwable => None // skip classes without default ctor
        }
      }
      .toList

    if (operatorInstances.isEmpty) {
      logger.warn("warmUpWithDummyPlan: no LogicalOp instances could be instantiated")
      return
    }

    val dummyPlan = LogicalPlanPojo(operatorInstances, List.empty[LogicalLink], Nil, Nil)
    val dummyReq = WorkflowExecuteRequest(
      executionName = "warmup",
      engineVersion = "0",
      logicalPlan = dummyPlan,
      replayFromExecution = None,
      workflowSettings = WorkflowContext.DEFAULT_WORKFLOW_SETTINGS,
      emailNotificationEnabled = false,
      computingUnitId = 0
    )

    val json = mapper.writeValueAsString(dummyReq)
    val t0 = System.nanoTime()
    mapper.readValue(json, classOf[WorkflowExecuteRequest])
    val ms = (System.nanoTime() - t0) / 1e6
    logger.info(
      s"warmUpWithDummyPlan parsed dummy WorkflowExecuteRequest in ${ms} ms with ${operatorInstances.size} operators"
    )
  }
}
