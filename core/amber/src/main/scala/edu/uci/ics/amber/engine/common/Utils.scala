package edu.uci.ics.amber.engine.common

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.noctordeser.NoCtorDeserModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.workflowruntimestate.WorkflowAggregatedState
import org.jooq.DSLContext
import org.jooq.impl.DSL

import java.nio.file.{Files, Path, Paths}
import java.text.SimpleDateFormat
import java.util.concurrent.locks.Lock
import scala.annotation.tailrec

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
      }
      throw new RuntimeException(
        "Finding texera home path failed. Current working directory is " + currentWorkingDirectory
      )
    }
  }
  val AMBER_HOME_FOLDER_NAME = "amber";

  /**
    * Retry the given logic with a backoff time interval. The attempts are executed sequentially, thus blocking the thread.
    * Backoff time is doubled after each attempt.
    * @param attempts total number of attempts. if n <= 1 then it will not retry at all, decreased by 1 for each recursion.
    * @param baseBackoffTimeInMS time to wait before next attempt, started with the base time, and doubled after each attempt.
    * @param fn the target function to execute.
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

  /** An unmodifiable set containing some common URL words that are not usually useful
    * for searching.
    */
  final val URL_STOP_WORDS_SET = List[String](
    "http",
    "https",
    "org",
    "net",
    "com",
    "store",
    "www",
    "html"
  )

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

  def withTransaction[T](dsl: DSLContext)(block: DSLContext => T): T = {
    var result: Option[T] = None

    dsl.transaction(configuration => {
      val ctx = DSL.using(configuration)
      result = Some(block(ctx))
    })

    result.getOrElse(throw new RuntimeException("Transaction failed without result!"))
  }
}
