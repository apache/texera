package edu.uci.ics.texera.web

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.util.mongo.MongoDatabaseManager
import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED
}
import edu.uci.ics.amber.engine.common.AmberRuntime.scheduleRecurringCallThroughActorSystem
import edu.uci.ics.amber.engine.common.Utils.{maptoStatusCode, objectMapper}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberRuntime, Utils}
import edu.uci.ics.amber.core.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.web.auth.JwtAuth.setupJwtAuth
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import io.dropwizard.Configuration
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.apache.commons.jcs3.access.exception.InvalidArgumentException
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter
import org.glassfish.jersey.media.multipart.MultiPartFeature

import java.net.URI
import java.time.Duration
import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt

object ComputingUnitMaster {

  def createAmberRuntime(
      workflowContext: WorkflowContext,
      physicalPlan: PhysicalPlan,
      conf: ControllerConfig,
      errorHandler: Throwable => Unit
  ): AmberClient = {
    new AmberClient(
      AmberRuntime.actorSystem,
      workflowContext,
      physicalPlan,
      conf,
      errorHandler
    )
  }

  type OptionMap = Map[Symbol, Any]

  def parseArgs(args: Array[String]): OptionMap = {
    @tailrec
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--cluster" :: value :: tail =>
          nextOption(map ++ Map(Symbol("cluster") -> value.toBoolean), tail)
        case option :: tail =>
          throw new InvalidArgumentException("unknown command-line arg")
      }
    }

    nextOption(Map(), args.toList)
  }

  def main(args: Array[String]): Unit = {
    val argMap = parseArgs(args)

    val clusterMode = argMap.get(Symbol("cluster")).asInstanceOf[Option[Boolean]].getOrElse(false)
    // start actor system master node
    AmberRuntime.startActorMaster(clusterMode)
    // start web server
    new ComputingUnitMaster().run(
      "server",
      Utils.amberHomePath
        .resolve("src")
        .resolve("main")
        .resolve("resources")
        .resolve("computing-unit-master-config.yml")
        .toString
    )
  }
}

class ComputingUnitMaster extends io.dropwizard.Application[Configuration] with LazyLogging {

  override def initialize(bootstrap: Bootstrap[Configuration]): Unit = {
    // add websocket bundle
    bootstrap.addBundle(new WebsocketBundle(classOf[WorkflowWebsocketResource]))
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(configuration: Configuration, environment: Environment): Unit = {

    val webSocketUpgradeFilter =
      WebSocketUpgradeFilter.configureContext(environment.getApplicationContext)
    webSocketUpgradeFilter.getFactory.getPolicy.setIdleTimeout(Duration.ofHours(1).toMillis)
    environment.getApplicationContext.setAttribute(
      classOf[WebSocketUpgradeFilter].getName,
      webSocketUpgradeFilter
    )

    // register SessionHandler
    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    // register MultiPartFeature
    environment.jersey.register(classOf[MultiPartFeature])

    setupJwtAuth(environment)

    if (AmberConfig.isUserSystemEnabled) {
      val timeToLive: Int = AmberConfig.sinkStorageTTLInSecs
      if (AmberConfig.cleanupAllExecutionResults) {
        // do one time cleanup of collections that were not closed gracefully before restart/crash
        // retrieve all executions that were executing before the reboot.
        val allExecutionsBeforeRestart: List[WorkflowExecutions] =
          WorkflowExecutionsResource.getExpiredExecutionsWithResultOrLog(-1)
        cleanExecutions(
          allExecutionsBeforeRestart,
          statusByte => {
            if (statusByte != maptoStatusCode(COMPLETED)) {
              maptoStatusCode(FAILED) // for incomplete executions, mark them as failed.
            } else {
              statusByte
            }
          }
        )
      }
      scheduleRecurringCallThroughActorSystem(
        2.seconds,
        AmberConfig.sinkStorageCleanUpCheckIntervalInSecs.seconds
      ) {
        recurringCheckExpiredResults(timeToLive)
      }
    }
  }

  /**
    * This function drops the collections.
    * MongoDB doesn't have an API of drop collection where collection name in (from a subquery), so the implementation is to retrieve
    * the entire list of those documents that have expired, then loop the list to drop them one by one
    */
  private def cleanExecutions(
      executions: List[WorkflowExecutions],
      statusChangeFunc: Byte => Byte
  ): Unit = {
    // drop the collection and update the status to ABORTED
    executions.foreach(execEntry => {
      dropCollections(execEntry.getResult)
      deleteReplayLog(execEntry.getLogLocation)
      // then delete the pointer from mySQL
      val executionIdentity = ExecutionIdentity(execEntry.getEid.longValue())
      ExecutionsMetadataPersistService.tryUpdateExistingExecution(executionIdentity) { execution =>
        execution.setResult("")
        execution.setLogLocation(null)
        execution.setStatus(statusChangeFunc(execution.getStatus))
      }
    })
  }

  private def dropCollections(result: String): Unit = {
    if (result == null || result.isEmpty) {
      return
    }
    // TODO: merge this logic to the server-side in-mem cleanup
    // parse the JSON
    try {
      val node = objectMapper.readTree(result)
      val collectionEntries = node.get("results")
      // loop every collection and drop it
      collectionEntries.forEach(collection => {
        val storageType = collection.get("storageType").asText()
        val collectionName = collection.get("storageKey").asText()
        storageType match {
          case DocumentFactory.ICEBERG =>
          // rely on the server-side result cleanup logic.
          case DocumentFactory.MONGODB =>
            MongoDatabaseManager.dropCollection(collectionName)
        }
      })
    } catch {
      case e: Throwable =>
        logger.warn("result collection cleanup failed.", e)
    }
  }

  private def deleteReplayLog(logLocation: String): Unit = {
    if (logLocation == null || logLocation.isEmpty) {
      return
    }
    val uri = new URI(logLocation)
    try {
      val storage = SequentialRecordStorage.getStorage(Some(uri))
      storage.deleteStorage()
    } catch {
      case throwable: Throwable =>
        logger.warn(s"failed to delete log at $logLocation", throwable)
    }
  }

  /**
    * This function is called periodically and checks all expired collections and deletes them
    */
  private def recurringCheckExpiredResults(
      timeToLive: Int
  ): Unit = {
    // retrieve all executions that are completed and their last update time goes beyond the ttl
    val expiredResults: List[WorkflowExecutions] =
      WorkflowExecutionsResource.getExpiredExecutionsWithResultOrLog(timeToLive)
    // drop the collections and clean the logs
    cleanExecutions(expiredResults, statusByte => statusByte)
  }
}
