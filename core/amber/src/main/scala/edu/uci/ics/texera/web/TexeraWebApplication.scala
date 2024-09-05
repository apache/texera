package edu.uci.ics.texera.web

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle
import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.common.AmberRuntime.scheduleRecurringCallThroughActorSystem
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberRuntime}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.Utils.{maptoStatusCode, objectMapper}
import edu.uci.ics.texera.web.auth.JwtAuth.jwtConsumer
import edu.uci.ics.texera.web.auth.{
  GuestAuthFilter,
  SessionUser,
  UserAuthenticator,
  UserRoleAuthorizer
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.resource.auth.{AuthResource, GoogleAuthResource}
import edu.uci.ics.texera.web.resource._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource
import edu.uci.ics.texera.web.resource.dashboard.admin.execution.AdminExecutionResource
import edu.uci.ics.texera.web.resource.dashboard.admin.user.AdminUserResource
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.{
  DatasetAccessResource,
  DatasetResource
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.`type`.{
  DatasetFileNode,
  DatasetFileNodeSerializer
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils.getAllDatasetDirectories
import edu.uci.ics.texera.web.resource.dashboard.user.project.{
  ProjectAccessResource,
  ProjectResource,
  PublicProjectResource
}
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource
import edu.uci.ics.texera.web.resource.dashboard.user.discussion.UserDiscussionResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.{
  WorkflowAccessResource,
  WorkflowExecutionsResource,
  WorkflowResource,
  WorkflowVersionResource
}
import edu.uci.ics.texera.web.resource.languageserver.PythonLanguageServerManager
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import edu.uci.ics.texera.web.storage.MongoDatabaseManager
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{COMPLETED, FAILED}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.servlet.ErrorPageErrorHandler
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter
import org.glassfish.jersey.media.multipart.MultiPartFeature
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature

import java.time.Duration
import scala.concurrent.duration.DurationInt
import org.apache.commons.jcs3.access.exception.InvalidArgumentException

import java.net.URI
import scala.annotation.tailrec

object TexeraWebApplication {

  // this method is used to abort uncommitted changes for every dataset
  def discardUncommittedChangesOfAllDatasets(): Unit = {
    val datasetPaths = getAllDatasetDirectories()

    datasetPaths.foreach(path => {
      GitVersionControlLocalFileStorage.discardUncommittedChanges(path)
    })
  }

  def createAmberRuntime(
      workflowContext: WorkflowContext,
      physicalPlan: PhysicalPlan,
      opResultStorage: OpResultStorage,
      conf: ControllerConfig,
      errorHandler: Throwable => Unit
  ): AmberClient = {
    new AmberClient(
      AmberRuntime.actorSystem,
      workflowContext,
      physicalPlan,
      opResultStorage,
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

    // TODO: figure out a safety way of calling discardUncommittedChangesOfAllDatasets
    // Currently in kubernetes, multiple pods calling this function can result into thread competition
    // discardUncommittedChangesOfAllDatasets()

    // start actor system master node
    AmberRuntime.startActorMaster(clusterMode)

    // start web server
    new TexeraWebApplication().run(
      "server",
      Utils.amberHomePath
        .resolve("src")
        .resolve("main")
        .resolve("resources")
        .resolve("web-config.yml")
        .toString
    )
    PythonLanguageServerManager.startLanguageServer()
  }
}

class TexeraWebApplication
    extends io.dropwizard.Application[TexeraWebConfiguration]
    with LazyLogging {

  override def initialize(bootstrap: Bootstrap[TexeraWebConfiguration]): Unit = {
    // serve static frontend GUI files
    bootstrap.addBundle(new FileAssetsBundle("../gui/dist", "/", "index.html"))
    // add websocket bundle
    bootstrap.addBundle(new WebsocketBundle(classOf[WorkflowWebsocketResource]))
    bootstrap.addBundle(new WebsocketBundle(classOf[CollaborationResource]))
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    // register a new custom module and add the custom serializer into it
    val customSerializerModule = new SimpleModule("CustomSerializers")
    customSerializerModule.addSerializer(classOf[DatasetFileNode], new DatasetFileNodeSerializer())
    bootstrap.getObjectMapper.registerModule(customSerializerModule)

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

  override def run(configuration: TexeraWebConfiguration, environment: Environment): Unit = {
    // serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    // redirect all 404 to index page, according to Angular routing requirements
    val eph = new ErrorPageErrorHandler
    eph.addErrorPage(404, "/")
    environment.getApplicationContext.setErrorHandler(eph)

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

    environment.jersey.register(classOf[SystemMetadataResource])
    // environment.jersey().register(classOf[MockKillWorkerResource])
    environment.jersey.register(classOf[SchemaPropagationResource])

    if (AmberConfig.isUserSystemEnabled) {
      // register JWT Auth layer
      environment.jersey.register(
        new AuthDynamicFeature(
          new JwtAuthFilter.Builder[SessionUser]()
            .setJwtConsumer(jwtConsumer)
            .setRealm("realm")
            .setPrefix("Bearer")
            .setAuthenticator(UserAuthenticator)
            .setAuthorizer(UserRoleAuthorizer)
            .buildAuthFilter()
        )
      )
    } else {
      // register Guest Auth layer
      environment.jersey.register(
        new AuthDynamicFeature(
          new GuestAuthFilter.Builder().setAuthorizer(UserRoleAuthorizer).buildAuthFilter()
        )
      )
    }

    environment.jersey.register(
      new AuthValueFactoryProvider.Binder[SessionUser](classOf[SessionUser])
    )
    environment.jersey.register(classOf[RolesAllowedDynamicFeature])

    environment.jersey.register(classOf[AuthResource])
    environment.jersey.register(classOf[GoogleAuthResource])
    environment.jersey.register(classOf[UserConfigResource])
    environment.jersey.register(classOf[AdminUserResource])
    environment.jersey.register(classOf[PublicProjectResource])
    environment.jersey.register(classOf[WorkflowAccessResource])
    environment.jersey.register(classOf[WorkflowResource])
    environment.jersey.register(classOf[WorkflowVersionResource])
    environment.jersey.register(classOf[DatasetResource])
    environment.jersey.register(classOf[DatasetAccessResource])
    environment.jersey.register(classOf[ProjectResource])
    environment.jersey.register(classOf[ProjectAccessResource])
    environment.jersey.register(classOf[WorkflowExecutionsResource])
    environment.jersey.register(classOf[DashboardResource])
    environment.jersey.register(classOf[GmailResource])
    environment.jersey.register(classOf[AdminExecutionResource])
    environment.jersey.register(classOf[UserQuotaResource])
    environment.jersey.register(classOf[UserDiscussionResource])
    environment.jersey.register(classOf[AiAssistantResource])
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

  def dropCollections(result: String): Unit = {
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
          case OpResultStorage.MEMORY =>
          // rely on the server-side result cleanup logic.
          case OpResultStorage.MONGODB =>
            MongoDatabaseManager.dropCollection(collectionName)
        }
      })
    } catch {
      case e: Throwable =>
        logger.warn("result collection cleanup failed.", e)
    }
  }

  def deleteReplayLog(logLocation: String): Unit = {
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
  def recurringCheckExpiredResults(
      timeToLive: Int
  ): Unit = {
    // retrieve all executions that are completed and their last update time goes beyond the ttl
    val expiredResults: List[WorkflowExecutions] =
      WorkflowExecutionsResource.getExpiredExecutionsWithResultOrLog(timeToLive)
    // drop the collections and clean the logs
    cleanExecutions(expiredResults, statusByte => statusByte)
  }
}
