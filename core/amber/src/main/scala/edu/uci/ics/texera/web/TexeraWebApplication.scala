package edu.uci.ics.texera.web

import akka.actor.{ActorSystem, Cancellable}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle
import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.TexeraWebApplication.scheduleRecurringCallThroughActorSystem
import edu.uci.ics.texera.web.auth.JwtAuth.jwtConsumer
import edu.uci.ics.texera.web.auth.{
  GuestAuthFilter,
  SessionUser,
  UserAuthenticator,
  UserRoleAuthorizer
}
import edu.uci.ics.texera.web.resource.auth.{AuthResource, GoogleAuthResource}
import edu.uci.ics.texera.web.resource._
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource
import edu.uci.ics.texera.web.resource.dashboard.admin.execution.AdminExecutionResource
import edu.uci.ics.texera.web.resource.dashboard.admin.user.AdminUserResource
import edu.uci.ics.texera.web.resource.dashboard.user.file.{
  UserFileAccessResource,
  UserFileResource
}
import edu.uci.ics.texera.web.resource.dashboard.user.project.{
  ProjectAccessResource,
  ProjectResource,
  PublicProjectResource
}
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource.ExecutionResultEntry
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.{
  WorkflowAccessResource,
  WorkflowExecutionsResource,
  WorkflowResource,
  WorkflowVersionResource
}
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import edu.uci.ics.texera.web.storage.MongoDatabaseManager
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.servlet.ErrorPageErrorHandler
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter
import org.glassfish.jersey.media.multipart.MultiPartFeature
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature

import java.time.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import org.apache.commons.jcs3.access.exception.InvalidArgumentException

import scala.annotation.tailrec

object TexeraWebApplication {

  def createAmberRuntime(
      workflow: Workflow,
      conf: ControllerConfig,
      errorHandler: Throwable => Unit
  ): AmberClient = {
    new AmberClient(actorSystem, workflow, conf, errorHandler)
  }

  def scheduleCallThroughActorSystem(delay: FiniteDuration)(call: => Unit): Cancellable = {
    actorSystem.scheduler.scheduleOnce(delay)(call)
  }

  def scheduleRecurringCallThroughActorSystem(initialDelay: FiniteDuration, delay: FiniteDuration)(
      call: => Unit
  ): Cancellable = {
    actorSystem.scheduler.scheduleWithFixedDelay(initialDelay, delay)(() => call)
  }

  private var actorSystem: ActorSystem = _

  type OptionMap = Map[Symbol, Any]
  def parseArgs(args: Array[String]): OptionMap = {
    @tailrec
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--cluster" :: value :: tail =>
          nextOption(map ++ Map('cluster -> value.toBoolean), tail)
        case option :: tail =>
          throw new InvalidArgumentException("unknown command-line arg")
      }
    }

    nextOption(Map(), args.toList)
  }

  def main(args: Array[String]): Unit = {
    val argMap = parseArgs(args)

    val clusterMode = argMap.get('cluster).asInstanceOf[Option[Boolean]].getOrElse(false)

    // start actor system master node
    actorSystem = AmberUtils.startActorMaster(clusterMode)

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
  }
}

class TexeraWebApplication extends io.dropwizard.Application[TexeraWebConfiguration] {

  override def initialize(bootstrap: Bootstrap[TexeraWebConfiguration]): Unit = {
    // serve static frontend GUI files
    bootstrap.addBundle(new FileAssetsBundle("../new-gui/dist", "/", "index.html"))
    // add websocket bundle
    bootstrap.addBundle(new WebsocketBundle(classOf[WorkflowWebsocketResource]))
    bootstrap.addBundle(new WebsocketBundle(classOf[CollaborationResource]))
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    if (AmberUtils.amberConfig.getString("storage.mode").equalsIgnoreCase("mongodb")) {
      val timeToLive: Int = AmberUtils.amberConfig.getInt("result-cleanup.ttl-in-seconds")
      // do one time cleanup of collections that were not closed gracefully before restart/crash
      // retrieve all executions that are not completed
      val incompleteExecutions: List[ExecutionResultEntry] =
        WorkflowExecutionsResource.getAllIncompleteResults()
      cleanOldCollections(incompleteExecutions, WorkflowAggregatedState.FAILED)
      scheduleRecurringCallThroughActorSystem(
        2.seconds,
        AmberUtils.amberConfig
          .getInt("result-cleanup.collection-check-interval-in-seconds")
          .seconds
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

    if (AmberUtils.amberConfig.getBoolean("user-sys.enabled")) {
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
    environment.jersey.register(classOf[UserFileAccessResource])
    environment.jersey.register(classOf[UserFileResource])
    environment.jersey.register(classOf[AdminUserResource])
    environment.jersey.register(classOf[PublicProjectResource])
    environment.jersey.register(classOf[WorkflowAccessResource])
    environment.jersey.register(classOf[WorkflowResource])
    environment.jersey.register(classOf[WorkflowVersionResource])
    environment.jersey.register(classOf[ProjectResource])
    environment.jersey.register(classOf[ProjectAccessResource])
    environment.jersey.register(classOf[WorkflowExecutionsResource])
    environment.jersey.register(classOf[DashboardResource])
    environment.jersey.register(classOf[GmailResource])
    environment.jersey.register(classOf[AdminExecutionResource])
    environment.jersey.register(classOf[UserQuotaResource])
  }

  /**
    * This function drops the collections.
    * MongoDB doesn't have an API of drop collection where collection name in (from a subquery), so the implementation is to retrieve
    * the entire list of those documents that have expired, then loop the list to drop them one by one
    */
  def cleanOldCollections(
      executions: List[ExecutionResultEntry],
      status: WorkflowAggregatedState
  ): Unit = {
    // drop the collection and update the status to ABORTED
    executions.foreach(execEntry => {
      dropCollections(execEntry.result)
      // then delete the pointer from mySQL
      ExecutionsMetadataPersistService.tryUpdateExecutionStatusAndPointers(
        execEntry.eId.longValue(),
        status
      )
    })
  }

  def dropCollections(result: String): Unit = {
    // parse the JSON
    val node = objectMapper.readTree(result)
    val collections = node.get("results")
    // loop every collection and drop it
    collections.forEach(collection => MongoDatabaseManager.dropCollection(collection.asText()))
  }

  /**
    * This function is called periodically and checks all expired collections and deletes them
    */
  def recurringCheckExpiredResults(
      timeToLive: Int
  ): Unit = {
    // retrieve all executions that are completed and their last update time goes beyond the ttl
    val expiredResults: List[ExecutionResultEntry] =
      WorkflowExecutionsResource.getExpiredResults(timeToLive)
    // drop the collection and update the status to COMPLETED
    cleanOldCollections(expiredResults, WorkflowAggregatedState.COMPLETED)
  }
}
