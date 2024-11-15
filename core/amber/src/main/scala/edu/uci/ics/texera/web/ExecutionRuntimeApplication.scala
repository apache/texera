package edu.uci.ics.texera.web

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.model.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.common.{AmberRuntime, Utils}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import io.dropwizard.Configuration
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.apache.commons.jcs3.access.exception.InvalidArgumentException
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter
import org.glassfish.jersey.media.multipart.MultiPartFeature

import java.time.Duration
import scala.annotation.tailrec

object ExecutionRuntimeApplication {

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
    // start actor system master node
    AmberRuntime.startActorMaster(clusterMode)
    // start web server
    new ExecutionRuntimeApplication().run(
      "server",
      Utils.amberHomePath
        .resolve("src")
        .resolve("main")
        .resolve("resources")
        .resolve("execution-runtime-config.yml")
        .toString
    )
  }
}

class ExecutionRuntimeApplication
    extends io.dropwizard.Application[Configuration]
    with LazyLogging {

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

  }
}
