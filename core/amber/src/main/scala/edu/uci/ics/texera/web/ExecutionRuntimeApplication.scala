package edu.uci.ics.texera.web

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.{AmberRuntime, Utils}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import io.dropwizard.Configuration
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.eclipse.jetty.server.session.SessionHandler
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter
import org.glassfish.jersey.media.multipart.MultiPartFeature

import java.time.Duration

object ExecutionRuntimeApplication {

  def main(args: Array[String]): Unit = {

    // start actor system master node
    AmberRuntime.startActorMaster(clusterMode = false)
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



class ExecutionRuntimeApplication extends io.dropwizard.Application[Configuration]
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