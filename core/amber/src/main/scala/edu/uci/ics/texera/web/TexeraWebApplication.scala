package edu.uci.ics.texera.web

import akka.actor.ActorSystem
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.resource.{MockKillWorkerResource, SchemaPropagationResource, SystemMetadataResource, WorkflowWebsocketResource}
import edu.uci.ics.texera.workflow.common.TexeraUtils
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.eclipse.jetty.servlet.ErrorPageErrorHandler

object TexeraWebApplication {

  var actorSystem: ActorSystem = _

  def main(args: Array[String]): Unit = {
    // start actor system master node
    actorSystem = AmberUtils.startActorMaster(true)

    // start web server
    val server = "server"
    val serverConfig =
      TexeraUtils.amberHomePath.resolve("../conf").resolve("web-config.yml").toString
    new TexeraWebApplication().run(server, serverConfig)
  }
}

class TexeraWebApplication extends io.dropwizard.Application[TexeraWebConfiguration] {

  override def initialize(bootstrap: Bootstrap[TexeraWebConfiguration]): Unit = {
    // serve static frontend GUI files
    bootstrap.addBundle(new FileAssetsBundle("../new-gui/dist", "/", "index.html"))
    // add websocket bundle
    bootstrap.addBundle(new WebsocketBundle(classOf[WorkflowWebsocketResource]))
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(configuration: TexeraWebConfiguration, environment: Environment): Unit = {
    // serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    // redirect all 404 to index page, according to Angular routing requirements
    val eph = new ErrorPageErrorHandler
    eph.addErrorPage(404, "/")
    environment.getApplicationContext.setErrorHandler(eph)

    environment.jersey().register(classOf[SystemMetadataResource])
    environment.jersey().register(classOf[MockKillWorkerResource])
    environment.jersey().register(classOf[SchemaPropagationResource])
  }

}
