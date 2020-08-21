package web

import akka.actor.ActorSystem
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.dirkraft.dropwizard.fileassets.FileAssetsBundle
import io.dropwizard.setup.{Bootstrap, Environment}
import io.dropwizard.websockets.WebsocketBundle
import org.eclipse.jetty.servlet.ErrorPageErrorHandler
import texera.common.TexeraUtils
import web.resource.{MockKillWorkerResource, SystemMetadataResource, WorkflowWebsocketResource}

object TexeraWebApplication {

  var actorSystem: ActorSystem = _

  def main(args: Array[String]): Unit = {

    val local = if (args.length > 1) args(0) else "remote"
    val localHost = "localhost".equalsIgnoreCase(local)

    // start actor master
    actorSystem = WebUtils.startActorMaster(true)

    // start web server
    val server = if (args.length > 2) args(1)
    else "server"
    val serverConfig = if (args.length > 3) args(2)
    else TexeraUtils.texeraHomePath.resolve("conf").resolve("web-config.yml").toString
    new TexeraWebApplication().run(server, serverConfig)
  }
}

class TexeraWebApplication extends io.dropwizard.Application[TexeraWebConfiguration] {

  override def initialize(bootstrap: Bootstrap[TexeraWebConfiguration]): Unit = {
    // serve static frontend GUI files
    bootstrap.addBundle(new FileAssetsBundle("./texera/core/new-gui/dist/", "/", "index.html"))
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
  }

}
