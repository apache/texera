package edu.uci.ics.texera.web.resource.execution

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.web.TexeraWebApplication
import akka.pattern._

class ControllerWrapper(workflow:Workflow, controllerConfig:ControllerConfig) {

  private val clientActor = TexeraWebApplication.actorSystem.actorOf(
    Controller.props(workflow, ControllerConfig.default)
  )

  def send[T](controlCommand:ControlCommand[T]):Future[T] = {
  }

}
