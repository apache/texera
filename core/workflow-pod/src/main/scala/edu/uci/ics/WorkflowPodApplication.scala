package edu.uci.ics

import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.core.{Application, Configuration}

class WorkflowPodApplication extends Application[Configuration]{
  override def initialize(bootstrap: Bootstrap[Configuration]): Unit = {}

  override def run(configuration: Configuration, environment: Environment): Unit = ???
}

object WorkflowPodApplication {
  def main(args: Array[String]): Unit = {
    new WorkflowPodApplication().run(args: _*)
  }
}