package edu.uci.ics.amber.engine.architecture.scheduling.config

case object OperatorConfig {
  def empty: OperatorConfig = {
    OperatorConfig(workerConfigs = List())
  }
}
case class OperatorConfig(
    workerConfigs: List[WorkerConfig]
)
