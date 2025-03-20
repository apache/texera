package edu.uci.ics.texera.service

import com.typesafe.config.{Config, ConfigFactory}

object KubernetesConfig {

  private val conf: Config = ConfigFactory.parseResources("kubernetes.conf").resolve()

  // Access the Kubernetes settings with environment variable fallback
  val computeUnitServiceName: String = conf.getString("kubernetes.compute-unit-service-name")
  val computeUnitPoolNamespace: String = conf.getString("kubernetes.compute-unit-pool-namespace")
  val computeUnitImageName: String = conf.getString("kubernetes.image-name")
  val computeUnitPortNumber: Int = conf.getInt("kubernetes.port-num")
}
