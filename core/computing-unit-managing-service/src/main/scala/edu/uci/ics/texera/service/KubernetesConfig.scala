package edu.uci.ics.texera.service

import com.typesafe.config.{Config, ConfigFactory}

object KubernetesConfig {

  private val conf: Config = ConfigFactory.parseResources("kubernetes.conf").resolve()

  // Access the Kubernetes settings with environment variable fallback
  val computeUnitServiceName: String = conf.getString("kubernetes.compute-unit-service-name")
  val computeUnitPoolNamespace: String = conf.getString("kubernetes.compute-unit-pool-namespace")
  val computeUnitImageName: String = conf.getString("kubernetes.image-name")
  val computeUnitPortNumber: Int = conf.getInt("kubernetes.port-num")

  val maxNumOfRunningComputingUnitsPerUser: Int =
    conf.getInt("kubernetes.max-num-of-running-computing-units-per-user")

  val cpuLimitOptions: List[String] =
    conf
      .getString("kubernetes.computing-unit-cpu-limit-options")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  val memoryLimitOptions: List[String] =
    conf
      .getString("kubernetes.computing-unit-memory-limit-options")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  val gpuLimitOptions: List[String] =
    conf
      .getString("kubernetes.computing-unit-gpu-limit-options")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  // GPU resource key used directly in Kubernetes resource specifications
  val gpuResourceKey: String = conf.getString("kubernetes.computing-unit-gpu-resource-key")
}
