package edu.uci.ics.texera.service

import com.typesafe.config.{Config, ConfigFactory}

object ComputingUnitConfig {

  private val conf: Config = ConfigFactory.parseResources("computing-unit.conf").resolve()

  val localComputingUnitEnabled: Boolean = conf.getBoolean("computing-unit.local.enabled")
}