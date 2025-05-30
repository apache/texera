/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.engine.common

import akka.actor.{ActorSystem, Address, Cancellable, DeadLetter, Props}
import akka.serialization.{Serialization, SerializationExtension}
import com.typesafe.config.ConfigFactory.defaultApplication
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.DeadLetterMonitorActor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.sys.process._

object AmberRuntime {

  private var _serde: Serialization = _
  private var _actorSystem: ActorSystem = _

  def serde: Serialization = {
    if (_serde == null) {
      if (_actorSystem == null) {
        _serde = SerializationExtension(ActorSystem("Amber", akkaConfig))
      } else {
        _serde = SerializationExtension(_actorSystem)
      }
    }
    _serde
  }

  def actorSystem: ActorSystem = {
    _actorSystem
  }

  def scheduleCallThroughActorSystem(delay: FiniteDuration)(call: => Unit): Cancellable = {
    _actorSystem.scheduler.scheduleOnce(delay)(call)
  }

  def scheduleRecurringCallThroughActorSystem(initialDelay: FiniteDuration, delay: FiniteDuration)(
      call: => Unit
  ): Cancellable = {
    _actorSystem.scheduler.scheduleWithFixedDelay(initialDelay, delay)(() => call)
  }

  def startActorMaster(clusterMode: Boolean): Unit = {
    var masterIpAddress = "localhost"
    var masterPort = 2552
    if (clusterMode) {
      masterIpAddress = AmberConfig.masterIpAddress
      masterPort = AmberConfig.masterPort
    }

    val masterConfig = ConfigFactory
      .parseString(s"""
        akka.remote.artery.canonical.port = $masterPort
        akka.remote.artery.canonical.hostname = $masterIpAddress
        akka.cluster.seed-nodes = [ "akka://Amber@$masterIpAddress:$masterPort" ]
        """)
      .withFallback(akkaConfig)
      .resolve()
    AmberConfig.masterNodeAddr = createMasterAddress(masterIpAddress, masterPort)
    createAmberSystem(masterConfig)
  }

  def akkaConfig: Config =
    ConfigFactory.load("cluster").withFallback(defaultApplication()).resolve()

  private def createMasterAddress(addr: String, port: Int): Address =
    Address("akka", "Amber", addr, port)

  def startActorWorker(clusterMode: Boolean): Unit = {
    var masterIpAddress = "localhost"
    var masterPort = 2552
    var nodeIp = "localhost"
    if (clusterMode) {
      masterIpAddress = AmberConfig.masterIpAddress
      masterPort = AmberConfig.masterPort
      nodeIp = "hostname -i".!!.trim // only supported by linux/unix
    }
    val workerConfig = ConfigFactory
      .parseString(s"""
        akka.remote.artery.canonical.hostname = $nodeIp
        akka.remote.artery.canonical.port = 0
        akka.cluster.seed-nodes = [ "akka://Amber@$masterIpAddress:$masterPort" ]
        """)
      .withFallback(akkaConfig)
      .resolve()
    AmberConfig.masterNodeAddr = createMasterAddress(masterIpAddress, masterPort)
    createAmberSystem(workerConfig)
  }

  private def createAmberSystem(actorSystemConf: Config): Unit = {
    _actorSystem = ActorSystem("Amber", actorSystemConf)
    _actorSystem.actorOf(Props[ClusterListener](), "cluster-info")
    val deadLetterMonitorActor =
      _actorSystem.actorOf(Props[DeadLetterMonitorActor](), name = "dead-letter-monitor-actor")
    _actorSystem.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])
    _serde = SerializationExtension(_actorSystem)
  }
}
