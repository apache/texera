package edu.uci.ics.amber.engine.common

import akka.actor.{ActorSystem, Address, DeadLetter, Props}
import akka.serialization.{Serialization, SerializationExtension}
import com.typesafe.config.ConfigFactory.defaultApplication
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.DeadLetterMonitorActor
import scala.jdk.CollectionConverters._
import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

object AmberUtils {

  var serde: Serialization = _

  def toImmutableMap[K, V](
      javaMap: java.util.Map[K, V]
  ): scala.collection.immutable.Map[K, V] = {
    javaMap.asScala.toMap
  }

  def startActorMaster(clusterMode: Boolean): ActorSystem = {
    var localIpAddress = "localhost"
    if (clusterMode) {
      try {
        val query = new URL("http://checkip.amazonaws.com")
        val in = new BufferedReader(new InputStreamReader(query.openStream()))
        localIpAddress = in.readLine()
      } catch {
        case e: Exception => throw e
      }
    }

    val masterConfig = ConfigFactory
      .parseString(s"""
        akka.remote.artery.canonical.port = 2552
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.cluster.seed-nodes = [ "akka://Amber@$localIpAddress:2552" ]
        """)
      .withFallback(akkaConfig)
    AmberConfig.masterNodeAddr = createMasterAddress(localIpAddress)
    createAmberSystem(masterConfig)
  }

  def akkaConfig: Config = ConfigFactory.load("cluster").withFallback(defaultApplication())

  def createMasterAddress(addr: String): Address = Address("akka", "Amber", addr, 2552)

  def startActorWorker(mainNodeAddress: Option[String]): ActorSystem = {
    val addr = mainNodeAddress.getOrElse("localhost")
    var localIpAddress = "localhost"
    if (mainNodeAddress.isDefined) {
      try {
        val query = new URL("http://checkip.amazonaws.com")
        val in = new BufferedReader(new InputStreamReader(query.openStream()))
        localIpAddress = in.readLine()
      } catch {
        case e: Exception => throw e
      }
    }
    val workerConfig = ConfigFactory
      .parseString(s"""
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.remote.artery.canonical.port = 0
        akka.cluster.seed-nodes = [ "akka://Amber@$addr:2552" ]
        """)
      .withFallback(akkaConfig)
    AmberConfig.masterNodeAddr = createMasterAddress(addr)
    createAmberSystem(workerConfig)
  }

  private def createAmberSystem(actorSystemConf: Config): ActorSystem = {
    val system = ActorSystem("Amber", actorSystemConf)
    system.actorOf(Props[ClusterListener](), "cluster-info")
    val deadLetterMonitorActor =
      system.actorOf(Props[DeadLetterMonitorActor](), name = "dead-letter-monitor-actor")
    system.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])
    serde = SerializationExtension(system)
    system
  }
}
