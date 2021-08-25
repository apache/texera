package edu.uci.ics.amber.engine.common

import akka.actor.{ActorSystem, DeadLetter, Props}
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.DeadLetterMonitorActor

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

object AmberUtils {

  def reverseMultimap[T1, T2](map: Map[T1, Set[T2]]): Map[T2, Set[T1]] =
    map.toSeq
      .flatMap { case (k, vs) => vs.map((_, k)) }
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)

  def startActorMaster(localhost: Boolean): ActorSystem = {
    var localIpAddress = "localhost"
    if (!localhost) {
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

    val system = ActorSystem("Amber", masterConfig)
    system.actorOf(Props[ClusterListener], "cluster-info")
    val deadLetterMonitorActor =
      system.actorOf(Props[DeadLetterMonitorActor], name = "dead-letter-monitor-actor")
    system.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])

    system
  }

  def akkaConfig: Config = ConfigFactory.load("cluster").withFallback(amberConfig)

  def amberConfig: Config = ConfigFactory.load()

  def startActorWorker(mainNodeAddress: Option[String]): ActorSystem = {
    val addr = mainNodeAddress.getOrElse("localhost")
    val localIpAddress = "localhost"
    val workerConfig = ConfigFactory
      .parseString(s"""
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.remote.artery.canonical.port = 0
        akka.cluster.seed-nodes = [ "akka://Amber@$addr:2552" ]
        """)
      .withFallback(akkaConfig)
    val system = ActorSystem("Amber", workerConfig)
    system.actorOf(Props[ClusterListener], "cluster-info")
    val deadLetterMonitorActor =
      system.actorOf(Props[DeadLetterMonitorActor], name = "dead-letter-monitor-actor")
    system.eventStream.subscribe(deadLetterMonitorActor, classOf[DeadLetter])
    Constants.masterNodeAddr = Option(addr)
    system
  }
}
