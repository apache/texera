package engine.common

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetAddress, URL}

import clustering.ClusterListener
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

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

    val config = ConfigFactory
      .parseString(s"""
        akka.remote.netty.tcp.hostname = $localIpAddress
        akka.remote.netty.tcp.port = 2552
        akka.remote.artery.canonical.port = 2552
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.cluster.seed-nodes = [ "akka.tcp://Amber@$localIpAddress:2552" ]
        """)
      .withFallback(ConfigFactory.load("clustered"))

    val system = ActorSystem("Amber", config)
    val info = system.actorOf(Props[ClusterListener], "cluster-info")

    system
  }

  def startActorWorker(mainNodeAddress: Option[String]): ActorSystem = {
    val addr = mainNodeAddress.getOrElse("localhost")
    val localIpAddress = "localhost"
    val config = ConfigFactory
      .parseString(s"""
        akka.remote.netty.tcp.hostname = $localIpAddress
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.cluster.seed-nodes = [ "akka.tcp://Amber@$addr:2552" ]
        """)
      .withFallback(ConfigFactory.load("clustered"))
    val system = ActorSystem("Amber", config)
    val info = system.actorOf(Props[ClusterListener], "cluster-info")
    Constants.masterNodeAddr = addr
    system
  }
}
