package edu.uci.ics.texera.web

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.texera.workflow.common.Utils

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

object WebUtils {
  val config: Config =
    ConfigFactory.parseFile(Utils.amberHomePath.resolve("../conf").resolve("web.conf").toFile)

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

}
