package edu.uci.ics.amber

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.clustering.ClusterListener
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.Constants
import play.api.libs.json.Json

import java.net._
import scala.annotation.tailrec
import scala.concurrent.duration._

@deprecated(
  "entry point has moved to edu.uci.ics.texera.web.TexeraWebApplication and edu.uci.ics.texera.web.TexeraRunWorker",
  since = "0.1-SNAPSHOT"
)
object AmberApp {
  type OptionMap = Map[Symbol, Any]
  val usage = """
    Usage: App [--main-node-addr addr]
  """
  implicit val timeout: Timeout = Timeout(5.seconds)

  @tailrec
  def argsToOptionMap(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--main-node-addr" :: value :: tail =>
        argsToOptionMap(map ++ Map('mainNodeAddr -> value), tail)
      case option :: tail =>
        println("Unknown option " + option)
        println(usage)
        sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    val options = argsToOptionMap(Map(), args.toList)
    var config: Config = null
    var localIpAddress = "0.0.0.0"
//    try{
//      val query = new URL("http://checkip.amazonaws.com")
//      val in:BufferedReader = new BufferedReader(new InputStreamReader(query.openStream()))
//      localIpAddress = in.readLine()
//    }catch{
//      case exception: Exception =>
//        val localhost: InetAddress = InetAddress.getLocalHost
//        localIpAddress = localhost.getHostAddress
//    }

    val localhost: InetAddress = InetAddress.getLocalHost
    localIpAddress = localhost.getHostAddress

    if (!options.contains('mainNodeAddr)) {
      //activate main node
      config = ConfigFactory
        .parseString(s"""
        akka.remote.netty.tcp.hostname = $localIpAddress
        akka.remote.netty.tcp.port = 2552
        akka.remote.artery.canonical.port = 2552
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.cluster.seed-nodes = [ "akka.tcp://Amber@$localIpAddress:2552" ]
        """)
        .withFallback(ConfigFactory.load("clustered"))
    } else {
      //activate any node
      val addr = options('mainNodeAddr)
      config = ConfigFactory
        .parseString(s"""
        akka.remote.netty.tcp.hostname = $localIpAddress
        akka.remote.artery.canonical.hostname = $localIpAddress
        akka.cluster.seed-nodes = [ "akka.tcp://Amber@$addr:2552" ]
        """)
        .withFallback(ConfigFactory.load("clustered"))
      Constants.masterNodeAddr = addr.toString
    }
    val system: ActorSystem = ActorSystem("Amber", config)
    val info = system.actorOf(Props[ClusterListener], "cluster-info")

    var controller: ActorRef = null
    val workflows = Array(
      """{
        |"operators":[
        |{"limit":<arg1>,"delay":<arg2>,"operatorID":"Gen","operatorType":"Generate"},
        |{"operatorID":"Count","operatorType":"Aggregation"},
        |{"operatorID":"Sink","operatorType":"Sink"}],
        |"links":[
        |{"origin":"Gen","destination":"Count"},
        |{"origin":"Count","destination":"Sink"}]
        |}""".stripMargin,
      s"""{
        |"operators":[
        |{"host":"${Constants.remoteHDFSPath}","tableName":"/datasets/<arg3>G/orders.tbl","operatorID":"Scan","operatorType":"HDFSScanSource","delimiter":"|","indicesToKeep":[4,8,10]},
        |{"operatorID":"Count","operatorType":"Aggregation"},
        |{"operatorID":"Sink","operatorType":"Sink"}],
        |"links":[
        |{"origin":"Scan","destination":"Count"},
        |{"origin":"Count","destination":"Sink"}]
        |}""".stripMargin,
      s"""{
         |"operators":[
         |{"host":"${Constants.remoteHDFSPath}","tableName":"/datasets/<arg3>G/lineitem.tbl","operatorID":"Scan","operatorType":"HDFSScanSource","delimiter":"|","indicesToKeep":[4,8,10]},
         |{"operatorID":"Filter","operatorType":"Filter","targetField":2,"filterType":"Greater","threshold":"1991-01-01"},
         |{"operatorID":"GroupBy","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Sum"},
         |{"operatorID":"Sort","operatorType":"Sort","targetField":0},
         |{"operatorID":"Sink","operatorType":"Sink"}],
         |"links":[
         |{"origin":"Scan","destination":"Filter"},
         |{"origin":"Filter","destination":"GroupBy"},
         |{"origin":"GroupBy","destination":"Sort"},
         |{"origin":"Sort","destination":"Sink"}]
         |}""".stripMargin,
      s"""{
         |"operators":[
         |{"host":"${Constants.remoteHDFSPath}","tableName":"/datasets/<arg3>G/customer.tbl","operatorID":"Scan1","operatorType":"HDFSScanSource","delimiter":"|","indicesToKeep":[0]},
         |{"host":"${Constants.remoteHDFSPath}","tableName":"/datasets/<arg3>G/orders.tbl","operatorID":"Scan2","operatorType":"HDFSScanSource","delimiter":"|","indicesToKeep":[0,1]},
         |{"operatorID":"Join","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":1},
         |{"operatorID":"GroupBy1","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
         |{"operatorID":"GroupBy2","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
         |{"operatorID":"Sort","operatorType":"Sort","targetField":0},
         |{"operatorID":"Sink","operatorType":"Sink"}],
         |"links":[
         |{"origin":"Scan1","destination":"Join"},
         |{"origin":"Scan2","destination":"Join"},
         |{"origin":"Join","destination":"GroupBy1"},
         |{"origin":"GroupBy1","destination":"GroupBy2"},
         |{"origin":"GroupBy2","destination":"Sort"},
         |{"origin":"Sort","destination":"Sink"}]
         |}""".stripMargin,
      s"""{
         |"operators":[
         |{"host":"${Constants.remoteHDFSPath}","tableName":"/datasets/<arg3>G/customer.tbl","operatorID":"Scan1","operatorType":"HDFSScanSource","delimiter":"|","indicesToKeep":[0]},
         |{"host":"${Constants.remoteHDFSPath}","tableName":"/datasets/<arg3>G/orders.tbl","operatorID":"Scan2","operatorType":"HDFSScanSource","delimiter":"|","indicesToKeep":[0,1]},
         |{"host":"${Constants.remoteHDFSPath}","tableName":"/datasets/<arg3>G/orders.tbl","operatorID":"Scan3","operatorType":"HDFSScanSource","delimiter":"|","indicesToKeep":[0,1]},
         |{"operatorID":"Join1","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":1},
         |{"operatorID":"Join2","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":1},
         |{"operatorID":"GroupBy1","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
         |{"operatorID":"GroupBy2","operatorType":"GroupBy","groupByField":1,"aggregateField":0,"aggregationType":"Count"},
         |{"operatorID":"Sort","operatorType":"Sort","targetField":0},
         |{"operatorID":"Sink","operatorType":"Sink"}],
         |"links":[
         |{"origin":"Scan1","destination":"Join1"},
         |{"origin":"Scan2","destination":"Join1"},
         |{"origin":"Join1","destination":"Join2"},
         |{"origin":"Scan3","destination":"Join2"},
         |{"origin":"Join2","destination":"GroupBy1"},
         |{"origin":"GroupBy1","destination":"GroupBy2"},
         |{"origin":"GroupBy2","destination":"Sort"},
         |{"origin":"Sort","destination":"Sink"}]
         |}""".stripMargin
    )
    if (!options.contains('mainNodeAddr)) {

      val demoUsage =
        """demo usage =
 0. choose
 1. configure
 2. show
 3. run
 4. pause
 5. resume
 6. set conditional breakpoint
 7. set count breakpoint
 8. set tau
      """

      var current = 4
      var limit = "100000"
      var delay = "0"
      var conditionalbp: Option[String] = None
      var countbp: Option[Int] = Some(100000000)

      while (true) {
        val input = scala.io.StdIn.readLine()
        input match {
          case "choose" =>
            try {
              print("please choose which workflow you want to execute (0 or 1):")
              val res = scala.io.StdIn.readInt()
              if (res < 0 || res > workflows.size) {
                throw new Exception()
              }
              current = res
              println("current workflow: " + current)
            } catch {
              case _: Throwable =>
                println("failed to choose workflow!")
            }
          case "configure" =>
            if (current == 0) {
              try {
                print("please enter how many tuples you want to generate:")
                limit = scala.io.StdIn.readInt().toString
                print("please enter the artificial delay of generating one tuple(ms):")
                delay = scala.io.StdIn.readInt().toString
                println("workflow is correctly configured!")
              } catch {
                case _: Throwable =>
                  println("workflow is not correctly configured!")
              }
            } else {
              println("workflow is correctly configured!")
            }
          case "set count breakpoint" =>
            if (current != 1) {
              println("count breakpoint not supported for this workflow")
            } else {
              try {
                print("please enter target number of tuples:")
                countbp = Some(scala.io.StdIn.readInt())
                println("count breakpoint set!")
              } catch {
                case _: Throwable =>
                  println("cannot set count breakpoint!")
              }
            }
          case "set conditional breakpoint" =>
            if (current != 1) {
              println("conditional breakpoint not supported for this workflow")
            } else {
              try {
                print(
                  "please enter break condition (lambda function = (x) => x.contains(condition)):"
                )
                conditionalbp = Some(scala.io.StdIn.readLine().trim)
                println("count breakpoint set!")
              } catch {
                case _: Throwable =>
                  println("cannot set count breakpoint!")
              }
            }
          case "show" =>
            val json =
              Json.parse(workflows(current).replace("<arg1>", limit).replace("<arg2>", delay))
            println(Json.prettyPrint(json))
          case "run" =>
            // will not work. We have removed the making workflow by parsing JSON string from controller
//            if (current == 0) {
//              controller = system.actorOf(
//                CONTROLLER.props(
//                  workflows(current).replace("<arg1>", limit).replace("<arg2>", delay)
//                )
//              )
//            } else {
//              controller = system.actorOf(
//                CONTROLLER.props(workflows(current).replace("<arg3>", Constants.dataset.toString))
//              )
//            }
//            controller ! AckedControllerInitialization
            //if (countbp.isDefined && current == 2) {
            //  controller ! PassBreakpointTo("Filter", new CountGlobalBreakpoint("CountBreakpoint", countbp.get))
            //}
//            if (conditionalbp.isDefined) {
//              controller ! PassBreakpointTo(
//                "KeywordSearch",
//                new ConditionalGlobalBreakpoint(
//                  "ConditionalBreakpoint",
//                  x => x.getString(15).contains(conditionalbp)
//                )
//              )
//            }
            controller ! StartWorkflow()
            println("workflow started!")
          case "pause" =>
            if (controller == null) {
              println("workflow is not initialized")
            } else {
              controller ! PauseWorkflow()
            }
          case "resume" =>
            if (controller == null) {
              println("workflow is not initialized")
            } else {
              controller ! ResumeWorkflow()
            }
          case "set tau" =>
            Constants.defaultTau = scala.io.StdIn.readInt().milliseconds
//          case "modify logic" =>
//            val newLogic = scala.io.StdIn.readLine()
//            controller ! ModifyLogic(newLogic)
          case other =>
            println("wrong command!")
            println(demoUsage)
        }
      }
    }
  }

}
